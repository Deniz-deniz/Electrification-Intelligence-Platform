"""Data Catalog and Metrics Glossary generation engine.

Architectural Rationale:
- Non-Drifting Metadata: Auto-discovers schemas, data types, and row counts dynamically
  from the actual physical Parquet files on disk, ensuring zero schema drift.
- Purview / Fabric Governance: Produces comprehensive data catalog and metric definitions
  matching enterprise governance standards (Microsoft Purview Data Map).
- Hand-Edited Curation: Separates operational metadata (types, counts, timestamps)
  from human business definitions and team ownership stored in catalog_config.json.
- Audit & Lineage: Captures upstream dependencies, source systems, DQ scores, and
  refresh telemetry across all Bronze, Silver, and Gold lakehouse tiers.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

from config import (
    BASE_DIR,
    BRONZE_DIR,
    CATALOG_CONFIG_FILE,
    CATALOG_JSON_FILE,
    DATA_CATALOG_FILE,
    DATA_DIR,
    GOLD_DIR,
    META_DIR,
    METRIC_GLOSSARY_FILE,
    METRICS_JSON_FILE,
    PIPELINE_RUNS_FILE,
    SILVER_DIR,
)
from pipeline.logging_utils import log_pipeline_run

logger = logging.getLogger(__name__)


def load_catalog_config(config_path: Path = CATALOG_CONFIG_FILE) -> Dict[str, Any]:
    """Load curated table and metric configurations from hand-edited config.

    Args:
        config_path: Path to the catalog_config.json file.

    Returns:
        Dictionary containing 'tables' and 'metrics' curated metadata.

    Raises:
        FileNotFoundError: If the configuration file is missing.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Catalog configuration file not found at: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def discover_parquet_tables(
    base_data_dir: Path = DATA_DIR,
    layers: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Scan disk directories to discover all existing Parquet datasets.

    Args:
        base_data_dir: Root data directory containing layer folders.
        layers: List of lakehouse layers to inspect. Defaults to ['bronze', 'silver', 'gold'].

    Returns:
        Mapping of table_name -> {'layer': layer_name, 'path': file_path}.
    """
    if layers is None:
        layers = ["bronze", "silver", "gold"]

    discovered: Dict[str, Dict[str, Any]] = {}
    for layer in layers:
        layer_dir = base_data_dir / layer
        if not layer_dir.exists():
            continue

        for file_path in sorted(layer_dir.glob("*.parquet")):
            table_name = file_path.stem
            discovered[table_name] = {
                "name": table_name,
                "layer": layer,
                "path": file_path,
            }

    return discovered


def extract_parquet_columns(file_path: Path) -> List[Dict[str, str]]:
    """Extract physical column names and data types from Parquet schema.

    Args:
        file_path: Path to the Parquet dataset file.

    Returns:
        List of dictionaries with 'name' and normalized 'type'.
    """
    schema = pq.read_schema(file_path)
    columns: List[Dict[str, str]] = []
    for field in schema:
        # Normalize pyarrow list representation: e.g. list<element: string> -> list<string>
        raw_type = str(field.type)
        clean_type = raw_type.replace("<element: ", "<").replace("<item: ", "<")
        columns.append({
            "name": field.name,
            "type": clean_type,
        })
    return columns


def get_parquet_row_count(file_path: Path) -> int:
    """Read the exact row count of a Parquet dataset efficiently via metadata.

    Args:
        file_path: Path to the Parquet dataset file.

    Returns:
        Integer count of records in the file.
    """
    return pq.read_metadata(file_path).num_rows


def get_table_refresh_time(
    table_name: str,
    layer: str,
    file_path: Path,
    runs_df: Optional[pd.DataFrame] = None,
) -> str:
    """Determine the latest refresh timestamp for a table.

    Prefers the execution timestamp from pipeline_runs.parquet; falls back to
    the file system modification time.

    Args:
        table_name: Canonical table identifier.
        layer: Medallion layer ('bronze', 'silver', 'gold').
        file_path: Path to physical Parquet file.
        runs_df: DataFrame containing historical pipeline runs.

    Returns:
        ISO 8601 formatted UTC timestamp string.
    """
    if runs_df is not None and not runs_df.empty:
        # Match table by source or specific pipeline step naming conventions
        step_candidates = [
            f"ingest_{table_name}",
            f"clean_{table_name}",
            f"create_{table_name}",
        ]
        if table_name in ("customers", "vehicles"):
            step_candidates.append("resolve_customers")

        matched = runs_df[
            ((runs_df["layer"] == layer) & (runs_df["source"] == table_name))
            | (runs_df["step"].isin(step_candidates))
        ]
        if not matched.empty:
            latest_run = matched.iloc[-1]
            started_at = str(latest_run["started_at"])
            if started_at:
                return started_at

    # Fallback to file modification timestamp
    mtime = file_path.stat().st_mtime
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def get_table_quality_score(
    table_name: str,
    layer: str,
    dq_df: Optional[pd.DataFrame] = None,
) -> Optional[float]:
    """Retrieve overall data quality score from data_quality_scores.parquet.

    Args:
        table_name: Canonical table identifier.
        layer: Medallion layer.
        dq_df: DataFrame of data quality scores if available.

    Returns:
        Floating point score (0-100) or None if not applicable.
    """
    if dq_df is not None and not dq_df.empty:
        matched = dq_df[(dq_df["layer"] == layer) & (dq_df["table_name"] == table_name)]
        if not matched.empty:
            return round(float(matched["overall_dq_score"].iloc[0]), 2)

    return None


def validate_catalog_completeness(
    catalog_data: Dict[str, Any],
    disk_tables: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Validate that every table existing on disk is present in the data catalog.

    Args:
        catalog_data: Loaded catalog dictionary.
        disk_tables: Discovered tables on disk. If None, auto-discovers disk tables.

    Raises:
        ValueError: If any table on disk is missing from the catalog.
    """
    if disk_tables is None:
        disk_tables = discover_parquet_tables()

    # Extract catalog table names from tables list or tables_by_name map
    if "tables" in catalog_data and isinstance(catalog_data["tables"], list):
        catalog_names = {t["name"] for t in catalog_data["tables"]}
    elif "tables" in catalog_data and isinstance(catalog_data["tables"], dict):
        catalog_names = set(catalog_data["tables"].keys())
    elif "tables_by_name" in catalog_data:
        catalog_names = set(catalog_data["tables_by_name"].keys())
    else:
        catalog_names = set(catalog_data.keys())

    disk_names = set(disk_tables.keys())
    missing_tables = disk_names - catalog_names

    if missing_tables:
        raise ValueError(
            f"Catalog completeness validation failed! Tables present on disk but "
            f"missing from catalog: {sorted(missing_tables)}"
        )


def build_catalog(run_id: Optional[str] = None) -> Dict[str, Any]:
    """Build the comprehensive data catalog combining disk introspection and curated metadata.

    Args:
        run_id: Pipeline execution UUID identifier.

    Returns:
        Structured catalog dictionary containing all tables and metadata.
    """
    config_data = load_catalog_config()
    curated_tables = config_data.get("tables", {})

    # Auto-discover actual Parquet tables on disk across Bronze, Silver, Gold
    disk_tables = discover_parquet_tables(layers=["bronze", "silver", "gold"])

    # Load run audit log and quality scores for enrichment
    runs_df = None
    if PIPELINE_RUNS_FILE.exists():
        try:
            runs_df = pd.read_parquet(PIPELINE_RUNS_FILE)
        except Exception as e:
            logger.warning(f"Failed to read pipeline_runs.parquet: {e}")

    dq_df = None
    dq_file = GOLD_DIR / "data_quality_scores.parquet"
    if dq_file.exists():
        try:
            dq_df = pd.read_parquet(dq_file)
        except Exception as e:
            logger.warning(f"Failed to read data_quality_scores.parquet: {e}")

    tables_list: List[Dict[str, Any]] = []
    tables_by_name: Dict[str, Dict[str, Any]] = {}

    for table_name, info in sorted(disk_tables.items()):
        layer = info["layer"]
        file_path = info["path"]

        # 1. Dynamic introspection from physical Parquet file
        raw_columns = extract_parquet_columns(file_path)
        row_count = get_parquet_row_count(file_path)
        refresh_time = get_table_refresh_time(table_name, layer, file_path, runs_df)
        quality_score = get_table_quality_score(table_name, layer, dq_df)

        # 2. Hand-edited curation from catalog_config.json
        curated_info = curated_tables.get(table_name, {})
        description = curated_info.get(
            "description",
            f"{table_name.replace('_', ' ').title()} table in {layer} lakehouse layer.",
        )
        owner = curated_info.get("owner", "Data Governance")
        source_systems = curated_info.get("source_systems", [])
        upstream_tables = curated_info.get("upstream_tables", [])
        col_descriptions = curated_info.get("columns", {})

        # 3. Assemble column-level metadata
        columns_meta: List[Dict[str, Any]] = []
        for col in raw_columns:
            c_name = col["name"]
            c_type = col["type"]
            c_desc = col_descriptions.get(c_name, f"{c_name} attribute")
            columns_meta.append({
                "name": c_name,
                "type": c_type,
                "description": c_desc,
            })

        table_entry = {
            "name": table_name,
            "table_name": table_name,
            "layer": layer,
            "description": description,
            "owner": owner,
            "source_systems": source_systems,
            "columns": columns_meta,
            "row_count": int(row_count),
            "last_refresh_time": refresh_time,
            "quality_score": quality_score,
            "upstream_tables": upstream_tables,
        }

        tables_list.append(table_entry)
        tables_by_name[table_name] = table_entry

    catalog_data = {
        "catalog_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id or str(uuid.uuid4()),
        "total_tables": len(tables_list),
        "tables": tables_list,
        "tables_by_name": tables_by_name,
    }

    # Validate that every disk table is present in the output
    validate_catalog_completeness(catalog_data, disk_tables)

    return catalog_data


def build_metrics_glossary(run_id: Optional[str] = None) -> Dict[str, Any]:
    """Build the business metrics glossary for Gold analytical KPIs.

    Args:
        run_id: Pipeline execution UUID identifier.

    Returns:
        Structured metrics glossary dictionary.
    """
    config_data = load_catalog_config()
    curated_metrics = config_data.get("metrics", [])

    metrics_list: List[Dict[str, Any]] = []
    metrics_by_name: Dict[str, Dict[str, Any]] = {}

    for metric in curated_metrics:
        entry = {
            "name": metric["name"],
            "business_definition": metric["business_definition"],
            "formula": metric["formula"],
            "source_tables": metric.get("source_tables", []),
            "owner": metric.get("owner", "Sales Data Team"),
            "refresh_frequency": metric.get("refresh_frequency", "Daily (On Pipeline Run)"),
        }
        metrics_list.append(entry)
        metrics_by_name[metric["name"]] = entry

    metrics_data = {
        "glossary_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id or str(uuid.uuid4()),
        "total_metrics": len(metrics_list),
        "metrics": metrics_list,
        "metrics_by_name": metrics_by_name,
    }

    return metrics_data


def export_catalog_and_metrics(
    catalog_data: Dict[str, Any],
    metrics_data: Dict[str, Any],
) -> None:
    """Save catalog.json, metrics.json, and their Parquet analytical representations.

    Args:
        catalog_data: Generated data catalog dictionary.
        metrics_data: Generated metrics glossary dictionary.
    """
    META_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Write data/meta/catalog.json
    with open(CATALOG_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(catalog_data, f, indent=2, ensure_ascii=False)

    # 2. Write data/meta/metrics.json
    with open(METRICS_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=2, ensure_ascii=False)

    # 3. Write data/meta/data_catalog.parquet (Microsoft Purview / Fabric flattened view)
    catalog_rows = []
    for t in catalog_data["tables"]:
        catalog_rows.append({
            "table_name": t["name"],
            "layer": t["layer"],
            "owner": t["owner"],
            "row_count": t["row_count"],
            "quality_score": t["quality_score"] if t["quality_score"] is not None else -1.0,
            "column_count": len(t["columns"]),
            "last_refresh_time": t["last_refresh_time"],
            "description": t["description"],
            "source_systems": ", ".join(t["source_systems"]),
            "upstream_tables": ", ".join(t["upstream_tables"]),
        })
    df_catalog = pd.DataFrame(catalog_rows)
    df_catalog.to_parquet(DATA_CATALOG_FILE, index=False)

    # 4. Write data/meta/metric_glossary.parquet
    metric_rows = []
    for m in metrics_data["metrics"]:
        metric_rows.append({
            "name": m["name"],
            "business_definition": m["business_definition"],
            "formula": m["formula"],
            "owner": m["owner"],
            "refresh_frequency": m["refresh_frequency"],
            "source_tables": ", ".join(m["source_tables"]),
        })
    df_metrics = pd.DataFrame(metric_rows)
    df_metrics.to_parquet(METRIC_GLOSSARY_FILE, index=False)


def run_catalog_pipeline(run_id: Optional[str] = None) -> Dict[str, Any]:
    """Execute the data catalog generation pipeline step.

    Args:
        run_id: Optional master execution run ID.

    Returns:
        Execution summary dictionary with status, durations, and counts.
    """
    exec_run_id = run_id or str(uuid.uuid4())
    start_time = time.time()
    started_at = datetime.now(timezone.utc).isoformat()

    try:
        catalog_data = build_catalog(run_id=exec_run_id)
        metrics_data = build_metrics_glossary(run_id=exec_run_id)
        export_catalog_and_metrics(catalog_data, metrics_data)

        duration = time.time() - start_time
        total_tables = catalog_data["total_tables"]
        total_metrics = metrics_data["total_metrics"]

        log_pipeline_run(
            run_id=exec_run_id,
            step="build_catalog",
            layer="meta",
            source="catalog_and_metrics",
            rows_in=total_tables,
            rows_out=total_tables,
            rows_rejected=0,
            started_at=started_at,
            duration_s=duration,
            status="SUCCESS",
            error="",
        )

        return {
            "status": "SUCCESS",
            "run_id": exec_run_id,
            "tables_cataloged": total_tables,
            "metrics_cataloged": total_metrics,
            "duration_s": duration,
        }

    except Exception as e:
        duration = time.time() - start_time
        err_msg = str(e)
        logger.error(f"Catalog generation failed: {err_msg}", exc_info=True)

        log_pipeline_run(
            run_id=exec_run_id,
            step="build_catalog",
            layer="meta",
            source="catalog_and_metrics",
            rows_in=0,
            rows_out=0,
            rows_rejected=0,
            started_at=started_at,
            duration_s=duration,
            status="FAILED",
            error=err_msg,
        )

        return {
            "status": "FAILED",
            "run_id": exec_run_id,
            "error": err_msg,
            "duration_s": duration,
        }


def load_catalog() -> Dict[str, Any]:
    """Read data/meta/catalog.json.

    Returns:
        Loaded catalog dictionary.
    """
    with open(CATALOG_JSON_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_metrics() -> Dict[str, Any]:
    """Read data/meta/metrics.json.

    Returns:
        Loaded metrics dictionary.
    """
    with open(METRICS_JSON_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def get_table(table_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve catalog entry for a specific table by name.

    Args:
        table_name: Canonical table identifier.

    Returns:
        Table catalog metadata dict or None if not found.
    """
    catalog = load_catalog()
    return catalog.get("tables_by_name", {}).get(table_name)


def get_metric(metric_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve glossary entry for a specific metric by name.

    Args:
        metric_name: Canonical metric name.

    Returns:
        Metric definition dict or None if not found.
    """
    glossary = load_metrics()
    return glossary.get("metrics_by_name", {}).get(metric_name)


def print_catalog_summary() -> None:
    """Print an executive summary of the data catalog in terminal."""
    catalog = load_catalog()
    metrics = load_metrics()

    print("\n" + "=" * 80)
    print("DATA CATALOG & METRICS GLOSSARY SUMMARY")
    print("=" * 80)
    print(f"Total Tables Cataloged : {catalog.get('total_tables', 0)}")
    print(f"Total Metrics Defined  : {metrics.get('total_metrics', 0)}")
    print(f"Catalog Generated At   : {catalog.get('generated_at')}")
    print("-" * 80)
    print(f"{'Table Name':<30} | {'Layer':<8} | {'Rows':>8} | {'Columns':>7} | {'DQ Score':>8} | {'Owner':<22}")
    print("-" * 80)

    for t in catalog.get("tables", []):
        score_str = f"{t['quality_score']:.1f}%" if t['quality_score'] is not None else "N/A"
        print(
            f"{t['name']:<30} | {t['layer']:<8} | {t['row_count']:>8,} | "
            f"{len(t['columns']):>7} | {score_str:>8} | {t['owner']:<22}"
        )

    print("-" * 80)
    print("GOLD METRICS GLOSSARY (SAMPLE):")
    for m in metrics.get("metrics", [])[:4]:
        print(f"  * {m['name']} ({m['owner']}):")
        print(f"    - Formula: {m['formula'][:85]}...")
    print("=" * 80)
