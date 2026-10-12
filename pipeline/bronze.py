"""Bronze layer ingestion pipeline.

Architectural Rationale:
- 1:1 Mapping to Microsoft Fabric Lakehouse Bronze:
  Ingests raw CSV files as-is into columnar Parquet format without type casting or data altering.
- Preserves Raw Fidelity: All columns are ingested strictly as strings so that no information
  (e.g., leading phone zeros, malformed dates) is lost or corrupted before Silver validation.
- Audit & Lineage Columns: Enriches every record with metadata (_source_system, _source_file,
  _ingested_at, _run_id, _row_hash) to ensure complete provenance and tamper detection.
- Fault Tolerance & Auditing: Every step is timed and logged to data/meta/pipeline_runs.parquet.
  A failure in one file is isolated and does not abort remaining tables.
"""

import hashlib
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from config import BRONZE_DIR, RAW_DIR
from pipeline.logging_utils import log_pipeline_run

logger = logging.getLogger(__name__)

# Expected column contracts per raw data source
BRONZE_SOURCES_CONFIG: Dict[str, Dict] = {
    "dms_satis": {
        "file_name": "dms_satis.csv",
        "source_system": "DMS",
        "expected_columns": [
            "sale_id",
            "musteri_adi",
            "telefon",
            "sasi_no",
            "marka",
            "model",
            "yakit_tipi",
            "satis_tarihi",
            "bayi_kodu",
            "satis_fiyati_tl",
        ],
    },
    "premium_sozlesme": {
        "file_name": "premium_sozlesme.csv",
        "source_system": "PREMIUM",
        "expected_columns": [
            "sozlesme_no",
            "musteri_unvani",
            "iletisim_tel",
            "vin",
            "sozlesme_tipi",
            "baslangic",
            "bitis",
            "aylik_tutar_tl",
        ],
    },
    "servis_kayit": {
        "file_name": "servis_kayit.csv",
        "source_system": "SERVIS",
        "expected_columns": [
            "servis_id",
            "sasi",
            "sahip_adi",
            "servis_tarihi",
            "servis_noktasi",
            "km",
            "tutar_tl",
            "is_tipi",
        ],
    },
    "next_islem": {
        "file_name": "next_islem.csv",
        "source_system": "NEXT",
        "expected_columns": [
            "islem_no",
            "vin_no",
            "musteri",
            "islem_tarihi",
            "islem_tipi",
            "teklif_tl",
        ],
    },
    "bayi_servis_noktalari": {
        "file_name": "bayi_servis_noktalari.csv",
        "source_system": "NETWORK",
        "expected_columns": [
            "dealer_code",
            "dealer_name",
            "city",
            "point_type",
        ],
    },
}


def check_schema(actual_columns: List[str], expected_columns: List[str]) -> Tuple[List[str], List[str]]:
    """Compare source columns against the expected schema contract.

    Why:
    Early schema drift detection. In an enterprise lakehouse, upstream systems may add
    or remove columns without notice. We flag warnings without failing ingestion.

    Returns:
        Tuple of (missing_columns, unexpected_columns).
    """
    actual_set = set(actual_columns)
    expected_set = set(expected_columns)
    missing = sorted(list(expected_set - actual_set))
    unexpected = sorted(list(actual_set - expected_set))
    return missing, unexpected


def compute_row_hash(df: pd.DataFrame, original_columns: List[str]) -> pd.Series:
    """Compute a deterministic SHA-256 hash for each row based on its original values.

    Why:
    Provides an immutable fingerprint of the raw data as it arrived from the source,
    enabling deduplication checks, idempotency, and data tampering audits.
    """
    if df.empty:
        return pd.Series([], dtype="object")

    # Vectorized string join of all original columns
    combined = df[original_columns].astype(str).agg("||".join, axis=1)
    return combined.apply(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())


def ingest_single_table(
    table_name: str,
    source_cfg: Dict,
    raw_dir: Path,
    bronze_dir: Path,
    run_id: str,
) -> Dict:
    """Ingest a single CSV into Bronze Parquet with metadata enrichment.

    Args:
        table_name: Canonical table identifier (e.g. 'dms_satis').
        source_cfg: Configuration dictionary containing file_name, source_system, and expected_columns.
        raw_dir: Directory where raw CSV files are stored.
        bronze_dir: Directory where target Parquet files will be written.
        run_id: Unique pipeline execution identifier.

    Returns:
        Dictionary summarizing the outcome (status, rows_in, rows_out, duration_s, error).
    """
    started_at = datetime.now(timezone.utc).isoformat()
    start_time = time.time()
    step_name = f"ingest_{table_name}"
    file_name = source_cfg["file_name"]
    source_system = source_cfg["source_system"]
    expected_cols = source_cfg["expected_columns"]
    csv_path = raw_dir / file_name

    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"Source file not found: {csv_path}")

        # Read all columns strictly as string without automatic type inference
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        rows_in = len(df)
        original_cols = list(df.columns)

        # Schema drift check
        missing_cols, unexpected_cols = check_schema(original_cols, expected_cols)
        schema_warning = ""
        if missing_cols or unexpected_cols:
            schema_warning = f"Schema warning in {table_name}: missing={missing_cols}, unexpected={unexpected_cols}"
            logger.warning(schema_warning)
            print(f"  [WARN] {schema_warning}")

        # Compute deterministic row hash across original raw columns
        row_hashes = compute_row_hash(df, original_cols)

        # Ingestion audit timestamp
        ingested_at = datetime.now(timezone.utc).isoformat()

        # Append metadata columns
        df["_source_system"] = source_system
        df["_source_file"] = file_name
        df["_ingested_at"] = ingested_at
        df["_run_id"] = str(run_id)
        df["_row_hash"] = row_hashes

        # Ensure target directory exists and save as Parquet
        bronze_dir.mkdir(parents=True, exist_ok=True)
        target_path = bronze_dir / f"{table_name}.parquet"
        df.to_parquet(target_path, index=False)

        duration_s = time.time() - start_time
        status = "WARNING" if schema_warning else "SUCCESS"

        log_pipeline_run(
            run_id=run_id,
            step=step_name,
            layer="bronze",
            source=table_name,
            rows_in=rows_in,
            rows_out=rows_in,
            rows_rejected=0,
            started_at=started_at,
            duration_s=duration_s,
            status=status,
            error=schema_warning,
        )

        return {
            "table": table_name,
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_in,
            "duration_s": duration_s,
            "error": schema_warning,
        }

    except Exception as exc:
        duration_s = time.time() - start_time
        err_msg = str(exc)
        logger.error(f"Failed to ingest {table_name}: {err_msg}", exc_info=True)
        print(f"  [FAIL] Ingestion failed for {table_name}: {err_msg}")

        log_pipeline_run(
            run_id=run_id,
            step=step_name,
            layer="bronze",
            source=table_name,
            rows_in=0,
            rows_out=0,
            rows_rejected=0,
            started_at=started_at,
            duration_s=duration_s,
            status="FAILED",
            error=err_msg,
        )

        return {
            "table": table_name,
            "status": "FAILED",
            "rows_in": 0,
            "rows_out": 0,
            "duration_s": duration_s,
            "error": err_msg,
        }


def run_bronze(
    run_id: Optional[str] = None,
    raw_dir: Optional[Path] = None,
    bronze_dir: Optional[Path] = None,
) -> Dict[str, Dict]:
    """Ingest all configured CSV sources into the Bronze Lakehouse layer.

    Args:
        run_id: Optional pipeline execution UUID. If omitted, a new UUID4 is generated.
        raw_dir: Directory containing raw CSV files (defaults to config.RAW_DIR).
        bronze_dir: Directory where Bronze Parquet files will be saved (defaults to config.BRONZE_DIR).

    Returns:
        Dictionary mapping table names to their execution outcome summaries.
    """
    active_run_id = run_id or str(uuid.uuid4())
    active_raw_dir = raw_dir or RAW_DIR
    active_bronze_dir = bronze_dir or BRONZE_DIR

    results = {}
    for table_name, source_cfg in BRONZE_SOURCES_CONFIG.items():
        outcome = ingest_single_table(
            table_name=table_name,
            source_cfg=source_cfg,
            raw_dir=active_raw_dir,
            bronze_dir=active_bronze_dir,
            run_id=active_run_id,
        )
        results[table_name] = outcome

    return results


if __name__ == "__main__":
    print("Running Bronze ingestion layer...")
    outcomes = run_bronze()
    for tbl, res in outcomes.items():
        print(f"  {tbl:25s}: {res['status']:8s} | {res['rows_out']:>6,} rows | {res['duration_s']:.3f}s")
