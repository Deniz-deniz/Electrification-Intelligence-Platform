"""Unit tests for Data Catalog and Metrics Glossary generation.

Architectural Rationale:
- Non-Drifting Contract: Tests ensure every physical Parquet table on disk across Bronze,
  Silver, and Gold is accurately cataloged with valid schemas, data types, row counts,
  and team ownership.
- Fail-Fast Completeness: Explicitly tests that if a table exists on disk but is omitted
  from the catalog, the validation fails.
- Purview / Fabric Governance: Verifies business definitions, lineage upstream tables,
  quality score linkage, and exact mathematical metric formulas.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from config import (
    BRONZE_DIR,
    CATALOG_CONFIG_FILE,
    CATALOG_JSON_FILE,
    DATA_CATALOG_FILE,
    DATA_DIR,
    GOLD_DIR,
    METRIC_GLOSSARY_FILE,
    METRICS_JSON_FILE,
    SILVER_DIR,
)
from pipeline.catalog import (
    build_catalog,
    build_metrics_glossary,
    discover_parquet_tables,
    extract_parquet_columns,
    get_parquet_row_count,
    get_table_quality_score,
    get_table_refresh_time,
    load_catalog,
    load_catalog_config,
    load_metrics,
    run_catalog_pipeline,
    validate_catalog_completeness,
)


def test_load_catalog_config():
    """Verify that catalog_config.json loads cleanly and contains required structure."""
    assert CATALOG_CONFIG_FILE.exists(), "catalog_config.json must exist in project root"
    config = load_catalog_config(CATALOG_CONFIG_FILE)
    assert "tables" in config, "Config must contain 'tables' mapping"
    assert "metrics" in config, "Config must contain 'metrics' list"
    assert len(config["tables"]) >= 20, "Config must define all standard pipeline tables"
    assert len(config["metrics"]) >= 10, "Config must define all required business metrics"


def test_discover_parquet_tables():
    """Verify disk auto-discovery finds all Bronze, Silver, and Gold Parquet tables."""
    tables = discover_parquet_tables(layers=["bronze", "silver", "gold"])
    assert len(tables) >= 20, f"Expected at least 20 tables on disk, found {len(tables)}"

    # Check representative tables across all layers
    assert "dms_satis" in tables
    assert tables["dms_satis"]["layer"] == "bronze"
    assert "sales" in tables
    assert tables["sales"]["layer"] == "silver"
    assert "customers" in tables
    assert tables["customers"]["layer"] == "silver"
    assert "ev_opportunities" in tables
    assert tables["ev_opportunities"]["layer"] == "gold"
    assert "data_quality_scores" in tables
    assert tables["data_quality_scores"]["layer"] == "gold"


def test_extract_parquet_columns_and_row_count():
    """Verify column names, data types, and row count extracted from actual Parquet files."""
    sales_path = SILVER_DIR / "sales.parquet"
    assert sales_path.exists()

    columns = extract_parquet_columns(sales_path)
    col_names = [c["name"] for c in columns]
    assert "sale_id" in col_names
    assert "vin" in col_names
    assert "sale_price_tl" in col_names
    assert "customer_id" in col_names

    # Check data types
    col_types = {c["name"]: c["type"] for c in columns}
    assert col_types["sale_id"] == "string"
    assert col_types["sale_price_tl"] == "int64"

    row_count = get_parquet_row_count(sales_path)
    assert row_count == 2554, f"Expected 2,554 sales rows, got {row_count}"


def test_build_and_export_catalog_pipeline():
    """Verify run_catalog_pipeline executes successfully and writes output files."""
    result = run_catalog_pipeline(run_id="test-catalog-run-001")
    assert result["status"] == "SUCCESS"
    assert result["tables_cataloged"] >= 20
    assert result["metrics_cataloged"] >= 10

    # Verify JSON artifacts exist
    assert CATALOG_JSON_FILE.exists()
    assert METRICS_JSON_FILE.exists()

    # Verify Parquet artifacts exist
    assert DATA_CATALOG_FILE.exists()
    assert METRIC_GLOSSARY_FILE.exists()


def test_catalog_table_completeness_matches_disk():
    """Verify that every single Parquet table on disk is present in catalog.json."""
    disk_tables = discover_parquet_tables(layers=["bronze", "silver", "gold"])
    catalog = load_catalog()

    catalog_names = {t["name"] for t in catalog["tables"]}
    for table_name in disk_tables.keys():
        assert table_name in catalog_names, f"Table '{table_name}' on disk is missing from catalog.json!"

    # Also verify tables_by_name lookup dictionary
    for table_name in disk_tables.keys():
        assert table_name in catalog["tables_by_name"]


def test_catalog_fails_if_table_on_disk_is_missing_from_catalog():
    """Non-negotiable requirement: test fails if a table exists on disk but is missing from the catalog."""
    catalog = load_catalog()

    # Create a simulated disk table dictionary that includes an uncataloged table
    simulated_disk_tables = dict(discover_parquet_tables(layers=["bronze", "silver", "gold"]))
    simulated_disk_tables["unexpected_new_model"] = {
        "name": "unexpected_new_model",
        "layer": "gold",
        "path": Path("data/gold/unexpected_new_model.parquet"),
    }

    # Validation must raise ValueError when disk table is omitted from catalog
    with pytest.raises(ValueError) as exc_info:
        validate_catalog_completeness(catalog, simulated_disk_tables)

    assert "unexpected_new_model" in str(exc_info.value)
    assert "Catalog completeness validation failed" in str(exc_info.value)


def test_all_lineage_upstream_tables_exist_in_catalog():
    """Verify that every upstream table referenced in lineage exists as a table in the catalog."""
    catalog = load_catalog()
    catalog_names = set(catalog.get("tables_by_name", {}).keys())

    missing_lineage_refs = []
    for table in catalog.get("tables", []):
        tname = table["name"]
        for upstream in table.get("upstream_tables", []):
            if upstream not in catalog_names:
                missing_lineage_refs.append(f"Table '{tname}' references non-existent upstream table '{upstream}'")

    assert not missing_lineage_refs, (
        f"Lineage references invalid upstream tables:\n" + "\n".join(missing_lineage_refs)
    )


def test_every_disk_column_has_description_in_catalog_config():
    """Verify that every column present in actual disk Parquet files has a description in catalog_config.json."""
    config = load_catalog_config()
    config_tables = config.get("tables", {})

    missing_column_descriptions = {}
    for layer in ["bronze", "silver", "gold"]:
        layer_dir = DATA_DIR / layer
        if not layer_dir.exists():
            continue

        for p in sorted(layer_dir.glob("*.parquet")):
            tbl_name = p.stem
            schema = pq.read_schema(p)
            disk_cols = [f.name for f in schema]
            cfg_cols = config_tables.get(tbl_name, {}).get("columns", {})

            missing = [c for c in disk_cols if c not in cfg_cols or not str(cfg_cols[c]).strip()]
            if missing:
                missing_column_descriptions[f"{layer}.{tbl_name}"] = missing

    assert not missing_column_descriptions, (
        f"Columns present on disk missing descriptions in catalog_config.json:\n"
        + "\n".join([f"  {k}: {v}" for k, v in missing_column_descriptions.items()])
    )


def test_gold_tables_lineage_and_dependencies():
    """Verify exact upstream lineage dependencies for all 7 Gold tables."""
    catalog = load_catalog()
    t_by_name = catalog.get("tables_by_name", {})

    # 1. ev_opportunities must include sales (for vehicle age from sale date)
    assert "ev_opportunities" in t_by_name
    opp_upstream = set(t_by_name["ev_opportunities"]["upstream_tables"])
    assert "sales" in opp_upstream, "ev_opportunities upstream_tables must include sales"
    assert opp_upstream == {"contracts", "customers", "sales", "service_records", "vehicles"}

    # 2. ev_share_by_dealer
    assert set(t_by_name["ev_share_by_dealer"]["upstream_tables"]) == {"dealers", "sales"}

    # 3. service_cost_comparison must include vehicles, sales, service_records
    srv_upstream = set(t_by_name["service_cost_comparison"]["upstream_tables"])
    assert "vehicles" in srv_upstream, "service_cost_comparison upstream_tables must include vehicles"
    assert srv_upstream == {"sales", "service_records", "vehicles"}

    # 4. trade_in_value_ratio
    assert set(t_by_name["trade_in_value_ratio"]["upstream_tables"]) == {"sales", "trade_ins"}

    # 5. data_quality_scores audits all 12 bronze and silver tables
    assert len(t_by_name["data_quality_scores"]["upstream_tables"]) == 12

    # 6. referential_integrity_checks audits fact tables vs vehicles & sales
    assert set(t_by_name["referential_integrity_checks"]["upstream_tables"]) == {
        "contracts", "sales", "service_records", "trade_ins", "vehicles"
    }

    # 7. fuel_type_verification_queue
    assert set(t_by_name["fuel_type_verification_queue"]["upstream_tables"]) == {"customers", "vehicles"}


def test_rejected_records_description_exact_wording():
    """Verify exact description requirement for rejected_records table."""
    catalog = load_catalog()
    t_by_name = catalog.get("tables_by_name", {})
    assert "rejected_records" in t_by_name
    expected_desc = (
        "One row per rejection event; a bronze row can have several events. "
        "Distinct rejected rows are counted in the pipeline run log (rows_rejected)."
    )
    assert t_by_name["rejected_records"]["description"] == expected_desc


def test_metrics_glossary_merged_and_new_metrics():
    """Verify metrics glossary merges overall_dq_score & retention_pct, and defines all new metrics."""
    glossary = load_metrics()
    metrics_by_name = glossary["metrics_by_name"]
    metric_names = set(metrics_by_name.keys())

    # Merged metrics checks:
    # 1. 'data quality score' merged into 'overall_dq_score'
    assert "overall_dq_score" in metric_names
    assert "data quality score" not in metric_names, "'data quality score' should be merged into 'overall_dq_score'"
    dq = metrics_by_name["overall_dq_score"]
    assert "0.40 * validity_pct" in dq["formula"]
    assert "0.35 * completeness_pct" in dq["formula"]
    assert "0.25 * uniqueness_pct" in dq["formula"]

    # 2. 'avg_retention_ratio' merged into 'retention_pct'
    assert "retention_pct" in metric_names
    assert "avg_retention_ratio" not in metric_names, "'avg_retention_ratio' should be merged into 'retention_pct'"

    # 3. Newly added metrics:
    # customer_match_precision, customer_match_recall, customer_match_f1
    for m in ["customer_match_precision", "customer_match_recall", "customer_match_f1"]:
        assert m in metric_names, f"Metric '{m}' missing from glossary"
        item = metrics_by_name[m]
        assert "matching_quality.json" in str(item["source_tables"])
        # Must clearly state measured against SYNTHETIC ground truth and quarantined records not evaluated
        combined_text = (item["business_definition"] + " " + item["formula"]).upper()
        assert "SYNTHETIC" in combined_text, f"{m} must state measured against SYNTHETIC ground truth"
        assert "QUARANTINED" in combined_text, f"{m} must state quarantined records are not evaluated"

    # rejection_rate
    assert "rejection_rate" in metric_names
    rej = metrics_by_name["rejection_rate"]
    assert "distinct rejected bronze rows" in rej["formula"]
    assert "bronze rows" in rej["formula"]

    # fuel_type_unknown_customers
    assert "fuel_type_unknown_customers" in metric_names
    unk = metrics_by_name["fuel_type_unknown_customers"]
    assert "fuel_type_verification_queue" in unk["source_tables"]


def test_catalog_table_schema_and_required_fields():
    """Verify all required catalog fields exist for every table in bronze, silver, and gold."""
    catalog = load_catalog()
    assert "tables" in catalog
    assert len(catalog["tables"]) >= 20

    allowed_layers = {"bronze", "silver", "gold"}
    for table in catalog["tables"]:
        tname = table["name"]

        # Required fields check
        assert table["layer"] in allowed_layers, f"Invalid layer '{table['layer']}' for table {tname}"
        assert isinstance(table["description"], str) and len(table["description"]) > 10, (
            f"Missing or brief description for table {tname}"
        )
        assert isinstance(table["owner"], str) and len(table["owner"]) > 0, (
            f"Missing owner for table {tname}"
        )
        assert isinstance(table["source_systems"], list), f"source_systems must be list for table {tname}"
        assert isinstance(table["columns"], list) and len(table["columns"]) > 0, (
            f"Table {tname} must have columns"
        )
        assert isinstance(table["row_count"], int) and table["row_count"] >= 0, (
            f"Invalid row count for table {tname}"
        )
        assert isinstance(table["last_refresh_time"], str) and len(table["last_refresh_time"]) > 0, (
            f"Missing refresh time for table {tname}"
        )
        assert isinstance(table["upstream_tables"], list), f"upstream_tables must be list for table {tname}"

        # Columns check
        for col in table["columns"]:
            assert "name" in col and len(col["name"]) > 0
            assert "type" in col and len(col["type"]) > 0
            assert "description" in col and len(col["description"]) > 0

        # Quality score check
        # Silver business tables and Bronze tables must have non-null quality_score from data_quality_scores
        if tname in ("sales", "contracts", "customers", "vehicles", "dms_satis"):
            assert table["quality_score"] is not None, f"Expected non-null quality score for {tname}"
            assert 0.0 <= table["quality_score"] <= 100.0
