"""Unit and integration tests for Power BI semantic model export module.

Verification Criteria:
- UTF-8 with BOM prefix (b'\\xef\\xbb\\xbf') present on all exported CSV files.
- Zero-PII Guarantee: No raw phone numbers or unmasked name columns exported.
- Star-schema entity integrity: Correct row counts and foreign keys.
- Audit Logging: Execution recorded in pipeline_runs.parquet.
"""

import re
from pathlib import Path

import pandas as pd
import pytest

from config import GOLD_DIR, PIPELINE_RUNS_FILE, POWERBI_EXPORT_DIR, SILVER_DIR
from pipeline.export_for_powerbi import (
    FORBIDDEN_PII_COLUMNS,
    RAW_PHONE_REGEX,
    export_all_for_powerbi,
    validate_exported_csv,
)

EXPECTED_FILES = [
    "customers_masked.csv",
    "vehicles.csv",
    "dealers.csv",
    "ev_opportunities.csv",
    "ev_share_by_dealer.csv",
    "service_cost_comparison.csv",
    "trade_in_value_ratio.csv",
    "data_quality_scores.csv",
    "referential_integrity_checks.csv",
    "fuel_type_verification_queue.csv",
]


def test_export_all_for_powerbi_creates_all_files():
    """Verify that export_all_for_powerbi generates all 10 target CSV files."""
    res = export_all_for_powerbi(POWERBI_EXPORT_DIR)
    assert res["status"] == "SUCCESS"
    assert res["total_files"] == 10
    assert set(res["files"]) == set(EXPECTED_FILES)

    for fn in EXPECTED_FILES:
        target = POWERBI_EXPORT_DIR / fn
        assert target.exists(), f"Missing expected export file: {fn}"
        assert target.stat().st_size > 0, f"File {fn} is empty!"


def test_exported_csv_files_have_utf8_bom():
    """Verify that every exported CSV file starts with the UTF-8 BOM byte sequence."""
    for fn in EXPECTED_FILES:
        file_path = POWERBI_EXPORT_DIR / fn
        with open(file_path, "rb") as f:
            bom = f.read(3)
            assert bom == b"\xef\xbb\xbf", (
                f"File '{fn}' lacks UTF-8 BOM prefix! Expected b'\\xef\\xbb\\xbf', got {bom!r}"
            )


def test_customers_masked_export_zero_pii():
    """Verify customers_masked.csv has zero raw PII and contains only masked fields."""
    file_path = POWERBI_EXPORT_DIR / "customers_masked.csv"
    assert file_path.exists()

    df = pd.read_csv(file_path, encoding="utf-8-sig")

    # 1. Check prohibited column headers
    for forbidden in FORBIDDEN_PII_COLUMNS:
        assert forbidden not in df.columns, (
            f"Prohibited column '{forbidden}' found in customers_masked.csv header!"
        )

    # 2. Check required masked column headers
    assert "customer_id" in df.columns
    assert "name_masked" in df.columns
    assert "phone_masked" in df.columns

    # 3. Check masked name content format (O*** Ş****)
    assert df["name_masked"].str.contains(r"\*", regex=True).all()

    # 4. Check masked phone content format (530***4226) or empty
    non_empty_phones = df["phone_masked"].dropna()
    non_empty_phones = non_empty_phones[non_empty_phones != ""]
    assert non_empty_phones.str.contains(r"\*\*\*", regex=True).all()

    # 5. Check row count matches Silver customers
    silver_df = pd.read_parquet(SILVER_DIR / "customers.parquet")
    assert len(df) == len(silver_df) == 2110


def test_all_exported_files_forbid_raw_phones():
    """Scan all exported CSV text for raw 10-digit mobile phone numbers."""
    for fn in EXPECTED_FILES:
        file_path = POWERBI_EXPORT_DIR / fn
        with open(file_path, "r", encoding="utf-8-sig") as f:
            content = f.read()
            matches = RAW_PHONE_REGEX.findall(content)
            assert len(matches) == 0, (
                f"Privacy violation in '{fn}': Found raw phone number(s): {matches[:3]}"
            )


def test_gold_table_export_row_counts_match_sources():
    """Verify that exported row counts strictly mirror source lakehouse tables."""
    checks = [
        ("ev_opportunities.csv", "ev_opportunities.parquet", 1057),
        ("fuel_type_verification_queue.csv", "fuel_type_verification_queue.parquet", 102),
        ("referential_integrity_checks.csv", "referential_integrity_checks.parquet", 3),
        ("dealers.csv", "dealers.parquet", 18),
    ]

    for csv_name, parquet_name, expected_count in checks:
        csv_path = POWERBI_EXPORT_DIR / csv_name
        parquet_path = (GOLD_DIR if "gold" in str(parquet_name) or csv_name.startswith("ev") or "check" in csv_name or "queue" in csv_name else SILVER_DIR) / parquet_name

        csv_df = pd.read_csv(csv_path, encoding="utf-8-sig")
        parquet_df = pd.read_parquet(parquet_path)

        assert len(csv_df) == len(parquet_df) == expected_count, (
            f"Row count mismatch for {csv_name}: CSV={len(csv_df)}, Parquet={len(parquet_df)}, Expected={expected_count}"
        )


def test_powerbi_export_pipeline_runs_audit():
    """Verify that export execution writes an audit log to pipeline_runs.parquet."""
    assert PIPELINE_RUNS_FILE.exists()
    runs_df = pd.read_parquet(PIPELINE_RUNS_FILE)

    export_runs = runs_df[runs_df["step"] == "export_for_powerbi"]
    assert not export_runs.empty, "No export_for_powerbi step found in pipeline_runs.parquet!"

    latest_run = export_runs.iloc[-1]
    assert latest_run["status"] == "SUCCESS"
    assert latest_run["layer"] == "export"
    assert latest_run["rows_out"] > 0
