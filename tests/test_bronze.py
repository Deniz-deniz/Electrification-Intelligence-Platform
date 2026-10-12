"""Unit tests for the Bronze ingestion layer.

Verifies:
1. Row count preservation (no rows dropped during raw -> bronze transition).
2. Pure string typing (no automatic coercion of phones, dates, or codes).
3. Presence and integrity of audit metadata columns (_source_system, _source_file, _ingested_at, _run_id, _row_hash).
4. Determinism of row hashes.
5. Fault tolerance (isolated failure logging when a source file is missing or unreadable).
6. Schema warning detection on missing or unexpected columns.
"""

import uuid
from pathlib import Path

import pandas as pd
import pytest

from config import BRONZE_DIR, RAW_DIR
from pipeline.bronze import (
    BRONZE_SOURCES_CONFIG,
    check_schema,
    compute_row_hash,
    ingest_single_table,
    run_bronze,
)
from pipeline.logging_utils import get_pipeline_runs


def test_compute_row_hash_determinism():
    """Verify that compute_row_hash is pure, deterministic, and sensitive to changes."""
    df1 = pd.DataFrame([{"colA": "val1", "colB": "val2"}, {"colA": "val3", "colB": "val4"}])
    df2 = pd.DataFrame([{"colA": "val1", "colB": "val2"}, {"colA": "val3", "colB": "val4"}])
    df_diff = pd.DataFrame([{"colA": "val1_changed", "colB": "val2"}, {"colA": "val3", "colB": "val4"}])

    h1 = compute_row_hash(df1, ["colA", "colB"])
    h2 = compute_row_hash(df2, ["colA", "colB"])
    h_diff = compute_row_hash(df_diff, ["colA", "colB"])

    assert list(h1) == list(h2), "Hash must be deterministic for identical rows"
    assert h1.iloc[0] != h_diff.iloc[0], "Hash must differ when a column value changes"
    assert len(h1.iloc[0]) == 64, "Hash must be 64-character SHA-256 hexadecimal string"


def test_schema_warning_detection():
    """Verify check_schema correctly identifies missing and unexpected columns."""
    expected = ["colA", "colB", "colC"]
    actual = ["colA", "colB", "extra_col"]

    missing, unexpected = check_schema(actual, expected)
    assert missing == ["colC"]
    assert unexpected == ["extra_col"]


def test_bronze_ingestion_preserves_counts_and_metadata():
    """Verify that run_bronze preserves 100% of rows and enriches required metadata."""
    test_run_id = f"test-{uuid.uuid4()}"
    results = run_bronze(run_id=test_run_id)

    # All 5 tables should complete successfully
    assert len(results) == 5
    for tbl_name, res in results.items():
        assert res["status"] in ("SUCCESS", "WARNING"), f"{tbl_name} failed: {res.get('error')}"

        # Parquet file must exist
        parquet_file = BRONZE_DIR / f"{tbl_name}.parquet"
        assert parquet_file.exists(), f"Missing Parquet output: {parquet_file}"

        # Read Parquet output
        df_bronze = pd.read_parquet(parquet_file)

        # Read original CSV
        raw_file = RAW_DIR / BRONZE_SOURCES_CONFIG[tbl_name]["file_name"]
        df_raw = pd.read_csv(raw_file, dtype=str, keep_default_na=False)

        # 1. Row counts must match exactly (Rule: zero rows dropped in Bronze)
        assert len(df_bronze) == len(df_raw), (
            f"Row count mismatch in {tbl_name}: {len(df_bronze)} vs raw {len(df_raw)}"
        )

        # 2. Metadata columns must be present and fully populated
        metadata_cols = ["_source_system", "_source_file", "_ingested_at", "_run_id", "_row_hash"]
        for meta_col in metadata_cols:
            assert meta_col in df_bronze.columns, f"Missing metadata column {meta_col} in {tbl_name}"
            assert df_bronze[meta_col].isna().sum() == 0, f"Null values found in {meta_col} for {tbl_name}"
            assert (df_bronze[meta_col] == "").sum() == 0, f"Empty values found in {meta_col} for {tbl_name}"

        # 3. Source columns must retain string types
        for orig_col in df_raw.columns:
            assert str(df_bronze[orig_col].dtype) == "object", (
                f"Column {orig_col} in {tbl_name} was coerced to {df_bronze[orig_col].dtype}"
            )


def test_bronze_failed_file_handled(tmp_path: Path):
    """Verify that missing/corrupted source files are caught, logged, and isolated."""
    test_run_id = f"fail-test-{uuid.uuid4()}"
    raw_tmp = tmp_path / "raw"
    bronze_tmp = tmp_path / "bronze"
    raw_tmp.mkdir()
    bronze_tmp.mkdir()

    # Create one valid CSV and leave the other missing
    valid_cfg = {
        "file_name": "valid.csv",
        "source_system": "TEST",
        "expected_columns": ["id", "val"],
    }
    missing_cfg = {
        "file_name": "does_not_exist.csv",
        "source_system": "TEST",
        "expected_columns": ["id", "val"],
    }

    # Write the valid CSV
    (raw_tmp / "valid.csv").write_text("id,val\n1,A\n2,B\n", encoding="utf-8")

    # Ingest the missing file - must not raise exception, but return FAILED
    outcome_missing = ingest_single_table(
        table_name="missing_table",
        source_cfg=missing_cfg,
        raw_dir=raw_tmp,
        bronze_dir=bronze_tmp,
        run_id=test_run_id,
    )
    assert outcome_missing["status"] == "FAILED"
    assert outcome_missing["rows_in"] == 0
    assert outcome_missing["rows_out"] == 0
    assert "not found" in outcome_missing["error"].lower()

    # Ingest valid table in the same run - must succeed despite earlier failure
    outcome_valid = ingest_single_table(
        table_name="valid_table",
        source_cfg=valid_cfg,
        raw_dir=raw_tmp,
        bronze_dir=bronze_tmp,
        run_id=test_run_id,
    )
    assert outcome_valid["status"] == "SUCCESS"
    assert outcome_valid["rows_in"] == 2
    assert outcome_valid["rows_out"] == 2

    # Verify both records were written to pipeline_runs log
    runs_df = get_pipeline_runs()
    test_runs = runs_df[runs_df["run_id"] == test_run_id]
    assert len(test_runs) == 2
    assert "FAILED" in test_runs["status"].values
    assert "SUCCESS" in test_runs["status"].values
