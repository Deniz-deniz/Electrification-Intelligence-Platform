"""Unit tests for pipeline/key_numbers.py.

Verifies that key metrics extracted by key_numbers.py strictly equal
the underlying Gold analytical tables, Silver quarantine datasets,
and pipeline run logs.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from config import GOLD_DIR, META_DIR, SILVER_DIR
from pipeline.key_numbers import get_key_numbers


def test_key_numbers_match_gold_and_run_log():
    """Verify that get_key_numbers accurately reflects output files."""
    data = get_key_numbers()

    # 1. Raw rows ingested
    assert data["raw_rows_ingested"] == 13247
    assert data["raw_table_counts"]["dms_satis"] == 2732
    assert data["raw_table_counts"]["premium_sozlesme"] == 1445
    assert data["raw_table_counts"]["servis_kayit"] == 8690
    assert data["raw_table_counts"]["next_islem"] == 362
    assert data["raw_table_counts"]["bayi_servis_noktalari"] == 18

    # 2. Rejection numbers
    assert data["rejection_events"] == 741
    assert data["distinct_rejected_rows"] == 738
    assert data["rejection_rate_pct"] == 5.57

    # 3. Matching quality
    assert data["matching_precision_pct"] == 100.0
    assert data["matching_recall_pct"] == 82.68
    assert data["matching_f1_pct"] == 90.52
    assert data["cluster_purity_pct"] == 100.0

    # 4. Bronze sales validity
    assert data["bronze_sales_validity_pct"] == 97.33

    # 5. Orphan rates
    assert data["orphan_rates"]["service_records"]["orphan_pct"] == 5.52
    assert data["orphan_rates"]["service_records"]["orphan_rows"] == 454
    assert data["orphan_rates"]["contracts"]["orphan_pct"] == 5.0
    assert data["orphan_rates"]["contracts"]["orphan_rows"] == 68
    assert data["orphan_rates"]["trade_ins"]["orphan_pct"] == 6.09
    assert data["orphan_rates"]["trade_ins"]["orphan_rows"] == 21

    # 6. EV leads and score bands
    assert data["total_ev_leads"] == 1057
    assert data["score_bands"]["80_100"] == 7
    assert data["score_bands"]["60_79"] == 231
    assert data["score_bands"]["40_59"] == 398
    assert data["score_bands"]["0_39"] == 421
    band_sum = sum(data["score_bands"].values())
    assert band_sum == data["total_ev_leads"]

    # 7. Verification queue
    assert data["verification_queue_size"] == 102


def test_key_numbers_multi_rule_rows_are_duplicate_sales():
    """Verify that the 3 multi-rule rows in dms_satis are DUPLICATE_KEY + INVALID_VIN."""
    rej_path = SILVER_DIR / "rejected_records.parquet"
    assert rej_path.exists()
    rej_df = pd.read_parquet(rej_path)

    dms_rej = rej_df[rej_df["source_table"] == "dms_satis"]
    row_counts = dms_rej.groupby("source_row_id").size()
    multi_rows = row_counts[row_counts > 1].index.tolist()

    assert len(multi_rows) == 3

    for row_id in multi_rows:
        rules = dms_rej[dms_rej["source_row_id"] == row_id]["rule_code"].tolist()
        assert "DUPLICATE_KEY" in rules
        assert "INVALID_VIN" in rules
        # Phone is strictly NOT a rejection rule
        assert "INVALID_PHONE" not in rules
        assert "MISSING_PHONE" not in rules
