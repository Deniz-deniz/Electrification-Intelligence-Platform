"""Unit tests for Customer Entity Resolution (Silver layer).

Verifies:
1. PII masking: Turkish name and 10-digit mobile phone masking.
2. Rule 1: Shared valid VIN links records across sources into the same customer.
3. Rule 2: Shared valid phone + token-sorted name links records (including swapped first/last names).
4. Missing phone handling: Records with missing phone still link if they share a valid VIN.
5. Rule 4: Never merge on name alone (identical name with different phone & VIN produces distinct customers).
6. Records with no valid VIN: Linked via phone/name rules; isolated records receive LOW confidence.
7. Full pipeline integration: Generation of customers.parquet, vehicles.parquet, customer_id backfill,
   and evaluation metrics against ground truth (matching_quality.json).
"""

from pathlib import Path

import pandas as pd
import pytest

from config import GROUND_TRUTH_DIR, META_DIR, SILVER_DIR
from pipeline.silver_customers import (
    evaluate_against_ground_truth,
    mask_name,
    mask_phone,
    resolve_customer_entities,
    run_customer_resolution,
)


def test_pii_masking():
    """Verify KVKK-compliant name and phone masking."""
    assert mask_name("Ozan Şahin") == "O*** Ş****"
    assert mask_name("Elif Çelik") == "E*** Ç****"
    assert mask_name("Murat Koç") == "M**** K**"
    assert mask_name("") == ""
    assert mask_name(None) == ""

    assert mask_phone("5305614226") == "530***4226"
    assert mask_phone("5532719583") == "553***9583"
    assert mask_phone("12345") == ""  # Invalid length
    assert mask_phone(None) == ""


def test_er_same_vin_links_different_sources():
    """Rule 1: Records sharing the same valid VIN belong to the same customer."""
    records = [
        {
            "record_key": "sales_S001",
            "table": "sales",
            "source_system": "DMS",
            "vin": "WBA11111111111111",
            "name_clean": "Ozan Şahin",
            "name_ascii": "ozan sahin",
            "phone_norm": "5305614226",
        },
        {
            "record_key": "service_SRV001",
            "table": "service_records",
            "source_system": "SERVIS",
            "vin": "WBA11111111111111",
            "name_clean": "Ozan Şahin",
            "name_ascii": "ozan sahin",
            "phone_norm": None,
        },
    ]

    customers, vehicle_owners, rec_to_cid = resolve_customer_entities(records)

    assert len(customers) == 1
    assert rec_to_cid["sales_S001"] == rec_to_cid["service_SRV001"]
    assert vehicle_owners["WBA11111111111111"] == customers[0]["customer_id"]
    assert customers[0]["record_count"] == 2
    assert "DMS" in customers[0]["source_systems"]
    assert "SERVIS" in customers[0]["source_systems"]


def test_er_swapped_name_with_phone_links_different_vehicles():
    """Rule 2: Shared valid phone + token-sorted name merges multi-vehicle owners with swapped names."""
    records = [
        {
            "record_key": "sales_S001",
            "table": "sales",
            "source_system": "DMS",
            "vin": "WBA11111111111111",
            "name_clean": "Ozan Şahin",
            "name_ascii": "ozan sahin",
            "phone_norm": "5305614226",
        },
        {
            "record_key": "contracts_PRM001",
            "table": "contracts",
            "source_system": "PREMIUM",
            "vin": "WBA22222222222222",
            "name_clean": "Şahin Ozan",  # Swapped first/last name
            "name_ascii": "sahin ozan",
            "phone_norm": "5305614226",
        },
    ]

    customers, vehicle_owners, rec_to_cid = resolve_customer_entities(records)

    # Must resolve to 1 single customer owning BOTH vehicles
    assert len(customers) == 1
    cid = customers[0]["customer_id"]
    assert rec_to_cid["sales_S001"] == cid
    assert rec_to_cid["contracts_PRM001"] == cid
    assert vehicle_owners["WBA11111111111111"] == cid
    assert vehicle_owners["WBA22222222222222"] == cid


def test_er_missing_phone_with_vin_links():
    """Records with missing phone still link if they share a valid VIN."""
    records = [
        {
            "record_key": "sales_S002",
            "table": "sales",
            "source_system": "DMS",
            "vin": "WBA33333333333333",
            "name_clean": "Elif Çelik",
            "name_ascii": "elif celik",
            "phone_norm": "5532719583",
        },
        {
            "record_key": "service_SRV002",
            "table": "service_records",
            "source_system": "SERVIS",
            "vin": "WBA33333333333333",
            "name_clean": "Elif Çelik",
            "name_ascii": "elif celik",
            "phone_norm": None,  # Service record has NO phone
        },
    ]

    customers, vehicle_owners, rec_to_cid = resolve_customer_entities(records)

    assert len(customers) == 1
    assert rec_to_cid["sales_S002"] == rec_to_cid["service_SRV002"]
    assert customers[0]["phone_norm"] == "5532719583"  # Phone retained from sales


def test_er_never_merge_on_name_alone():
    """Rule 4: Never merge on name alone without a shared VIN or verified phone."""
    records = [
        {
            "record_key": "sales_S003",
            "table": "sales",
            "source_system": "DMS",
            "vin": "WBA44444444444444",
            "name_clean": "Ahmet Kaya",
            "name_ascii": "ahmet kaya",
            "phone_norm": "5321111111",
        },
        {
            "record_key": "sales_S004",
            "table": "sales",
            "source_system": "DMS",
            "vin": "WBA55555555555555",
            "name_clean": "Ahmet Kaya",  # Same common name
            "name_ascii": "ahmet kaya",
            "phone_norm": "5332222222",  # Different phone, different VIN
        },
    ]

    customers, vehicle_owners, rec_to_cid = resolve_customer_entities(records)

    # MUST NOT merge into 1 customer! Must remain 2 separate customers.
    assert len(customers) == 2
    assert rec_to_cid["sales_S003"] != rec_to_cid["sales_S004"]


def test_er_record_with_no_vin_handling():
    """Records with no VIN link via phone/name, while isolated records get LOW confidence."""
    records = [
        {
            "record_key": "rec_no_vin_1",
            "table": "sales",
            "source_system": "DMS",
            "vin": None,
            "name_clean": "Can Demir",
            "name_ascii": "can demir",
            "phone_norm": "5351234567",
        },
        {
            "record_key": "rec_with_vin_2",
            "table": "contracts",
            "source_system": "PREMIUM",
            "vin": "WBA66666666666666",
            "name_clean": "Can Demir",
            "name_ascii": "can demir",
            "phone_norm": "5351234567",
        },
        {
            "record_key": "rec_isolated_3",
            "table": "service_records",
            "source_system": "SERVIS",
            "vin": None,
            "name_clean": "Gizem Yıldız",
            "name_ascii": "gizem yildiz",
            "phone_norm": None,
        },
    ]

    customers, vehicle_owners, rec_to_cid = resolve_customer_entities(records)

    # Record 1 and Record 2 link via Phone + Name
    assert rec_to_cid["rec_no_vin_1"] == rec_to_cid["rec_with_vin_2"]
    linked_cid = rec_to_cid["rec_no_vin_1"]
    linked_customer = next(c for c in customers if c["customer_id"] == linked_cid)
    assert linked_customer["match_confidence"] == "HIGH"

    # Record 3 remains isolated without VIN or phone -> LOW confidence
    isolated_cid = rec_to_cid["rec_isolated_3"]
    assert isolated_cid != linked_cid
    isolated_customer = next(c for c in customers if c["customer_id"] == isolated_cid)
    assert isolated_customer["match_confidence"] == "LOW"


def test_full_silver_customers_pipeline():
    """Run full entity resolution pipeline and verify outputs, schema, and quality metrics."""
    result = run_customer_resolution()

    assert result["status"] == "SUCCESS"
    assert result["resolved_customers"] > 1800
    assert result["resolved_vehicles"] > 2500

    # 1. Verify data/silver/customers.parquet
    cust_path = SILVER_DIR / "customers.parquet"
    assert cust_path.exists()
    df_cust = pd.read_parquet(cust_path)
    expected_cust_cols = [
        "customer_id",
        "display_name",
        "phone_norm",
        "phone_masked",
        "name_masked",
        "source_systems",
        "record_count",
        "match_rule_summary",
        "match_confidence",
    ]
    for col in expected_cust_cols:
        assert col in df_cust.columns

    # 2. Verify data/silver/vehicles.parquet
    veh_path = SILVER_DIR / "vehicles.parquet"
    assert veh_path.exists()
    df_veh = pd.read_parquet(veh_path)
    expected_veh_cols = ["vin", "brand", "model", "fuel_type", "owner_customer_id"]
    for col in expected_veh_cols:
        assert col in df_veh.columns

    # 3. Verify customer_id backfilled in all silver fact tables
    for fact_table in ["sales", "contracts", "service_records", "trade_ins"]:
        f_path = SILVER_DIR / f"{fact_table}.parquet"
        assert f_path.exists()
        df_fact = pd.read_parquet(f_path)
        assert "customer_id" in df_fact.columns
        assert df_fact["customer_id"].isna().sum() == 0
        assert (df_fact["customer_id"] == "").sum() == 0

    # 4. Verify data/meta/matching_quality.json
    metrics = result["metrics"]
    assert metrics["pairwise_precision"] == 1.0  # Zero false merges
    assert metrics["pairwise_recall"] >= 0.80   # High recall
    assert metrics["share_vehicles_assigned_correct"] == 1.0
    assert metrics["cluster_purity"] == 1.0

    meta_json = META_DIR / "matching_quality.json"
    assert meta_json.exists()
