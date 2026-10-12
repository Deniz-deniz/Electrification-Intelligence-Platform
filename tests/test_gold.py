"""Unit tests for Gold Layer analytical models, Opportunity Radar, and Data Quality KPIs.

Architectural Rationale:
- Semantic Layer Verification: Tests ensure business-critical analytical tables conform
  to domain rules, exact schema requirements, and deterministic reference date calculations.
- Explainable AI / Rule Engine Tests: Verifies additive opportunity scoring bounds (0-100),
  reason generation, and strict exclusion of existing EV owners.
- Data Quality Audit Tests: Verifies KPI formulas and data governance metrics across layers.
- Full Lakehouse Integration: Tests materialization of all 5 Gold Parquet tables
  and audit logging into data/meta/pipeline_runs.parquet.
"""

import uuid
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from config import (
    GOLD_DIR,
    GOLD_TABLES,
    OPPORTUNITY_RULES_CONFIG,
    PIPELINE_RUNS_FILE,
    REFERENCE_DATE,
    SILVER_DIR,
)
from pipeline.gold import (
    build_data_quality_scores,
    build_ev_opportunities,
    build_ev_share_by_dealer,
    build_fuel_type_verification_queue,
    build_referential_integrity_checks,
    build_service_cost_comparison,
    build_trade_in_value_ratio,
    calculate_opportunity_score_for_vehicle,
    compute_silver_validity_pct,
    compute_table_validity_pct,
    run_gold_pipeline,
)


# ----------------------------------------------------------------------
# 1. Tests for Opportunity Radar Scoring Function
# ----------------------------------------------------------------------

def test_calculate_opportunity_score_rules():
    """Verify individual additive score components and reason generation."""
    ref_date = date(2026, 10, 9)

    veh_row = pd.Series({"vin": "WBA11111111111111", "brand": "BMW", "fuel_type": "Benzin"})

    # 1. Old vehicle (> 4 years: +25), Kiralama ending in 30 days (+30),
    # High service cost (+20), No recent service (> 12 months: +15), Brand with EV (+10)
    sales = pd.DataFrame([{"vin": "WBA11111111111111", "sale_date": "2022-01-01"}])  # ~4.76 years
    contracts = pd.DataFrame([{"vin": "WBA11111111111111", "contract_type": "Kiralama", "end_date": "2026-11-08"}])  # 30 days
    service = pd.DataFrame([
        {"vin": "WBA11111111111111", "service_date": "2025-05-01", "cost_tl": 35000},  # > 30k, ~17 months ago
    ])

    score, reasons, attrs = calculate_opportunity_score_for_vehicle(
        veh_row, contracts, service, sales, ref_date
    )

    # 25 + 30 + 20 + 15 + 10 = 100
    assert score == 100
    assert len(reasons) == 5
    assert attrs["vehicle_age_years"] > 4.0
    assert attrs["days_to_contract_end"] == 30
    assert attrs["contract_type"] == "Kiralama"
    assert attrs["months_since_last_service"] > 12.0


def test_calculate_opportunity_score_capping_and_intermediate():
    """Verify score capping at 100 and moderate threshold additions."""
    ref_date = date(2026, 10, 9)

    # Defender brand (no EV in list -> +0 for brand)
    veh_row = pd.Series({"vin": "SAL22222222222222", "brand": "Defender", "fuel_type": "Dizel"})

    # Sale date 3.5 years ago (+15 for > 3 years)
    sales = pd.DataFrame([{"vin": "SAL22222222222222", "sale_date": "2023-04-09"}])
    # Finans contract ending in 120 days (+20 for 91-180 days)
    contracts = pd.DataFrame([{"vin": "SAL22222222222222", "contract_type": "Finans", "end_date": "2027-02-06"}])
    # Moderate service spend (20,000 TL -> +10) and recent service (1 month ago -> +0)
    service = pd.DataFrame([
        {"vin": "SAL22222222222222", "service_date": "2026-09-09", "cost_tl": 20000},
    ])

    score, reasons, attrs = calculate_opportunity_score_for_vehicle(
        veh_row, contracts, service, sales, ref_date
    )

    # 15 (age) + 20 (contract 180) + 10 (moderate spend) + 0 (recent srv) + 0 (brand) = 45
    assert score == 45
    assert len(reasons) == 3
    assert attrs["days_to_contract_end"] == 120


def test_contract_signal_kasko_vs_replacement_and_expiry_limits():
    """Verify Rule 1: Only Kiralama/Finans drive replacement points; Kasko gives <= 5 pts; >180d gives 0 pts."""
    ref_date = date(2026, 10, 9)
    veh_row = pd.Series({"vin": "WBA33333333333333", "brand": "BMW", "fuel_type": "Benzin"})
    sales = pd.DataFrame()
    service = pd.DataFrame()

    # Case A: Kasko only ending in 30 days -> gives only +5 pts (not +30), plus +15 (no service) + 10 (brand EV)
    contracts_kasko = pd.DataFrame([{"vin": "WBA33333333333333", "contract_type": "Kasko", "end_date": "2026-11-08"}])
    score_k, reasons_k, attrs_k = calculate_opportunity_score_for_vehicle(veh_row, contracts_kasko, service, sales, ref_date)
    assert score_k == 30  # 5 (kasko) + 15 (no service) + 10 (brand)
    assert any("Kasko" in r and "sigorta temas noktası" in r for r in reasons_k)
    assert not any("acil araç yenileme" in r for r in reasons_k)

    # Case B: Contract ended > 180 days ago (e.g. 200 days ago) -> gives 0 contract points
    contracts_expired_old = pd.DataFrame([{"vin": "WBA33333333333333", "contract_type": "Kiralama", "end_date": "2026-03-23"}])
    score_old, reasons_old, _ = calculate_opportunity_score_for_vehicle(veh_row, contracts_expired_old, service, sales, ref_date)
    assert score_old == 25  # 0 (contract) + 15 (no service) + 10 (brand)
    assert not any("sözleşmesi" in r for r in reasons_old)

    # Case C: Contract ended within 180 days (e.g. 60 days ago) -> gives +10 reduced points
    contracts_expired_recent = pd.DataFrame([{"vin": "WBA33333333333333", "contract_type": "Finans", "end_date": "2026-08-10"}])
    score_rec, reasons_rec, _ = calculate_opportunity_score_for_vehicle(veh_row, contracts_expired_recent, service, sales, ref_date)
    assert score_rec == 35  # 10 (contract) + 15 (no service) + 10 (brand)
    assert any("Finans sözleşmesi 60 gün önce bitti" in r for r in reasons_rec)


# ----------------------------------------------------------------------
# 2. Tests for build_ev_opportunities Table
# ----------------------------------------------------------------------

def test_build_ev_opportunities_excludes_ev_owners():
    """Rule: Customers who own at least one electric vehicle must be excluded."""
    df_customers = pd.DataFrame([
        {"customer_id": "C001", "name_masked": "A*** B****", "phone_masked": "532***1111"},
        {"customer_id": "C002", "name_masked": "C*** D****", "phone_masked": "533***2222"},
        {"customer_id": "C003", "name_masked": "E*** F****", "phone_masked": "534***3333"},
    ])

    # C001 owns 1 ICE and 1 EV -> MUST BE EXCLUDED
    # C002 owns 1 ICE -> INCLUDED
    # C003 owns 1 EV only -> EXCLUDED
    df_vehicles = pd.DataFrame([
        {"vin": "VIN_001", "brand": "BMW", "model": "320i", "fuel_type": "Benzin", "owner_customer_id": "C001"},
        {"vin": "VIN_002", "brand": "BMW", "model": "i4", "fuel_type": "Elektrik", "owner_customer_id": "C001"},
        {"vin": "VIN_003", "brand": "MINI", "model": "Cooper", "fuel_type": "Benzin", "owner_customer_id": "C002"},
        {"vin": "VIN_004", "brand": "MINI", "model": "Cooper SE", "fuel_type": "Elektrik", "owner_customer_id": "C003"},
    ])

    df_sales = pd.DataFrame([
        {"vin": "VIN_003", "sale_date": "2022-01-01"},
    ])
    df_contracts = pd.DataFrame()
    df_service = pd.DataFrame()

    df_opp = build_ev_opportunities(
        df_customers, df_vehicles, df_sales, df_contracts, df_service, ref_date=REFERENCE_DATE
    )

    # Exactly 1 customer qualifies (C002)
    assert len(df_opp) == 1
    assert df_opp.iloc[0]["customer_id"] == "C002"
    assert df_opp.iloc[0]["vin"] == "VIN_003"
    assert df_opp.iloc[0]["name_masked"] == "C*** D****"


def test_build_ev_opportunities_schema():
    """Verify exact column schema required by the project brief."""
    expected_cols = [
        "customer_id",
        "name_masked",
        "phone_masked",
        "vin",
        "brand",
        "model",
        "fuel_type",
        "vehicle_age_years",
        "contract_type",
        "contract_end_date",
        "days_to_contract_end",
        "last_service_date",
        "months_since_last_service",
        "has_unknown_vehicle",
        "opportunity_score",
        "score_reasons",
    ]

    # Run with empty datasets to check schema
    df_customers = pd.DataFrame(columns=["customer_id", "name_masked", "phone_masked"])
    df_vehicles = pd.DataFrame(columns=["vin", "brand", "model", "fuel_type", "owner_customer_id"])
    df_sales = pd.DataFrame(columns=["vin", "sale_date"])
    df_contracts = pd.DataFrame(columns=["vin", "contract_type", "end_date"])
    df_service = pd.DataFrame(columns=["vin", "service_date", "cost_tl"])

    df_opp = build_ev_opportunities(
        df_customers, df_vehicles, df_sales, df_contracts, df_service, ref_date=REFERENCE_DATE
    )

    # Check columns
    for col in expected_cols:
        assert col in df_opp.columns, f"Missing required column in ev_opportunities: {col}"


def test_build_ev_opportunities_has_unknown_vehicle():
    """Verify customers with unknown fuel type vehicles are included ONLY if they also own a known ICE vehicle."""
    df_customers = pd.DataFrame([
        {"customer_id": "C001", "name_masked": "A*** B****", "phone_masked": "532***1111"},
        {"customer_id": "C002", "name_masked": "C*** D****", "phone_masked": "533***2222"},
        {"customer_id": "C003", "name_masked": "E*** F****", "phone_masked": "534***3333"},
    ])
    df_vehicles = pd.DataFrame([
        # C001 owns only known ICE
        {"vin": "VIN_ICE_1", "brand": "BMW", "model": "320i", "fuel_type": "Benzin", "owner_customer_id": "C001"},
        # C002 owns known ICE AND an orphan unknown vehicle
        {"vin": "VIN_ICE_2", "brand": "BMW", "model": "520d", "fuel_type": "Dizel", "owner_customer_id": "C002"},
        {"vin": "VIN_ORPHAN_1", "brand": "UNKNOWN", "model": "UNKNOWN", "fuel_type": "UNKNOWN", "owner_customer_id": "C002"},
        # C003 owns ONLY an orphan unknown vehicle (MUST BE EXCLUDED from opportunities)
        {"vin": "VIN_ORPHAN_2", "brand": "UNKNOWN", "model": "UNKNOWN", "fuel_type": "UNKNOWN", "owner_customer_id": "C003"},
    ])
    df_sales = pd.DataFrame([
        {"vin": "VIN_ICE_1", "sale_date": "2024-01-01"},
        {"vin": "VIN_ICE_2", "sale_date": "2024-01-01"},
    ])
    df_contracts = pd.DataFrame()
    df_service = pd.DataFrame([
        {"vin": "VIN_ORPHAN_1", "service_date": "2025-01-01", "cost_tl": 10000},
        {"vin": "VIN_ORPHAN_2", "service_date": "2025-01-01", "cost_tl": 10000},
    ])

    df_opp = build_ev_opportunities(
        df_customers, df_vehicles, df_sales, df_contracts, df_service, ref_date=REFERENCE_DATE
    )

    # Exactly 2 customers qualify (C001, C002). C003 must NOT be in ev_opportunities.
    assert len(df_opp) == 2
    assert set(df_opp["customer_id"]) == {"C001", "C002"}

    c1 = df_opp[df_opp["customer_id"] == "C001"].iloc[0]
    c2 = df_opp[df_opp["customer_id"] == "C002"].iloc[0]

    assert c1["has_unknown_vehicle"] == False
    assert not any("Yakıt tipi bilinmeyen" in r for r in c1["score_reasons"])

    assert c2["has_unknown_vehicle"] == True
    assert any("Yakıt tipi bilinmeyen araç var (veri doğrulaması gerekir)" in r for r in c2["score_reasons"])


def test_build_fuel_type_verification_queue():
    """Verify fuel_type_verification_queue collects customers who own ONLY vehicles with unknown fuel type."""
    df_customers = pd.DataFrame([
        {"customer_id": "C001", "name_masked": "A*** B****", "phone_masked": "532***1111"},
        {"customer_id": "C002", "name_masked": "C*** D****", "phone_masked": "533***2222"},
        {"customer_id": "C003", "name_masked": "E*** F****", "phone_masked": "534***3333"},
    ])
    df_vehicles = pd.DataFrame([
        {"vin": "VIN_ICE_1", "brand": "BMW", "model": "320i", "fuel_type": "Benzin", "owner_customer_id": "C001"},
        {"vin": "VIN_ICE_2", "brand": "BMW", "model": "520d", "fuel_type": "Dizel", "owner_customer_id": "C002"},
        {"vin": "VIN_ORPHAN_1", "brand": "UNKNOWN", "model": "UNKNOWN", "fuel_type": "UNKNOWN", "owner_customer_id": "C002"},
        {"vin": "VIN_ORPHAN_2", "brand": "UNKNOWN", "model": "UNKNOWN", "fuel_type": "UNKNOWN", "owner_customer_id": "C003"},
    ])

    df_queue = build_fuel_type_verification_queue(df_customers, df_vehicles)

    # Only C003 has ONLY unknown vehicle(s)
    assert len(df_queue) == 1
    row = df_queue.iloc[0]
    assert row["customer_id"] == "C003"
    assert row["vin"] == "VIN_ORPHAN_2"
    assert row["reason"] == "fuel type unknown, needs verification"


def test_build_fuel_type_verification_queue_actual_silver():
    """Verify fuel_type_verification_queue on actual Silver data contains exactly 102 excluded customers."""
    df_customers = pd.read_parquet(SILVER_DIR / "customers.parquet")
    df_vehicles = pd.read_parquet(SILVER_DIR / "vehicles.parquet")

    df_queue = build_fuel_type_verification_queue(df_customers, df_vehicles)
    assert len(df_queue) == 102
    assert "customer_id" in df_queue.columns
    assert "vin" in df_queue.columns
    assert (df_queue["reason"] == "fuel type unknown, needs verification").all()


# ----------------------------------------------------------------------
# 3. Tests for build_ev_share_by_dealer Table
# ----------------------------------------------------------------------

def test_build_ev_share_by_dealer():
    """Verify dealer electrification share calculation and dealer metadata enrichment."""
    df_sales = pd.DataFrame([
        {"sale_id": "S1", "sale_date": "2025-06-01", "dealer_code": "D01", "brand": "MINI", "fuel_type": "Elektrik"},
        {"sale_id": "S2", "sale_date": "2025-07-01", "dealer_code": "D01", "brand": "MINI", "fuel_type": "Elektrik"},
        {"sale_id": "S3", "sale_date": "2025-08-01", "dealer_code": "D01", "brand": "MINI", "fuel_type": "Benzin"},
        {"sale_id": "S4", "sale_date": "2025-09-01", "dealer_code": "D02", "brand": "BMW", "fuel_type": "Benzin"},
    ])
    df_dealers = pd.DataFrame([
        {"dealer_code": "D01", "dealer_name": "Borusan Oto Avcılar", "city": "İstanbul"},
        {"dealer_code": "D02", "dealer_name": "Borusan Oto Çayyolu", "city": "Ankara"},
    ])

    df_share = build_ev_share_by_dealer(df_sales, df_dealers)

    assert len(df_share) == 2
    mini_row = df_share[df_share["dealer_code"] == "D01"].iloc[0]
    assert mini_row["year"] == 2025
    assert mini_row["total_sales"] == 3
    assert mini_row["ev_sales"] == 2
    assert mini_row["ev_share_pct"] == pytest.approx(66.67, 0.01)
    assert mini_row["dealer_name"] == "Borusan Oto Avcılar"
    assert mini_row["city"] == "İstanbul"

    bmw_row = df_share[df_share["dealer_code"] == "D02"].iloc[0]
    assert bmw_row["total_sales"] == 1
    assert bmw_row["ev_sales"] == 0
    assert bmw_row["ev_share_pct"] == 0.0


# ----------------------------------------------------------------------
# 4. Tests for build_service_cost_comparison Table
# ----------------------------------------------------------------------

def test_build_service_cost_comparison():
    """Verify annual service cost aggregation electric vs ICE/hybrid and exclusion of orphan vehicles."""
    df_vehicles = pd.DataFrame([
        {"vin": "VIN_ICE", "brand": "BMW", "fuel_type": "Benzin"},
        {"vin": "VIN_EV", "brand": "BMW", "fuel_type": "Elektrik"},
    ])
    df_sales = pd.DataFrame([
        {"vin": "VIN_ICE", "sale_date": "2024-10-09"},  # 2.0 years active
        {"vin": "VIN_EV", "sale_date": "2024-10-09"},   # 2.0 years active
    ])
    df_service = pd.DataFrame([
        {"service_id": "SRV1", "vin": "VIN_ICE", "cost_tl": 10000, "km": 30000},
        {"service_id": "SRV2", "vin": "VIN_ICE", "cost_tl": 20000, "km": 45000},
        {"service_id": "SRV3", "vin": "VIN_EV", "cost_tl": 8000, "km": 30000},
        {"service_id": "SRV_ORPHAN", "vin": "VIN_UNKNOWN", "cost_tl": 9000, "km": 25000},  # Orphan vehicle
    ])

    df_comp = build_service_cost_comparison(df_service, df_vehicles, df_sales, ref_date=REFERENCE_DATE)

    # Orphan vehicles must be strictly excluded: no UNKNOWN brand
    assert "UNKNOWN" not in df_comp["brand"].values
    assert len(df_comp) == 2
    ice_row = df_comp[df_comp["powertrain_type"] == "İçten Yanmalı / Hibrit"].iloc[0]
    ev_row = df_comp[df_comp["powertrain_type"] == "Elektrik"].iloc[0]

    assert ice_row["vehicle_count"] == 1
    assert ice_row["total_service_visits"] == 2
    assert ice_row["total_service_spend_tl"] == 30000
    assert ice_row["avg_cost_per_visit_tl"] == 15000.0
    # 30,000 TL / 2.0 years = 15,000 TL/yr
    assert ice_row["avg_annual_cost_per_vehicle_tl"] == pytest.approx(15000.0, 10.0)

    assert ev_row["vehicle_count"] == 1
    assert ev_row["total_service_visits"] == 1
    assert ev_row["total_service_spend_tl"] == 8000
    assert ev_row["avg_cost_per_visit_tl"] == 8000.0
    # 8,000 TL / 2.0 years = 4,000 TL/yr
    assert ev_row["avg_annual_cost_per_vehicle_tl"] == pytest.approx(4000.0, 10.0)


# ----------------------------------------------------------------------
# 5. Tests for build_trade_in_value_ratio Table
# ----------------------------------------------------------------------

def test_build_trade_in_value_ratio():
    """Verify secondary market retention ratio calculation and inner join audit tracking."""
    df_trades = pd.DataFrame([
        {"trade_in_id": "T1", "vin": "VIN_EV", "offer_tl": 1100000},
        {"trade_in_id": "T2", "vin": "VIN_ICE", "offer_tl": 1400000},
        {"trade_in_id": "T3", "vin": "VIN_ORPHAN", "offer_tl": 1200000},
    ])
    df_sales = pd.DataFrame([
        {"vin": "VIN_EV", "brand": "MINI", "fuel_type": "Elektrik", "sale_price_tl": 2000000},
        {"vin": "VIN_ICE", "brand": "MINI", "fuel_type": "Benzin", "sale_price_tl": 2000000},
    ])

    df_retention, audit = build_trade_in_value_ratio(df_trades, df_sales, return_audit=True)

    assert len(df_retention) == 2
    assert audit["rows_in"] == 3
    assert audit["rows_matched"] == 2
    assert audit["rows_unmatched"] == 1
    assert df_retention.attrs["audit"]["rows_unmatched"] == 1

    ev_row = df_retention[df_retention["powertrain_type"] == "Elektrik"].iloc[0]
    ice_row = df_retention[df_retention["powertrain_type"] == "İçten Yanmalı / Hibrit"].iloc[0]

    # EV retention: 1.1M / 2.0M = 55%
    assert ev_row["retention_pct"] == pytest.approx(55.0, 0.1)
    # ICE retention: 1.4M / 2.0M = 70%
    assert ice_row["retention_pct"] == pytest.approx(70.0, 0.1)


def test_build_trade_in_value_ratio_actual_silver():
    """Verify trade_in_value_ratio audit counts on actual Silver dataset (345 in, 324 matched, 21 unmatched)."""
    df_trades = pd.read_parquet(SILVER_DIR / "trade_ins.parquet")
    df_sales = pd.read_parquet(SILVER_DIR / "sales.parquet")

    df_gold, audit = build_trade_in_value_ratio(df_trades, df_sales, return_audit=True)

    assert audit["rows_in"] == 345
    assert audit["rows_matched"] == 324
    assert audit["rows_unmatched"] == 21
    assert not df_gold.empty


# ----------------------------------------------------------------------
# 6. Tests for build_referential_integrity_checks Table
# ----------------------------------------------------------------------

def test_build_referential_integrity_checks():
    """Verify referential integrity audit across Silver fact tables and vehicle dimension."""
    df_service = pd.DataFrame([
        {"service_id": "SRV1", "vin": "VIN_MATCHED"},
        {"service_id": "SRV2", "vin": "VIN_ORPHAN"},
    ])
    df_contracts = pd.DataFrame([
        {"contract_id": "C1", "vin": "VIN_MATCHED"},
    ])
    df_trades = pd.DataFrame([
        {"trade_in_id": "T1", "vin": "VIN_ORPHAN"},
    ])
    df_vehicles = pd.DataFrame([
        {"vin": "VIN_MATCHED"},
        {"vin": "VIN_ORPHAN"},
    ])
    df_sales = pd.DataFrame([
        {"vin": "VIN_MATCHED"},
    ])

    df_ref = build_referential_integrity_checks(df_service, df_contracts, df_trades, df_vehicles, df_sales)

    assert len(df_ref) == 3
    expected_cols = [
        "fact_table",
        "foreign_key",
        "parent_table",
        "total_fact_rows",
        "unmatched_dim_vehicles_rows",
        "unmatched_dim_vehicles_pct",
        "orphan_unmatched_sales_rows",
        "orphan_unmatched_sales_pct",
        "orphan_distinct_vins",
        "root_cause_explanation",
    ]
    for col in expected_cols:
        assert col in df_ref.columns

    # Check dim_vehicles matching: all VINs present in df_vehicles
    assert (df_ref["unmatched_dim_vehicles_rows"] == 0).all()

    # Check sales lineage: VIN_ORPHAN lacks sales row
    srv_row = df_ref[df_ref["fact_table"] == "service_records"].iloc[0]
    assert srv_row["orphan_unmatched_sales_rows"] == 1
    assert srv_row["orphan_distinct_vins"] == 1


# ----------------------------------------------------------------------
# 7. Tests for build_data_quality_scores Table
# ----------------------------------------------------------------------

def test_build_data_quality_scores():
    """Verify unified data quality scores calculation across Bronze and Silver layers."""
    df_dq = build_data_quality_scores()

    assert not df_dq.empty
    expected_cols = [
        "layer",
        "table_name",
        "rows",
        "completeness_pct",
        "validity_pct",
        "uniqueness_pct",
        "overall_dq_score",
        "formula_documentation",
    ]
    for col in expected_cols:
        assert col in df_dq.columns

    # Verify score formulas and bounds
    for _, row in df_dq.iterrows():
        comp = row["completeness_pct"]
        val = row["validity_pct"]
        uniq = row["uniqueness_pct"]
        overall = row["overall_dq_score"]

        assert 0.0 <= comp <= 100.0
        assert 0.0 <= val <= 100.0
        assert 0.0 <= uniq <= 100.0
        assert 0.0 <= overall <= 100.0

        expected_calc = round(0.40 * val + 0.35 * comp + 0.25 * uniq, 2)
        assert overall == pytest.approx(expected_calc, 0.01)

    silver_rows = df_dq[df_dq["layer"] == "silver"].set_index("table_name")
    bronze_rows = df_dq[df_dq["layer"] == "bronze"].set_index("table_name")

    # In Silver, non-null values conform 100% to domain rules (invalid records quarantined)
    assert silver_rows.loc["sales", "validity_pct"] == 100.0
    assert silver_rows.loc["contracts", "validity_pct"] == 100.0
    assert silver_rows.loc["customers", "validity_pct"] == 100.0
    assert silver_rows.loc["service_records", "validity_pct"] == 100.0

    # Silver validity is strictly greater than or equal to Bronze validity
    assert silver_rows.loc["sales", "validity_pct"] >= bronze_rows.loc["dms_satis", "validity_pct"]
    assert silver_rows.loc["contracts", "validity_pct"] >= bronze_rows.loc["premium_sozlesme", "validity_pct"]
    assert silver_rows.loc["service_records", "validity_pct"] >= bronze_rows.loc["servis_kayit", "validity_pct"]

    # Completeness verification: vehicles must no longer be 100% when 117+ vehicles have UNKNOWN brand
    veh_comp = silver_rows.loc["vehicles", "completeness_pct"]
    assert veh_comp < 100.0
    assert veh_comp == pytest.approx(97.12, 0.1)

    # Customers, sales, and contracts completeness excludes derived/redundant columns
    cust_comp = silver_rows.loc["customers", "completeness_pct"]
    assert cust_comp < 100.0
    assert cust_comp == pytest.approx(98.38, 0.1)

    sales_comp = silver_rows.loc["sales", "completeness_pct"]
    assert sales_comp == pytest.approx(99.37, 0.1)

    contracts_comp = silver_rows.loc["contracts", "completeness_pct"]
    assert contracts_comp == pytest.approx(99.33, 0.1)


def test_compute_silver_validity_pct_edge_cases():
    """Verify compute_table_validity_pct catches bad VINs, dates, and contract order."""
    ref_date = date(2026, 10, 9)

    # 1. VIN check: 1 good, 1 bad -> 50%
    df_vin = pd.DataFrame([{"vin": "WBA11111111111111"}, {"vin": "INVALID"}])
    assert compute_silver_validity_pct(df_vin, ref_date=ref_date) == 50.0

    # 2. Date check: 1 good, 1 future (> ref_date) -> 50%
    df_date = pd.DataFrame([{"sale_date": "2024-01-01"}, {"sale_date": "2030-01-01"}])
    assert compute_silver_validity_pct(df_date, ref_date=ref_date) == 50.0

    # 3. Contract date order: 1 good (end >= start), 1 inverted (end < start) -> 50%
    df_order = pd.DataFrame([
        {"start_date": "2024-01-01", "end_date": "2026-01-01"},
        {"start_date": "2024-01-01", "end_date": "2023-01-01"},
    ])
    assert compute_silver_validity_pct(df_order, ref_date=ref_date) == 50.0

    # 4. Phone check: 1 valid, 1 invalid -> 50%
    df_phone = pd.DataFrame([
        {"has_valid_phone": True},
        {"has_valid_phone": False},
    ])
    assert compute_silver_validity_pct(df_phone, ref_date=ref_date) == 50.0

    # 5. Non-null phone format check: null phone is not penalized in validity
    df_phone_null = pd.DataFrame([
        {"phone_norm": "5321234567"},
        {"phone_norm": None},
    ])
    assert compute_table_validity_pct(df_phone_null, ref_date=ref_date) == 100.0


# ----------------------------------------------------------------------
# 8. End-to-End Gold Pipeline Materialization Test
# ----------------------------------------------------------------------

def test_run_gold_pipeline_end_to_end(tmp_path):
    """Verify execution of run_gold_pipeline writes parquet files and logs to pipeline_runs."""
    gold_out = tmp_path / "gold"
    unique_run_id = f"test-gold-{uuid.uuid4().hex[:8]}"
    results = run_gold_pipeline(
        run_id=unique_run_id,
        silver_dir=SILVER_DIR,
        gold_dir=gold_out,
        ref_date=REFERENCE_DATE,
    )

    # All 7 gold models must succeed
    assert len(results) == len(GOLD_TABLES) == 7
    for table_name in GOLD_TABLES:
        assert table_name in results
        assert results[table_name]["status"] == "SUCCESS"
        assert results[table_name]["rows_out"] > 0

        # Parquet file must exist and have content
        pq_path = gold_out / f"{table_name}.parquet"
        assert pq_path.exists()
        df = pd.read_parquet(pq_path)
        assert len(df) == results[table_name]["rows_out"]

    # Verify pipeline_runs.parquet contains gold entries
    df_runs = pd.read_parquet(PIPELINE_RUNS_FILE)
    gold_logged = df_runs[df_runs["run_id"] == unique_run_id]
    assert len(gold_logged) == 7
    assert set(gold_logged["layer"].unique()) == {"gold"}
    assert (gold_logged["status"] == "SUCCESS").all()
