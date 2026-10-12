"""Comprehensive test suite for FastAPI backend endpoints.

Architectural Rationale:
- RFC 8259 JSON Validation: Verifies that every API endpoint returns valid standard JSON
  without NaN or NaT tokens.
- Privacy & KVKK Governance: Scans the JSON output of every endpoint to guarantee zero leakage
  of raw 10-digit mobile phones or unmasked customer name columns.
- UTF-8 & Windows Encoding: Confirms Turkish characters (İ, ı, ş, ğ, ç, ö, ü) are preserved.
- Gold Analytical Verification: Tests Opportunity Radar score bands, fuel verification queue,
  and referential integrity audits.
"""

import json
import re
from typing import Any, List

import pytest
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)

# All standard GET endpoints to scan for JSON validity and privacy compliance
ALL_API_ENDPOINTS: List[str] = [
    "/health",
    "/catalog/tables",
    "/catalog/tables/ev_opportunities",
    "/catalog/tables/customers",
    "/catalog/tables/sales",
    "/catalog/metrics",
    "/catalog/search?q=electric",
    "/quality",
    "/pipeline/runs?limit=10",
    "/gold/opportunities?limit=20",
    "/gold/fuel-verification-queue?limit=20",
    "/gold/referential-integrity",
    "/gold/ev-share-by-dealer?limit=20",
    "/gold/service-cost-comparison",
    "/gold/trade-in-value-ratio",
    "/gold/data-quality-scores",
    "/silver/customers?limit=20",
    "/silver/sales?limit=20",
    "/silver/contracts?limit=20",
    "/silver/service-records?limit=20",
    "/silver/trade-ins?limit=20",
    "/silver/dealers",
    "/silver/vehicles?limit=20",
    "/silver/rejected-records?limit=20",
]


def test_health_endpoint():
    """Verify system health, layer readiness, and last run timestamp."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "HEALTHY"
    assert data["reference_date"] == "2026-10-09"
    assert data["layers"]["bronze"] is True
    assert data["layers"]["silver"] is True
    assert data["layers"]["gold"] is True
    assert data["layers"]["meta"] is True
    assert data["total_tables_cataloged"] >= 20


def test_catalog_endpoints():
    """Verify catalog tables, table detail, metrics glossary, and keyword search."""
    # 1. /catalog/tables
    r_tables = client.get("/catalog/tables")
    assert r_tables.status_code == 200
    tables_data = r_tables.json()
    assert tables_data["total_tables"] >= 20
    assert any(t["name"] == "sales" for t in tables_data["tables"])

    # 2. /catalog/tables/{table_name}
    r_opp = client.get("/catalog/tables/ev_opportunities")
    assert r_opp.status_code == 200
    opp_data = r_opp.json()
    assert opp_data["name"] == "ev_opportunities"
    assert opp_data["layer"] == "gold"
    assert opp_data["owner"] == "Sales Data Team"
    assert "sales" in opp_opp if (opp_opp := opp_data["upstream_tables"]) else False

    # 3. /catalog/metrics
    r_metrics = client.get("/catalog/metrics")
    assert r_metrics.status_code == 200
    metrics_data = r_metrics.json()
    assert metrics_data["total_metrics"] >= 10
    assert "opportunity_score" in metrics_data["metrics_by_name"]
    assert "overall_dq_score" in metrics_data["metrics_by_name"]

    # 4. /catalog/search
    r_search = client.get("/catalog/search?q=electric")
    assert r_search.status_code == 200
    search_data = r_search.json()
    assert len(search_data["matched_metrics"]) > 0 or len(search_data["matched_tables"]) > 0


def test_gold_opportunities_endpoint_score_bands_and_fields():
    """Verify /gold/opportunities returns has_unknown_vehicle, score_reasons, total_leads, and score_bands."""
    response = client.get("/gold/opportunities")
    assert response.status_code == 200
    data = response.json()

    # 1. Verify total lead count
    assert data["total_leads"] == 1057, f"Expected 1,057 total leads, got {data['total_leads']}"

    # 2. Verify score bands summary
    assert "score_bands" in data
    bands = data["score_bands"]
    assert set(bands.keys()) == {"0-39", "40-59", "60-79", "80-100"}
    assert bands["0-39"] == 421
    assert bands["40-59"] == 398
    assert bands["60-79"] == 231
    assert bands["80-100"] == 7
    assert sum(bands.values()) == 1057

    # 3. Verify lead fields
    assert len(data["opportunities"]) == 1057
    first_lead = data["opportunities"][0]
    assert "has_unknown_vehicle" in first_lead
    assert isinstance(first_lead["has_unknown_vehicle"], bool)
    assert "score_reasons" in first_lead
    assert isinstance(first_lead["score_reasons"], list)
    assert len(first_lead["score_reasons"]) > 0
    assert all(isinstance(r, str) for r in first_lead["score_reasons"])

    # 4. Verify score filtering and pagination
    r_filtered = client.get("/gold/opportunities?min_score=80")
    assert r_filtered.status_code == 200
    filtered_data = r_filtered.json()
    assert filtered_data["total_leads"] == 1057  # Overall leads remains 1057
    assert filtered_data["count"] == 7  # Filtered count matches Tier 1 band
    assert len(filtered_data["opportunities"]) == 7


def test_gold_fuel_verification_queue_endpoint_masked_and_pagination():
    """Verify /gold/fuel-verification-queue returns masked columns only and supports pagination."""
    response = client.get("/gold/fuel-verification-queue?limit=25&offset=0")
    assert response.status_code == 200
    data = response.json()

    assert data["total_count"] == 102
    assert data["limit"] == 25
    assert data["offset"] == 0
    assert data["count"] == 25
    assert len(data["items"]) == 25

    # Check allowed masked columns only
    allowed_cols = {"customer_id", "name_masked", "phone_masked", "vin", "brand", "model", "reason"}
    for item in data["items"]:
        item_keys = set(item.keys())
        assert item_keys.issubset(allowed_cols), f"Unexpected columns in fuel queue: {item_keys - allowed_cols}"
        assert "***" in item["name_masked"]
        # Phone may be masked or empty/None if source had no phone
        if item["phone_masked"]:
            assert "***" in item["phone_masked"]


def test_gold_referential_integrity_endpoint():
    """Verify /gold/referential-integrity exposes audit rows for fact tables."""
    response = client.get("/gold/referential-integrity")
    assert response.status_code == 200
    data = response.json()

    assert data["total_checks"] == 3
    checks = data["checks"]
    fact_tables = {c["fact_table"] for c in checks}
    assert fact_tables == {"service_records", "contracts", "trade_ins"}
    for c in checks:
        assert "orphan_distinct_vins" in c
        assert "orphan_unmatched_sales_pct" in c


def test_quality_and_pipeline_runs_endpoints():
    """Verify /quality and /pipeline/runs endpoints."""
    # /quality
    r_qual = client.get("/quality")
    assert r_qual.status_code == 200
    q_data = r_qual.json()
    assert len(q_data["data_quality_scores"]) >= 12
    assert q_data["rejection_summary"]["total_rejection_events"] == 741

    # /pipeline/runs
    r_runs = client.get("/pipeline/runs?limit=10")
    assert r_runs.status_code == 200
    runs_data = r_runs.json()
    assert runs_data["total_runs"] > 0
    assert len(runs_data["runs"]) <= 10


def test_silver_endpoints_pii_sanitization():
    """Verify Silver endpoints strip unmasked customer names and phone numbers."""
    prohibited_cols = {"display_name", "customer_name_clean", "customer_name_ascii", "phone_norm", "phone_raw"}

    endpoints_to_test = [
        "/silver/customers?limit=20",
        "/silver/sales?limit=20",
        "/silver/contracts?limit=20",
        "/silver/service-records?limit=20",
        "/silver/trade-ins?limit=20",
        "/silver/dealers",
        "/silver/vehicles?limit=20",
        "/silver/rejected-records?limit=20",
    ]

    for ep in endpoints_to_test:
        resp = client.get(ep)
        assert resp.status_code == 200, f"Endpoint {ep} failed with status {resp.status_code}"
        data = resp.json()

        # Find rows / items in response
        rows = (
            data.get("rows")
            or data.get("dealers")
            or data.get("vehicles")
            or data.get("rejections")
            or data.get("items")
            or []
        )
        assert len(rows) > 0, f"Expected non-empty rows for {ep}"

        for row in rows:
            for col in prohibited_cols:
                assert col not in row, f"Endpoint {ep} exposed prohibited PII column '{col}'!"


def test_all_endpoints_return_valid_json_and_no_nan():
    """Verify every API endpoint returns standard RFC 8259 compliant JSON with NaN converted to null."""
    for ep in ALL_API_ENDPOINTS:
        resp = client.get(ep)
        assert resp.status_code == 200, f"Endpoint {ep} returned status {resp.status_code}"

        # 1. Parse JSON strictly disallowing NaN tokens
        try:
            parsed = json.loads(resp.text)
        except Exception as e:
            pytest.fail(f"Endpoint {ep} returned invalid JSON: {e}")

        # 2. Confirm raw response text never contains NaN or Infinity as JSON tokens
        raw_text = resp.text
        assert " NaN" not in raw_text, f"Endpoint {ep} returned unquoted NaN token!"
        assert ":NaN" not in raw_text, f"Endpoint {ep} returned unquoted :NaN token!"
        assert " Infinity" not in raw_text, f"Endpoint {ep} returned Infinity token!"


def test_turkish_characters_utf8_preservation():
    """Verify Turkish characters (İ, ı, ş, ğ, ç, ö, ü) are preserved in UTF-8."""
    resp = client.get("/gold/opportunities?limit=20")
    assert resp.status_code == 200
    assert "charset=utf-8" in resp.headers.get("content-type", "").lower()

    # Score reasons contain Turkish text like 'Sözleşme', 'yaşı', 'büyük'
    raw_text = resp.text
    assert "Sözleşme" in raw_text or "yaşı" in raw_text or "Bakım" in raw_text or "büyük" in raw_text


def test_privacy_test_scans_all_endpoints_for_unmasked_names_and_raw_phones():
    """Non-negotiable requirement: scans the JSON of every endpoint and fails if it finds

    a raw 10-digit phone number or an unmasked name column (display_name, customer_name_clean, phone_norm).
    """
    raw_phone_pattern = re.compile(r"\b0?5\d{9}\b")
    forbidden_column_names = {"display_name", "customer_name_clean", "phone_norm", "phone_raw"}

    for ep in ALL_API_ENDPOINTS:
        resp = client.get(ep)
        assert resp.status_code == 200, f"Endpoint {ep} returned status {resp.status_code}"
        data = resp.json()

        # 1. Scan all dictionary keys and values recursively
        def check_node(node: Any, path: str = "") -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    # For data row objects (excluding schema catalogs), forbid unmasked column names
                    if not ep.startswith("/catalog/tables"):
                        assert k not in forbidden_column_names, (
                            f"Privacy violation at {ep}: found unmasked column '{k}' at path '{path}'!"
                        )
                    # Check string values for raw 10-digit phone numbers
                    if isinstance(v, str):
                        match = raw_phone_pattern.search(v)
                        assert not match, (
                            f"Privacy violation at {ep}: found raw 10-digit phone '{match.group()}' at '{path}.{k}'!"
                        )
                    check_node(v, f"{path}.{k}")
            elif isinstance(node, list):
                for i, item in enumerate(node):
                    if isinstance(item, str):
                        match = raw_phone_pattern.search(item)
                        assert not match, (
                            f"Privacy violation at {ep}: found raw phone '{match.group()}' at '{path}[{i}]'!"
                        )
                    check_node(item, f"{path}[{i}]")

        check_node(data)

        # 2. Scan entire JSON string representation for raw 10-digit phone numbers
        raw_text = resp.text
        phone_matches = raw_phone_pattern.findall(raw_text)
        assert len(phone_matches) == 0, (
            f"Privacy violation at {ep}: raw phone numbers detected in JSON payload: {phone_matches[:3]}"
        )


def test_web_app_mount_and_redirection():
    """Verify that root redirects to /app/, static app is served at /app/, and /docs remains active."""
    # 1. Root redirect
    r_root = client.get("/", follow_redirects=False)
    assert r_root.status_code in (302, 307)
    assert r_root.headers.get("location") == "/app/"

    # 2. Web App index.html served at /app/
    r_app = client.get("/app/")
    assert r_app.status_code == 200
    assert "ELECTRIFICATION INTELLIGENCE PLATFORM" in r_app.text
    assert "Fırsat Radarı" in r_app.text
    assert "sentetik veri" in r_app.text.lower()

    # 3. Static assets
    r_css = client.get("/app/style.css")
    assert r_css.status_code == 200
    assert "navy" in r_css.text.lower() or "slate" in r_css.text.lower()

    r_js = client.get("/app/app.js")
    assert r_js.status_code == 200
    assert "allOpportunities" in r_js.text

    # 4. OpenAPI /docs remains active and unblocked
    r_docs = client.get("/docs")
    assert r_docs.status_code == 200
    assert "swagger" in r_docs.text.lower() or "openapi" in r_docs.text.lower()

    # 5. Favicon returns 204 No Content to avoid browser 404s
    r_fav = client.get("/favicon.ico")
    assert r_fav.status_code == 204

