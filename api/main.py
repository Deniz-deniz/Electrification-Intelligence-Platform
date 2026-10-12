"""FastAPI backend application for the Electrification Intelligence Platform.

Architectural Rationale:
- Zero-PII Presentation Tier: Masks customer names and phone numbers across all Silver
  and Gold endpoints. Raw 10-digit phones and unmasked name columns are strictly prohibited.
- RFC 8259 Compliant JSON: Converts all Pandas NaN and NaT values to null and NumPy scalar
  types to native Python primitives via SafeJSONResponse.
- Windows & UTF-8 Safe: Encodes Turkish characters (İ, ı, ş, ğ, ç, ö, ü) faithfully without
  ASCII escaping.
- Comprehensive REST APIs: Exposes health, data catalog, metric glossary, quality audits,
  pipeline runs, Gold analytical models, and Silver dimensions/facts.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from api.utils import SafeJSONResponse, read_parquet_safe, sanitize_silver_dataframe
from config import (
    BASE_DIR,
    BRONZE_DIR,
    CATALOG_JSON_FILE,
    DATA_DIR,
    GOLD_DIR,
    META_DIR,
    METRICS_JSON_FILE,
    PIPELINE_RUNS_FILE,
    REFERENCE_DATE,
    SILVER_DIR,
)

STATIC_DIR = BASE_DIR / "app" / "static"

app = FastAPI(
    title="Electrification Intelligence Platform API",
    description="Enterprise REST API for Borusan Automotive EV Opportunity Radar, Lakehouse Data Catalog, and Quality Governance.",
    version="1.0.0",
    default_response_class=SafeJSONResponse,
)

# Enable CORS for frontend applications
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def redirect_to_app():
    """Redirect root path to Opportunity Radar web application."""
    return RedirectResponse(url="/app/")


@app.get("/favicon.ico", include_in_schema=False)
def get_favicon():
    """Return 204 No Content for browser favicon requests to avoid 404 noise."""
    return Response(status_code=204)


# ==============================================================================
# 1. System Health & Readiness
# ==============================================================================

@app.get("/health", summary="System Health & Layer Readiness")
def get_health() -> SafeJSONResponse:
    """Check readiness across lakehouse tiers and return latest execution telemetry."""
    bronze_ok = bool(BRONZE_DIR.exists() and len(list(BRONZE_DIR.glob("*.parquet"))) > 0)
    silver_ok = bool(SILVER_DIR.exists() and len(list(SILVER_DIR.glob("*.parquet"))) > 0)
    gold_ok = bool(GOLD_DIR.exists() and len(list(GOLD_DIR.glob("*.parquet"))) > 0)
    meta_ok = bool(META_DIR.exists() and PIPELINE_RUNS_FILE.exists())

    last_run_timestamp = None
    if PIPELINE_RUNS_FILE.exists():
        try:
            runs_df = pd.read_parquet(PIPELINE_RUNS_FILE)
            if not runs_df.empty:
                last_run_timestamp = str(runs_df["started_at"].iloc[-1])
        except Exception:
            pass

    table_count = 0
    if CATALOG_JSON_FILE.exists():
        try:
            with open(CATALOG_JSON_FILE, "r", encoding="utf-8") as f:
                cat = json.load(f)
                table_count = int(cat.get("total_tables", 0))
        except Exception:
            pass

    return SafeJSONResponse(content={
        "status": "HEALTHY",
        "reference_date": REFERENCE_DATE.isoformat(),
        "last_run_timestamp": last_run_timestamp,
        "layers": {
            "bronze": bronze_ok,
            "silver": silver_ok,
            "gold": gold_ok,
            "meta": meta_ok,
        },
        "total_tables_cataloged": table_count,
    })


# ==============================================================================
# 2. Data Catalog & Metrics Glossary Endpoints
# ==============================================================================

@app.get("/catalog/tables", summary="List All Cataloged Tables")
def get_catalog_tables(layer: Optional[str] = Query(None, description="Filter by layer: bronze, silver, gold")) -> SafeJSONResponse:
    """Return all cataloged tables across lakehouse layers with schema and ownership."""
    if not CATALOG_JSON_FILE.exists():
        raise HTTPException(status_code=404, detail="Data catalog not yet generated.")

    with open(CATALOG_JSON_FILE, "r", encoding="utf-8") as f:
        cat = json.load(f)

    tables = cat.get("tables", [])
    if layer:
        tables = [t for t in tables if t.get("layer") == layer.lower()]

    return SafeJSONResponse(content={
        "total_tables": len(tables),
        "tables": tables,
    })


@app.get("/catalog/tables/{table_name}", summary="Get Detailed Table Catalog Entry")
def get_catalog_table_details(table_name: str) -> SafeJSONResponse:
    """Return detailed metadata, column types, descriptions, row count, and quality score for a specific table."""
    if not CATALOG_JSON_FILE.exists():
        raise HTTPException(status_code=404, detail="Data catalog not yet generated.")

    with open(CATALOG_JSON_FILE, "r", encoding="utf-8") as f:
        cat = json.load(f)

    table_entry = cat.get("tables_by_name", {}).get(table_name)
    if not table_entry:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in catalog.")

    return SafeJSONResponse(content=table_entry)


@app.get("/catalog/metrics", summary="List All Business Metrics")
def get_catalog_metrics() -> SafeJSONResponse:
    """Return definitions, exact mathematical formulas, and business owners for all Gold metrics."""
    if not METRICS_JSON_FILE.exists():
        raise HTTPException(status_code=404, detail="Metrics glossary not yet generated.")

    with open(METRICS_JSON_FILE, "r", encoding="utf-8") as f:
        met = json.load(f)

    return SafeJSONResponse(content=met)


@app.get("/catalog/search", summary="Search Catalog Tables, Columns, and Metrics")
def search_catalog(q: str = Query("", description="Search term for tables, columns, or metrics")) -> SafeJSONResponse:
    """Search tables, column descriptions, and metrics by keywords."""
    query = q.strip().lower()
    matched_tables = []
    matched_columns = []
    matched_metrics = []

    if CATALOG_JSON_FILE.exists() and query:
        with open(CATALOG_JSON_FILE, "r", encoding="utf-8") as f:
            cat = json.load(f)

        for t in cat.get("tables", []):
            t_name = t.get("name", "").lower()
            t_desc = t.get("description", "").lower()
            if query in t_name or query in t_desc:
                matched_tables.append({
                    "table_name": t.get("name"),
                    "layer": t.get("layer"),
                    "description": t.get("description"),
                })

            for c in t.get("columns", []):
                c_name = c.get("name", "").lower()
                c_desc = c.get("description", "").lower()
                if query in c_name or query in c_desc:
                    matched_columns.append({
                        "table_name": t.get("name"),
                        "column_name": c.get("name"),
                        "type": c.get("type"),
                        "description": c.get("description"),
                    })

    if METRICS_JSON_FILE.exists() and query:
        with open(METRICS_JSON_FILE, "r", encoding="utf-8") as f:
            met = json.load(f)

        for m in met.get("metrics", []):
            m_name = m.get("name", "").lower()
            m_desc = m.get("business_definition", "").lower()
            m_form = m.get("formula", "").lower()
            if query in m_name or query in m_desc or query in m_form:
                matched_metrics.append(m)

    return SafeJSONResponse(content={
        "query": q,
        "matched_tables": matched_tables,
        "matched_columns": matched_columns,
        "matched_metrics": matched_metrics,
    })


# ==============================================================================
# 3. Quality & Governance Dashboard Endpoints
# ==============================================================================

@app.get("/quality", summary="Data Quality & Integrity Dashboard Data")
def get_quality_dashboard() -> SafeJSONResponse:
    """Return layer-by-layer completeness, validity, uniqueness, rejections, and referential integrity."""
    dq_scores = []
    dq_file = GOLD_DIR / "data_quality_scores.parquet"
    if dq_file.exists():
        df_dq = read_parquet_safe(dq_file)
        dq_scores = df_dq.to_dict(orient="records")

    rejections_summary = {}
    rej_file = SILVER_DIR / "rejected_records.parquet"
    if rej_file.exists():
        df_rej = read_parquet_safe(rej_file)
        rejections_summary = {
            "total_rejection_events": len(df_rej),
            "by_rule_code": df_rej["rule_code"].value_counts().to_dict(),
            "by_source_table": df_rej["source_table"].value_counts().to_dict(),
        }

    ref_integrity = []
    ref_file = GOLD_DIR / "referential_integrity_checks.parquet"
    if ref_file.exists():
        df_ref = read_parquet_safe(ref_file)
        ref_integrity = df_ref.to_dict(orient="records")

    er_quality = {}
    er_file = META_DIR / "matching_quality.json"
    if er_file.exists():
        try:
            with open(er_file, "r", encoding="utf-8") as f:
                er_quality = json.load(f)
        except Exception:
            pass

    return SafeJSONResponse(content={
        "data_quality_scores": dq_scores,
        "rejection_summary": rejections_summary,
        "referential_integrity": ref_integrity,
        "entity_resolution_quality": er_quality,
    })


@app.get("/pipeline/runs", summary="Pipeline Execution Audit Log")
def get_pipeline_runs(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    layer: Optional[str] = Query(None, description="Filter by layer"),
    status: Optional[str] = Query(None, description="Filter by status: SUCCESS, FAILED"),
) -> SafeJSONResponse:
    """Return historical pipeline execution telemetry sorted by newest first."""
    if not PIPELINE_RUNS_FILE.exists():
        return SafeJSONResponse(content={"total_runs": 0, "limit": limit, "offset": offset, "runs": []})

    df_runs = read_parquet_safe(PIPELINE_RUNS_FILE)

    if layer:
        df_runs = df_runs[df_runs["layer"] == layer.lower()]
    if status:
        df_runs = df_runs[df_runs["status"] == status.upper()]

    total_runs = len(df_runs)
    # Reverse to present newest runs first
    df_runs = df_runs.iloc[::-1]
    paged_runs = df_runs.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "total_runs": total_runs,
        "limit": limit,
        "offset": offset,
        "runs": paged_runs,
    })


# ==============================================================================
# 4. Gold Analytical Model Endpoints (Opportunity Radar & Electrification KPIs)
# ==============================================================================

@app.get("/gold/opportunities", summary="EV Transition Opportunities (Opportunity Radar)")
@app.get("/gold/ev_opportunities", include_in_schema=False)
@app.get("/gold/ev-opportunities", include_in_schema=False)
def get_gold_opportunities(
    min_score: Optional[int] = Query(None, ge=0, le=100),
    max_score: Optional[int] = Query(None, ge=0, le=100),
    brand: Optional[str] = Query(None),
    dealer: Optional[str] = Query(None),
    has_unknown_vehicle: Optional[bool] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return ranked ICE vehicle owners targeted for EV conversion with score-band breakdown."""
    file_path = GOLD_DIR / "ev_opportunities.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="ev_opportunities table not found.")

    df = read_parquet_safe(file_path)
    total_leads = len(df)

    # Enrich with dealer_name from sales & dealers if available
    sales_path = SILVER_DIR / "sales.parquet"
    dealers_path = SILVER_DIR / "dealers.parquet"
    if sales_path.exists() and dealers_path.exists():
        sales_df = read_parquet_safe(sales_path)[["vin", "dealer_code"]].drop_duplicates("vin")
        dealers_df = read_parquet_safe(dealers_path)[["dealer_code", "dealer_name"]]
        dealer_map = sales_df.merge(dealers_df, on="dealer_code", how="left").set_index("vin")["dealer_name"].to_dict()
        df["dealer_name"] = df["vin"].map(dealer_map).fillna("Bilinmiyor")

    # Compute overall score-band distribution across full lead population
    score_series = df["opportunity_score"]
    score_bands = {
        "0-39": int((score_series < 40).sum()),
        "40-59": int(((score_series >= 40) & (score_series < 60)).sum()),
        "60-79": int(((score_series >= 60) & (score_series < 80)).sum()),
        "80-100": int((score_series >= 80).sum()),
    }

    # Apply optional query filters
    if min_score is not None:
        df = df[df["opportunity_score"] >= min_score]
    if max_score is not None:
        df = df[df["opportunity_score"] <= max_score]
    if brand:
        df = df[df["brand"].str.lower() == brand.lower()]
    if dealer:
        df = df[df["dealer_name"].str.lower().str.contains(dealer.lower())]
    if has_unknown_vehicle is not None:
        df = df[df["has_unknown_vehicle"] == has_unknown_vehicle]

    # Sort descending by propensity score
    df = df.sort_values(by="opportunity_score", ascending=False)

    filtered_count = len(df)
    if limit is not None:
        df = df.iloc[offset : offset + limit]
    elif offset > 0:
        df = df.iloc[offset:]

    records = df.to_dict(orient="records")

    return SafeJSONResponse(content={
        "total_leads": total_leads,
        "filtered_leads": filtered_count,
        "score_bands": score_bands,
        "limit": limit,
        "offset": offset,
        "count": len(records),
        "opportunities": records,
    })


@app.get("/gold/fuel-verification-queue", summary="Fuel Type Verification Queue (Masked Only)")
@app.get("/gold/fuel_type_verification_queue", include_in_schema=False)
@app.get("/gold/fuel_verification_queue", include_in_schema=False)
def get_fuel_verification_queue(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return operational queue of customers owning exclusively unknown-fuel vehicles with pagination."""
    file_path = GOLD_DIR / "fuel_type_verification_queue.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="fuel_type_verification_queue table not found.")

    df = read_parquet_safe(file_path)
    total_count = len(df)

    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "items": paged,
    })


@app.get("/gold/referential-integrity", summary="Referential Integrity Checks")
@app.get("/gold/referential_integrity", include_in_schema=False)
@app.get("/gold/referential_integrity_checks", include_in_schema=False)
def get_referential_integrity() -> SafeJSONResponse:
    """Return fact table referential integrity audits against vehicle and sales lineage."""
    file_path = GOLD_DIR / "referential_integrity_checks.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="referential_integrity_checks table not found.")

    df = read_parquet_safe(file_path)
    records = df.to_dict(orient="records")

    return SafeJSONResponse(content={
        "total_checks": len(records),
        "checks": records,
    })


@app.get("/gold/ev-share-by-dealer", summary="EV Adoption Share by Dealer and Brand")
@app.get("/gold/ev_share_by_dealer", include_in_schema=False)
def get_ev_share_by_dealer(
    year: Optional[int] = Query(None),
    brand: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return electrification sales volume and penetration percentage by dealer and brand."""
    file_path = GOLD_DIR / "ev_share_by_dealer.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="ev_share_by_dealer table not found.")

    df = read_parquet_safe(file_path)
    if year:
        df = df[df["year"] == year]
    if brand:
        df = df[df["brand"].str.lower() == brand.lower()]

    total = len(df)
    if limit is not None:
        df = df.iloc[offset : offset + limit]

    return SafeJSONResponse(content={
        "total_count": total,
        "count": len(df),
        "items": df.to_dict(orient="records"),
    })


@app.get("/gold/service-cost-comparison", summary="Service Cost Comparison (Electric vs ICE)")
@app.get("/gold/service_cost_comparison", include_in_schema=False)
def get_service_cost_comparison() -> SafeJSONResponse:
    """Return comparative maintenance spend and annual cost per vehicle-year by brand and powertrain."""
    file_path = GOLD_DIR / "service_cost_comparison.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="service_cost_comparison table not found.")

    df = read_parquet_safe(file_path)
    records = df.to_dict(orient="records")

    return SafeJSONResponse(content={
        "total_cohorts": len(records),
        "comparison": records,
    })


@app.get("/gold/trade-in-value-ratio", summary="Trade-In Residual Value Retention Ratios")
@app.get("/gold/trade_in_value_ratio", include_in_schema=False)
def get_trade_in_value_ratio() -> SafeJSONResponse:
    """Return residual value retention ratios comparing trade-in offers against original sales price."""
    file_path = GOLD_DIR / "trade_in_value_ratio.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="trade_in_value_ratio table not found.")

    df = read_parquet_safe(file_path)
    records = df.to_dict(orient="records")

    return SafeJSONResponse(content={
        "total_cohorts": len(records),
        "retention_ratios": records,
    })


@app.get("/gold/data-quality-scores", summary="Layer-by-Layer Data Quality Scores")
@app.get("/gold/data_quality_scores", include_in_schema=False)
def get_data_quality_scores() -> SafeJSONResponse:
    """Return completeness, validity, uniqueness, and overall DQ scores for all lakehouse tables."""
    file_path = GOLD_DIR / "data_quality_scores.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="data_quality_scores table not found.")

    df = read_parquet_safe(file_path)
    return SafeJSONResponse(content={
        "total_audited_tables": len(df),
        "scores": df.to_dict(orient="records"),
    })


@app.get("/gold/{table_name}", summary="Generic Gold Table Access")
def get_generic_gold_table(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Dynamically serve any Gold table with pagination."""
    clean_name = table_name.replace("-", "_")
    file_path = GOLD_DIR / f"{clean_name}.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Gold table '{clean_name}' does not exist.")

    df = read_parquet_safe(file_path)
    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": clean_name,
        "layer": "gold",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


# ==============================================================================
# 5. Silver Lakehouse Endpoints (Sanitized PII Only)
# ==============================================================================

@app.get("/silver/customers", summary="Silver Customers (Masked Tokens Only)")
def get_silver_customers(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return golden customer dimension with unmasked name and phone stripped."""
    file_path = SILVER_DIR / "customers.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "customers",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


@app.get("/silver/sales", summary="Silver Sales Transactions (Masked Only)")
def get_silver_sales(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return clean deduplicated vehicle sales transactions with unmasked PII stripped."""
    file_path = SILVER_DIR / "sales.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "sales",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


@app.get("/silver/contracts", summary="Silver Contracts (Masked Only)")
def get_silver_contracts(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return clean financial services contracts with unmasked PII stripped."""
    file_path = SILVER_DIR / "contracts.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "contracts",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


@app.get("/silver/service-records", summary="Silver Workshop Records (Masked Only)")
@app.get("/silver/service_records", include_in_schema=False)
def get_silver_service_records(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return clean workshop service orders with customer names stripped."""
    file_path = SILVER_DIR / "service_records.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "service_records",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


@app.get("/silver/trade-ins", summary="Silver Trade-In Transactions (Masked Only)")
@app.get("/silver/trade_ins", include_in_schema=False)
def get_silver_trade_ins(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return clean trade-in appraisals with customer names stripped."""
    file_path = SILVER_DIR / "trade_ins.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "trade_ins",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


@app.get("/silver/dealers", summary="Silver Dealership Directory")
def get_silver_dealers() -> SafeJSONResponse:
    """Return standardized dealership network directory."""
    file_path = SILVER_DIR / "dealers.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    records = df.to_dict(orient="records")
    return SafeJSONResponse(content={
        "table_name": "dealers",
        "layer": "silver",
        "total_count": len(records),
        "dealers": records,
    })


@app.get("/silver/vehicles", summary="Silver Master Vehicle Dimension")
def get_silver_vehicles(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Return master vehicles catalog linking VIN, brand, model, fuel type, and owner ID."""
    file_path = SILVER_DIR / "vehicles.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "vehicles",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "vehicles": paged,
    })


@app.get("/silver/rejected-records", summary="Silver Quarantine Rejections")
@app.get("/silver/rejected_records", include_in_schema=False)
def get_silver_rejected_records(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    rule_code: Optional[str] = Query(None),
) -> SafeJSONResponse:
    """Return quarantined rejection records with explicit rule violation codes."""
    file_path = SILVER_DIR / "rejected_records.parquet"
    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    if rule_code:
        df = df[df["rule_code"] == rule_code]

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": "rejected_records",
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rejections": paged,
    })


@app.get("/silver/{table_name}", summary="Generic Silver Table Access (Strictly Sanitized)")
def get_generic_silver_table(
    table_name: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> SafeJSONResponse:
    """Dynamically serve any Silver table with unmasked PII stripped and pagination."""
    clean_name = table_name.replace("-", "_")
    file_path = SILVER_DIR / f"{clean_name}.parquet"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Silver table '{clean_name}' does not exist.")

    df = read_parquet_safe(file_path)
    df = sanitize_silver_dataframe(df)

    total_count = len(df)
    paged = df.iloc[offset : offset + limit].to_dict(orient="records")

    return SafeJSONResponse(content={
        "table_name": clean_name,
        "layer": "silver",
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "count": len(paged),
        "rows": paged,
    })


# ==============================================================================
# 6. Web Application Static Files Mount
# ==============================================================================
if STATIC_DIR.exists():
    app.mount("/app", StaticFiles(directory=str(STATIC_DIR), html=True), name="app")

