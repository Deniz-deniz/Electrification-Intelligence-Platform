"""Central configuration module for the Electrification Intelligence Platform.

Architectural Rationale:
- Single Source of Truth: All paths, schemas, reference dates, and scoring weights
  are centralized here so that pipeline steps remain pure, decoupled, and testable.
- Deterministic Time-Travel: Real-world pipelines must never depend on the system clock
  (`datetime.today()`), allowing backtesting, deterministic unit tests, and replayability.
- No External Dependencies / No LLM: Fully self-contained and offline.
"""

from datetime import date
from pathlib import Path
from typing import Dict, List

# ----------------------------------------------------------------------
# 1. Project Root and Storage Paths (Medallion Lakehouse layout)
# ----------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent

DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
BRONZE_DIR: Path = DATA_DIR / "bronze"
SILVER_DIR: Path = DATA_DIR / "silver"
GOLD_DIR: Path = DATA_DIR / "gold"
META_DIR: Path = DATA_DIR / "meta"
GROUND_TRUTH_DIR: Path = DATA_DIR / "ground_truth"
EXPORT_DIR: Path = DATA_DIR / "export"
POWERBI_EXPORT_DIR: Path = EXPORT_DIR / "powerbi"

# Audit Log (Non-negotiable pipeline run log)
PIPELINE_RUNS_FILE: Path = META_DIR / "pipeline_runs.parquet"

# Data Catalog & Metric Glossary
DATA_CATALOG_FILE: Path = META_DIR / "data_catalog.parquet"
METRIC_GLOSSARY_FILE: Path = META_DIR / "metric_glossary.parquet"
CATALOG_JSON_FILE: Path = META_DIR / "catalog.json"
METRICS_JSON_FILE: Path = META_DIR / "metrics.json"
CATALOG_CONFIG_FILE: Path = BASE_DIR / "catalog_config.json"

# ----------------------------------------------------------------------
# 2. Time-Based Logic & Reference Date
# ----------------------------------------------------------------------
# Rule 4: The reference date for all time-based logic is configurable.
# Never call date.today() inside business logic.
REFERENCE_DATE: date = date(2026, 10, 9)

# ----------------------------------------------------------------------
# 3. Opportunity Radar Scoring Weights (Placeholder & Defaults)
# ----------------------------------------------------------------------
# Business weights used to identify and rank internal combustion engine (ICE)
# owners who represent prime candidates for an Electric Vehicle (EV) upgrade.
#
# Weights breakdown:
# - contract_expiry: Proximity of lease/finance end-date (higher urgency to renew).
# - vehicle_age: Age of currently owned vehicle (older vehicles ready for replacement).
# - maintenance_burden: Cumulative service spend/frequency (incentive to switch to lower-maintenance EV).
# - trade_in_intent: Valuation or trade-in requests submitted to Borusan Next.
OPPORTUNITY_SCORE_WEIGHTS: Dict[str, float] = {
    "contract_expiry": 0.35,
    "vehicle_age": 0.25,
    "maintenance_burden": 0.20,
    "trade_in_intent": 0.20,
}

# Additive Opportunity Score Rules (Simple, explainable, additive up to 100 points)
OPPORTUNITY_RULES_CONFIG: Dict[str, int] = {
    # Vehicle replacement contracts (Kiralama / Finans ONLY)
    "contract_ending_90_days": 30,          # Kiralama/Finans ending within 90 days (high urgency renewal)
    "contract_ending_180_days": 20,         # Kiralama/Finans ending within 91-180 days (planning window)
    "contract_expired_within_180_days": 10, # Kiralama/Finans expired within last 180 days (recently ended)
    # Insurance touchpoint (Kasko ONLY - not replacement signal, max 5 points)
    "kasko_renewal_soon": 5,                # Kasko ending within 60 days (minor insurance touchpoint)
    # Vehicle age
    "vehicle_age_gt_4_years": 25,           # Vehicle older than 4.0 years
    "vehicle_age_gt_3_years": 15,           # Vehicle older than 3.0 years
    # Maintenance burden
    "high_service_cost": 20,                # Historical maintenance spend > 30,000 TL
    "moderate_service_cost": 10,            # Historical maintenance spend > 15,000 TL
    "no_recent_service": 15,                # No workshop visit in over 12 months (> 365 days) or no service visit
    # Brand electrification availability
    "ev_model_available": 10,               # Brand with active EV models in group portfolio
}

# Contract signal configuration
CONTRACT_REPLACEMENT_TYPES: List[str] = ["Kiralama", "Finans"]
CONTRACT_MAX_EXPIRED_DAYS: int = 180        # Contracts ended > 180 days ago give 0 contract points
KASKO_RENEWAL_WINDOW_DAYS: int = 60         # Kasko ending within 60 days gives minor insurance touchpoint (max 5 pts)

# Brands with production EV models in group portfolio (MINI, BMW, Jaguar)
BRANDS_WITH_EV: List[str] = [
    "BMW",
    "MINI",
    "Jaguar",
]

# ----------------------------------------------------------------------
# 4. Brand & Taxonomy Reference Data
# ----------------------------------------------------------------------
SUPPORTED_BRANDS: List[str] = [
    "BMW",
    "MINI",
    "BMW Motorrad",
    "Range Rover",
    "Defender",
    "Discovery",
    "Jaguar",
]

POWERTRAIN_TYPES: List[str] = [
    "Benzin",
    "Dizel",
    "Hibrit",
    "Elektrik",
]

# ----------------------------------------------------------------------
# 5. Pipeline Run Audit & Quarantine Schema Definitions
# ----------------------------------------------------------------------
PIPELINE_RUNS_COLUMNS: List[str] = [
    "run_id",
    "step",
    "layer",
    "source",
    "rows_in",
    "rows_out",
    "rows_rejected",
    "started_at",
    "duration_s",
    "status",
    "error",
]

# Quarantine Rejection Table Schema
REJECTED_RECORDS_COLUMNS: List[str] = [
    "source_table",
    "source_row_id",
    "source_business_key",
    "rule_code",
    "rule_description",
    "raw_value",
    "run_id",
]

# Standard Rejection Codes
REJECTION_CODES: Dict[str, str] = {
    "INVALID_VIN": "VIN is malformed, not 17 characters, or contains invalid characters (I, O, Q)",
    "MISSING_VIN": "VIN field is null, empty, or whitespace",
    "INVALID_DATE": "Date is unparseable or outside permissible domain boundaries",
    "CONTRACT_END_BEFORE_START": "Contract end date is prior to start date",
    "DUPLICATE_KEY": "Duplicate primary or transaction key",
}

# Standard Gold Tables
GOLD_TABLES: List[str] = [
    "ev_opportunities",
    "ev_share_by_dealer",
    "service_cost_comparison",
    "trade_in_value_ratio",
    "data_quality_scores",
    "referential_integrity_checks",
    "fuel_type_verification_queue",
]

# ----------------------------------------------------------------------
# 6. PII Masking Configuration (KVKK Compliance)
# ----------------------------------------------------------------------
MASKING_CONFIG: Dict[str, str] = {
    "name_mask_char": "*",
    "phone_mask_format": "{area}***{last4}",  # e.g., "530***4226"
}
