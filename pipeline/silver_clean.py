"""Silver cleaning stage for the Electrification Intelligence Platform.

Architectural Rationale:
- Medallion Silver Layer: Standardizes, validates, deduplicates, and enriches raw Bronze data.
- Small Pure Functions: Core transformations (naming, phone normalization, date parsing, VIN validation,
  brand alignment) are decoupled into pure, deterministic, unit-tested functions.
- Zero Silent Drops (Non-Negotiable Rule 2): Records failing critical validation (missing/invalid VIN,
  invalid dates, inverted contracts, duplicate keys) are quarantined to data/silver/rejected_records.parquet
  with explicit error codes, row identifiers, and provenance. Non-critical anomalies (missing phone)
  remain in clean tables with a quality flag.
- Precise Row Reconciliation: Every bronze row is deterministically categorized as either clean or rejected.
  Strict assertion enforces: rows_in == rows_out + rows_rejected.
- Idempotency & Audit Logging (Non-Negotiable Rules 1 & 6): Every step logs rows_in, rows_out, and
  rows_rejected (distinct rows) to data/meta/pipeline_runs.parquet.
"""

import logging
import re
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from config import BRONZE_DIR, REJECTION_CODES, REJECTED_RECORDS_COLUMNS, SILVER_DIR
from pipeline.logging_utils import log_pipeline_run

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# 1. Pure Transformation Functions
# ----------------------------------------------------------------------

TR_LOWER_MAP = {"I": "ı", "İ": "i", "Ğ": "ğ", "Ü": "ü", "Ş": "ş", "Ö": "ö", "Ç": "ç"}
TR_UPPER_MAP = {"ı": "I", "i": "İ", "ğ": "Ğ", "ü": "Ü", "ş": "Ş", "ö": "Ö", "ç": "Ç"}
TR_ASCII_MAP = str.maketrans("ĞÜŞİÖÇğüşıöç", "GUSIOCgusioc")

CANONICAL_BRANDS = [
    "BMW",
    "MINI",
    "Range Rover",
    "Defender",
    "Discovery",
    "Jaguar",
]

BRAND_LOOKUP = {
    "bmw": "BMW",
    "mini": "MINI",
    "range rover": "Range Rover",
    "rangerover": "Range Rover",
    "defender": "Defender",
    "discovery": "Discovery",
    "jaguar": "Jaguar",
}


def turkish_lower(s: str) -> str:
    """Convert string to lowercase respecting Turkish dotted/dotless I rules."""
    return "".join(TR_LOWER_MAP.get(c, c.lower()) for c in s)


def turkish_upper(s: str) -> str:
    """Convert string to uppercase respecting Turkish dotted/dotless I rules."""
    return "".join(TR_UPPER_MAP.get(c, c.upper()) for c in s)


def turkish_title(s: str) -> str:
    """Convert string to Title Case respecting Turkish casing rules."""
    words = s.strip().split()
    res = []
    for w in words:
        if not w:
            continue
        first = TR_UPPER_MAP.get(w[0], w[0].upper())
        rest = turkish_lower(w[1:]) if len(w) > 1 else ""
        res.append(first + rest)
    return " ".join(res)


def normalize_name(name: Optional[str]) -> Tuple[str, str]:
    """Trim, collapse whitespace, apply Turkish Title Case, and produce ASCII-folded matching key.

    Why:
    Cleans typos and inconsistent casing across DMS, Premium, and Service records.
    The ASCII matching key enables cross-system phonetic and spelling linkage in later MDM stages.
    Does NOT attempt to swap name-surname tokens at this stage.

    Returns:
        Tuple of (clean_title_cased_name, ascii_folded_key).
    """
    if name is None or not str(name).strip():
        return "", ""

    raw_str = str(name).strip()
    collapsed = re.sub(r"\s+", " ", raw_str)

    title_cased = turkish_title(collapsed)

    # Produce ASCII-folded key: translate TR chars to ASCII equivalents and lowercase
    ascii_key = collapsed.translate(TR_ASCII_MAP).lower()

    return title_cased, ascii_key


def normalize_phone(phone: Optional[str]) -> Tuple[Optional[str], bool]:
    """Extract digits, strip international +90 and trunk 0, and validate 10-digit mobile format.

    Why:
    Turkish mobile numbers follow 5XXXXXXXXX (10 digits).
    Non-matching formats are set to None with has_valid_phone=False to preserve the record
    without compromising downstream linkage keys.

    Returns:
        Tuple of (clean_10digit_phone_or_None, is_valid_bool).
    """
    if phone is None or not str(phone).strip():
        return None, False

    digits = re.sub(r"\D", "", str(phone))

    # Strip international country code 90 if 12 digits
    if digits.startswith("90") and len(digits) == 12:
        digits = digits[2:]

    # Strip trunk prefix 0 if 11 digits
    if digits.startswith("0") and len(digits) == 11:
        digits = digits[1:]

    # Valid if exactly 10 digits starting with '5'
    if len(digits) == 10 and digits.startswith("5"):
        return digits, True

    return None, False


def parse_date(val: Optional[str]) -> Tuple[Optional[date], bool]:
    """Parse date string supporting ISO (YYYY-MM-DD), Dot (DD.MM.YYYY), and Slash (DD/MM/YYYY).

    Why:
    Different legacy applications export dates in contrasting European and ISO formats.
    Guarantees strict calendar validation (rejects invalid days like 2024-02-30).

    Returns:
        Tuple of (datetime.date_or_None, is_valid_bool).
    """
    if val is None or not str(val).strip():
        return None, False

    cleaned = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(cleaned, fmt).date()
            return parsed, True
        except ValueError:
            continue

    return None, False


def normalize_vin(vin: Optional[str]) -> Tuple[str, Optional[str]]:
    """Uppercase, trim, and validate 17-character VIN syntax excluding I, O, Q.

    Why:
    ISO 3779 requires 17 alphanumeric characters and explicitly bans letters
    I, O, and Q to prevent confusion with 1 and 0. Lowercase letters from source
    systems are capitalized.

    Returns:
        Tuple of (normalized_vin_string, rejection_code_or_None).
        If valid, rejection_code_or_None is None.
        If invalid, returns reason 'MISSING_VIN' or 'INVALID_VIN'.
    """
    if vin is None:
        return "", "MISSING_VIN"

    cleaned = str(vin).strip().upper()
    if not cleaned:
        return "", "MISSING_VIN"

    if len(cleaned) != 17:
        return cleaned, "INVALID_VIN"

    if not cleaned.isalnum() or any(c in cleaned for c in ("I", "O", "Q")):
        return cleaned, "INVALID_VIN"

    return cleaned, None


def normalize_brand(brand: Optional[str]) -> str:
    """Map spelling, casing, and whitespace variants to canonical group brand names.

    Why:
    Source systems record 'bmw', 'BMW ', 'RANGE ROVER ', etc.
    Harmonizes all variants into standard enterprise taxonomy.
    """
    if brand is None or not str(brand).strip():
        return ""

    key = str(brand).strip().lower()
    return BRAND_LOOKUP.get(key, str(brand).strip())


# ----------------------------------------------------------------------
# 2. Table-Specific Cleaning & Rejection Workflows
# ----------------------------------------------------------------------

def clean_sales_table(
    df_raw: pd.DataFrame,
    run_id: str,
) -> Tuple[pd.DataFrame, List[Dict]]:
    """Clean DMS sales: deduplicate sale_id, normalize VINs, dates, brands, and names.

    Returns:
        Tuple of (clean_df, rejected_records_list).
    """
    clean_rows = []
    rejected_rows = []
    seen_sale_ids: Set[str] = set()

    df_indexed = df_raw.reset_index(drop=True)

    for idx, row in df_indexed.iterrows():
        row_id = f"row_{idx}"
        sale_id = str(row["sale_id"]).strip()
        rejections_for_row = []

        # 1. Deduplication check
        if sale_id in seen_sale_ids:
            rejections_for_row.append({
                "source_table": "dms_satis",
                "source_row_id": row_id,
                "source_business_key": sale_id,
                "rule_code": "DUPLICATE_KEY",
                "rule_description": REJECTION_CODES["DUPLICATE_KEY"],
                "raw_value": sale_id,
                "run_id": run_id,
            })
        seen_sale_ids.add(sale_id)

        # 2. VIN validation
        clean_vin, vin_err = normalize_vin(row.get("sasi_no"))
        if vin_err:
            rejections_for_row.append({
                "source_table": "dms_satis",
                "source_row_id": row_id,
                "source_business_key": sale_id,
                "rule_code": vin_err,
                "rule_description": REJECTION_CODES[vin_err],
                "raw_value": str(row.get("sasi_no") or ""),
                "run_id": run_id,
            })

        # 3. Date validation
        parsed_date, date_valid = parse_date(row.get("satis_tarihi"))
        if not date_valid:
            rejections_for_row.append({
                "source_table": "dms_satis",
                "source_row_id": row_id,
                "source_business_key": sale_id,
                "rule_code": "INVALID_DATE",
                "rule_description": REJECTION_CODES["INVALID_DATE"],
                "raw_value": str(row.get("satis_tarihi") or ""),
                "run_id": run_id,
            })

        # If any rejectable error occurred, quarantine row
        if rejections_for_row:
            rejected_rows.extend(rejections_for_row)
            continue

        # Non-critical cleaning
        name_clean, name_ascii = normalize_name(row.get("musteri_adi"))
        phone_norm, has_valid_phone = normalize_phone(row.get("telefon"))
        brand_clean = normalize_brand(row.get("marka"))

        try:
            price = int(float(row.get("satis_fiyati_tl", 0)))
        except (ValueError, TypeError):
            price = 0

        clean_rows.append({
            "sale_id": sale_id,
            "vin": clean_vin,
            "customer_name_clean": name_clean,
            "customer_name_ascii": name_ascii,
            "phone_raw": str(row.get("telefon") or ""),
            "phone_norm": phone_norm,
            "has_valid_phone": has_valid_phone,
            "brand": brand_clean,
            "model": str(row.get("model", "")).strip(),
            "fuel_type": str(row.get("yakit_tipi", "")).strip(),
            "sale_date": parsed_date.isoformat(),
            "dealer_code": str(row.get("bayi_kodu", "")).strip(),
            "sale_price_tl": price,
            "_source_system": row.get("_source_system", "DMS"),
            "_source_file": row.get("_source_file", "dms_satis.csv"),
            "_ingested_at": row.get("_ingested_at", ""),
            "_run_id": run_id,
            "_row_hash": row.get("_row_hash", ""),
        })

    return pd.DataFrame(clean_rows), rejected_rows


def clean_contracts_table(
    df_raw: pd.DataFrame,
    run_id: str,
) -> Tuple[pd.DataFrame, List[Dict]]:
    """Clean Premium contracts: validate VIN, dates, and enforce start <= end logic."""
    clean_rows = []
    rejected_rows = []

    df_indexed = df_raw.reset_index(drop=True)

    for idx, row in df_indexed.iterrows():
        row_id = f"row_{idx}"
        contract_id = str(row["sozlesme_no"]).strip()
        rejections_for_row = []

        # 1. VIN validation
        clean_vin, vin_err = normalize_vin(row.get("vin"))
        if vin_err:
            rejections_for_row.append({
                "source_table": "premium_sozlesme",
                "source_row_id": row_id,
                "source_business_key": contract_id,
                "rule_code": vin_err,
                "rule_description": REJECTION_CODES[vin_err],
                "raw_value": str(row.get("vin") or ""),
                "run_id": run_id,
            })

        # 2. Date validation
        start_dt, start_valid = parse_date(row.get("baslangic"))
        end_dt, end_valid = parse_date(row.get("bitis"))

        if not start_valid:
            rejections_for_row.append({
                "source_table": "premium_sozlesme",
                "source_row_id": row_id,
                "source_business_key": contract_id,
                "rule_code": "INVALID_DATE",
                "rule_description": REJECTION_CODES["INVALID_DATE"],
                "raw_value": f"baslangic={row.get('baslangic')}",
                "run_id": run_id,
            })
        if not end_valid:
            rejections_for_row.append({
                "source_table": "premium_sozlesme",
                "source_row_id": row_id,
                "source_business_key": contract_id,
                "rule_code": "INVALID_DATE",
                "rule_description": REJECTION_CODES["INVALID_DATE"],
                "raw_value": f"bitis={row.get('bitis')}",
                "run_id": run_id,
            })

        # 3. Temporal logic: end must not be before start
        if start_valid and end_valid and end_dt < start_dt:
            rejections_for_row.append({
                "source_table": "premium_sozlesme",
                "source_row_id": row_id,
                "source_business_key": contract_id,
                "rule_code": "CONTRACT_END_BEFORE_START",
                "rule_description": REJECTION_CODES["CONTRACT_END_BEFORE_START"],
                "raw_value": f"baslangic={row.get('baslangic')}, bitis={row.get('bitis')}",
                "run_id": run_id,
            })

        if rejections_for_row:
            rejected_rows.extend(rejections_for_row)
            continue

        # Clean non-critical attributes
        name_clean, name_ascii = normalize_name(row.get("musteri_unvani"))
        phone_norm, has_valid_phone = normalize_phone(row.get("iletisim_tel"))

        try:
            monthly_tl = int(float(row.get("aylik_tutar_tl", 0)))
        except (ValueError, TypeError):
            monthly_tl = 0

        clean_rows.append({
            "contract_id": contract_id,
            "vin": clean_vin,
            "customer_name_clean": name_clean,
            "customer_name_ascii": name_ascii,
            "phone_raw": str(row.get("iletisim_tel") or ""),
            "phone_norm": phone_norm,
            "has_valid_phone": has_valid_phone,
            "contract_type": str(row.get("sozlesme_tipi", "")).strip(),
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "monthly_amount_tl": monthly_tl,
            "_source_system": row.get("_source_system", "PREMIUM"),
            "_source_file": row.get("_source_file", "premium_sozlesme.csv"),
            "_ingested_at": row.get("_ingested_at", ""),
            "_run_id": run_id,
            "_row_hash": row.get("_row_hash", ""),
        })

    return pd.DataFrame(clean_rows), rejected_rows


def clean_service_table(
    df_raw: pd.DataFrame,
    run_id: str,
) -> Tuple[pd.DataFrame, List[Dict]]:
    """Clean service records: validate VIN and date; note service records have NO phone column."""
    clean_rows = []
    rejected_rows = []

    df_indexed = df_raw.reset_index(drop=True)

    for idx, row in df_indexed.iterrows():
        row_id = f"row_{idx}"
        service_id = str(row["servis_id"]).strip()
        rejections_for_row = []

        # 1. VIN validation
        clean_vin, vin_err = normalize_vin(row.get("sasi"))
        if vin_err:
            rejections_for_row.append({
                "source_table": "servis_kayit",
                "source_row_id": row_id,
                "source_business_key": service_id,
                "rule_code": vin_err,
                "rule_description": REJECTION_CODES[vin_err],
                "raw_value": str(row.get("sasi") or ""),
                "run_id": run_id,
            })

        # 2. Date validation
        parsed_date, date_valid = parse_date(row.get("servis_tarihi"))
        if not date_valid:
            rejections_for_row.append({
                "source_table": "servis_kayit",
                "source_row_id": row_id,
                "source_business_key": service_id,
                "rule_code": "INVALID_DATE",
                "rule_description": REJECTION_CODES["INVALID_DATE"],
                "raw_value": str(row.get("servis_tarihi") or ""),
                "run_id": run_id,
            })

        if rejections_for_row:
            rejected_rows.extend(rejections_for_row)
            continue

        name_clean, name_ascii = normalize_name(row.get("sahip_adi"))

        try:
            km = int(float(row.get("km", 0)))
        except (ValueError, TypeError):
            km = 0

        try:
            cost_tl = int(float(row.get("tutar_tl", 0)))
        except (ValueError, TypeError):
            cost_tl = 0

        clean_rows.append({
            "service_id": service_id,
            "vin": clean_vin,
            "customer_name_clean": name_clean,
            "customer_name_ascii": name_ascii,
            "service_date": parsed_date.isoformat(),
            "service_point_code": str(row.get("servis_noktasi", "")).strip(),
            "km": km,
            "cost_tl": cost_tl,
            "job_type": str(row.get("is_tipi", "")).strip(),
            "_source_system": row.get("_source_system", "SERVIS"),
            "_source_file": row.get("_source_file", "servis_kayit.csv"),
            "_ingested_at": row.get("_ingested_at", ""),
            "_run_id": run_id,
            "_row_hash": row.get("_row_hash", ""),
        })

    return pd.DataFrame(clean_rows), rejected_rows


def clean_trade_ins_table(
    df_raw: pd.DataFrame,
    run_id: str,
) -> Tuple[pd.DataFrame, List[Dict]]:
    """Clean Borusan Next trade-in records: validate VIN and date."""
    clean_rows = []
    rejected_rows = []

    df_indexed = df_raw.reset_index(drop=True)

    for idx, row in df_indexed.iterrows():
        row_id = f"row_{idx}"
        trade_in_id = str(row["islem_no"]).strip()
        rejections_for_row = []

        # 1. VIN validation
        clean_vin, vin_err = normalize_vin(row.get("vin_no"))
        if vin_err:
            rejections_for_row.append({
                "source_table": "next_islem",
                "source_row_id": row_id,
                "source_business_key": trade_in_id,
                "rule_code": vin_err,
                "rule_description": REJECTION_CODES[vin_err],
                "raw_value": str(row.get("vin_no") or ""),
                "run_id": run_id,
            })

        # 2. Date validation
        parsed_date, date_valid = parse_date(row.get("islem_tarihi"))
        if not date_valid:
            rejections_for_row.append({
                "source_table": "next_islem",
                "source_row_id": row_id,
                "source_business_key": trade_in_id,
                "rule_code": "INVALID_DATE",
                "rule_description": REJECTION_CODES["INVALID_DATE"],
                "raw_value": str(row.get("islem_tarihi") or ""),
                "run_id": run_id,
            })

        if rejections_for_row:
            rejected_rows.extend(rejections_for_row)
            continue

        name_clean, name_ascii = normalize_name(row.get("musteri"))

        try:
            offer_tl = int(float(row.get("teklif_tl", 0)))
        except (ValueError, TypeError):
            offer_tl = 0

        clean_rows.append({
            "trade_in_id": trade_in_id,
            "vin": clean_vin,
            "customer_name_clean": name_clean,
            "customer_name_ascii": name_ascii,
            "trade_in_date": parsed_date.isoformat(),
            "trade_in_type": str(row.get("islem_tipi", "")).strip(),
            "offer_tl": offer_tl,
            "_source_system": row.get("_source_system", "NEXT"),
            "_source_file": row.get("_source_file", "next_islem.csv"),
            "_ingested_at": row.get("_ingested_at", ""),
            "_run_id": run_id,
            "_row_hash": row.get("_row_hash", ""),
        })

    return pd.DataFrame(clean_rows), rejected_rows


def clean_dealers_table(
    df_raw: pd.DataFrame,
    run_id: str,
) -> Tuple[pd.DataFrame, List[Dict]]:
    """Clean reference network dimension table."""
    clean_rows = []
    df_indexed = df_raw.reset_index(drop=True)
    for idx, row in df_indexed.iterrows():
        clean_rows.append({
            "dealer_code": str(row.get("dealer_code", "")).strip(),
            "dealer_name": str(row.get("dealer_name", "")).strip(),
            "city": str(row.get("city", "")).strip(),
            "point_type": str(row.get("point_type", "")).strip(),
            "_source_system": row.get("_source_system", "NETWORK"),
            "_source_file": row.get("_source_file", "bayi_servis_noktalari.csv"),
            "_ingested_at": row.get("_ingested_at", ""),
            "_run_id": run_id,
            "_row_hash": row.get("_row_hash", ""),
        })
    return pd.DataFrame(clean_rows), []


# ----------------------------------------------------------------------
# 3. Main Silver Pipeline Runner
# ----------------------------------------------------------------------

def run_silver_clean(
    run_id: Optional[str] = None,
    bronze_dir: Optional[Path] = None,
    silver_dir: Optional[Path] = None,
) -> Dict[str, Dict]:
    """Execute Silver cleaning, standardizing, and quarantine across all sources.

    Enforces strict reconciliation assertion: rows_in == rows_out + rows_rejected.

    Args:
        run_id: Pipeline execution identifier.
        bronze_dir: Directory where Bronze Parquet files reside.
        silver_dir: Directory where clean Silver Parquet tables will be saved.

    Returns:
        Summary dictionary containing row counts, execution times, and rejection totals.
    """
    active_run_id = run_id or str(uuid.uuid4())
    active_bronze_dir = bronze_dir or BRONZE_DIR
    active_silver_dir = silver_dir or SILVER_DIR

    active_silver_dir.mkdir(parents=True, exist_ok=True)

    pipeline_configs = [
        ("dms_satis", "sales", clean_sales_table),
        ("premium_sozlesme", "contracts", clean_contracts_table),
        ("servis_kayit", "service_records", clean_service_table),
        ("next_islem", "trade_ins", clean_trade_ins_table),
        ("bayi_servis_noktalari", "dealers", clean_dealers_table),
    ]

    all_rejected_records = []
    results_summary = {}

    for bronze_name, silver_name, clean_func in pipeline_configs:
        start_time = time.time()
        started_at = datetime.now().isoformat()
        bronze_path = active_bronze_dir / f"{bronze_name}.parquet"

        if not bronze_path.exists():
            err_msg = f"Bronze source file not found: {bronze_path}"
            log_pipeline_run(
                run_id=active_run_id,
                step=f"clean_{silver_name}",
                layer="silver",
                source=bronze_name,
                rows_in=0,
                rows_out=0,
                rows_rejected=0,
                started_at=started_at,
                duration_s=0.0,
                status="FAILED",
                error=err_msg,
            )
            results_summary[silver_name] = {
                "status": "FAILED",
                "rows_in": 0,
                "rows_out": 0,
                "rows_rejected": 0,
                "rejection_events": 0,
                "duration_s": 0.0,
                "error": err_msg,
            }
            continue

        df_bronze = pd.read_parquet(bronze_path)
        rows_in = len(df_bronze)

        clean_df, table_rejections = clean_func(df_bronze, active_run_id)
        rows_out = len(clean_df)

        # Count DISTINCT bronze rows rejected (using bronze row position identifier)
        rejected_row_ids = {r["source_row_id"] for r in table_rejections}
        rows_rejected = len(rejected_row_ids)
        events_count = len(table_rejections)

        duration_s = time.time() - start_time

        # ------------------------------------------------------------------
        # Reconciliation Assertion: rows_in == rows_out + rows_rejected
        # ------------------------------------------------------------------
        if rows_in != rows_out + rows_rejected:
            delta = rows_in - (rows_out + rows_rejected)
            err_msg = (
                f"Reconciliation assertion failed for {silver_name}: "
                f"rows_in ({rows_in:,}) != rows_out ({rows_out:,}) + rows_rejected ({rows_rejected:,}) "
                f"[Discrepancy: {delta:+d} rows]"
            )
            logger.error(err_msg)
            log_pipeline_run(
                run_id=active_run_id,
                step=f"clean_{silver_name}",
                layer="silver",
                source=bronze_name,
                rows_in=rows_in,
                rows_out=rows_out,
                rows_rejected=rows_rejected,
                started_at=started_at,
                duration_s=duration_s,
                status="FAILED",
                error=err_msg,
            )
            raise AssertionError(err_msg)

        all_rejected_records.extend(table_rejections)

        # Write clean table
        target_parquet = active_silver_dir / f"{silver_name}.parquet"
        clean_df.to_parquet(target_parquet, index=False)

        log_pipeline_run(
            run_id=active_run_id,
            step=f"clean_{silver_name}",
            layer="silver",
            source=bronze_name,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_rejected=rows_rejected,
            started_at=started_at,
            duration_s=duration_s,
            status="SUCCESS",
            error="",
        )

        results_summary[silver_name] = {
            "source_table": bronze_name,
            "status": "SUCCESS",
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_rejected": rows_rejected,
            "rejection_events": events_count,
            "duration_s": duration_s,
            "error": "",
        }

    # Write unified rejected records table
    rejected_df = pd.DataFrame(all_rejected_records, columns=REJECTED_RECORDS_COLUMNS)
    rejected_path = active_silver_dir / "rejected_records.parquet"
    rejected_df.to_parquet(rejected_path, index=False)

    return results_summary


def print_rejection_summary():
    """Read and display a formatted breakdown of rejected records by rule and source."""
    rej_path = SILVER_DIR / "rejected_records.parquet"
    if not rej_path.exists():
        print("No rejected records file found.")
        return

    df_rej = pd.read_parquet(rej_path)
    if df_rej.empty:
        print("No records were rejected in the Silver layer.")
        return

    print("\n" + "=" * 80)
    print("SILVER QUARANTINE: RECONCILIATION SUMMARY (DISTINCT ROWS VS EVENTS)")
    print("=" * 80)

    # Per source table comparison: Distinct Rejected Rows vs Rejection Events
    stats = []
    for src in sorted(df_rej["source_table"].unique()):
        sub = df_rej[df_rej["source_table"] == src]
        distinct_rows = sub["source_row_id"].nunique()
        events = len(sub)
        stats.append({
            "source_table": src,
            "distinct_rejected_rows": distinct_rows,
            "rejection_events": events,
            "multi_rule_rows": events - distinct_rows,
        })
    df_stats = pd.DataFrame(stats)
    print(df_stats.to_string(index=False))

    print("\n" + "-" * 80)
    print("REJECTION EVENTS BREAKDOWN BY RULE CODE:")
    print("-" * 80)

    pivot = df_rej.pivot_table(
        index="rule_code",
        columns="source_table",
        values="source_row_id",
        aggfunc="count",
        fill_value=0,
    )
    pivot["TOTAL_EVENTS"] = pivot.sum(axis=1)

    print(pivot.to_string())
    print("-" * 80)
    total_distinct = df_rej.groupby("source_table")["source_row_id"].nunique().sum()
    print(f"Total Rejection Events: {len(df_rej):,} | Total Distinct Rejected Rows: {total_distinct:,}")
    print("=" * 80)


if __name__ == "__main__":
    print("Executing Silver clean pipeline...")
    outcomes = run_silver_clean()
    for tbl, res in outcomes.items():
        print(
            f"  {tbl:20s}: {res['status']:8s} | In: {res['rows_in']:>6,} | "
            f"Out: {res['rows_out']:>6,} | Rej (distinct): {res['rows_rejected']:>5,} | "
            f"Events: {res['rejection_events']:>5,}"
        )
    print_rejection_summary()
