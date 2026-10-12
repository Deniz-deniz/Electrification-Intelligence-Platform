"""Power BI semantic model CSV export engine.

Architectural Rationale:
- UTF-8 with BOM Encoding (utf-8-sig): Guarantees that Power BI Desktop on Windows
  automatically detects and renders Turkish characters (İ, ı, ş, ğ, ç, ö, ü) without
  encoding glitches or requiring manual code-page configuration.
- Strict Zero-PII Policy: Under no circumstance are raw phone numbers (phone_raw, phone_norm)
  or unmasked customer names (display_name, customer_name_clean) exported. Only masked
  attributes (name_masked, phone_masked) are permitted in the dimensional model.
- Star-Schema Model Readiness: Exports Gold analytical tables (facts) and Silver entity-resolved
  dimensions (customers_masked, vehicles, dealers) into data/export/powerbi/.
- Automated Audit Trail: Logs export operations directly into data/meta/pipeline_runs.parquet.
"""

import logging
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config import GOLD_DIR, POWERBI_EXPORT_DIR, REFERENCE_DATE, SILVER_DIR
from pipeline.logging_utils import log_pipeline_run

logger = logging.getLogger(__name__)

# Strict PII detection patterns
FORBIDDEN_PII_COLUMNS = {
    "display_name",
    "customer_name_clean",
    "customer_name_ascii",
    "phone_norm",
    "phone_raw",
}
RAW_PHONE_REGEX = re.compile(r"\b0?5\d{9}\b")


def _sanitize_list_or_array(val: Any, delimiter: str = " | ") -> str:
    """Format array or list attributes cleanly for tabular CSV representation."""
    if isinstance(val, (list, np.ndarray)):
        return delimiter.join(str(item) for item in val if item is not None)
    if pd.isna(val):
        return ""
    return str(val)


def export_customers_masked(export_dir: Path = POWERBI_EXPORT_DIR) -> Path:
    """Export silver customers dimension with unmasked PII stripped.

    Args:
        export_dir: Target directory for Power BI CSV exports.

    Returns:
        Path to the exported CSV file.

    Raises:
        FileNotFoundError: If silver customers.parquet is missing.
        ValueError: If forbidden PII columns or raw phone numbers are detected.
    """
    src_file = SILVER_DIR / "customers.parquet"
    if not src_file.exists():
        raise FileNotFoundError(f"Silver customers table not found at: {src_file}")

    df = pd.read_parquet(src_file)

    # Strictly strip all unmasked PII attributes and internal hashes
    cols_to_drop = [c for c in df.columns if c in FORBIDDEN_PII_COLUMNS or c.startswith("_")]
    df_clean = df.drop(columns=cols_to_drop, errors="ignore").copy()

    # Format list/array column (source_systems)
    if "source_systems" in df_clean.columns:
        df_clean["source_systems"] = df_clean["source_systems"].apply(
            lambda x: _sanitize_list_or_array(x, delimiter=", ")
        )

    # Verification: assert NO forbidden column remains
    for col in df_clean.columns:
        if col in FORBIDDEN_PII_COLUMNS:
            raise ValueError(f"CRITICAL PRIVACY VIOLATION: Forbidden column '{col}' detected in export!")

    out_file = export_dir / "customers_masked.csv"
    df_clean.to_csv(out_file, encoding="utf-8-sig", index=False)
    return out_file


def export_vehicles(export_dir: Path = POWERBI_EXPORT_DIR) -> Path:
    """Export silver vehicles dimension for vehicle and fuel-type relationships."""
    src_file = SILVER_DIR / "vehicles.parquet"
    if not src_file.exists():
        raise FileNotFoundError(f"Silver vehicles table not found at: {src_file}")

    df = pd.read_parquet(src_file)

    # Standardize foreign key naming for Power BI star-schema
    df_clean = df.copy()
    if "owner_customer_id" in df_clean.columns and "customer_id" not in df_clean.columns:
        df_clean["customer_id"] = df_clean["owner_customer_id"]

    cols_to_drop = [c for c in df_clean.columns if c.startswith("_")]
    df_clean = df_clean.drop(columns=cols_to_drop, errors="ignore")

    out_file = export_dir / "vehicles.csv"
    df_clean.to_csv(out_file, encoding="utf-8-sig", index=False)
    return out_file


def export_dealers(export_dir: Path = POWERBI_EXPORT_DIR) -> Path:
    """Export silver dealers dimension table."""
    src_file = SILVER_DIR / "dealers.parquet"
    if not src_file.exists():
        raise FileNotFoundError(f"Silver dealers table not found at: {src_file}")

    df = pd.read_parquet(src_file)
    cols_to_drop = [c for c in df.columns if c.startswith("_")]
    df_clean = df.drop(columns=cols_to_drop, errors="ignore").copy()

    out_file = export_dir / "dealers.csv"
    df_clean.to_csv(out_file, encoding="utf-8-sig", index=False)
    return out_file


def export_gold_table(table_name: str, export_dir: Path = POWERBI_EXPORT_DIR) -> Path:
    """Export a single Gold lakehouse table to UTF-8 BOM CSV.

    Args:
        table_name: Base name of the Gold table (e.g. 'ev_opportunities').
        export_dir: Target directory for Power BI CSV exports.

    Returns:
        Path to the exported CSV file.
    """
    src_file = GOLD_DIR / f"{table_name}.parquet"
    if not src_file.exists():
        raise FileNotFoundError(f"Gold table '{table_name}' not found at: {src_file}")

    df = pd.read_parquet(src_file)
    df_clean = df.copy()

    # Format list/array columns (e.g. score_reasons in ev_opportunities)
    for col in df_clean.columns:
        if col == "score_reasons" or df_clean[col].apply(lambda x: isinstance(x, (list, np.ndarray))).any():
            df_clean[col] = df_clean[col].apply(lambda x: _sanitize_list_or_array(x, delimiter=" | "))

    out_file = export_dir / f"{table_name}.csv"
    df_clean.to_csv(out_file, encoding="utf-8-sig", index=False)
    return out_file


def validate_exported_csv(file_path: Path) -> None:
    """Verify that an exported CSV has a valid UTF-8 BOM and contains zero raw PII.

    Args:
        file_path: Absolute path to the CSV file to validate.

    Raises:
        AssertionError: If UTF-8 BOM is missing, or raw PII / forbidden columns are present.
    """
    assert file_path.exists(), f"File does not exist: {file_path}"
    assert file_path.stat().st_size > 0, f"File is empty: {file_path}"

    with open(file_path, "rb") as f:
        bom = f.read(3)
        assert bom == b"\xef\xbb\xbf", f"File '{file_path.name}' is missing UTF-8 BOM prefix (expected \\xef\\xbb\\xbf)!"

    # Read and scan header and content for PII
    df = pd.read_csv(file_path, encoding="utf-8-sig")

    # Check header
    for col in df.columns:
        assert col not in FORBIDDEN_PII_COLUMNS, (
            f"PRIVACY VIOLATION: Forbidden column '{col}' found in '{file_path.name}'!"
        )

    # Check raw phone numbers in text representation
    with open(file_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
        phone_matches = RAW_PHONE_REGEX.findall(content)
        assert len(phone_matches) == 0, (
            f"PRIVACY VIOLATION: Raw 10-digit phone number found in '{file_path.name}': {phone_matches[:3]}"
        )


def export_all_for_powerbi(export_dir: Path = POWERBI_EXPORT_DIR) -> Dict[str, Any]:
    """Execute complete Power BI export batch across dimensions and Gold marts.

    Args:
        export_dir: Destination folder path for CSV output.

    Returns:
        Summary dictionary with counts, file sizes, and status.
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    start_time = time.time()

    export_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Exporting Power BI datasets to: %s", export_dir)

    exported_files: List[Path] = []
    total_rows = 0

    try:
        # 1. Export Dimensions (Strictly Masked)
        f_cust = export_customers_masked(export_dir)
        exported_files.append(f_cust)
        total_rows += len(pd.read_csv(f_cust, encoding="utf-8-sig"))

        f_veh = export_vehicles(export_dir)
        exported_files.append(f_veh)
        total_rows += len(pd.read_csv(f_veh, encoding="utf-8-sig"))

        f_dlr = export_dealers(export_dir)
        exported_files.append(f_dlr)
        total_rows += len(pd.read_csv(f_dlr, encoding="utf-8-sig"))

        # 2. Export Gold Analytical Tables
        gold_tables = [
            "ev_opportunities",
            "ev_share_by_dealer",
            "service_cost_comparison",
            "trade_in_value_ratio",
            "data_quality_scores",
            "referential_integrity_checks",
            "fuel_type_verification_queue",
        ]

        for gt in gold_tables:
            f_gt = export_gold_table(gt, export_dir)
            exported_files.append(f_gt)
            total_rows += len(pd.read_csv(f_gt, encoding="utf-8-sig"))

        # 3. Privacy & Encoding Validation
        for f in exported_files:
            validate_exported_csv(f)

        duration_s = round(time.time() - start_time, 3)

        # 4. Audit Trail Logging
        log_pipeline_run(
            run_id=run_id,
            step="export_for_powerbi",
            layer="export",
            source="silver_and_gold",
            rows_in=total_rows,
            rows_out=total_rows,
            rows_rejected=0,
            started_at=started_at,
            duration_s=duration_s,
            status="SUCCESS",
            error="",
        )

        return {
            "status": "SUCCESS",
            "run_id": run_id,
            "export_dir": str(export_dir),
            "total_files": len(exported_files),
            "total_rows": total_rows,
            "duration_s": duration_s,
            "files": [f.name for f in exported_files],
        }

    except Exception as exc:
        duration_s = round(time.time() - start_time, 3)
        log_pipeline_run(
            run_id=run_id,
            step="export_for_powerbi",
            layer="export",
            source="silver_and_gold",
            rows_in=total_rows,
            rows_out=0,
            rows_rejected=0,
            started_at=started_at,
            duration_s=duration_s,
            status="FAILED",
            error=str(exc),
        )
        logger.error("Power BI export failed: %s", exc)
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print("\n" + "=" * 80)
    print("POWER BI DATASET EXPORT ENGINE")
    print("=" * 80)
    res = export_all_for_powerbi()
    print(f"\nStatus       : {res['status']}")
    print(f"Export Dir   : {res['export_dir']}")
    print(f"Total Files  : {res['total_files']}")
    print(f"Total Rows   : {res['total_rows']:,}")
    print(f"Duration     : {res['duration_s']}s")
    print("-" * 80)
    print("Exported Datasets:")
    for fn in res["files"]:
        print(f"  • {fn}")
    print("=" * 80 + "\n")
