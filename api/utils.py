"""Utility functions and safe serialization response class for FastAPI.

Architectural Rationale:
- RFC 8259 JSON Conformance: Standard JSON prohibits NaN, Infinity, and NaT values.
  `SafeJSONResponse` recursively normalizes NaN/NaT to null and casts NumPy types to native
  Python scalars, ensuring client applications and browsers never crash.
- UTF-8 Turkish Character Preservation: Enforces charset=utf-8 with ensure_ascii=False
  so Turkish characters (İ, ı, ş, ğ, ç, ö, ü) are preserved faithfully across Windows environments.
- Zero-PII Guarantee: Strictly strips unmasked customer name and phone columns
  (display_name, customer_name_clean, phone_norm, etc.) before serving any Silver table.
"""

import json
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd
from fastapi.responses import Response

# Unmasked PII attributes that must NEVER be exposed via API endpoints
PII_COLUMNS: Set[str] = {
    "display_name",
    "customer_name_clean",
    "customer_name_ascii",
    "phone_norm",
    "phone_raw",
    "_row_hash",
}

# Regex matching raw 10-digit Turkish mobile numbers (e.g., 5305614226 or 05305614226)
RAW_PHONE_REGEX = re.compile(r"\b0?5\d{9}\b")


def sanitize_value(val: Any) -> Any:
    """Recursively convert NaN, NaT, Inf, and NumPy data types into JSON-safe Python primitives.

    Args:
        val: Any object, primitive, collection, or NumPy/Pandas scalar.

    Returns:
        JSON-compliant primitive where NaNs/NaTs are None and NumPy types are native.
    """
    if val is None:
        return None

    # Check Pandas NaN, NaT, pd.NA
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    # Floating point numbers and numpy floats
    if isinstance(val, (float, np.floating)):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val)

    # Booleans and numpy bools FIRST (in Python, bool is a subclass of int)
    if isinstance(val, (bool, np.bool_)):
        return bool(val)

    # Integers and numpy integers
    if isinstance(val, (int, np.integer)):
        return int(val)

    # Dates and timestamps
    if isinstance(val, (datetime, date, pd.Timestamp)):
        return val.isoformat()

    # Lists, tuples, numpy arrays, and pandas Series
    if isinstance(val, (list, tuple, np.ndarray, pd.Series)):
        return [sanitize_value(x) for x in val]

    # Dictionaries
    if isinstance(val, dict):
        return {str(k): sanitize_value(v) for k, v in val.items()}

    return val


class SafeJSONResponse(Response):
    """FastAPI Response subclass that strictly prohibits NaN and ensures UTF-8 Turkish encoding."""

    media_type = "application/json; charset=utf-8"

    def render(self, content: Any) -> bytes:
        """Render Python data structure into UTF-8 JSON bytes with allow_nan=False."""
        sanitized = sanitize_value(content)
        encoded = json.dumps(
            sanitized,
            ensure_ascii=False,
            allow_nan=False,  # Crashes if any NaN escapes sanitizer, enforcing strict validity
            indent=None,
        ).encode("utf-8")
        return encoded


def read_parquet_safe(file_path: Path) -> pd.DataFrame:
    """Read a Parquet file safely from disk.

    Args:
        file_path: Absolute or relative Path to Parquet file.

    Returns:
        pd.DataFrame containing the table data.

    Raises:
        FileNotFoundError: If the specified Parquet file does not exist.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Requested dataset does not exist at: {file_path}")
    return pd.read_parquet(file_path)


def sanitize_silver_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Strip all unmasked customer PII columns from a Silver dataset before JSON serialization.

    Args:
        df: Input Silver DataFrame.

    Returns:
        DataFrame with unmasked name and phone columns removed.
    """
    cols_to_drop = [c for c in df.columns if c in PII_COLUMNS]
    if cols_to_drop:
        return df.drop(columns=cols_to_drop)
    return df
