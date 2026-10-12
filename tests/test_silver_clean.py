"""Unit tests for the Silver cleaning and quarantine stage.

Verifies:
1. Pure function behavior: normalize_name, normalize_phone, parse_date, normalize_vin, normalize_brand.
2. Rejection rules: INVALID_VIN, MISSING_VIN, INVALID_DATE, CONTRACT_END_BEFORE_START, DUPLICATE_KEY.
3. Quality flags: non-critical missing phone numbers stay in clean table with has_valid_phone=False.
4. Schema compliance and metadata preservation across all clean silver tables.
5. Strict reconciliation: rows_in == rows_out + rows_rejected across all silver tables.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from config import REJECTED_RECORDS_COLUMNS, SILVER_DIR
from pipeline.silver_clean import (
    clean_contracts_table,
    clean_sales_table,
    normalize_brand,
    normalize_name,
    normalize_phone,
    normalize_vin,
    parse_date,
    run_silver_clean,
)


# ----------------------------------------------------------------------
# 1. Tests for Pure Helper Functions
# ----------------------------------------------------------------------

def test_normalize_name():
    """Verify name cleaning, Turkish Title Casing, and ASCII-folding."""
    # Turkish lowercase and uppercase casing
    clean, ascii_key = normalize_name("ozan şahin")
    assert clean == "Ozan Şahin"
    assert ascii_key == "ozan sahin"

    clean, ascii_key = normalize_name("OZAN ŞAHİN")
    assert clean == "Ozan Şahin"
    assert ascii_key == "ozan sahin"

    # Excessive whitespace collapsed
    clean, ascii_key = normalize_name("   Ozan    Şahin   ")
    assert clean == "Ozan Şahin"
    assert ascii_key == "ozan sahin"

    # Turkish dotted/dotless I behavior
    clean, ascii_key = normalize_name("İPEK ÖZTÜRK")
    assert clean == "İpek Öztürk"
    assert ascii_key == "ipek ozturk"

    clean, ascii_key = normalize_name("ipek öztürk")
    assert clean == "İpek Öztürk"
    assert ascii_key == "ipek ozturk"

    clean, ascii_key = normalize_name("IŞIK ÇELİK")
    assert clean == "Işık Çelik"
    assert ascii_key == "isik celik"

    clean, ascii_key = normalize_name("ışık çelik")
    assert clean == "Işık Çelik"
    assert ascii_key == "isik celik"

    # Swapped names are NOT reordered here
    clean, ascii_key = normalize_name("Şahin Ozan")
    assert clean == "Şahin Ozan"
    assert ascii_key == "sahin ozan"

    # Empty and None inputs
    assert normalize_name(None) == ("", "")
    assert normalize_name("") == ("", "")
    assert normalize_name("   ") == ("", "")


def test_normalize_phone():
    """Verify phone digit extraction, prefix stripping, and 10-digit mobile rule."""
    # Standard 10-digit mobile
    phone, valid = normalize_phone("5305614226")
    assert valid is True
    assert phone == "5305614226"

    # International +90 prefix stripped
    phone, valid = normalize_phone("+905305614226")
    assert valid is True
    assert phone == "5305614226"

    # Leading 0 stripped
    phone, valid = normalize_phone("05305614226")
    assert valid is True
    assert phone == "5305614226"

    # Formatted with parens and spaces
    phone, valid = normalize_phone("0 (530) 561 42 26")
    assert valid is True
    assert phone == "5305614226"

    # Non-mobile landline (starts with 2 or 3) -> invalid
    phone, valid = normalize_phone("02123456789")
    assert valid is False
    assert phone is None

    # Too short -> invalid
    phone, valid = normalize_phone("530123")
    assert valid is False
    assert phone is None

    # Empty/None -> invalid
    assert normalize_phone(None) == (None, False)
    assert normalize_phone("") == (None, False)


def test_parse_date():
    """Verify multi-format date parsing and rejection of impossible calendar dates."""
    # ISO format
    d, valid = parse_date("2024-08-12")
    assert valid is True
    assert d == date(2024, 8, 12)

    # Dot format (European)
    d, valid = parse_date("12.08.2024")
    assert valid is True
    assert d == date(2024, 8, 12)

    # Slash format (European)
    d, valid = parse_date("12/08/2024")
    assert valid is True
    assert d == date(2024, 8, 12)

    # Impossible date (Feb 30th)
    d, valid = parse_date("2024-02-30")
    assert valid is False
    assert d is None

    # Impossible date (April 31st)
    d, valid = parse_date("31.04.2024")
    assert valid is False
    assert d is None

    # Malformed / empty strings
    assert parse_date("not-a-date") == (None, False)
    assert parse_date("") == (None, False)
    assert parse_date(None) == (None, False)


def test_normalize_vin():
    """Verify VIN standardization, uppercase transformation, and rejection rules."""
    # Standard valid 17-char VIN
    v, err = normalize_vin("WBA527WF0JSRNWCES")
    assert err is None
    assert v == "WBA527WF0JSRNWCES"

    # Lowercase VIN converted to uppercase and validated
    v, err = normalize_vin("wbawyfdd0n09c75fr")
    assert err is None
    assert v == "WBAWYFDD0N09C75FR"

    # Missing VIN (None, empty, whitespace)
    assert normalize_vin(None) == ("", "MISSING_VIN")
    assert normalize_vin("") == ("", "MISSING_VIN")
    assert normalize_vin("   ") == ("", "MISSING_VIN")

    # Invalid length (e.g. 16 characters)
    v, err = normalize_vin("WBA527WF0JSRNWCE")
    assert err == "INVALID_VIN"
    assert v == "WBA527WF0JSRNWCE"

    # Invalid characters (I, O, Q forbidden by ISO 3779)
    v, err = normalize_vin("WBA527WF0ISRNWCES")
    assert err == "INVALID_VIN"
    v, err = normalize_vin("WBA527WF0OSRNWCES")
    assert err == "INVALID_VIN"
    v, err = normalize_vin("WBA527WF0QSRNWCES")
    assert err == "INVALID_VIN"


def test_normalize_brand():
    """Verify canonical brand mapping."""
    assert normalize_brand("bmw") == "BMW"
    assert normalize_brand("BMW ") == "BMW"
    assert normalize_brand("mini") == "MINI"
    assert normalize_brand("MINI ") == "MINI"
    assert normalize_brand("range rover") == "Range Rover"
    assert normalize_brand("RANGE ROVER ") == "Range Rover"
    assert normalize_brand("defender") == "Defender"
    assert normalize_brand("DISCOVERY ") == "Discovery"
    assert normalize_brand("jaguar") == "Jaguar"
    assert normalize_brand("") == ""
    assert normalize_brand(None) == ""


# ----------------------------------------------------------------------
# 2. Tests for Table Cleaning Workflows & Quarantine Rules
# ----------------------------------------------------------------------

def test_clean_sales_table_deduplication_and_flags():
    """Verify deduplication by sale_id, bronze row identifiers, and quality flags."""
    df_sample = pd.DataFrame([
        {
            "sale_id": "S001",
            "musteri_adi": "Ozan Şahin",
            "telefon": "05305614226",
            "sasi_no": "WBA527WF0JSRNWCES",
            "marka": "BMW ",
            "model": "3 Serisi",
            "yakit_tipi": "Benzin",
            "satis_tarihi": "2024-08-12",
            "bayi_kodu": "B01",
            "satis_fiyati_tl": "2500000",
        },
        {
            # Exact duplicate sale_id -> MUST BE REJECTED with DUPLICATE_KEY
            "sale_id": "S001",
            "musteri_adi": "Ozan Şahin",
            "telefon": "05305614226",
            "sasi_no": "WBA527WF0JSRNWCES",
            "marka": "BMW ",
            "model": "3 Serisi",
            "yakit_tipi": "Benzin",
            "satis_tarihi": "2024-08-12",
            "bayi_kodu": "B01",
            "satis_fiyati_tl": "2500000",
        },
        {
            # Missing phone -> STAYS in clean table with has_valid_phone=False
            "sale_id": "S002",
            "musteri_adi": "Elif Çelik",
            "telefon": "",
            "sasi_no": "WBA1NPPP1NL9C69E7",
            "marka": "MINI",
            "model": "Cooper",
            "yakit_tipi": "Elektrik",
            "satis_tarihi": "12.08.2024",
            "bayi_kodu": "B02",
            "satis_fiyati_tl": "1900000",
        },
        {
            # Invalid VIN -> MUST BE REJECTED with INVALID_VIN
            "sale_id": "S003",
            "musteri_adi": "Mehmet Demir",
            "telefon": "05321112233",
            "sasi_no": "SHORTVIN123",
            "marka": "Jaguar",
            "model": "I-Pace",
            "yakit_tipi": "Elektrik",
            "satis_tarihi": "2024-05-10",
            "bayi_kodu": "B01",
            "satis_fiyati_tl": "4000000",
        },
    ])

    clean_df, rejections = clean_sales_table(df_sample, run_id="test-run")

    # Check clean rows: only S001 (first occurrence) and S002
    assert len(clean_df) == 2
    assert set(clean_df["sale_id"]) == {"S001", "S002"}

    # S002 must have has_valid_phone = False
    s002 = clean_df[clean_df["sale_id"] == "S002"].iloc[0]
    assert not s002["has_valid_phone"]
    assert s002["phone_norm"] is None

    # Check rejections
    rej_codes = [r["rule_code"] for r in rejections]
    assert "DUPLICATE_KEY" in rej_codes
    assert "INVALID_VIN" in rej_codes

    # Verify source_row_id uses bronze row position and source_business_key preserves domain key
    dup_event = next(r for r in rejections if r["rule_code"] == "DUPLICATE_KEY")
    assert dup_event["source_row_id"] == "row_1"
    assert dup_event["source_business_key"] == "S001"


def test_clean_contracts_end_before_start():
    """Verify CONTRACT_END_BEFORE_START rejection and row identifiers."""
    df_sample = pd.DataFrame([
        {
            # Valid contract
            "sozlesme_no": "PRM001",
            "musteri_unvani": "Ahmet Yılmaz",
            "iletisim_tel": "05331234567",
            "vin": "WBA527WF0JSRNWCES",
            "sozlesme_tipi": "Kiralama",
            "baslangic": "2024-01-01",
            "bitis": "2026-01-01",
            "aylik_tutar_tl": "45000",
        },
        {
            # Illogical date: end < start -> MUST BE REJECTED with CONTRACT_END_BEFORE_START
            "sozlesme_no": "PRM002",
            "musteri_unvani": "Ayşe Kaya",
            "iletisim_tel": "05421234567",
            "vin": "WBA1NPPP1NL9C69E7",
            "sozlesme_tipi": "Finans",
            "baslangic": "2024-01-01",
            "bitis": "2023-01-01",
            "aylik_tutar_tl": "35000",
        },
    ])

    clean_df, rejections = clean_contracts_table(df_sample, run_id="test-run")

    assert len(clean_df) == 1
    assert clean_df.iloc[0]["contract_id"] == "PRM001"

    assert len(rejections) == 1
    assert rejections[0]["rule_code"] == "CONTRACT_END_BEFORE_START"
    assert rejections[0]["source_row_id"] == "row_1"
    assert rejections[0]["source_business_key"] == "PRM002"


def test_run_silver_clean_end_to_end():
    """Verify execution of the full Silver cleaning pipeline and output files."""
    results = run_silver_clean(run_id="test-silver-run")

    # 5 tables cleaned
    expected_tables = ["sales", "contracts", "service_records", "trade_ins", "dealers"]
    for tbl in expected_tables:
        assert tbl in results
        assert results[tbl]["status"] == "SUCCESS"
        assert (SILVER_DIR / f"{tbl}.parquet").exists()

    # Rejection table created and structured properly
    rej_file = SILVER_DIR / "rejected_records.parquet"
    assert rej_file.exists()
    df_rej = pd.read_parquet(rej_file)

    for col in REJECTED_RECORDS_COLUMNS:
        assert col in df_rej.columns

    # Verify that all 5 required rejection codes are present across the dataset
    observed_codes = set(df_rej["rule_code"].unique())
    expected_codes = {
        "INVALID_VIN",
        "MISSING_VIN",
        "CONTRACT_END_BEFORE_START",
        "DUPLICATE_KEY",
    }
    assert expected_codes.issubset(observed_codes)


def test_reconciliation_exact_for_all_silver_tables():
    """Rule 3: Verify exact reconciliation: rows_in == rows_out + rows_rejected for every silver table."""
    results = run_silver_clean(run_id="test-reconcile-run")

    for tbl_name, res in results.items():
        rows_in = res["rows_in"]
        rows_out = res["rows_out"]
        rows_rej = res["rows_rejected"]
        assert rows_in == rows_out + rows_rej, (
            f"Reconciliation failure in {tbl_name}: {rows_in} != {rows_out} + {rows_rej}"
        )

    # Specific assertions for sales
    sales_res = results["sales"]
    assert sales_res["rows_in"] == 2732
    assert sales_res["rows_out"] == 2554
    assert sales_res["rows_rejected"] == 178  # Exact expected distinct rejected rows
    assert sales_res["rejection_events"] == 181  # 3 rows had multiple rejection codes
