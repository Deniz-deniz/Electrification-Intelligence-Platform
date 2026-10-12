"""Profiling script for raw CSV files in data/raw.

Architectural Rationale:
- Data Profiling as First Step: Before designing Silver ingestion and cleaning rules,
  we must quantitatively audit the exact distribution of noise, anomalies, and structural
  inconsistencies deliberately injected into the raw source data.
- Transparency: Quantifies null rates, cardinality, format distributions (dates, phones, VINs),
  and domain constraint violations (duplicate keys, date inversions).
- Output: Produces data/meta/raw_profile.md and prints a concise terminal report.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from config import META_DIR, RAW_DIR


def parse_flexible_date(val: str):
    """Safely parse mixed date formats (YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY)."""
    if pd.isna(val) or not str(val).strip():
        return None
    val_str = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(val_str, fmt).date()
        except ValueError:
            continue
    return None


def detect_date_format(val: str) -> str:
    """Classify the date string representation into standard format groups."""
    if pd.isna(val) or not str(val).strip():
        return "empty/null"
    val_str = str(val).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val_str):
        return "YYYY-MM-DD (ISO)"
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", val_str):
        return "DD.MM.YYYY (Dot)"
    if re.match(r"^\d{2}/\d{2}/\d{4}$", val_str):
        return "DD/MM/YYYY (Slash)"
    return "Other / Malformed"


def detect_phone_format(val: str) -> str:
    """Classify phone representation into pattern groups."""
    if pd.isna(val) or not str(val).strip():
        return "empty/null"
    s = str(val).strip()
    if s.startswith("+90"):
        return "+90 (International)"
    if s.startswith("0 (") or " " in s:
        return "Formatted with spaces/parens"
    if s.startswith("0") and len(s) == 11 and s.isdigit():
        return "11-digit leading 0 (05XXXXXXXXX)"
    if len(s) == 10 and s.isdigit():
        return "10-digit clean (5XXXXXXXXX)"
    return f"Other (len={len(s)})"


def analyze_vins(series: pd.Series) -> Dict[str, int]:
    """Inspect VIN format anomalies: empty, length != 17, lowercase characters."""
    total = len(series)
    empty_cnt = 0
    not_17_cnt = 0
    lowercase_cnt = 0
    valid_format_cnt = 0

    for raw in series:
        if pd.isna(raw) or not str(raw).strip():
            empty_cnt += 1
            continue
        v = str(raw).strip()
        has_lower = any(c.islower() for c in v)
        if has_lower:
            lowercase_cnt += 1
        if len(v) != 17:
            not_17_cnt += 1
        if len(v) == 17 and not has_lower and v.isalnum():
            valid_format_cnt += 1

    return {
        "total": total,
        "empty": empty_cnt,
        "length_not_17": not_17_cnt,
        "contains_lowercase": lowercase_cnt,
        "strictly_valid_format": valid_format_cnt,
    }


def analyze_brand_variants(series: pd.Series) -> Dict[str, int]:
    """Inspect brand text variations including casing and whitespace."""
    counts = {}
    for raw in series.dropna():
        s = str(raw)
        counts[s] = counts.get(s, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))


def profile_dms_satis(df: pd.DataFrame) -> Dict:
    """Profile DMS Sales data quality anomalies."""
    dup_sales = df[df.duplicated(subset=["sale_id"], keep=False)]
    dup_count = len(df) - df["sale_id"].nunique()

    # Date formats in satis_tarihi
    date_formats = df["satis_tarihi"].map(detect_date_format).value_counts().to_dict()

    # Phone formats in telefon
    phone_formats = df["telefon"].map(detect_phone_format).value_counts().to_dict()

    # VIN issues in sasi_no
    vin_stats = analyze_vins(df["sasi_no"])

    # Brand issues
    brand_stats = analyze_brand_variants(df["marka"])

    return {
        "row_count": len(df),
        "duplicate_sales_rows": dup_count,
        "date_formats": date_formats,
        "phone_formats": phone_formats,
        "vin_stats": vin_stats,
        "brand_variants": brand_stats,
    }


def profile_premium_sozlesme(df: pd.DataFrame) -> Dict:
    """Profile Premium contracts for inverted dates and formatting issues."""
    date_start_formats = df["baslangic"].map(detect_date_format).value_counts().to_dict()
    date_end_formats = df["bitis"].map(detect_date_format).value_counts().to_dict()
    phone_formats = df["iletisim_tel"].map(detect_phone_format).value_counts().to_dict()
    vin_stats = analyze_vins(df["vin"])

    # Check temporal inversion: bitis < baslangic
    inverted_dates_count = 0
    for _, row in df.iterrows():
        d_start = parse_flexible_date(row["baslangic"])
        d_end = parse_flexible_date(row["bitis"])
        if d_start and d_end and d_end < d_start:
            inverted_dates_count += 1

    return {
        "row_count": len(df),
        "inverted_date_contracts": inverted_dates_count,
        "date_start_formats": date_start_formats,
        "date_end_formats": date_end_formats,
        "phone_formats": phone_formats,
        "vin_stats": vin_stats,
    }


def profile_servis_kayit(df: pd.DataFrame) -> Dict:
    """Profile Service records."""
    date_formats = df["servis_tarihi"].map(detect_date_format).value_counts().to_dict()
    vin_stats = analyze_vins(df["sasi"])
    has_phone_col = "telefon" in df.columns or "tel" in df.columns

    return {
        "row_count": len(df),
        "has_phone_column": has_phone_col,
        "date_formats": date_formats,
        "vin_stats": vin_stats,
    }


def profile_next_islem(df: pd.DataFrame) -> Dict:
    """Profile Borusan Next trade-in transactions."""
    date_formats = df["islem_tarihi"].map(detect_date_format).value_counts().to_dict()
    vin_stats = analyze_vins(df["vin_no"])

    return {
        "row_count": len(df),
        "date_formats": date_formats,
        "vin_stats": vin_stats,
    }


def profile_bayi_servis(df: pd.DataFrame) -> Dict:
    """Profile Dealer & Service reference points."""
    return {
        "row_count": len(df),
        "dealer_types": df["point_type"].value_counts().to_dict() if "point_type" in df.columns else {},
        "cities": df["city"].nunique() if "city" in df.columns else 0,
    }


def generate_markdown_report(all_stats: Dict[str, Dict], general_profiles: Dict[str, pd.DataFrame]) -> str:
    """Format full profiling metrics into a structured markdown document."""
    lines = [
        "# Raw Data Quality Profiling Report",
        "",
        "> Generated by `pipeline.profile_raw`.",
        "> Audits raw source datasets in `data/raw/` prior to Bronze ingestion and Silver cleansing.",
        "",
        "## 1. Executive Summary: Table Inventory & Counts",
        "",
        "| Source File | Observed Rows | Domain Model Expected | Status | Primary Quality Challenges |",
        "|---|---:|---:|:---:|---|",
    ]

    expected_counts = {
        "dms_satis.csv": 2732,
        "premium_sozlesme.csv": 1445,
        "servis_kayit.csv": 8690,
        "next_islem.csv": 362,
        "bayi_servis_noktalari.csv": 18,
    }

    for fname, exp in expected_counts.items():
        obs = all_stats[fname]["row_count"]
        status = "MATCH" if obs == exp else f"DIFF ({obs} vs {exp})"
        challenges = []
        if fname == "dms_satis.csv":
            challenges.append(f"{all_stats[fname]['duplicate_sales_rows']} duplicate sale_ids; mixed dates/phones; malformed VINs")
        elif fname == "premium_sozlesme.csv":
            challenges.append(f"{all_stats[fname]['inverted_date_contracts']} inverted contracts (end < start); mixed date formats")
        elif fname == "servis_kayit.csv":
            challenges.append("No phone column; malformed/missing VINs; name noise across 8k+ visits")
        elif fname == "next_islem.csv":
            challenges.append("Name inversion/noise; trade-in appraisal types; malformed VINs")
        elif fname == "bayi_servis_noktalari.csv":
            challenges.append("Clean reference dimension")

        lines.append(f"| `{fname}` | {obs:,} | {exp:,} | {status} | {'; '.join(challenges)} |")

    lines.extend([
        "",
        "---",
        "",
        "## 2. Injected Data Quality Anomalies Breakdown",
        "",
        "### A. DMS Sales (`dms_satis.csv`)",
        f"- **Total Rows:** {all_stats['dms_satis.csv']['row_count']:,}",
        f"- **Duplicate `sale_id` Records:** {all_stats['dms_satis.csv']['duplicate_sales_rows']} rows",
        "- **Date Format Distribution (`satis_tarihi`):**",
    ])
    for fmt, cnt in all_stats["dms_satis.csv"]["date_formats"].items():
        lines.append(f"  - `{fmt}`: {cnt:,} ({cnt/all_stats['dms_satis.csv']['row_count']*100:.1f}%)")

    lines.append("- **Phone Format Distribution (`telefon`):**")
    for fmt, cnt in all_stats["dms_satis.csv"]["phone_formats"].items():
        lines.append(f"  - `{fmt}`: {cnt:,} ({cnt/all_stats['dms_satis.csv']['row_count']*100:.1f}%)")

    lines.append("- **VIN Format Health (`sasi_no`):**")
    for k, v in all_stats["dms_satis.csv"]["vin_stats"].items():
        lines.append(f"  - `{k}`: {v:,}")

    lines.append("- **Brand Spelling Variants (`marka`):**")
    for b, c in all_stats["dms_satis.csv"]["brand_variants"].items():
        lines.append(f"  - `{repr(b)}`: {c:,}")

    lines.extend([
        "",
        "### B. Premium Contracts (`premium_sozlesme.csv`)",
        f"- **Total Rows:** {all_stats['premium_sozlesme.csv']['row_count']:,}",
        f"- **Inverted Date Contracts (`bitis < baslangic`):** {all_stats['premium_sozlesme.csv']['inverted_date_contracts']} records (violates business logic)",
        "- **Start Date Formats (`baslangic`):**",
    ])
    for fmt, cnt in all_stats["premium_sozlesme.csv"]["date_start_formats"].items():
        lines.append(f"  - `{fmt}`: {cnt:,} ({cnt/all_stats['premium_sozlesme.csv']['row_count']*100:.1f}%)")

    lines.append("- **Phone Format Distribution (`iletisim_tel`):**")
    for fmt, cnt in all_stats["premium_sozlesme.csv"]["phone_formats"].items():
        lines.append(f"  - `{fmt}`: {cnt:,} ({cnt/all_stats['premium_sozlesme.csv']['row_count']*100:.1f}%)")

    lines.append("- **VIN Format Health (`vin`):**")
    for k, v in all_stats["premium_sozlesme.csv"]["vin_stats"].items():
        lines.append(f"  - `{k}`: {v:,}")

    lines.extend([
        "",
        "### C. Workshop Service Records (`servis_kayit.csv`)",
        f"- **Total Rows:** {all_stats['servis_kayit.csv']['row_count']:,}",
        f"- **Phone Column Present:** `{all_stats['servis_kayit.csv']['has_phone_column']}` (Confirms: NO phone column, only customer name `sahip_adi`)",
        "- **Date Format Distribution (`servis_tarihi`):**",
    ])
    for fmt, cnt in all_stats["servis_kayit.csv"]["date_formats"].items():
        lines.append(f"  - `{fmt}`: {cnt:,} ({cnt/all_stats['servis_kayit.csv']['row_count']*100:.1f}%)")

    lines.append("- **VIN Format Health (`sasi`):**")
    for k, v in all_stats["servis_kayit.csv"]["vin_stats"].items():
        lines.append(f"  - `{k}`: {v:,}")

    lines.extend([
        "",
        "### D. Borusan Next Trade-In Records (`next_islem.csv`)",
        f"- **Total Rows:** {all_stats['next_islem.csv']['row_count']:,}",
        "- **Date Format Distribution (`islem_tarihi`):**",
    ])
    for fmt, cnt in all_stats["next_islem.csv"]["date_formats"].items():
        lines.append(f"  - `{fmt}`: {cnt:,} ({cnt/all_stats['next_islem.csv']['row_count']*100:.1f}%)")

    lines.append("- **VIN Format Health (`vin_no`):**")
    for k, v in all_stats["next_islem.csv"]["vin_stats"].items():
        lines.append(f"  - `{k}`: {v:,}")

    lines.extend([
        "",
        "---",
        "",
        "## 3. Column-Level Statistics (Null Counts & Distinct Cardinality)",
        "",
    ])

    for fname, prof in general_profiles.items():
        lines.extend([
            f"### `{fname}`",
            "",
            "| Column Name | Inferred Type | Null Count | Null % | Distinct Values | Sample Values |",
            "|---|---|---:|---:|---:|---|",
        ])
        for _, row in prof.iterrows():
            samples = ", ".join([f"`{str(x)[:20]}`" for x in row["sample_values"][:3]])
            lines.append(
                f"| `{row['column']}` | `{row['dtype']}` | {row['null_count']:,} | "
                f"{row['null_pct']:.2f}% | {row['distinct_count']:,} | {samples} |"
            )
        lines.append("")

    return "\n".join(lines)


def run_profiling() -> Tuple[Dict, str]:
    """Execute profiling for all CSV files in data/raw."""
    files = [
        "dms_satis.csv",
        "premium_sozlesme.csv",
        "servis_kayit.csv",
        "next_islem.csv",
        "bayi_servis_noktalari.csv",
    ]

    all_stats = {}
    general_profiles = {}

    for fname in files:
        fpath = RAW_DIR / fname
        if not fpath.exists():
            raise FileNotFoundError(f"Source file not found: {fpath}")

        df = pd.read_csv(fpath)

        # General profiling per column
        col_records = []
        for col in df.columns:
            null_cnt = int(df[col].isna().sum())
            null_pct = (null_cnt / len(df)) * 100 if len(df) > 0 else 0.0
            distinct_cnt = int(df[col].nunique(dropna=True))
            samples = df[col].dropna().unique()[:3].tolist()
            col_records.append({
                "column": col,
                "dtype": str(df[col].dtype),
                "null_count": null_cnt,
                "null_pct": null_pct,
                "distinct_count": distinct_cnt,
                "sample_values": samples,
            })
        general_profiles[fname] = pd.DataFrame(col_records)

        # Specialized anomaly detection
        if fname == "dms_satis.csv":
            all_stats[fname] = profile_dms_satis(df)
        elif fname == "premium_sozlesme.csv":
            all_stats[fname] = profile_premium_sozlesme(df)
        elif fname == "servis_kayit.csv":
            all_stats[fname] = profile_servis_kayit(df)
        elif fname == "next_islem.csv":
            all_stats[fname] = profile_next_islem(df)
        elif fname == "bayi_servis_noktalari.csv":
            all_stats[fname] = profile_bayi_servis(df)

    # Generate Markdown Report
    report_md = generate_markdown_report(all_stats, general_profiles)

    # Save to data/meta/raw_profile.md
    META_DIR.mkdir(parents=True, exist_ok=True)
    out_file = META_DIR / "raw_profile.md"
    out_file.write_text(report_md, encoding="utf-8")

    return all_stats, report_md


def main():
    print("=" * 70)
    print("RUNNING RAW DATA QUALITY PROFILER (`pipeline.profile_raw`)")
    print("=" * 70)

    stats, report = run_profiling()

    expected = {
        "dms_satis.csv": 2732,
        "premium_sozlesme.csv": 1445,
        "servis_kayit.csv": 8690,
        "next_islem.csv": 362,
        "bayi_servis_noktalari.csv": 18,
    }

    print("\n[1] ROW COUNT VERIFICATION AGAINST 01_domain_model.md:")
    all_match = True
    for fname, exp in expected.items():
        obs = stats[fname]["row_count"]
        match_str = "MATCH" if obs == exp else f"MISMATCH (Observed: {obs}, Expected: {exp})"
        print(f"  - {fname:27s}: {obs:>6,} rows | Expected: {exp:>6,} | [{match_str}]")
        if obs != exp:
            all_match = False

    if all_match:
        print("  -> ALL ROW COUNTS MATCH EXPECTED SPECIFICATIONS EXACTLY.")
    else:
        print("  -> WARNING: One or more row counts differ from 01_domain_model.md!")

    print("\n[2] INJECTED ANOMALIES SUMMARY:")
    dms = stats["dms_satis.csv"]
    print(f"  - DMS Sales Duplicate Rows      : {dms['duplicate_sales_rows']:,}")
    print(f"  - DMS Sales VIN Anomaly Count    : Not 17 chars: {dms['vin_stats']['length_not_17']}, Lowercase: {dms['vin_stats']['contains_lowercase']}, Empty: {dms['vin_stats']['empty']}")
    print(f"  - DMS Sales Brand Variants       : {list(dms['brand_variants'].keys())}")
    print(f"  - DMS Sales Phone Format Groups  : {dms['phone_formats']}")
    print(f"  - DMS Sales Date Format Groups   : {dms['date_formats']}")

    prm = stats["premium_sozlesme.csv"]
    print(f"  - Premium Inverted Contracts     : {prm['inverted_date_contracts']:,} (bitis < baslangic)")
    print(f"  - Premium Date Start Formats     : {prm['date_start_formats']}")
    print(f"  - Premium Phone Format Groups    : {prm['phone_formats']}")

    srv = stats["servis_kayit.csv"]
    print(f"  - Service Has Phone Column       : {srv['has_phone_column']} (Owner name only, no phone)")
    print(f"  - Service VIN Anomaly Count      : Not 17 chars: {srv['vin_stats']['length_not_17']}, Lowercase: {srv['vin_stats']['contains_lowercase']}, Empty: {srv['vin_stats']['empty']}")

    nxt = stats["next_islem.csv"]
    print(f"  - Next Trade-Ins Date Formats    : {nxt['date_formats']}")
    print(f"  - Next Trade-Ins VIN Anomalies   : {nxt['vin_stats']}")

    print(f"\n[3] Profile report successfully saved to: {META_DIR / 'raw_profile.md'}")
    print("=" * 70)


if __name__ == "__main__":
    main()
