"""Customer Entity Resolution (Master Data Management) pipeline.

Architectural Rationale:
- Heuristic Record Linkage without ML: Disjoint source systems lack a shared customer identifier.
  Entity resolution builds a unified Golden Customer record (dim_customers) and vehicle fleet (dim_vehicles)
  using transparent, deterministic graph components (Union-Find).
- Rule-Based Hierarchy:
  1. Strong Link (Vehicle Anchor): Records sharing a valid 17-char VIN belong to the same vehicle / owner.
  2. Strong Link (Contact Anchor): Records sharing a valid normalized 10-digit mobile phone AND
     token-sorted ASCII name belong to the same customer (handles name inversion, Turkish casing, typos).
  3. Weaker Link: Token-sorted ASCII name with at least one shared contact/vehicle anchor.
  4. Non-Negotiable Constraint: Never merge on name alone. Common names without shared phone or VIN
     remain separated to prevent catastrophic false merges (cluster contamination).
- KVKK Compliance: Output Golden Records strictly generate masked PII tokens for UI and API tiers.
- Ground Truth Benchmarking: Evaluates clusters against data/ground_truth/vin_to_customer.csv to calculate
  pairwise precision, recall, and vehicle ownership accuracy.
"""

import json
import logging
import re
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from config import GROUND_TRUTH_DIR, META_DIR, SILVER_DIR
from pipeline.logging_utils import log_pipeline_run

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# 1. PII Masking Utilities
# ----------------------------------------------------------------------

def mask_name(name: Optional[str]) -> str:
    """Mask full name according to KVKK privacy rules (e.g., 'Ozan Şahin' -> 'O*** Ş****').

    Why:
    Preserves the initial letter of each name token while replacing the remainder
    with asterisks so that identifiable customer names never leak into reports or APIs.
    """
    if not name or not str(name).strip():
        return ""
    words = str(name).strip().split()
    masked = []
    for w in words:
        if len(w) <= 1:
            masked.append(w + "***")
        else:
            masked.append(w[0] + "*" * (len(w) - 1))
    return " ".join(masked)


def mask_phone(phone: Optional[str]) -> str:
    """Mask 10-digit Turkish mobile number (e.g., '5305614226' -> '530***4226').

    Why:
    Retains the 3-digit operator code and trailing 4 digits for verification while
    masking the middle 3 digits to safeguard customer privacy.
    """
    if not phone or len(str(phone)) != 10:
        return ""
    s = str(phone)
    return f"{s[:3]}***{s[6:]}"


def get_token_sorted_key(ascii_name: str) -> Tuple[str, ...]:
    """Tokenize and sort ASCII-folded name to match swapped first/last names.

    Example:
    'ozan sahin' -> ('ozan', 'sahin')
    'sahin ozan' -> ('ozan', 'sahin')
    """
    if not ascii_name or not str(ascii_name).strip():
        return ()
    return tuple(sorted(str(ascii_name).strip().split()))


# ----------------------------------------------------------------------
# 2. Union-Find (Disjoint Set Union)
# ----------------------------------------------------------------------

class UnionFind:
    """Disjoint-set data structure with path compression for connected components."""

    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, i: int) -> int:
        path = []
        while self.parent[i] != i:
            path.append(i)
            i = self.parent[i]
        for node in path:
            self.parent[node] = i
        return i

    def union(self, i: int, j: int) -> None:
        root_i = self.find(i)
        root_j = self.find(j)
        if root_i != root_j:
            self.parent[root_i] = root_j


# ----------------------------------------------------------------------
# 3. Entity Resolution Core Logic
# ----------------------------------------------------------------------

def resolve_customer_entities(records: List[Dict]) -> Tuple[List[Dict], List[Dict], Dict[str, str]]:
    """Cluster raw touchpoint records into unified Golden Customer entities.

    Args:
        records: List of touchpoint dictionaries containing:
                 'record_key', 'table', 'source_system', 'vin',
                 'name_clean', 'name_ascii', 'phone_norm'.

    Returns:
        Tuple of:
        - customers_list (golden customer records)
        - vehicles_list (vehicle fleet with resolved owner_customer_id)
        - record_to_customer_id (mapping from record_key to resolved customer_id)
    """
    n_records = len(records)
    uf = UnionFind(n_records)

    # Rule 1: Strong link on valid VIN (same vehicle -> same owner)
    vin_to_indices = defaultdict(list)
    for i, r in enumerate(records):
        v = r.get("vin")
        if v and len(v) == 17:
            vin_to_indices[v].append(i)

    for v, idxs in vin_to_indices.items():
        for k in range(1, len(idxs)):
            uf.union(idxs[0], idxs[k])

    # Rule 2: Strong link on valid normalized phone AND similar name key
    # (Token-sorted ASCII name handles swapped first/last name and typos)
    phone_name_to_indices = defaultdict(list)
    for i, r in enumerate(records):
        phone = r.get("phone_norm")
        name_ascii = r.get("name_ascii", "")
        if phone and len(phone) == 10 and name_ascii:
            token_key = get_token_sorted_key(name_ascii)
            if token_key:
                phone_name_to_indices[(phone, token_key)].append(i)

    for key, idxs in phone_name_to_indices.items():
        for k in range(1, len(idxs)):
            uf.union(idxs[0], idxs[k])

    # Group records by connected component root
    clusters = defaultdict(list)
    for i in range(n_records):
        clusters[uf.find(i)].append(i)

    # Sort clusters deterministically for stable customer IDs
    # Sorting key: most frequent display name, then minimum record index
    def cluster_sort_key(item):
        root_idx, member_indices = item
        names = [records[m]["name_clean"] for m in member_indices if records[m]["name_clean"]]
        top_name = names[0] if names else ""
        return (top_name, min(member_indices))

    sorted_clusters = sorted(clusters.items(), key=cluster_sort_key)

    customers = []
    record_to_cid = {}
    vehicle_owners = {}  # vin -> customer_id

    for cid_num, (root_idx, member_indices) in enumerate(sorted_clusters, start=1):
        cid = f"C{cid_num:06d}"

        cluster_records = [records[m] for m in member_indices]

        # Extract names, phones, systems, and VINs
        names = [r["name_clean"] for r in cluster_records if r.get("name_clean")]
        phones = [r["phone_norm"] for r in cluster_records if r.get("phone_norm")]
        vins = {r["vin"] for r in cluster_records if r.get("vin") and len(r["vin"]) == 17}
        systems = sorted(list({r["source_system"] for r in cluster_records if r.get("source_system")}))

        # Best display name: most common clean name in cluster
        display_name = Counter(names).most_common(1)[0][0] if names else "Bilinmeyen Müşteri"

        # Best phone: most common valid phone in cluster
        phone_norm = Counter(phones).most_common(1)[0][0] if phones else None

        # Masking
        p_masked = mask_phone(phone_norm)
        n_masked = mask_name(display_name)

        # Determine matching rule summary & confidence
        has_vin = len(vins) > 0
        has_phone = phone_norm is not None
        rec_count = len(cluster_records)

        if has_vin and has_phone:
            rule_summary = "VIN + Phone + Name Match"
            confidence = "HIGH"
        elif has_vin:
            rule_summary = "VIN Match"
            confidence = "HIGH"
        elif has_phone:
            rule_summary = "Phone + Name Match (No VIN)"
            confidence = "MEDIUM"
        else:
            rule_summary = "Isolated Single Record"
            confidence = "LOW"

        customers.append({
            "customer_id": cid,
            "display_name": display_name,
            "phone_norm": phone_norm,
            "phone_masked": p_masked,
            "name_masked": n_masked,
            "source_systems": systems,
            "record_count": rec_count,
            "match_rule_summary": rule_summary,
            "match_confidence": confidence,
        })

        for m in member_indices:
            record_to_cid[records[m]["record_key"]] = cid
            v = records[m].get("vin")
            if v and len(v) == 17:
                vehicle_owners[v] = cid

    return customers, vehicle_owners, record_to_cid


# ----------------------------------------------------------------------
# 4. Accuracy Evaluation Against Ground Truth
# ----------------------------------------------------------------------

def evaluate_against_ground_truth(
    vehicle_owners: Dict[str, str],
    truth_csv_path: Optional[Path] = None,
) -> Dict:
    """Evaluate resolved vehicle-customer linkage against synthetic ground truth.

    Metrics:
    - true_customer_count: Number of unique customer IDs in ground truth.
    - resolved_customer_count: Number of resolved customer IDs in dim_customers.
    - pairwise_precision: TP / (TP + FP) for all vehicle pairs placed in same cluster.
    - pairwise_recall: TP / (TP + FN) for all vehicle pairs of the same true customer.
    - share_vehicles_assigned_correct: Fraction of vehicles assigned to majority true customer in cluster.
    - cluster_purity: Fraction of clusters containing vehicles belonging exclusively to one true customer.
    """
    truth_path = truth_csv_path or (GROUND_TRUTH_DIR / "vin_to_customer.csv")
    if not truth_path.exists():
        logger.warning(f"Ground truth file not found at {truth_path}. Skipping evaluation.")
        return {}

    truth_df = pd.read_csv(truth_path)
    vin_to_true_cid = dict(zip(truth_df["vin"], truth_df["true_customer_id"]))

    common_vins = [v for v in vehicle_owners if v in vin_to_true_cid]
    if not common_vins:
        return {}

    # True clusters of vehicles
    true_clusters = defaultdict(list)
    for v in common_vins:
        true_clusters[vin_to_true_cid[v]].append(v)

    # Resolved clusters of vehicles
    res_clusters = defaultdict(list)
    for v in common_vins:
        res_clusters[vehicle_owners[v]].append(v)

    # Calculate total true pairs
    true_pairs_count = sum(len(v_list) * (len(v_list) - 1) // 2 for v_list in true_clusters.values())

    # Calculate TP and FP
    tp = 0
    fp = 0
    pred_pairs_count = 0
    for v_list in res_clusters.values():
        k = len(v_list)
        pred_pairs_count += k * (k - 1) // 2
        for v1, v2 in combinations(v_list, 2):
            if vin_to_true_cid[v1] == vin_to_true_cid[v2]:
                tp += 1
            else:
                fp += 1

    fn = true_pairs_count - tp
    precision = tp / pred_pairs_count if pred_pairs_count > 0 else 1.0
    recall = tp / true_pairs_count if true_pairs_count > 0 else 1.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    # Cluster Purity and Vehicle Accuracy
    pure_clusters_count = 0
    correct_vehicles_count = 0

    for cluster_id, v_list in res_clusters.items():
        true_ids_in_cluster = [vin_to_true_cid[v] for v in v_list]
        counts = Counter(true_ids_in_cluster)
        majority_cid, majority_cnt = counts.most_common(1)[0]
        correct_vehicles_count += majority_cnt
        if len(counts) == 1:
            pure_clusters_count += 1

    cluster_purity = pure_clusters_count / len(res_clusters) if res_clusters else 1.0
    vehicle_accuracy = correct_vehicles_count / len(common_vins) if common_vins else 1.0

    metrics = {
        "true_customer_count": int(truth_df["true_customer_id"].nunique()),
        "evaluated_vehicles_count": len(common_vins),
        "total_truth_vehicles": len(truth_df),
        "true_vehicle_pairs": true_pairs_count,
        "predicted_vehicle_pairs": pred_pairs_count,
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "pairwise_precision": round(precision, 4),
        "pairwise_recall": round(recall, 4),
        "pairwise_f1": round(f1, 4),
        "share_vehicles_assigned_correct": round(vehicle_accuracy, 4),
        "cluster_purity": round(cluster_purity, 4),
        "evaluation_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Save to data/meta/matching_quality.json
    META_DIR.mkdir(parents=True, exist_ok=True)
    out_path = META_DIR / "matching_quality.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    return metrics


# ----------------------------------------------------------------------
# 5. Full Pipeline Runner
# ----------------------------------------------------------------------

def run_customer_resolution(
    run_id: Optional[str] = None,
    silver_dir: Optional[Path] = None,
) -> Dict:
    """Execute customer entity resolution and populate Silver customer/vehicle tables.

    Steps:
    1. Load clean Silver fact tables (sales, contracts, service_records, trade_ins).
    2. Extract touchpoint records and run rule-based Union-Find clustering.
    3. Generate dim_customers (customers.parquet) with masked PII and metadata.
    4. Generate dim_vehicles (vehicles.parquet) with resolved owner_customer_id.
    5. Backfill customer_id into every clean Silver fact table.
    6. Evaluate against ground truth and write data/meta/matching_quality.json.
    7. Log step metrics to data/meta/pipeline_runs.parquet.
    """
    start_time = time.time()
    started_at = datetime.now().isoformat()
    active_run_id = run_id or str(uuid.uuid4())
    active_silver_dir = silver_dir or SILVER_DIR

    # Load clean silver tables
    sales_path = active_silver_dir / "sales.parquet"
    contracts_path = active_silver_dir / "contracts.parquet"
    service_path = active_silver_dir / "service_records.parquet"
    trades_path = active_silver_dir / "trade_ins.parquet"

    if not all(p.exists() for p in (sales_path, contracts_path, service_path, trades_path)):
        missing = [str(p) for p in (sales_path, contracts_path, service_path, trades_path) if not p.exists()]
        raise FileNotFoundError(f"Missing required silver tables for entity resolution: {missing}")

    df_sales = pd.read_parquet(sales_path)
    df_contracts = pd.read_parquet(contracts_path)
    df_service = pd.read_parquet(service_path)
    df_trades = pd.read_parquet(trades_path)

    # Collect touchpoint records across all 4 operational sources
    touchpoints = []
    for _, r in df_sales.iterrows():
        touchpoints.append({
            "record_key": f"sales_{r['sale_id']}",
            "table": "sales",
            "source_system": r.get("_source_system", "DMS"),
            "vin": r["vin"],
            "name_clean": r["customer_name_clean"],
            "name_ascii": r["customer_name_ascii"],
            "phone_norm": r.get("phone_norm"),
        })

    for _, r in df_contracts.iterrows():
        touchpoints.append({
            "record_key": f"contracts_{r['contract_id']}",
            "table": "contracts",
            "source_system": r.get("_source_system", "PREMIUM"),
            "vin": r["vin"],
            "name_clean": r["customer_name_clean"],
            "name_ascii": r["customer_name_ascii"],
            "phone_norm": r.get("phone_norm"),
        })

    for _, r in df_service.iterrows():
        touchpoints.append({
            "record_key": f"service_{r['service_id']}",
            "table": "service_records",
            "source_system": r.get("_source_system", "SERVIS"),
            "vin": r["vin"],
            "name_clean": r["customer_name_clean"],
            "name_ascii": r["customer_name_ascii"],
            "phone_norm": None,
        })

    for _, r in df_trades.iterrows():
        touchpoints.append({
            "record_key": f"trades_{r['trade_in_id']}",
            "table": "trade_ins",
            "source_system": r.get("_source_system", "NEXT"),
            "vin": r["vin"],
            "name_clean": r["customer_name_clean"],
            "name_ascii": r["customer_name_ascii"],
            "phone_norm": None,
        })

    rows_in = len(touchpoints)

    # Run Entity Resolution
    customers, vehicle_owners, record_to_cid = resolve_customer_entities(touchpoints)

    # 1. Save data/silver/customers.parquet
    df_customers = pd.DataFrame(customers)
    df_customers.to_parquet(active_silver_dir / "customers.parquet", index=False)

    # 2. Build and save data/silver/vehicles.parquet
    # Lookup brand, model, fuel_type from sales records
    vin_specs = {}
    for _, r in df_sales.iterrows():
        v = r["vin"]
        if v not in vin_specs:
            vin_specs[v] = {
                "brand": r["brand"],
                "model": r["model"],
                "fuel_type": r["fuel_type"],
            }

    vehicles_rows = []
    for vin, owner_cid in sorted(vehicle_owners.items()):
        spec = vin_specs.get(vin, {"brand": "UNKNOWN", "model": "UNKNOWN", "fuel_type": "UNKNOWN"})
        vehicles_rows.append({
            "vin": vin,
            "brand": spec["brand"],
            "model": spec["model"],
            "fuel_type": spec["fuel_type"],
            "owner_customer_id": owner_cid,
        })
    df_vehicles = pd.DataFrame(vehicles_rows)
    df_vehicles.to_parquet(active_silver_dir / "vehicles.parquet", index=False)

    # 3. Add customer_id to each silver fact table
    df_sales["customer_id"] = [record_to_cid[f"sales_{sid}"] for sid in df_sales["sale_id"]]
    df_sales.to_parquet(sales_path, index=False)

    df_contracts["customer_id"] = [record_to_cid[f"contracts_{cid}"] for cid in df_contracts["contract_id"]]
    df_contracts.to_parquet(contracts_path, index=False)

    df_service["customer_id"] = [record_to_cid[f"service_{sid}"] for sid in df_service["service_id"]]
    df_service.to_parquet(service_path, index=False)

    df_trades["customer_id"] = [record_to_cid[f"trades_{tid}"] for tid in df_trades["trade_in_id"]]
    df_trades.to_parquet(trades_path, index=False)

    # 4. Evaluate against Ground Truth
    metrics = evaluate_against_ground_truth(vehicle_owners)
    metrics["resolved_customer_count"] = len(customers)

    duration_s = time.time() - start_time

    # 5. Log Pipeline Run
    log_pipeline_run(
        run_id=active_run_id,
        step="resolve_customers",
        layer="silver",
        source="dim_customers",
        rows_in=rows_in,
        rows_out=len(customers),
        rows_rejected=0,
        started_at=started_at,
        duration_s=duration_s,
        status="SUCCESS",
        error="",
    )

    return {
        "status": "SUCCESS",
        "rows_in": rows_in,
        "resolved_customers": len(customers),
        "resolved_vehicles": len(vehicles_rows),
        "duration_s": duration_s,
        "metrics": metrics,
    }


def print_matching_quality_report(metrics: Dict) -> None:
    """Print executive matching quality report."""
    if not metrics:
        print("No matching quality metrics available.")
        return

    print("\n" + "=" * 80)
    print("CUSTOMER ENTITY RESOLUTION — ACCURACY & BENCHMARKING REPORT")
    print("=" * 80)
    print(f"Ground Truth Customers     : {metrics.get('true_customer_count', 0):>6,}")
    print(f"Resolved Customer Entities : {metrics.get('resolved_customer_count', 0):>6,}")
    print(f"Evaluated Vehicles         : {metrics.get('evaluated_vehicles_count', 0):>6,}")
    print("-" * 80)
    print(f"Pairwise Precision         : {metrics.get('pairwise_precision', 0.0) * 100:>6.2f}% (False merges = {metrics.get('false_positives', 0)})")
    print(f"Pairwise Recall            : {metrics.get('pairwise_recall', 0.0) * 100:>6.2f}% (Splits / False negatives = {metrics.get('false_negatives', 0)})")
    print(f"Pairwise F1 Score          : {metrics.get('pairwise_f1', 0.0) * 100:>6.2f}%")
    print(f"Vehicle Ownership Accuracy : {metrics.get('share_vehicles_assigned_correct', 0.0) * 100:>6.2f}%")
    print(f"Cluster Purity             : {metrics.get('cluster_purity', 0.0) * 100:>6.2f}%")
    print("=" * 80)


if __name__ == "__main__":
    outcome = run_customer_resolution()
    print_matching_quality_report(outcome["metrics"])
