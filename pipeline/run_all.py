"""Master pipeline runner for the Electrification Intelligence Platform.

Architectural Rationale:
- Single Entrypoint: Non-negotiable rule 6 states that the entire pipeline must be
  runnable from one command: `python -m pipeline.run_all`.
- Unified Run ID: Every layer (Bronze, Silver, Gold, Meta) executed within this process
  shares the same master `run_id` for end-to-end lineage tracking in pipeline_runs.parquet.
- Incremental Layer Architecture: Sequentially orchestrates Bronze ingestion, Silver
  cleaning and entity resolution, and Gold business aggregations.
"""

import sys
import uuid
from datetime import datetime

from pipeline.bronze import run_bronze
from pipeline.catalog import print_catalog_summary, run_catalog_pipeline
from pipeline.gold import print_gold_summary, run_gold_pipeline
from pipeline.silver_clean import print_rejection_summary, run_silver_clean
from pipeline.silver_customers import print_matching_quality_report, run_customer_resolution

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def run_all(run_id: str = None) -> bool:
    """Execute all active layers of the data pipeline.

    Args:
        run_id: Optional UUID string for the execution. If None, a new UUID is generated.

    Returns:
        True if all pipeline steps completed successfully, False otherwise.
    """
    master_run_id = run_id or str(uuid.uuid4())
    print("=" * 80)
    print("ELECTRIFICATION INTELLIGENCE PLATFORM — PIPELINE EXECUTION")
    print(f"Master Run ID : {master_run_id}")
    print(f"Started At    : {datetime.now().isoformat()}")
    print("=" * 80)

    # ------------------------------------------------------------------
    # Step 1: Ingest Bronze Layer
    # ------------------------------------------------------------------
    print("\n[Layer 1/4] Executing Bronze Layer Ingestion...")
    bronze_results = run_bronze(run_id=master_run_id)

    bronze_success = True
    for table_name, res in bronze_results.items():
        status = res["status"]
        rows = res["rows_out"]
        duration = res["duration_s"]
        print(f"  - {table_name:24s}: {status:8s} | {rows:>6,} rows | {duration:.3f}s")
        if status == "FAILED":
            bronze_success = False

    # ------------------------------------------------------------------
    # Step 2A: Clean Silver Layer & Reconciliation
    # ------------------------------------------------------------------
    print("\n[Layer 2A/4] Executing Silver Cleaning & Quarantine...")
    silver_results = run_silver_clean(run_id=master_run_id)

    silver_success = True
    print("\n" + "-" * 80)
    print("SILVER LAYER RECONCILIATION TABLE:")
    print(f"{'Silver Table':<18} | {'Status':<7} | {'Rows In':>8} | {'Rows Out':>8} | {'Rows Rej':>8} | {'Events':>7} | {'Reconciled?':<11}")
    print("-" * 80)

    for table_name, res in silver_results.items():
        status = res["status"]
        rows_in = res["rows_in"]
        rows_out = res["rows_out"]
        rows_rej = res["rows_rejected"]
        events = res["rejection_events"]
        reconciled = "YES" if rows_in == (rows_out + rows_rej) else "NO (ERROR)"
        print(
            f"{table_name:<18} | {status:<7} | {rows_in:>8,} | "
            f"{rows_out:>8,} | {rows_rej:>8,} | {events:>7,} | {reconciled:<11}"
        )
        if status == "FAILED" or reconciled != "YES":
            silver_success = False

    print("-" * 80)
    print_rejection_summary()

    # ------------------------------------------------------------------
    # Step 2B: Silver Customer Entity Resolution (Master Data Management)
    # ------------------------------------------------------------------
    print("\n[Layer 2B/4] Executing Silver Customer Entity Resolution...")
    er_result = run_customer_resolution(run_id=master_run_id)
    er_success = er_result["status"] == "SUCCESS"

    print(
        f"  - dim_customers  : {er_result['status']:8s} | {er_result['resolved_customers']:>6,} unique customers | {er_result['duration_s']:.3f}s"
    )
    print(
        f"  - dim_vehicles   : {er_result['status']:8s} | {er_result['resolved_vehicles']:>6,} vehicles linked | {er_result['duration_s']:.3f}s"
    )

    print_matching_quality_report(er_result["metrics"])

    # ------------------------------------------------------------------
    # Step 3: Materialize Gold Analytical Models & Dashboards
    # ------------------------------------------------------------------
    print("\n[Layer 3/4] Executing Gold Layer Aggregations...")
    gold_results = run_gold_pipeline(run_id=master_run_id)

    gold_success = True
    for table_name, res in gold_results.items():
        status = res["status"]
        rows = res["rows_out"]
        duration = res["duration_s"]
        print(f"  - {table_name:24s}: {status:8s} | {rows:>6,} rows | {duration:.3f}s")
        if status == "FAILED":
            gold_success = False

    print_gold_summary()

    # ------------------------------------------------------------------
    # Step 4: Data Catalog & Metrics Glossary Generation
    # ------------------------------------------------------------------
    print("\n[Layer 4/5] Generating Data Catalog & Metrics Glossary...")
    catalog_result = run_catalog_pipeline(run_id=master_run_id)
    catalog_success = catalog_result["status"] == "SUCCESS"
    print(
        f"  - data_catalog    : {catalog_result['status']:8s} | "
        f"{catalog_result.get('tables_cataloged', 0):>2} tables | "
        f"{catalog_result.get('metrics_cataloged', 0):>2} metrics | "
        f"{catalog_result['duration_s']:.3f}s"
    )
    print_catalog_summary()

    # ------------------------------------------------------------------
    # Step 5: Export Power BI Datasets (UTF-8 BOM CSV & Zero-PII)
    # ------------------------------------------------------------------
    print("\n[Layer 5/5] Exporting Semantic Model Datasets for Power BI...")
    from pipeline.export_for_powerbi import export_all_for_powerbi

    export_result = export_all_for_powerbi()
    export_success = export_result["status"] == "SUCCESS"
    print(
        f"  - powerbi_export  : {export_result['status']:8s} | "
        f"{export_result['total_files']} files | {export_result['total_rows']:,} rows | "
        f"{export_result['duration_s']:.3f}s"
    )

    all_success = (
        bronze_success
        and silver_success
        and er_success
        and gold_success
        and catalog_success
        and export_success
    )
    print("\n" + "=" * 80)
    if all_success:
        print(f"Pipeline Run {master_run_id} completed SUCCESSFULLY.")
    else:
        print(f"Pipeline Run {master_run_id} completed with ERRORS.")
    print("=" * 80)

    return all_success


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
