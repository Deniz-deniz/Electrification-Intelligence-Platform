"""Pipeline execution logging and audit trail utility.

Architectural Rationale:
- Non-Negotiable Rule 1: Every pipeline step must write an audit record to
  data/meta/pipeline_runs.parquet. This provides step-level lineage, execution duration,
  row throughput, and failure telemetry matching Microsoft Fabric pipeline monitoring.
- Offline & Serverless: Stores telemetry directly in columnar Parquet format rather than
  requiring an external operational database.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from config import META_DIR, PIPELINE_RUNS_COLUMNS, PIPELINE_RUNS_FILE


def get_pipeline_runs() -> pd.DataFrame:
    """Read the pipeline run audit log.

    Returns:
        pd.DataFrame containing all historical pipeline runs, or an empty DataFrame
        with standard schema if no runs have been logged yet.
    """
    if PIPELINE_RUNS_FILE.exists():
        return pd.read_parquet(PIPELINE_RUNS_FILE)

    # Empty DataFrame with explicit dtypes
    return pd.DataFrame({
        "run_id": pd.Series(dtype="object"),
        "step": pd.Series(dtype="object"),
        "layer": pd.Series(dtype="object"),
        "source": pd.Series(dtype="object"),
        "rows_in": pd.Series(dtype="int64"),
        "rows_out": pd.Series(dtype="int64"),
        "rows_rejected": pd.Series(dtype="int64"),
        "started_at": pd.Series(dtype="object"),
        "duration_s": pd.Series(dtype="float64"),
        "status": pd.Series(dtype="object"),
        "error": pd.Series(dtype="object"),
    })


def log_pipeline_run(
    run_id: str,
    step: str,
    layer: str,
    source: str,
    rows_in: int,
    rows_out: int,
    rows_rejected: int,
    started_at: str,
    duration_s: float,
    status: str,
    error: Optional[str] = "",
) -> None:
    """Record a single pipeline step execution to data/meta/pipeline_runs.parquet.

    Args:
        run_id: Unique execution identifier (e.g., UUID string).
        step: Name of the pipeline step (e.g., 'ingest_dms_satis').
        layer: Lakehouse tier ('bronze', 'silver', 'gold', 'meta').
        source: Name of source entity or file.
        rows_in: Number of input records received.
        rows_out: Number of records successfully written to target table.
        rows_rejected: Number of records quarantined or failed validation.
        started_at: ISO 8601 timestamp string when the step began.
        duration_s: Elapsed runtime in seconds.
        status: Execution outcome ('SUCCESS', 'FAILED', 'WARNING').
        error: Error message or traceback summary if status is 'FAILED'.
    """
    META_DIR.mkdir(parents=True, exist_ok=True)

    new_row = {
        "run_id": str(run_id),
        "step": str(step),
        "layer": str(layer),
        "source": str(source),
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "rows_rejected": int(rows_rejected),
        "started_at": str(started_at),
        "duration_s": round(float(duration_s), 4),
        "status": str(status),
        "error": str(error or ""),
    }

    new_df = pd.DataFrame([new_row], columns=PIPELINE_RUNS_COLUMNS)

    if PIPELINE_RUNS_FILE.exists():
        existing_df = pd.read_parquet(PIPELINE_RUNS_FILE)
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        updated_df = new_df

    updated_df.to_parquet(PIPELINE_RUNS_FILE, index=False)
