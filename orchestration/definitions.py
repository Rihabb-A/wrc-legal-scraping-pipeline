"""Dagster orchestration: ingestion before transformation.

Both stages already work as standalone commands:

    scrapy crawl decisions -a start_date=2025-08-01 -a end_date=2025-08-31
    python -m transform.transform --start-date 2025-08-01 --end-date 2025-08-31

Dagster adds nothing to what either command does - it only enforces the
dependency between them: transformation does not start until ingestion has
finished, and does not run at all if ingestion raised, crashed, or left
records unaccounted for.

Run with:

    dagster dev -f orchestration/definitions.py

then materialize ``wrc_pipeline`` from the UI with a run config such as:

    ops:
      ingest_decisions:
        config:
          start_date: "2025-08-01"
          end_date: "2025-08-31"
      transform_decisions:
        config:
          start_date: "2025-08-01"
          end_date: "2025-08-31"

Each op keeps its own config rather than sharing one block. That is more
typing per run, but it means either date range can be pointed anywhere -
useful for re-transforming an older range without re-scraping it.
"""

import subprocess
import sys

from dagster import (
    Config,
    Definitions,
    Failure,
    In,
    Nothing,
    OpExecutionContext,
    RetryPolicy,
    job,
    op,
)

from common.config import env
from common.logging_config import read_events

LOG_FILE = env("LOG_FILE", "structured_log.jsonl")


class DateRangeConfig(Config):
    start_date: str
    end_date: str


class IngestConfig(DateRangeConfig):
    partition_size: str = "monthly"
    bodies: str = ""  # comma-separated body names; empty means all four
    refresh: bool = False


class TransformConfig(DateRangeConfig):
    refresh: bool = False


def _event_count():
    """How many lines the structured log currently has, used as a marker."""
    try:
        return len(read_events(LOG_FILE))
    except FileNotFoundError:
        return 0


def _run_summary_since(offset):
    """The ingestion run's own ``run_summary`` event, if one was written."""
    try:
        new_events = read_events(LOG_FILE)[offset:]
    except FileNotFoundError:
        return None
    for event in reversed(new_events):
        if event.get("event") == "run_summary":
            return event
    return None


@op(retry_policy=RetryPolicy(max_retries=2))
def ingest_decisions(context: OpExecutionContext, config: IngestConfig) -> None:
    """Scrape one date range, then check the spider's own reconciliation.

    The spider always exits 0 - Scrapy has no concept of "the site returned
    fewer records than expected" - so a crashed process is not the only
    failure to watch for. The structured log's ``run_summary`` event is the
    thing that actually says whether every listed record was downloaded,
    skipped as unchanged, or explicitly failed.
    """
    offset = _event_count()

    command = [
        sys.executable, "-m", "scrapy", "crawl", "decisions",
        "-a", f"start_date={config.start_date}",
        "-a", f"end_date={config.end_date}",
        "-a", f"partition_size={config.partition_size}",
    ]
    if config.bodies:
        command += ["-a", f"bodies={config.bodies}"]
    if config.refresh:
        command += ["-a", "refresh=true"]

    result = subprocess.run(command)
    if result.returncode != 0:
        raise Failure(f"scrapy crawl decisions exited {result.returncode}")

    summary = _run_summary_since(offset)
    if summary is None:
        raise Failure(
            f"ingestion produced no run_summary event in {LOG_FILE}; "
            "cannot confirm every record was accounted for"
        )

    context.log.info("ingestion run_summary: %s", summary)
    if summary.get("unaccounted"):
        raise Failure(
            f"ingestion left {summary['unaccounted']} record(s) unaccounted "
            f"for in slices {summary.get('mismatched_slices')}"
        )


@op(retry_policy=RetryPolicy(max_retries=2), ins={"start": In(Nothing)})
def transform_decisions(context: OpExecutionContext, config: TransformConfig) -> None:
    """Clean Landing Zone documents for the range into the Curated Zone.

    ``ins={"start": In(Nothing)}`` is a pure ordering dependency - no data
    passes between the ops, since both already agree on Landing Zone via
    MongoDB/MinIO. It exists only so Dagster refuses to run this op unless
    ``ingest_decisions`` completed without raising.
    """
    command = [
        sys.executable, "-m", "transform.transform",
        "--start-date", config.start_date,
        "--end-date", config.end_date,
    ]
    if config.refresh:
        command.append("--refresh")

    result = subprocess.run(command)
    if result.returncode != 0:
        raise Failure(f"transform.transform exited {result.returncode}")

    context.log.info(
        "transformation completed for %s..%s", config.start_date, config.end_date
    )


@job
def wrc_pipeline():
    """INGESTION -> TRANSFORMATION for one date range."""
    transform_decisions(start=ingest_decisions())


defs = Definitions(jobs=[wrc_pipeline])
