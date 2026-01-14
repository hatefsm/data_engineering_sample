"""
Bronze Compactor (Bronze CDC -> Compacted Bronze Parquet)

Purpose
-------
This job operates purely in the Bronze layer.

It:
- Reads CDC run folders produced by bronze_cdc_writer
- Validates that the run completed successfully (_SUCCESS marker)
- Merges multiple small Parquet part files into a single Parquet file
- Preserves raw payload and ingestion semantics
- Does NOT apply any business logic, deduplication, or CDC interpretation

Why this exists
---------------
CDC writer produces many small Parquet files per run for streaming efficiency.
Downstream systems (Silver processing, query engines) work better with fewer,
larger Parquet files.

This job is a technical optimization step inside Bronze.

Layer responsibility
--------------------
Bronze = preserve truth, improve storage efficiency, maintain traceability.
Silver = interpret CDC, apply business logic, enforce keys.

This job never changes business meaning.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pyarrow
import pyarrow.parquet

from app.app_config import load_config
from app.app_logging import create_run_logger


# ----------------------------
# Configuration container
# ----------------------------

@dataclass(frozen=True)
class CompactorOptions:
    """
    Runtime options for Bronze compaction.

    prune_parts:
        If True, original events_*.parquet files are deleted after compaction.

    only_date:
        Optional ingestion_date filter (YYYY-MM-DD).
        If set, only runs for this date will be compacted.
    """
    prune_parts: bool
    only_date: Optional[str]


# ----------------------------
# Discovery helpers
# ----------------------------

def _run_dirs(bronze_base: Path) -> List[Path]:
    """
    Return all CDC run folders.

    Expected layout (produced by CDC writer):

        bronze_base/cdc/
            source=.../
                table=.../
                    ingestion_date=YYYY-MM-DD/
                        run_id=.../

    Each run_id folder represents one isolated CDC writer execution.
    """
    return sorted((bronze_base / "cdc").glob("source=*/table=*/ingestion_date=*/run_id=*"))


def _is_successful(run_dir: Path) -> bool:
    """
    A run is considered valid for compaction only if CDC writer
    finished successfully.
    """
    return (run_dir / "_SUCCESS").exists()


def _already_compacted(run_dir: Path) -> bool:
    """
    Idempotency check.

    If a run folder already contains:
        - events_compacted.parquet
        - _COMPACTED marker

    it will be skipped.
    """
    return (run_dir / "_COMPACTED").exists() and (run_dir / "events_compacted.parquet").exists()


def _ingestion_date_from_run_dir(run_dir: Path) -> Optional[str]:
    """
    Extract ingestion_date from folder path.

    Example:
        .../ingestion_date=2026-01-14/run_id=abc
        -> "2026-01-14"
    """
    for part in run_dir.parts[::-1]:
        if part.startswith("ingestion_date="):
            return part.split("=", 1)[1]
    return None


# ----------------------------
# Core compaction logic
# ----------------------------

def _compact_parquet_files(files: List[Path]) -> pyarrow.Table:
    """
    Read multiple Parquet files and merge them into a single Arrow table.

    All files are expected to have identical schema:
        ingest_ts : string
        raw_json  : string
    """
    tables = [pyarrow.parquet.read_table(fp) for fp in files]
    return pyarrow.concat_tables(tables, promote=True)


def compact_one_run(run_dir: Path, opts: CompactorOptions, logger) -> None:
    """
    Compact one CDC run folder.

    Steps:
    1. Validate run success
    2. Apply optional ingestion_date filter
    3. Read all events_*.parquet files
    4. Merge into one Parquet file
    5. Write events_compacted.parquet
    6. Write _COMPACTED marker
    7. Optionally prune original part files
    """

    # Only compact completed CDC runs
    if not _is_successful(run_dir):
        return

    # Idempotency: skip if already compacted
    if _already_compacted(run_dir):
        return

    # Optional date filter
    if opts.only_date:
        d = _ingestion_date_from_run_dir(run_dir)
        if d != opts.only_date:
            return

    # Discover part files
    part_files = sorted(run_dir.glob("events_*.parquet"))
    if not part_files:
        logger.info(f"Skip (no events_*.parquet): {run_dir}")
        return

    # Merge Parquet parts
    merged = _compact_parquet_files(part_files)

    # Write compacted output
    out_fp = run_dir / "events_compacted.parquet"
    pyarrow.parquet.write_table(merged, out_fp, compression="snappy")

    # Mark compaction completion
    (run_dir / "_COMPACTED").write_text("", encoding="utf-8")

    logger.info(
        f"Compacted {len(part_files)} parts -> {out_fp.name} | rows={merged.num_rows}"
    )

    # Optional cleanup
    if opts.prune_parts:
        for fp in part_files:
            try:
                fp.unlink()
            except Exception as e:
                logger.warning(f"Could not delete {fp.name}: {e}")


# ----------------------------
# CLI entrypoint
# ----------------------------

def main() -> int:
    """
    Bronze compactor entrypoint.

    This job is designed to be executed by a scheduler (cron, Airflow, etc.).
    Execution frequency (hourly/daily) is defined outside the application.

    The job itself only decides WHAT to compact, not WHEN.
    """

    cfg = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-base", type=str, default=None)
    parser.add_argument("--prune", action="store_true", help="Delete events_*.parquet after compaction")
    parser.add_argument("--date", type=str, default=None, help="Only compact runs for this ingestion date (YYYY-MM-DD)")
    args = parser.parse_args()

    bronze_base = Path(args.bronze_base) if args.bronze_base else cfg.output.bronze_base_path

    logger = create_run_logger("bronze_compactor", "manual", cfg.ops.log_base_path)
    logger.info(f"Bronze base: {bronze_base}")

    opts = CompactorOptions(prune_parts=bool(args.prune), only_date=args.date)

    processed = 0

    for rd in _run_dirs(bronze_base):
        before = (rd / "events_compacted.parquet").exists()
        compact_one_run(rd, opts, logger)
        after = (rd / "events_compacted.parquet").exists()

        if (not before) and after:
            processed += 1

    logger.info(f"Bronze compaction completed. compacted_runs={processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
