"""
Silver CDC Merge (Stage Events -> Current State)

Purpose
-------
This job builds/updates Silver "current state" tables incrementally.

It:
- Reads Silver stage events produced by silver_cdc_stage_builder:
    data/silver/stage_events/table=<users|orders>/event_date=YYYY-MM-DD/part-*.parquet
- Applies CDC logic (c/u/d) in correct order (lsn, ts_ms)
- Produces ONE current table file per entity:
    data/silver/current/table=<users|orders>/current.parquet
- Maintains a checkpoint of processed event_date partitions:
    data/silver/current/_checkpoints/table=<users|orders>.json

CDC rules
---------
- c/u/r (insert/update/read snapshot) => UPSERT into current by PK
- d (delete) => remove from current by PK

Notes
-----
- This is an incremental stateful job:
  it updates yesterday's current with today's events.
- It does NOT build SCD2 history (that's a separate job if needed).
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from app.metadata.metadata_store import SQLiteMetadataStore

# ----------------------------
# Configuration
# ----------------------------

JOB_NAME = "silver_cdc_merge_current"


@dataclass(frozen=True)
class MergeOptions:
    table: str                   # "users" or "orders"
    silver_base: Path            # default: data/silver
    db_path: Path                # sqlite metadata db path
    only_date: Optional[str]     # if set, process only this event_date (YYYY-MM-DD)
    force: bool                  # if True, reprocess even if metadata says done


def _pk_col(table: str) -> str:
    if table == "users":
        return "user_id"
    if table == "orders":
        return "order_id"
    raise ValueError(f"Unsupported table: {table}")


# ----------------------------
# Paths
# ----------------------------

def _stage_table_dir(opts: MergeOptions) -> Path:
    return opts.silver_base / "stage_events" / f"table={opts.table}"

def _current_dir(opts: MergeOptions) -> Path:
    return opts.silver_base / "current" / f"table={opts.table}"

def _current_file(opts: MergeOptions) -> Path:
    return _current_dir(opts) / "current.parquet"


# ----------------------------
# Discovery
# ----------------------------

def _discover_event_dates(opts: MergeOptions) -> List[str]:
    base = _stage_table_dir(opts)
    if not base.exists():
        return []
    dates = []
    for p in base.glob("event_date=*"):
        if p.is_dir():
            dates.append(p.name.split("=", 1)[1])
    return sorted(dates)

def _dates_to_process(opts: MergeOptions, store: SQLiteMetadataStore) -> List[str]:
    all_dates = _discover_event_dates(opts)

    if opts.only_date:
        return [d for d in all_dates if d == opts.only_date]

    if opts.force:
        return all_dates

    processed = store.get_processed_silver_partitions(JOB_NAME, opts.table)
    return [d for d in all_dates if d not in processed]


# ----------------------------
# Core merge logic
# ----------------------------

def _read_stage_for_date(opts: MergeOptions, event_date: str) -> pd.DataFrame:
    part_dir = _stage_table_dir(opts) / f"event_date={event_date}"
    files = sorted(part_dir.glob("part-*.parquet"))
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    needed = {"op", "ts_ms", "lsn", "is_delete", _pk_col(opts.table)}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Stage data missing columns {missing} in {part_dir}")

    # Drop exact event replays (safe at event level)
    df = df.drop_duplicates(subset=[_pk_col(opts.table), "lsn", "ts_ms", "op"], keep="last")

    # Deterministic ordering
    df = df.sort_values(by=["lsn", "ts_ms"], na_position="last").reset_index(drop=True)
    return df


def _read_current(opts: MergeOptions) -> pd.DataFrame:
    fp = _current_file(opts)
    if not fp.exists():
        return pd.DataFrame()
    return pd.read_parquet(fp)


def _apply_events_to_current(opts: MergeOptions, current: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    pk = _pk_col(opts.table)

    # Build index of current state by PK
    if current.empty:
        current_idx: Dict[int, Dict] = {}
    else:
        current = current.drop_duplicates(subset=[pk], keep="last")
        current_idx = {int(r[pk]): r for r in current.to_dict(orient="records")}

    # Apply CDC events in order
    for r in events.to_dict(orient="records"):
        k = r.get(pk)
        if k is None:
            continue
        k = int(k)

        op = r.get("op")
        is_del = bool(r.get("is_delete")) or (op == "d")

        if is_del:
            current_idx.pop(k, None)
            continue

        # UPSERT: keep the whole staged row (includes CDC metadata + entity columns)
        current_idx[k] = r

    out = pd.DataFrame(list(current_idx.values()))
    if out.empty:
        return out

    out = out.drop_duplicates(subset=[pk], keep="last")
    return out


def _write_current(opts: MergeOptions, df: pd.DataFrame) -> Path:
    out_dir = _current_dir(opts)
    out_dir.mkdir(parents=True, exist_ok=True)

    fp = _current_file(opts)
    df.to_parquet(fp, index=False)
    return fp


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=["users", "orders"])
    parser.add_argument("--silver-base", default="data/silver")
    parser.add_argument("--db-path", default="data/metadata/metadata.db", help="SQLite metadata DB path")
    parser.add_argument("--date", default=None, help="Process only this event_date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Reprocess even if metadata says already processed")
    args = parser.parse_args()

    opts = MergeOptions(
        table=args.table,
        silver_base=Path(args.silver_base),
        db_path=Path(args.db_path),
        only_date=args.date,
        force=bool(args.force),
    )

    # Ensure DB folder exists
    opts.db_path.parent.mkdir(parents=True, exist_ok=True)

    store = SQLiteMetadataStore(opts.db_path)
    run_id = uuid.uuid4().hex

    try:
        store.start_silver_run(run_id, JOB_NAME, opts.table)

        dates = _dates_to_process(opts, store)
        if not dates:
            print("Nothing to process.")
            store.end_silver_run(run_id, "SUCCESS", {"message": "nothing_to_process"})
            return 0

        current = _read_current(opts)

        for d in dates:
            events = _read_stage_for_date(opts, d)
            if events.empty:
                # still mark processed to avoid endless retries
                store.mark_silver_partition_processed(
                    JOB_NAME, opts.table, d, run_id, rows_in=0, rows_out=(0 if current.empty else len(current)), bad_rows=0
                )
                continue

            before_n = 0 if current.empty else len(current)
            current = _apply_events_to_current(opts, current, events)
            after_n = 0 if current.empty else len(current)

            print(f"Processed date={d} | events={len(events)} | current_rows: {before_n} -> {after_n}")

            store.mark_silver_partition_processed(
                JOB_NAME,
                opts.table,
                d,
                run_id,
                rows_in=int(len(events)),
                rows_out=int(after_n),
                bad_rows=0,
            )

        fp = _write_current(opts, current)
        store.end_silver_run(run_id, "SUCCESS", {"current_file": str(fp), "processed_dates": dates})
        print(f"Wrote current: {fp}")
        print(f"Metadata DB: {opts.db_path}")
        return 0

    except Exception as e:
        store.end_silver_run(run_id, "FAILED", {"error": str(e)})
        raise
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
