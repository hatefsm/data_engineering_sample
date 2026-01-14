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
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd


# ----------------------------
# Configuration
# ----------------------------

@dataclass(frozen=True)
class MergeOptions:
    table: str                   # "users" or "orders"
    silver_base: Path            # default: data/silver
    only_date: Optional[str]     # if set, process only this event_date (YYYY-MM-DD)
    force: bool                  # if True, process even if checkpoint says done


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

def _checkpoint_file(opts: MergeOptions) -> Path:
    return opts.silver_base / "current" / "_checkpoints" / f"table={opts.table}.json"


# ----------------------------
# Checkpoint helpers
# ----------------------------

def _load_checkpoint(fp: Path) -> Set[str]:
    if not fp.exists():
        return set()
    data = json.loads(fp.read_text(encoding="utf-8"))
    return set(data.get("processed_event_dates", []))

def _save_checkpoint(fp: Path, processed: Set[str]) -> None:
    fp.parent.mkdir(parents=True, exist_ok=True)
    payload = {"processed_event_dates": sorted(processed)}
    fp.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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

def _dates_to_process(opts: MergeOptions) -> List[str]:
    all_dates = _discover_event_dates(opts)
    if opts.only_date:
        return [d for d in all_dates if d == opts.only_date]

    ck = _load_checkpoint(_checkpoint_file(opts))
    if opts.force:
        return all_dates
    return [d for d in all_dates if d not in ck]


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

    # minimal expectations from stage builder
    needed = {"op", "ts_ms", "lsn", "is_delete", _pk_col(opts.table)}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Stage data missing columns {missing} in {part_dir}")

    # Dedup exact replays (safe at event level)
    df = df.drop_duplicates(subset=[_pk_col(opts.table), "lsn", "ts_ms", "op"], keep="last")

    # Order for deterministic application
    df = df.sort_values(by=["lsn", "ts_ms"], na_position="last").reset_index(drop=True)
    return df


def _read_current(opts: MergeOptions) -> pd.DataFrame:
    fp = _current_file(opts)
    if not fp.exists():
        return pd.DataFrame()
    return pd.read_parquet(fp)


def _apply_events_to_current(opts: MergeOptions, current: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    pk = _pk_col(opts.table)

    # Normalize current index
    if current.empty:
        current_idx: Dict[int, Dict] = {}
    else:
        # keep last occurrence per pk if any duplicates exist
        current = current.drop_duplicates(subset=[pk], keep="last")
        current_idx = {int(r[pk]): r for r in current.to_dict(orient="records")}

    # Apply events in order
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

        # UPSERT: store the full row (excluding helper fields you don't want in current)
        # Keep entity columns + optionally useful CDC metadata
        current_idx[k] = r

    # Build dataframe
    out = pd.DataFrame(list(current_idx.values()))
    if out.empty:
        return out

    # Current table should be 1 row per PK
    out = out.drop_duplicates(subset=[pk], keep="last")

    return out


def _write_current(opts: MergeOptions, df: pd.DataFrame) -> Path:
    out_dir = _current_dir(opts)
    out_dir.mkdir(parents=True, exist_ok=True)

    fp = _current_file(opts)
    df.to_parquet(fp, index=False)
    return fp


# ----------------------------
# CLI
# ----------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=["users", "orders"])
    parser.add_argument("--silver-base", default="data/silver")
    parser.add_argument("--date", default=None, help="Process only this event_date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Reprocess even if checkpoint says done")
    args = parser.parse_args()

    opts = MergeOptions(
        table=args.table,
        silver_base=Path(args.silver_base),
        only_date=args.date,
        force=bool(args.force),
    )

    dates = _dates_to_process(opts)
    if not dates:
        print("Nothing to process.")
        return 0

    # Load current once
    current = _read_current(opts)

    processed = _load_checkpoint(_checkpoint_file(opts))

    for d in dates:
        events = _read_stage_for_date(opts, d)
        if events.empty:
            # mark as processed to avoid looping forever
            processed.add(d)
            continue

        before_n = 0 if current.empty else len(current)
        current = _apply_events_to_current(opts, current, events)
        after_n = 0 if current.empty else len(current)

        print(f"Processed date={d} | events={len(events)} | current_rows: {before_n} -> {after_n}")
        processed.add(d)

    fp = _write_current(opts, current)
    _save_checkpoint(_checkpoint_file(opts), processed)

    print(f"Wrote current: {fp}")
    print(f"Checkpoint: {_checkpoint_file(opts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

