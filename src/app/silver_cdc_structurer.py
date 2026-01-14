"""
Silver Staging (Bronze Compacted CDC -> Silver Stage Events)

Purpose
-------
This job converts Bronze compacted Debezium CDC events into a structured,
typed "Silver staging" dataset.

It:
- Reads events_compacted.parquet produced by bronze_compactor
- Parses Debezium JSON envelope (payload.op, payload.source, before/after)
- Enforces entity schema (users/orders) with type casting
- Writes partitioned Silver stage Parquet by event_date=YYYY-MM-DD
- Quarantines malformed/unusable events with reason codes

It does NOT:
- Merge events into "current state" tables (that is the next job)
- Deduplicate updates across time (that requires CDC ordering + state)
- Apply business aggregations (Gold responsibility)

Why this exists
---------------
Bronze is raw truth. Silver staging is the first step where we:
- turn JSON into typed columns
- enforce required fields and types
- prepare data for CDC merge logic (latest-state / SCD2)

Input (Bronze)
--------------
Parquet schema (from your compactor docs):
    ingest_ts : string
    raw_json  : string

Output (Silver stage)
---------------------
Partitioned Parquet files:
    <out_base>/stage_events/table=<users|orders>/event_date=YYYY-MM-DD/part-<id>.parquet

Plus a quarantine file:
    <out_base>/stage_events_quarantine/table=<...>/quarantine-<id>.jsonl
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import pyarrow as pa
import pyarrow.parquet as pq


# ----------------------------
# Configuration
# ----------------------------

@dataclass(frozen=True)
class JobOptions:
    table: str                 # "users" or "orders"
    input_file: Path           # events_compacted.parquet
    out_base: Path             # e.g. data/silver
    keep_raw_json: bool        # keep raw_json column in stage output


# ----------------------------
# Utilities
# ----------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ts_ms_to_event_date(ts_ms: int) -> str:
    # ts_ms is milliseconds since epoch
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")

def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None

def _safe_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        if v.lower() in ("true", "t", "1", "yes"):
            return True
        if v.lower() in ("false", "f", "0", "no"):
            return False
    if isinstance(v, (int, float)):
        return bool(v)
    return None

def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    return str(v)

def _safe_timestamptz(v: Any) -> Optional[str]:
    """
    Debezium ZonedTimestamp arrives as ISO string like:
      "2026-01-12T21:50:09.953905Z"
    For staging we keep ISO string (stable, lossless) and optionally parse later.
    """
    if v is None:
        return None
    s = str(v)
    # very light sanity check
    if "T" not in s:
        return None
    return s

def _safe_decimal_str(v: Any) -> Optional[str]:
    """
    For NUMERIC(12,2) we store as string in stage to avoid float rounding.
    Later, when building 'current' tables, we can cast to Arrow decimal safely.
    """
    if v is None:
        return None
    # Accept numeric or string
    try:
        return str(v)
    except Exception:
        return None


# ----------------------------
# Debezium parsing
# ----------------------------

@dataclass(frozen=True)
class ParsedEvent:
    table: str
    op: str                 # c/u/d/r
    ts_ms: int
    lsn: Optional[int]
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    raw_json: str
    ingest_ts: str

def _parse_debezium(raw_json: str, ingest_ts: str) -> Tuple[Optional[ParsedEvent], Optional[str]]:
    """
    Returns (event, error_reason). error_reason is a short reason code.
    """
    try:
        msg = json.loads(raw_json)
    except Exception:
        return None, "JSON_PARSE"

    payload = msg.get("payload")
    if not isinstance(payload, dict):
        return None, "MISSING_PAYLOAD"

    op = payload.get("op")
    if not isinstance(op, str):
        return None, "MISSING_OP"

    src = payload.get("source")
    if not isinstance(src, dict):
        return None, "MISSING_SOURCE"

    table = src.get("table")
    if not isinstance(table, str):
        return None, "MISSING_SOURCE_TABLE"

    ts_ms = payload.get("ts_ms")
    ts_ms_i = _safe_int(ts_ms)
    if ts_ms_i is None:
        return None, "MISSING_TS_MS"

    lsn_i = _safe_int(src.get("lsn"))  # may be None depending on config

    before = payload.get("before")
    after = payload.get("after")

    # op-specific minimal presence checks (still technical)
    if op in ("c", "u") and after is None:
        return None, "AFTER_NULL_FOR_CU"
    if op == "d" and before is None:
        # Depending on Debezium settings, deletes should usually have before
        return None, "BEFORE_NULL_FOR_D"

    return ParsedEvent(
        table=table,
        op=op,
        ts_ms=ts_ms_i,
        lsn=lsn_i,
        before=before if isinstance(before, dict) else None,
        after=after if isinstance(after, dict) else None,
        raw_json=raw_json,
        ingest_ts=ingest_ts,
    ), None


# ----------------------------
# Entity extraction (schema enforcement happens here)
# ----------------------------

def _extract_users(e: ParsedEvent) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    row = e.after if e.op in ("c", "u", "r") else e.before
    if not isinstance(row, dict):
        return None, "ROW_NULL"

    user_id = _safe_int(row.get("user_id"))
    email = _safe_str(row.get("email"))
    country_code = _safe_str(row.get("country_code"))
    created_at = _safe_timestamptz(row.get("created_at"))
    updated_at = _safe_timestamptz(row.get("updated_at"))

    # Required fields (entity-level)
    if user_id is None:
        return None, "USER_ID_NULL"
    if not email:
        return None, "EMAIL_NULL"
    if not country_code:
        return None, "COUNTRY_CODE_NULL"

    return {
        "user_id": user_id,
        "email": email,
        "country_code": country_code,
        "created_at": created_at,
        "updated_at": updated_at,
    }, None


def _extract_orders(e: ParsedEvent) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    row = e.after if e.op in ("c", "u", "r") else e.before
    if not isinstance(row, dict):
        return None, "ROW_NULL"

    order_id = _safe_int(row.get("order_id"))
    user_id = _safe_int(row.get("user_id"))
    status = _safe_str(row.get("status"))
    total_amount = _safe_decimal_str(row.get("total_amount"))
    currency = _safe_str(row.get("currency"))
    ordered_at = _safe_timestamptz(row.get("ordered_at"))
    updated_at = _safe_timestamptz(row.get("updated_at"))
    is_deleted = _safe_bool(row.get("is_deleted"))

    # Required fields (entity-level)
    if order_id is None:
        return None, "ORDER_ID_NULL"
    if user_id is None:
        return None, "USER_ID_NULL"
    if not status:
        return None, "STATUS_NULL"
    if total_amount is None:
        return None, "TOTAL_AMOUNT_NULL"
    if not currency:
        return None, "CURRENCY_NULL"
    if ordered_at is None:
        return None, "ORDERED_AT_NULL"

    return {
        "order_id": order_id,
        "user_id": user_id,
        "status": status,
        "total_amount": total_amount,  # keep as string in stage (lossless)
        "currency": currency,
        "ordered_at": ordered_at,
        "updated_at": updated_at,
        "is_deleted": is_deleted,
    }, None


def _extract_entity(table: str, e: ParsedEvent) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if table == "users":
        return _extract_users(e)
    if table == "orders":
        return _extract_orders(e)
    return None, "UNSUPPORTED_TABLE"


# ----------------------------
# Writing outputs
# ----------------------------

def _write_partition(out_dir: Path, rows: List[Dict[str, Any]]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / f"part-{uuid.uuid4().hex}.parquet"

    # Use Arrow inference. We already keep decimals as strings for safety.
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_fp, compression="snappy")
    return out_fp

def _write_quarantine(out_dir: Path, bad_rows: List[Dict[str, Any]]) -> Optional[Path]:
    if not bad_rows:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / f"quarantine-{uuid.uuid4().hex}.jsonl"
    with out_fp.open("w", encoding="utf-8") as f:
        for r in bad_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return out_fp


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=["users", "orders"], help="Which entity schema to enforce")
    parser.add_argument("--input-file", required=True, type=str, help="Path to events_compacted.parquet")
    parser.add_argument("--out-base", default="data/silver", type=str, help="Base output folder for Silver")
    parser.add_argument("--keep-raw-json", action="store_true", help="Keep raw_json in stage output rows")
    args = parser.parse_args()

    opts = JobOptions(
        table=args.table,
        input_file=Path(args.input_file),
        out_base=Path(args.out_base),
        keep_raw_json=bool(args.keep_raw_json),
    )

    if not opts.input_file.exists():
        raise FileNotFoundError(f"Input file not found: {opts.input_file}")

    # Read Bronze compacted file
    bronze_tbl = pq.read_table(opts.input_file)
    cols = set(bronze_tbl.column_names)
    if not {"ingest_ts", "raw_json"}.issubset(cols):
        raise ValueError(f"Expected columns ingest_ts, raw_json. Found: {bronze_tbl.column_names}")

    ingest_ts_col = bronze_tbl["ingest_ts"].to_pylist()
    raw_json_col = bronze_tbl["raw_json"].to_pylist()

    stage_rows_by_date: Dict[str, List[Dict[str, Any]]] = {}
    bad: List[Dict[str, Any]] = []

    for i, (ingest_ts, raw_json) in enumerate(zip(ingest_ts_col, raw_json_col)):
        ingest_ts_s = _safe_str(ingest_ts) or _utc_now_iso()
        raw_json_s = _safe_str(raw_json) or ""

        e, err = _parse_debezium(raw_json_s, ingest_ts_s)
        if err:
            bad.append({"row": i, "error": err, "ingest_ts": ingest_ts_s, "raw_json": raw_json_s})
            continue

        # Ensure we are processing the intended table
        if e.table != opts.table:
            bad.append({"row": i, "error": "TABLE_MISMATCH", "expected_table": opts.table, "actual_table": e.table,
                        "ingest_ts": ingest_ts_s})
            continue

        entity, err2 = _extract_entity(opts.table, e)
        if err2:
            bad.append({"row": i, "error": err2, "table": e.table, "op": e.op, "ts_ms": e.ts_ms,
                        "lsn": e.lsn, "ingest_ts": e.ingest_ts})
            continue

        event_date = _ts_ms_to_event_date(e.ts_ms)

        out_row = {
            # CDC metadata (for next step merge)
            "table": e.table,
            "op": e.op,
            "ts_ms": e.ts_ms,
            "lsn": e.lsn,
            "event_date": event_date,
            "ingest_ts": e.ingest_ts,
            "is_delete": True if e.op == "d" else False,
            # Entity columns (schema enforced)
            **entity,
        }

        if opts.keep_raw_json:
            out_row["raw_json"] = e.raw_json

        stage_rows_by_date.setdefault(event_date, []).append(out_row)

    # Write stage partitions
    written: List[Path] = []
    for event_date, rows in sorted(stage_rows_by_date.items()):
        out_dir = opts.out_base / "stage_events" / f"table={opts.table}" / f"event_date={event_date}"
        fp = _write_partition(out_dir, rows)
        written.append(fp)

    # Write quarantine
    q_dir = opts.out_base / "stage_events_quarantine" / f"table={opts.table}"
    q_fp = _write_quarantine(q_dir, bad)

    # Print summary (minimal)
    total = len(raw_json_col)
    ok = sum(len(v) for v in stage_rows_by_date.values())
    print(f"Input rows: {total}")
    print(f"Stage rows: {ok}")
    print(f"Bad rows:   {len(bad)}")
    for fp in written[:5]:
        print(f"Wrote: {fp}")
    if len(written) > 5:
        print(f"... +{len(written)-5} more partitions")
    if q_fp:
        print(f"Quarantine: {q_fp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
