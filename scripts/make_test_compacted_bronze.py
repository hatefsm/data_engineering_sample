"""
Test Data Generator (Bronze Compacted CDC for users + orders)

What it does
------------
- Creates TWO Bronze run folders (users + orders)
- Writes events_compacted.parquet in each run folder with MANY Debezium-like events
- Includes a mix of:
  - valid events (c/u/d)
  - duplicates (replays)
  - bad events (missing fields / invalid JSON) to verify quarantine paths later

Output folders
--------------
data/bronze/cdc/source=source_db/table=users/ingestion_date=YYYY-MM-DD/run_id=TEST/
data/bronze/cdc/source=source_db/table=orders/ingestion_date=YYYY-MM-DD/run_id=TEST/

Each contains:
- events_compacted.parquet  (ingest_ts, raw_json)
- _COMPACTED
- _SUCCESS
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq


# ----------------------------
# Helpers
# ----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def iso_from_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def event_date_from_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")

def make_envelope(
    *,
    table: str,
    op: str,  # c/u/d
    before: Optional[Dict[str, Any]],
    after: Optional[Dict[str, Any]],
    ts_ms: int,
    lsn: int,
    txid: int,
) -> Dict[str, Any]:
    # Minimal Debezium-like shape that your stage builder expects:
    # payload.op, payload.ts_ms, payload.source.table, payload.source.lsn, payload.before/after
    return {
        "schema": {"type": "struct", "optional": False, "name": f"source_db.public.{table}.Envelope"},
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "2.7.3.Final",
                "connector": "postgresql",
                "name": "source_db",
                "ts_ms": ts_ms,
                "snapshot": "false",
                "db": "sample_source",
                "schema": "public",
                "table": table,
                "txId": txid,
                "lsn": lsn,
            },
            "transaction": None,
            "op": op,
            "ts_ms": ts_ms,
        },
    }

def write_compacted(out_dir: Path, raw_json_rows: List[str]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    ingest_ts = utc_now_iso()

    tbl = pa.table(
        {
            "ingest_ts": [ingest_ts] * len(raw_json_rows),
            "raw_json": raw_json_rows,
        }
    )
    out_fp = out_dir / "events_compacted.parquet"
    pq.write_table(tbl, out_fp, compression="snappy")

    (out_dir / "_COMPACTED").write_text("", encoding="utf-8")
    (out_dir / "_SUCCESS").write_text("", encoding="utf-8")
    return out_fp


# ----------------------------
# Test dataset generation
# ----------------------------

@dataclass(frozen=True)
class GenConfig:
    ingestion_date: str
    run_id: str
    base_dir: Path
    n_users: int
    n_orders: int
    updates_per_user: int
    updates_per_order: int
    add_deletes: bool
    add_duplicates: bool
    add_bad_events: bool
    seed: int


def gen_users_events(cfg: GenConfig, start_ts_ms: int, start_lsn: int) -> List[str]:
    rng = random.Random(cfg.seed)
    lsn = start_lsn
    txid = 1000
    ts_ms = start_ts_ms

    rows: List[str] = []

    # Create users (op=c)
    for user_id in range(1, cfg.n_users + 1):
        ts_ms += rng.randint(50, 300)
        lsn += rng.randint(1, 5)
        txid += 1

        email = f"user{user_id}@example.com"
        cc = rng.choice(["SE", "DE", "US", "GB", "NO"])

        after = {
            "user_id": user_id,
            "email": email,
            "country_code": cc,
            "created_at": iso_from_ts_ms(ts_ms),
            "updated_at": iso_from_ts_ms(ts_ms),
        }

        env = make_envelope(
            table="users", op="c", before=None, after=after, ts_ms=ts_ms, lsn=lsn, txid=txid
        )
        rows.append(json.dumps(env, ensure_ascii=False))

        # Updates (op=u)
        for _ in range(cfg.updates_per_user):
            ts_ms += rng.randint(50, 300)
            lsn += rng.randint(1, 5)
            txid += 1

            # change country_code sometimes; email rarely
            new_cc = rng.choice(["SE", "DE", "US", "GB", "NO"])
            new_email = email if rng.random() < 0.9 else f"user{user_id}+alt@example.com"

            before = after
            after = {
                "user_id": user_id,
                "email": new_email,
                "country_code": new_cc,
                "created_at": before["created_at"],
                "updated_at": iso_from_ts_ms(ts_ms),
            }

            env_u = make_envelope(
                table="users", op="u", before=before, after=after, ts_ms=ts_ms, lsn=lsn, txid=txid
            )
            rows.append(json.dumps(env_u, ensure_ascii=False))

            # Optional duplicates (exact replay)
            if cfg.add_duplicates and rng.random() < 0.05:
                rows.append(json.dumps(env_u, ensure_ascii=False))

        # Optional deletes (rare)
        if cfg.add_deletes and rng.random() < 0.03:
            ts_ms += rng.randint(50, 300)
            lsn += rng.randint(1, 5)
            txid += 1

            env_d = make_envelope(
                table="users", op="d", before=after, after=None, ts_ms=ts_ms, lsn=lsn, txid=txid
            )
            rows.append(json.dumps(env_d, ensure_ascii=False))

    # Bad events (to test quarantine)
    if cfg.add_bad_events:
        rows.append("{not valid json")  # JSON_PARSE
        rows.append(json.dumps({"payload": {"op": "c"}}, ensure_ascii=False))  # missing source/table/ts_ms
        rows.append(json.dumps({"payload": {"source": {"table": "users", "lsn": 1}, "ts_ms": ts_ms}}, ensure_ascii=False))  # missing op

    return rows


def gen_orders_events(cfg: GenConfig, start_ts_ms: int, start_lsn: int) -> List[str]:
    rng = random.Random(cfg.seed + 999)
    lsn = start_lsn
    txid = 2000
    ts_ms = start_ts_ms

    rows: List[str] = []

    statuses = ["created", "confirmed", "cancelled", "refunded"]
    currencies = ["SEK", "EUR", "USD", "GBP"]

    # Create orders (op=c)
    for order_id in range(1, cfg.n_orders + 1):
        ts_ms += rng.randint(50, 300)
        lsn += rng.randint(1, 5)
        txid += 1

        user_id = rng.randint(1, cfg.n_users)  # FK to users by design (mostly valid)
        status = rng.choice(statuses)
        currency = rng.choice(currencies)
        total = round(rng.uniform(10, 5000), 2)

        after = {
            "order_id": order_id,
            "user_id": user_id,
            "status": status,
            "total_amount": f"{total:.2f}",   # keep as string (Debezium may differ; stage builder accepts str/num)
            "currency": currency,
            "ordered_at": iso_from_ts_ms(ts_ms),
            "updated_at": iso_from_ts_ms(ts_ms),
            "is_deleted": False,
        }

        env = make_envelope(
            table="orders", op="c", before=None, after=after, ts_ms=ts_ms, lsn=lsn, txid=txid
        )
        rows.append(json.dumps(env, ensure_ascii=False))

        # Updates (op=u)
        for _ in range(cfg.updates_per_order):
            ts_ms += rng.randint(50, 300)
            lsn += rng.randint(1, 5)
            txid += 1

            before = after
            # move status forward sometimes, change amount slightly
            new_status = rng.choice(statuses)
            new_total = max(0.0, float(before["total_amount"]) + rng.uniform(-5, 15))

            after = {
                **before,
                "status": new_status,
                "total_amount": f"{new_total:.2f}",
                "updated_at": iso_from_ts_ms(ts_ms),
            }

            env_u = make_envelope(
                table="orders", op="u", before=before, after=after, ts_ms=ts_ms, lsn=lsn, txid=txid
            )
            rows.append(json.dumps(env_u, ensure_ascii=False))

            if cfg.add_duplicates and rng.random() < 0.05:
                rows.append(json.dumps(env_u, ensure_ascii=False))

        # Optional deletes for some orders
        if cfg.add_deletes and rng.random() < 0.05:
            ts_ms += rng.randint(50, 300)
            lsn += rng.randint(1, 5)
            txid += 1

            env_d = make_envelope(
                table="orders", op="d", before=after, after=None, ts_ms=ts_ms, lsn=lsn, txid=txid
            )
            rows.append(json.dumps(env_d, ensure_ascii=False))

    # Bad events
    if cfg.add_bad_events:
        rows.append("{broken json")
        rows.append(json.dumps({"payload": {"op": "u", "source": {"table": "orders"}}}, ensure_ascii=False))  # missing ts_ms
        # valid envelope but missing required entity fields -> should become entity-level quarantine later
        ts_ms += 100
        lsn += 1
        txid += 1
        bad_after = {"order_id": None, "user_id": None, "status": None, "total_amount": None, "currency": None, "ordered_at": None}
        rows.append(json.dumps(make_envelope(table="orders", op="c", before=None, after=bad_after, ts_ms=ts_ms, lsn=lsn, txid=txid), ensure_ascii=False))

    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="ingestion_date=YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--run-id", default="TEST", help="run_id folder name")
    parser.add_argument("--base", default="data/bronze", help="bronze base folder")
    parser.add_argument("--users", type=int, default=50, help="number of users")
    parser.add_argument("--orders", type=int, default=200, help="number of orders")
    parser.add_argument("--user-updates", type=int, default=2, help="updates per user")
    parser.add_argument("--order-updates", type=int, default=2, help="updates per order")
    parser.add_argument("--no-deletes", action="store_true")
    parser.add_argument("--no-duplicates", action="store_true")
    parser.add_argument("--no-bad", action="store_true")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    ingestion_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    cfg = GenConfig(
        ingestion_date=ingestion_date,
        run_id=args.run_id,
        base_dir=Path(args.base),
        n_users=args.users,
        n_orders=args.orders,
        updates_per_user=args.user_updates,
        updates_per_order=args.order_updates,
        add_deletes=(not args.no_deletes),
        add_duplicates=(not args.no_duplicates),
        add_bad_events=(not args.no_bad),
        seed=args.seed,
    )

    # shared starting points
    start_ts_ms = int(time.time() * 1000)
    start_lsn = 1_000_000

    users_dir = cfg.base_dir / "cdc" / "source=source_db" / "table=users" / f"ingestion_date={cfg.ingestion_date}" / f"run_id={cfg.run_id}"
    orders_dir = cfg.base_dir / "cdc" / "source=source_db" / "table=orders" / f"ingestion_date={cfg.ingestion_date}" / f"run_id={cfg.run_id}"

    users_rows = gen_users_events(cfg, start_ts_ms, start_lsn)
    orders_rows = gen_orders_events(cfg, start_ts_ms + 10_000, start_lsn + 50_000)

    users_fp = write_compacted(users_dir, users_rows)
    orders_fp = write_compacted(orders_dir, orders_rows)

    print("Wrote users:", users_fp, "rows=", len(users_rows))
    print("Wrote orders:", orders_fp, "rows=", len(orders_rows))
    print("Users dir:", users_dir)
    print("Orders dir:", orders_dir)

    return 0


if __name__ == "__main__":
    import argparse
    raise SystemExit(main())

