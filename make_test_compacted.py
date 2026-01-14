
# scripts/make_test_compacted.py
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq


def write_events_compacted(out_dir: Path, events: list[dict]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    ingest_ts = datetime.now(timezone.utc).isoformat()

    table = pa.table(
        {
            "ingest_ts": [ingest_ts] * len(events),
            "raw_json": [json.dumps(e, separators=(",", ":"), ensure_ascii=False) for e in events],
        }
    )

    out_fp = out_dir / "events_compacted.parquet"
    pq.write_table(table, out_fp, compression="snappy")
    (out_dir / "_COMPACTED").write_text("", encoding="utf-8")  # match your compactor marker
    (out_dir / "_SUCCESS").write_text("", encoding="utf-8")    # optional, for symmetry
    return out_fp


if __name__ == "__main__":
    # TODO: paste 3-6 debbezium messages here (users/orders: c, u, d)
    events: list[dict] = []

    # Set this to a REAL run folder you already use in Bronze:
    # .../cdc/source=source_db/table=users/ingestion_date=YYYY-MM-DD/run_id=TEST/
    OUT_DIR = Path("data/bronze/cdc/source=source_db/table=users/ingestion_date=2026-01-14/run_id=TEST")

    fp = write_events_compacted(OUT_DIR, events)
    print("Wrote:", fp)
