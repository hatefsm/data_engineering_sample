"""
SQLite-based metadata store for ingestion pipelines.

Stores:
- pipeline_runs: run lifecycle (STARTED/SUCCESS/FAILED)
- cdc_checkpoints: last committed Kafka offsets per (topic, partition)

Design:
- Reusable across CDC, snapshot, incremental pipelines
- WAL mode enabled for safer concurrent reads/writes
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class RunResult:
    run_id: str
    status: str
    details: Optional[dict] = None


class SQLiteMetadataStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
              run_id TEXT PRIMARY KEY,
              start_time TEXT NOT NULL,
              end_time TEXT,
              status TEXT NOT NULL,
              details TEXT
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_checkpoints (
              topic TEXT NOT NULL,
              partition INTEGER NOT NULL,
              last_committed_offset INTEGER NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (topic, partition)
            );
            """
        )
        self.conn.commit()

    # ----------------------------
    # Pipeline runs
    # ----------------------------

    def start_run(self, run_id: str) -> None:
        self.conn.execute(
            "INSERT INTO pipeline_runs(run_id, start_time, status, details) VALUES (?, ?, ?, ?)",
            (run_id, utc_now_iso(), "STARTED", None),
        )
        self.conn.commit()

    def end_run(self, run_id: str, status: str, details: Optional[dict] = None) -> None:
        details_str = json.dumps(details, ensure_ascii=False) if details else None
        self.conn.execute(
            "UPDATE pipeline_runs SET end_time=?, status=?, details=? WHERE run_id=?",
            (utc_now_iso(), status, details_str, run_id),
        )
        self.conn.commit()

    # ----------------------------
    # CDC checkpoints
    # ----------------------------

    def get_checkpoint(self, topic: str, partition: int) -> Optional[int]:
        cur = self.conn.execute(
            "SELECT last_committed_offset FROM cdc_checkpoints WHERE topic=? AND partition=?",
            (topic, partition),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None

    def upsert_checkpoint(self, topic: str, partition: int, offset: int) -> None:
        self.conn.execute(
            """
            INSERT INTO cdc_checkpoints(topic, partition, last_committed_offset, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(topic, partition) DO UPDATE SET
              last_committed_offset=excluded.last_committed_offset,
              updated_at=excluded.updated_at;
            """,
            (topic, partition, int(offset), utc_now_iso()),
        )

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
