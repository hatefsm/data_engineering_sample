"""
SQLite-based metadata store for ingestion pipelines.

Stores
------
Bronze:
- pipeline_runs: run lifecycle (STARTED/SUCCESS/FAILED)
- cdc_checkpoints: last committed Kafka offsets per (topic, partition)

Silver (optional but recommended for auditability):
- silver_runs: run lifecycle for Silver jobs (STARTED/SUCCESS/FAILED)
- silver_partitions_processed: which event_date partitions were processed per job/table (+ basic stats)

Design
------
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
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def _init_schema(self) -> None:
        # Bronze: pipeline runs
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

        # Bronze: CDC checkpointing (Kafka offsets)
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

        # Silver: run tracking
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_runs (
              run_id TEXT PRIMARY KEY,
              job_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              start_time TEXT NOT NULL,
              end_time TEXT,
              status TEXT NOT NULL,
              details TEXT
            );
            """
        )

        # Silver: partition processing checkpoint (+ stats)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_partitions_processed (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              event_date TEXT NOT NULL,
              processed_at TEXT NOT NULL,
              run_id TEXT NOT NULL,
              rows_in INTEGER,
              rows_out INTEGER,
              bad_rows INTEGER,
              UNIQUE(job_name, table_name, event_date)
            );
            """
        )

        self.conn.commit()

    # ----------------------------
    # Bronze pipeline runs
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

    def get_run(self, run_id: str) -> Optional[RunResult]:
        cur = self.conn.execute(
            "SELECT run_id, status, details FROM pipeline_runs WHERE run_id=?",
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        details = json.loads(row[2]) if row[2] else None
        return RunResult(run_id=row[0], status=row[1], details=details)

    # ----------------------------
    # Bronze CDC checkpoints
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

    # ----------------------------
    # Silver run tracking
    # ----------------------------

    def start_silver_run(self, run_id: str, job_name: str, table_name: str) -> None:
        self.conn.execute(
            "INSERT INTO silver_runs(run_id, job_name, table_name, start_time, status, details) VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, job_name, table_name, utc_now_iso(), "STARTED", None),
        )
        self.conn.commit()

    def end_silver_run(self, run_id: str, status: str, details: Optional[dict] = None) -> None:
        details_str = json.dumps(details, ensure_ascii=False) if details else None
        self.conn.execute(
            "UPDATE silver_runs SET end_time=?, status=?, details=? WHERE run_id=?",
            (utc_now_iso(), status, details_str, run_id),
        )
        self.conn.commit()

    def mark_silver_partition_processed(
        self,
        job_name: str,
        table_name: str,
        event_date: str,
        run_id: str,
        rows_in: int = 0,
        rows_out: int = 0,
        bad_rows: int = 0,
    ) -> None:
        # INSERT OR REPLACE works because we have UNIQUE(job_name, table_name, event_date)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO silver_partitions_processed(
              job_name, table_name, event_date, processed_at, run_id, rows_in, rows_out, bad_rows
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (job_name, table_name, event_date, utc_now_iso(), run_id, rows_in, rows_out, bad_rows),
        )
        self.conn.commit()

    def get_processed_silver_partitions(self, job_name: str, table_name: str) -> set[str]:
        cur = self.conn.execute(
            "SELECT event_date FROM silver_partitions_processed WHERE job_name=? AND table_name=?",
            (job_name, table_name),
        )
        return {r[0] for r in cur.fetchall()}

    # ----------------------------
    # Housekeeping
    # ----------------------------

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

