# src/app/cdc_writer.py
"""
CDC Writer (Kafka -> Bronze JSONL + SQLite metadata)

Consumes Debezium CDC events from Kafka topics and writes them to an append-only
Bronze layer with run isolation. Also persists:
- pipeline run history
- Kafka offset checkpoints (topic, partition -> last committed offset)

Uses shared modules:
- config.load_config()
- logging_utils.create_run_logger()
- metadata.sqlite_store.SQLiteMetadataStore
"""

from __future__ import annotations

import json
import signal
import time
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


from kafka import KafkaConsumer, TopicPartition  # pip install kafka-python

from config import load_config
from logging_utils import create_run_logger
from metadata.sqlite_store import SQLiteMetadataStore


# ----------------------------
# Helpers
# ----------------------------

def local_date_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def safe_json_loads(s: str) -> object:
    try:
        return json.loads(s)
    except Exception:
        return s


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def parse_source_table_from_topic(topic: str) -> Tuple[str, str]:
    """
    Expected Debezium topic: <prefix>.<schema>.<table>
    Example: source_db.public.users -> source=source_db, table=users
    """
    parts = topic.split(".")
    if len(parts) >= 3:
        return parts[0], parts[-1]
    return topic, "unknown"


# ----------------------------
# Bronze Writer (local)
# ----------------------------

@dataclass(frozen=True)
class BronzeTarget:
    source: str
    table: str
    ingestion_date: str
    run_id: str


class LocalBronzeWriter:
    """Append-only JSONL writer with per-run folder isolation."""

    def __init__(self, bronze_base_path: Path) -> None:
        self.base = bronze_base_path
        self.files_written: List[str] = []

    def run_dir(self, t: BronzeTarget) -> Path:
        return (
            self.base
            / "cdc"
            / f"source={t.source}"
            / f"table={t.table}"
            / f"ingestion_date={t.ingestion_date}"
            / f"run_id={t.run_id}"
        )

    def write_events_file(self, t: BronzeTarget, file_index: int, lines: List[str]) -> Path:
        rd = self.run_dir(t)
        ensure_dir(rd)

        fp = rd / f"events_{file_index:05d}.parquet"

        ingest_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        table = pa.table({
            "ingest_ts": [ingest_ts] * len(lines),
            "raw_json": lines,
        })

        pq.write_table(table, fp, compression="snappy")

        rel = str(fp.relative_to(self.base))
        self.files_written.append(rel)
        return fp

    def write_manifest(self, t: BronzeTarget, manifest: dict) -> None:
        rd = self.run_dir(t)
        ensure_dir(rd)
        fp = rd / "manifest.json"
        fp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    def mark_success(self, t: BronzeTarget) -> None:
        rd = self.run_dir(t)
        ensure_dir(rd)
        (rd / "_SUCCESS").write_text("", encoding="utf-8")


# ----------------------------
# CDC App
# ----------------------------

class CDCWriterApp:
    def __init__(self) -> None:
        self.cfg = load_config()

        # Run context
        self.run_id = uuid.uuid4().hex
        self.ingestion_date = local_date_str()

        # Logger
        self.logger = create_run_logger("cdc_writer", self.run_id, self.cfg.ops.log_base_path)

        # Writer + metadata
        self.writer = LocalBronzeWriter(self.cfg.output.bronze_base_path)
        self.meta = SQLiteMetadataStore(self.cfg.ops.metadata_db_path)

        # Control flags
        self._stop = False
        self._did_seek = False
        self._last_flush_time = time.time()

        # Buffers
        self._buffers: Dict[Tuple[str, str], List[str]] = {}    # (source, table) -> json lines
        self._file_index: Dict[Tuple[str, str], int] = {}       # (source, table) -> next file index
        self._last_offsets: Dict[TopicPartition, int] = {}      # last seen offset per tp

        # Kafka consumer
        if not self.cfg.cdc.topics:
            raise RuntimeError("Config error: CDC topics list is empty (TOPICS env var).")

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.cfg.cdc.kafka_bootstrap,
            group_id=self.cfg.cdc.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: b.decode("utf-8", errors="replace"),
            key_deserializer=lambda b: b.decode("utf-8", errors="replace") if b else None,
        )

    def _handle_signals(self) -> None:
        def _sig_handler(_signum, _frame):
            self._stop = True

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)

    def _seek_from_checkpoints_once(self) -> None:
        if self._did_seek:
            return
        assignment = self.consumer.assignment()
        if not assignment:
            return

        for tp in assignment:
            ck = self.meta.get_checkpoint(tp.topic, tp.partition)
            if ck is not None and ck >= 0:
                self.consumer.seek(tp, ck + 1)

        self._did_seek = True
        self.logger.info("Applied checkpoint seeks (if any).")

    def _buffer_event(self, topic: str, partition: int, offset: int, ts_ms: Optional[int], key: Optional[str], value: str) -> None:
        source, table = parse_source_table_from_topic(topic)
        buf_key = (source, table)

        self._buffers.setdefault(buf_key, [])
        self._file_index.setdefault(buf_key, 1)

        event = {
            "kafka": {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp_ms": ts_ms,
                "key": key,
            },
            "payload": safe_json_loads(value),
        }

        self._buffers[buf_key].append(json.dumps(event, ensure_ascii=False))
        self._last_offsets[TopicPartition(topic, partition)] = offset

    def _flush_if_needed(self, force: bool = False) -> None:
        now = time.time()
        due_time = (now - self._last_flush_time) >= self.cfg.cdc.flush_seconds
        due_size = any(len(v) >= self.cfg.cdc.batch_size for v in self._buffers.values())

        if not force and not (due_time or due_size):
            return

        flushed_any = False

        for (source, table), lines in list(self._buffers.items()):
            while lines:
                chunk = lines[: self.cfg.cdc.batch_size]
                del lines[: self.cfg.cdc.batch_size]

                t = BronzeTarget(
                    source=source,
                    table=table,
                    ingestion_date=self.ingestion_date,
                    run_id=self.run_id,
                )
                idx = self._file_index[(source, table)]
                self.writer.write_events_file(t, idx, chunk)
                self._file_index[(source, table)] = idx + 1

                flushed_any = True

        if flushed_any:
            self._commit_offsets_and_checkpoints()

        self._last_flush_time = now

    def _commit_offsets_and_checkpoints(self) -> None:
        for tp, off in self._last_offsets.items():
            self.meta.upsert_checkpoint(tp.topic, tp.partition, off)
        self.meta.commit()
        self.consumer.commit()

    def _finalize_run(self, status: str, error: Optional[str] = None) -> None:
        # Write manifest + _SUCCESS per touched table folder
        touched = set()
        for rel in self.writer.files_written:
            parts = rel.split("/")
            # cdc/source=.../table=.../ingestion_date=.../run_id=.../events_...
            if len(parts) >= 5:
                source = parts[1].split("=", 1)[1]
                table = parts[2].split("=", 1)[1]
                touched.add((source, table))

        offsets_summary: Dict[str, Dict[str, int]] = {}
        for tp, off in self._last_offsets.items():
            offsets_summary.setdefault(tp.topic, {})
            offsets_summary[tp.topic][str(tp.partition)] = off

        manifest = {
            "run_id": self.run_id,
            "ingestion_date": self.ingestion_date,
            "status": status,
            "kafka_group_id": self.cfg.cdc.kafka_group_id,
            "topics": self.cfg.cdc.topics,
            "files_written": self.writer.files_written,
            "last_offsets": offsets_summary,
        }
        if error:
            manifest["error"] = error

        for source, table in touched:
            t = BronzeTarget(source=source, table=table, ingestion_date=self.ingestion_date, run_id=self.run_id)
            self.writer.write_manifest(t, manifest)
            if status == "SUCCESS":
                self.writer.mark_success(t)

    def run(self) -> int:
        self._handle_signals()

        self.meta.start_run(self.run_id)
        self.logger.info("CDC writer started")
        self.logger.info(f"Topics: {self.cfg.cdc.topics}")
        self.logger.info(f"Kafka: {self.cfg.cdc.kafka_bootstrap} | Group: {self.cfg.cdc.kafka_group_id}")

        try:
            self.consumer.subscribe(self.cfg.cdc.topics)

            while not self._stop:
                pack = self.consumer.poll(timeout_ms=1000)
                self._seek_from_checkpoints_once()

                for tp, records in pack.items():
                    for r in records:
                        self._buffer_event(
                            topic=r.topic,
                            partition=r.partition,
                            offset=r.offset,
                            ts_ms=r.timestamp,
                            key=r.key,
                            value=r.value,
                        )

                self._flush_if_needed(force=False)

            # graceful shutdown
            self._flush_if_needed(force=True)

            self._finalize_run(status="SUCCESS")
            self.meta.end_run(self.run_id, "SUCCESS", {"message": "completed normally"})
            self.logger.info("CDC writer completed successfully")
            return 0

        except Exception as e:
            # flush what we have, but do not mark _SUCCESS
            try:
                self._flush_if_needed(force=True)
                self._finalize_run(status="FAILED", error=str(e))
            except Exception:
                pass

            self.meta.end_run(self.run_id, "FAILED", {"error": str(e)})
            self.logger.error("CDC writer failed", exc_info=True)
            return 1

        finally:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.meta.close()


def main() -> None:
    rc = CDCWriterApp().run()
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
