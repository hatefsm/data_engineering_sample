# src/app/config.py
"""
Centralized configuration for ingestion pipelines.

Design principles:
- Typed dataclasses (frozen) for safety and clarity
- Environment-driven (.env) with sensible defaults
- Reusable across CDC, snapshot, incremental pipelines
"""


from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Optional .env support
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass


# ----------------------------
# Source DB (for snapshot / incremental modes)
# ----------------------------

@dataclass(frozen=True)
class SourceDBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str = "prefer"

    def sqlalchemy_url(self) -> str:
        # psycopg3 driver
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}?sslmode={self.sslmode}"
        )


# ----------------------------
# Output
# ----------------------------

@dataclass(frozen=True)
class OutputConfig:
    writer_type: str              # local | (later: s3 | azure)
    bronze_base_path: Path        # base folder for bronze outputs


# ----------------------------
# CDC / Kafka
# ----------------------------

@dataclass(frozen=True)
class CDCConfig:
    kafka_bootstrap: str
    kafka_group_id: str
    topics: list[str]
    batch_size: int
    flush_seconds: int


# ----------------------------
# Operational config
# ----------------------------

@dataclass(frozen=True)
class OpsConfig:
    metadata_db_path: Path
    log_base_path: Path


# ----------------------------
# App config (single object)
# ----------------------------

@dataclass(frozen=True)
class AppConfig:
    source: SourceDBConfig
    output: OutputConfig
    cdc: CDCConfig
    ops: OpsConfig


def _parse_csv(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def load_config() -> AppConfig:
    return AppConfig(
        source=SourceDBConfig(
            host=os.getenv("SOURCE_DB_HOST", "localhost"),
            port=int(os.getenv("SOURCE_DB_PORT", "5432")),
            dbname=os.getenv("SOURCE_DB_NAME", "sample_source"),
            user=os.getenv("SOURCE_DB_USER", "demo_user"),
            password=os.getenv("SOURCE_DB_PASSWORD", "demo_pass"),
            sslmode=os.getenv("SOURCE_DB_SSLMODE", "prefer"),
        ),
        output=OutputConfig(
            writer_type=os.getenv("WRITER_TYPE", "local"),
            bronze_base_path=Path(os.getenv("BRONZE_BASE_PATH", "./data/bronze")),
        ),
        cdc=CDCConfig(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "cdc_bronze_writer"),
            topics=_parse_csv(os.getenv("TOPICS", "")),
            batch_size=int(os.getenv("BATCH_SIZE", "500")),
            flush_seconds=int(os.getenv("FLUSH_SECONDS", "5")),
        ),
        ops=OpsConfig(
            metadata_db_path=Path(os.getenv("METADATA_DB", "./data/metadata/pipeline_meta.sqlite")),
            log_base_path=Path(os.getenv("LOG_BASE_PATH", "./data/logs")),
        ),
    )
