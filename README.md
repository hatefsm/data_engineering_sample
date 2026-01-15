# Data Engineering Sample — CDC → Bronze → Silver (Stage + Current)

## What this repo does
This project is a small end-to-end Change Data Capture (CDC) pipeline. It captures row-level changes from a PostgreSQL database using Debezium and Kafka, writes raw change events into an append-only Bronze data lake (Parquet, per-run folders), then compacts and converts those events into Silver datasets that are easier to query. The pipeline is designed for local execution with Docker, while keeping configuration environment-driven for future cloud portability.

---

## Workflow
1. PostgreSQL is configured for logical replication.
2. Debezium captures database changes and publishes them to Kafka topics.
3. Bronze CDC Writer consumes Kafka topics and writes append-only Parquet files to Bronze storage, while storing Kafka offsets in SQLite.
4. Bronze CDC Compactor merges small Parquet chunks into a single events_compacted.parquet per run.
5. Silver CDC Structurer parses JSON CDC events into typed, queryable stage tables.
6. Silver CDC Merge builds current-state tables from stage events.

---


---

## Component descriptions

### bronze_cdc_writer.py
Consumes CDC messages from Kafka and writes them as append-only Parquet files into Bronze storage. Each execution has its own run_id. Kafka offsets and run metadata are stored in SQLite to support safe restarts and observability.

### bronze_cdc_compactor.py
Reads multiple small Bronze Parquet files from a run and merges them into a single compacted file. This reduces small-file overhead while preserving Bronze immutability.

### silver_cdc_structurer.py
Transforms compacted Bronze CDC JSON payloads into typed Silver stage tables with explicit columns and partitions, making the data easier to validate and query.

### silver_cdc_merge_current.py
Merges Silver stage events into current-state tables by selecting the latest record per primary key. Tracks processed partitions in SQLite to enable incremental processing.

### metadata_store.py
Stores pipeline operational metadata such as Kafka offsets, run status, and processed partitions using SQLite.

### app_config.py
Defines environment-driven configuration using typed structures so pipeline logic remains clean and portable.

### app_logging.py
Provides consistent logging across pipeline stages with per-run log files.

---

## Configuration
All runtime settings are controlled using environment variables:

- KAFKA_BOOTSTRAP
- TOPICS
- BRONZE_BASE_PATH
- METADATA_DB
- LOG_BASE_PATH

---

## Design considerations
- Parquet files are treated as immutable.
- Each run is isolated by run_id.
- SQLite is used for lightweight local metadata persistence.
- Bronze keeps raw truth, Silver provides structured and current views.
- The pipeline is modular and stage-oriented for easy extension.

---

## Running locally
Use docker-compose to start PostgreSQL, Kafka, and Debezium. Then run each Python stage in order:

1. bronze_cdc_writer.py
2. bronze_cdc_compactor.py
3. silver_cdc_structurer.py
4. silver_cdc_merge_current.py


