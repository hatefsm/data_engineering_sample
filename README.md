This repository contains a hands-on data engineering sample project demonstrating a modern, modular, and production-oriented pipeline design using Python, Docker, Kafka, PostgreSQL, and structured data-lake concepts.

The goal of this project is to showcase realistic data-engineering practices, not just scripts.

🎯 Project Objectives

Build an end-to-end ingestion and validation pipeline

Apply schema enforcement and data quality checks

Store data in a layered bronze structure

Keep the project clean, reproducible, and environment-independent

Follow professional repository and Git practices

🏗 Architecture Overview
Source CSV / DB
        ↓
Ingestion Layer (Python)
        ↓
Schema Enforcement
        ↓
Validation Rules
        ↓
Bronze Storage Layer
        ↓
Future: Silver / Gold Modeling


Current scope focuses on Bronze layer ingestion and validation.

📁 Project Structure
data_engineering_sample/
│
├── bronze_ingest.py
├── validate_bronze.py
├── etl_booking_ingest_validate.py
│
├── data/               # Ignored in git (local storage)
├── .env                # Ignored (credentials)
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── README.md

⚙️ Configuration

Environment variables are managed via .env:

SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=5433
SOURCE_DB_NAME=sample_source
SOURCE_DB_USER=demo_user
SOURCE_DB_PASSWORD=demo_pass
SOURCE_DB_SSLMODE=prefer

BRONZE_BASE_PATH=./data/bronze
WRITER_TYPE=local

🚀 Running the Pipeline

Example execution:

python etl_booking_ingest_validate.py


The pipeline will:

Load source CSV / DB data

Enforce schema

Validate records

Split valid and invalid rows

Write outputs to bronze storage

✅ Validation Logic

Validation ensures:

Required fields are present

Data types are respected

Business constraints are applied

Invalid records are isolated for quarantine

This mirrors real production data-quality pipelines.

🧪 Design Principles

Idempotent processing

Clear separation of responsibilities

Environment-agnostic execution

Easy transition to cloud storage (S3 / Azure / GCS)

Future-ready for orchestration (Airflow / Prefect / Dagster)

🔮 Future Extensions

Planned next steps:

Silver transformation layer

Gold analytics marts

Kafka streaming ingestion

dbt transformations

Cloud deployment

CI/CD integration

👤 Author

Hatef Seyed Mahdavi
Data Engineering / Data Architecture

📌 Purpose

This repository is designed as:

A learning reference

A portfolio project

A foundation for scalable data platform design
