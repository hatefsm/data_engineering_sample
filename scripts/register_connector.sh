#!/usr/bin/env bash
set -euo pipefail

curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg_source_connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "source_db",
      "database.port": "5432",
      "database.user": "demo_user",
      "database.password": "demo_pass",
      "database.dbname": "sample_source",

      "topic.prefix": "source_db",
      "schema.include.list": "public",
      "table.include.list": "public.travelers,public.orders",

      "plugin.name": "pgoutput",
      "snapshot.mode": "initial",
      "tombstones.on.delete": "false"
    }
  }' | jq .
