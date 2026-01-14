from __future__ import annotations

import subprocess
from pathlib import Path


def run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.returncode == 0, f"Command failed:\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
    return p.stdout


def test_stage_users_and_orders(tmp_path):
    # 1) Generate test bronze compacted files into the real project folders (data/bronze)
    run(["python", "scripts/make_test_compacted_bronze.py", "--date", "2026-01-14", "--users", "10", "--orders", "20"])

    # 2) Run stage builder for users
    users_in = "data/bronze/cdc/source=source_db/table=users/ingestion_date=2026-01-14/run_id=TEST/events_compacted.parquet"
    out_users = run(["python", "src/app/silver_cdc_stage_builder.py", "--table", "users", "--input-file", users_in])
    assert "Stage rows:" in out_users

    # 3) Run stage builder for orders
    orders_in = "data/bronze/cdc/source=source_db/table=orders/ingestion_date=2026-01-14/run_id=TEST/events_compacted.parquet"
    out_orders = run(["python", "src/app/silver_cdc_stage_builder.py", "--table", "orders", "--input-file", orders_in])
    assert "Stage rows:" in out_orders

    # 4) Check outputs exist (at least one partition written)
    users_out_dir = Path("data/silver/stage_events/table=users")
    orders_out_dir = Path("data/silver/stage_events/table=orders")
    assert users_out_dir.exists(), "users stage output dir not created"
    assert orders_out_dir.exists(), "orders stage output dir not created"

