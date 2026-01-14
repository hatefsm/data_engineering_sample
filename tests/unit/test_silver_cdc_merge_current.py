from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd


def run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    assert p.returncode == 0, (
        "Command failed\n"
        f"CMD: {' '.join(cmd)}\n"
        f"STDOUT:\n{p.stdout}\n"
        f"STDERR:\n{p.stderr}\n"
    )
    return p.stdout


def test_incremental_merge_users_and_orders():
    # ---- 1) Generate test Bronze compacted files for both tables
    run([
        "python", "scripts/make_test_compacted_bronze.py",
        "--date", "2026-01-14",
        "--users", "20",
        "--orders", "60",
        "--user-updates", "2",
        "--order-updates", "2",
    ])

    # ---- 2) Run Silver stage builder for both tables
    users_in = "data/bronze/cdc/source=source_db/table=users/ingestion_date=2026-01-14/run_id=TEST/events_compacted.parquet"
    orders_in = "data/bronze/cdc/source=source_db/table=orders/ingestion_date=2026-01-14/run_id=TEST/events_compacted.parquet"

    run(["python", "src/app/silver_cdc_stage_builder.py", "--table", "users", "--input-file", users_in])
    run(["python", "src/app/silver_cdc_stage_builder.py", "--table", "orders", "--input-file", orders_in])

    # ---- 3) Run incremental merge (current state) for both tables
    out_users = run(["python", "src/app/silver_cdc_merge_current.py", "--table", "users", "--force"])
    out_orders = run(["python", "src/app/silver_cdc_merge_current.py", "--table", "orders", "--force"])
    assert "Wrote current:" in out_users
    assert "Wrote current:" in out_orders

    # ---- 4) Verify outputs exist
    users_current_fp = Path("data/silver/current/table=users/current.parquet")
    orders_current_fp = Path("data/silver/current/table=orders/current.parquet")
    assert users_current_fp.exists(), "users current.parquet not created"
    assert orders_current_fp.exists(), "orders current.parquet not created"

    # ---- 5) Verify one row per PK
    users_df = pd.read_parquet(users_current_fp)
    assert users_df["user_id"].notna().all()
    assert users_df["user_id"].is_unique

    orders_df = pd.read_parquet(orders_current_fp)
    assert orders_df["order_id"].notna().all()
    assert orders_df["order_id"].is_unique

    # ---- 6) Check checkpoints exist
    ck_users = Path("data/silver/current/_checkpoints/table=users.json")
    ck_orders = Path("data/silver/current/_checkpoints/table=orders.json")
    assert ck_users.exists(), "users checkpoint not created"
    assert ck_orders.exists(), "orders checkpoint not created"

