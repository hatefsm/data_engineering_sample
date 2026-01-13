"""
Reusable logging utilities for ingestion pipelines.

Design:
- One log file per pipeline run.
- Logs go to both console and file.
- Reusable for CDC, snapshot, incremental pipelines.

Log layout:
  <LOG_BASE_PATH>/<pipeline_name>/run_id=<run_id>.log
"""

from __future__ import annotations

import logging
from pathlib import Path


def create_run_logger(pipeline_name: str, run_id: str, log_base_path: Path) -> logging.Logger:
    log_dir = log_base_path / pipeline_name
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"run_id={run_id}.log"

    logger = logging.getLogger(f"{pipeline_name}_{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid duplicate handlers if the module reloads
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
