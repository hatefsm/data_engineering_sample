from __future__ import annotations

from pathlib import Path

import pyarrow
import pyarrow.parquet

from app.bronze.compactor import CompactorOptions, compact_one_run


def _write_part(fp: Path, n: int) -> None:
    t = pyarrow.table(
        {
            "ingest_ts": ["2026-01-14T00:00:00Z"] * n,
            "raw_json": [f'{{"i":{i}}}' for i in range(n)],
        }
    )
    pyarrow.parquet.write_table(t, fp, compression="snappy")


def test_compacts_successful_run(tmp_path: Path):
    # Make a fake run folder matching your layout
    run_dir = tmp_path / "cdc" / "source=src" / "table=t" / "ingestion_date=2026-01-14" / "run_id=abc"
    run_dir.mkdir(parents=True)

    # Writer success marker
    (run_dir / "_SUCCESS").write_text("", encoding="utf-8")

    # Two part files
    _write_part(run_dir / "events_00001.parquet", 3)
    _write_part(run_dir / "events_00002.parquet", 2)

    class DummyLogger:
        def info(self, *_args, **_kwargs): pass
        def warning(self, *_args, **_kwargs): pass

    compact_one_run(run_dir, CompactorOptions(prune_parts=False, only_date=None), DummyLogger())

    out_fp = run_dir / "events_compacted.parquet"
    assert out_fp.exists()
    assert (run_dir / "_COMPACTED").exists()

    out = pyarrow.parquet.read_table(out_fp)
    assert out.num_rows == 5


def test_skips_without_success(tmp_path: Path):
    run_dir = tmp_path / "cdc" / "source=src" / "table=t" / "ingestion_date=2026-01-14" / "run_id=abc"
    run_dir.mkdir(parents=True)

    _write_part(run_dir / "events_00001.parquet", 1)

    class DummyLogger:
        def info(self, *_args, **_kwargs): pass
        def warning(self, *_args, **_kwargs): pass

    compact_one_run(run_dir, CompactorOptions(prune_parts=False, only_date=None), DummyLogger())
    assert not (run_dir / "events_compacted.parquet").exists()

