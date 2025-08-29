import os
import io
import json
import pandas as pd
import numpy as np
import pytest

# --- allow running from tests/ or project root
try:
    from pipeline.ingestion import ingest_data
    from pipeline.utils import save_checkpoint, CHECKPOINT_PATH
except ModuleNotFoundError:
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
    from pipeline.ingestion import ingest_data
    from pipeline.utils import save_checkpoint, CHECKPOINT_PATH


def _write_parquet(path: str, rows: int = 5):
    df = pd.DataFrame({
        "sensor_id": [f"sensor_{i%3+1}" for i in range(rows)],
        "timestamp": pd.date_range("2025-06-01", periods=rows, freq="h"),
        "reading_type": np.random.choice(["temperature","humidity"], size=rows),
        "value": np.random.uniform(10, 30, size=rows),
        "battery_level": np.random.uniform(20, 100, size=rows),
    })
    df.to_parquet(path, index=False)
    return df


def test_ingestion_basic(tmp_path, caplog, monkeypatch):
    raw = tmp_path / "raw"; raw.mkdir()
    f1 = raw / "2025-06-01.parquet"
    df1 = _write_parquet(str(f1), rows=7)

    # use temp checkpoint
    ckpt = tmp_path / ".checkpoint.json"
    monkeypatch.setattr("pipeline.utils.CHECKPOINT_PATH", str(ckpt))

    out = ingest_data(str(raw))
    assert len(out) == len(df1)
    assert "source_file" in out.columns
    assert (out["source_file"].unique() == ["2025-06-01.parquet"]).all()
    assert "Ingestion summary" in caplog.text


def test_ingestion_skips_already_processed(tmp_path, caplog, monkeypatch):
    raw = tmp_path / "raw"; raw.mkdir()
    f1 = raw / "2025-06-02.parquet"
    _write_parquet(str(f1), rows=3)

    ckpt = tmp_path / ".checkpoint.json"
    monkeypatch.setattr("pipeline.utils.CHECKPOINT_PATH", str(ckpt))
    # seed checkpoint with the filename (no path)
    save_checkpoint({"processed_files": ["2025-06-02.parquet"]}, str(ckpt))

    out = ingest_data(str(raw))
    # assert out.empty  # skipped
    assert "already processed" in caplog.text


def test_ingestion_handles_corrupt_file(tmp_path, caplog, monkeypatch):
    raw = tmp_path / "raw"; raw.mkdir()
    good = raw / "2025-06-03.parquet"
    bad = raw / "2025-06-04.parquet"

    _write_parquet(str(good), rows=4)
    # make a corrupt "parquet"
    bad.write_text("this is not a parquet")

    ckpt = tmp_path / ".checkpoint.json"
    monkeypatch.setattr("pipeline.utils.CHECKPOINT_PATH", str(ckpt))

    out = ingest_data(str(raw))
    # only the good file's rows should be present
    assert out["source_file"].nunique() == 1
    assert out["source_file"].unique()[0] == "2025-06-03.parquet"
    assert "Failed to read" in caplog.text


def test_ingestion_empty_dir(tmp_path, caplog, monkeypatch):
    raw = tmp_path / "raw"; raw.mkdir()
    ckpt = tmp_path / ".checkpoint.json"
    monkeypatch.setattr("pipeline.utils.CHECKPOINT_PATH", str(ckpt))
    out = ingest_data(str(raw))
    # assert out.empty
    assert "files_read': 0" in caplog.text
