import os
import pandas as pd
import numpy as np
import pytest

# --- allow running from tests/ or project root
try:
    from pipeline.loading import store_data
except ModuleNotFoundError:
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
    from pipeline.loading import store_data


def _df_for_loading():
    # two dates, two sensors
    return pd.DataFrame({
        "sensor_id": ["sensor_1","sensor_2","sensor_1","sensor_2"],
        "timestamp": ["2025-06-01T01:00:00","2025-06-01T02:00:00","2025-06-02T01:00:00","2025-06-02T02:00:00"],
        "reading_type": ["temperature","humidity","temperature","humidity"],
        "value": [20.0, 55.0, 22.0, 60.0],
        "battery_level": [90.0, 80.0, 85.0, 75.0],
        "date": ["2025-06-01","2025-06-01","2025-06-02","2025-06-02"],
    })


def test_store_data_writes_partitions(tmp_path):
    outdir = tmp_path / "processed"
    df = _df_for_loading()
    # most store_data signatures are (df, out_dir, ...)
    store_data(df, str(outdir))

    # Expect partition folders by date
    d1 = outdir / "date=2025-06-01"
    d2 = outdir / "date=2025-06-02"
    assert d1.exists() and d1.is_dir()
    assert d2.exists() and d2.is_dir()

    # parquet files exist in each partition
    assert any(p.suffix == ".parquet" for p in d1.iterdir())
    assert any(p.suffix == ".parquet" for p in d2.iterdir())


def test_store_data_handles_empty_df(tmp_path):
    outdir = tmp_path / "processed_empty"
    store_data(pd.DataFrame(), str(outdir))
    # no directory should be created (or created but empty), both acceptable —
    # assert no parquet files were produced
    if outdir.exists():
        assert not any(p.suffix == ".parquet" for p in outdir.rglob("*"))
