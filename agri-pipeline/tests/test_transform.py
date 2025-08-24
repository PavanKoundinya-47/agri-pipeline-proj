import pandas as pd
import numpy as np
import pytest

# --- allow running from tests/ or project root
try:
    from pipeline.transform import transform_data
    from pipeline.utils import CALIBRATION, EXPECTED_RANGES
except ModuleNotFoundError:
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
    from pipeline.transform import transform_data
    from pipeline.utils import CALIBRATION, EXPECTED_RANGES


def _base_df():
    data = [
        # good temp
        ("sensor_1", "2025-06-01T01:00:00", "temperature", 25.0, 80.0),
        # duplicate row (should be dropped)
        ("sensor_1", "2025-06-01T01:00:00", "temperature", 25.0, 80.0),
        # humidity outlier (should be flagged)
        ("sensor_1", "2025-06-01T02:00:00", "humidity", 150.0, 79.0),
        # missing battery level (filled)
        ("sensor_2", "2025-06-02T01:00:00", "temperature", 30.0, None),
        # multiple days to exercise rolling 7d
        ("sensor_1", "2025-06-03T01:00:00", "temperature", 26.0, 75.0),
        ("sensor_1", "2025-06-04T01:00:00", "temperature", 24.0, 74.0),
    ]
    return pd.DataFrame(data, columns=["sensor_id","timestamp","reading_type","value","battery_level"])


def test_transform_columns_and_types():
    df = _base_df()
    out = transform_data(df)

    must_have = [
        "timestamp_iso","timestamp_ist","date","hour",
        "value", "battery_level", "reading_type", "sensor_id",
        "anomalous_reading", "daily_avg", "rolling_7d"
    ]
    for c in must_have:
        assert c in out.columns

    # battery_level should be filled (no nulls)
    assert out["battery_level"].isna().sum() == 0


def test_calibration_and_anomaly_logic():
    df = _base_df()
    out = transform_data(df)

    # Find a temperature row and verify calibration used (approx)
    temp_row = out[(out["reading_type"]=="temperature") & (out["timestamp_iso"].str.startswith("2025-06-01"))].iloc[0]
    mult = CALIBRATION["temperature"]["multiplier"]
    off = CALIBRATION["temperature"]["offset"]
    expected = 25.0 * mult + off
    assert np.isclose(temp_row["value"], expected, rtol=1e-6, atol=1e-6)

    # Humidity 150 should be anomalous (outside 0..100)
    hum = out[(out["reading_type"]=="humidity") & (out["timestamp_iso"].str.startswith("2025-06-01T02:00:00"))]
    assert not hum.empty
    assert bool(hum.iloc[0]["anomalous_reading"]) is True


def test_rolling_7d():
    df = _base_df()
    out = transform_data(df)

    # collapse to one row per (sensor, reading_type, date) to read rolling_7d
    daily = (out[["sensor_id","reading_type","date","daily_avg","rolling_7d"]]
             .drop_duplicates(["sensor_id","reading_type","date"])
             .sort_values(["sensor_id","reading_type","date"]))

    # pick the last day for sensor_1 / temperature
    tail = daily[(daily.sensor_id=="sensor_1") & (daily.reading_type=="temperature")].tail(1).iloc[0]
    # rolling_7d should be mean of available daily_avg values up to that date
    block = daily[(daily.sensor_id=="sensor_1") & (daily.reading_type=="temperature")]
    expected = block["daily_avg"].mean()
    assert np.isclose(tail["rolling_7d"], expected, rtol=1e-6, atol=1e-6)
