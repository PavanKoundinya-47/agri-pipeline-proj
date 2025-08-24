import os
import pandas as pd
import numpy as np
import pytest

# --- allow running from tests/ or project root
try:
    from pipeline.validation import validate_data
except ModuleNotFoundError:
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
    from pipeline.validation import validate_data


def test_validation_skips_empty_df(tmp_path, caplog):
    report = tmp_path / "dq.csv"
    out = validate_data(pd.DataFrame(), str(report))
    # function should early-return empty df and not create report
    assert isinstance(out, pd.DataFrame)
    assert out.empty
    assert not report.exists()
    assert "No data to validate" in caplog.text


def test_validation_reports_ranges_and_gaps(tmp_path):
    # Build 1 sensor hourly data with one gap
    ts = pd.date_range("2025-06-01 00:00:00", periods=6, freq="h")
    ts = ts.delete(3)  # remove one hour to create a gap
    df = pd.DataFrame({
        "sensor_id": ["sensor_1"] * len(ts),
        "timestamp": ts.astype(str),  # strings as expected
        "reading_type": ["temperature"] * len(ts),
        "value": [20, 21, 19, 18, 22],  # simple values
        "battery_level": [90, 90, 90, 90, 90],
    })

    report_path = tmp_path / "dq.csv"
    profile = validate_data(df, str(report_path))
    assert report_path.exists()

    # Read report text and assert sections are present
    txt = report_path.read_text()
    assert "## type_checks" in txt
    assert "## range_checks" in txt
    assert "## missing" in txt
    assert "## gaps" in txt
    assert "## profile" in txt

    # Gaps section should have at least one missing hour
    assert "missing_hours" in txt
    # Also, type checks should not flag value types as invalid
    assert "invalid_value_type" in txt


def test_validation_detects_bad_types(tmp_path):
    # value as string and timestamp as non-string to trigger type warnings
    df = pd.DataFrame({
        "sensor_id": ["sensor_1"],
        "timestamp": [pd.Timestamp("2025-06-01 00:00:00")],  # not string
        "reading_type": ["humidity"],
        "value": ["oops"],  # string value
        "battery_level": [50.0],
    })

    report_path = tmp_path / "dq_types.csv"
    _ = validate_data(df, str(report_path))
    assert report_path.exists()
    txt = report_path.read_text()
    assert "invalid_value_type" in txt
    assert "invalid_timestamp_type" in txt
