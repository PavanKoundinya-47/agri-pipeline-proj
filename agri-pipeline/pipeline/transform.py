from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import timedelta
import pytz
from .utils import CALIBRATION, EXPECTED_RANGES

IST = pytz.timezone("Asia/Kolkata")

def _apply_calibration(df: pd.DataFrame) -> pd.DataFrame:
    def calibrate(row):
        params = CALIBRATION.get(row["reading_type"], {"multiplier":1.0, "offset":0.0})
        return row["value"] * params["multiplier"] + params["offset"]
    df["value_calibrated"] = df.apply(calibrate, axis=1)
    return df

def _flag_anomaly(df: pd.DataFrame) -> pd.DataFrame:
    def is_anom(row):
        r = EXPECTED_RANGES.get(row["reading_type"])
        if r is None or pd.isna(row["value_calibrated"]):
            return False
        return (row["value_calibrated"] < r["min"]) or (row["value_calibrated"] > r["max"])
    df["anomalous_reading"] = df.apply(is_anom, axis=1)
    return df

def _zscore_outliers(df: pd.DataFrame) -> pd.DataFrame:
    def zscore(s):
        if s.std(ddof=0) == 0 or len(s) < 3:
            return pd.Series([0]*len(s), index=s.index)
        return (s - s.mean())/s.std(ddof=0)
    df["zscore"] = df.groupby("reading_type")["value_calibrated"].transform(zscore)
    def correct(group):
        mean = group["value_calibrated"].mean()
        std = group["value_calibrated"].std(ddof=0)
        if std == 0 or np.isnan(std):
            return group["value_calibrated"]
        hi = mean + 3*std
        lo = mean - 3*std
        return group["value_calibrated"].clip(lower=lo, upper=hi)
    df["value_corrected"] = df.groupby("reading_type", group_keys=False).apply(correct)
    return df

def _timestamp_processing(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["timestamp_ist"] = ts.dt.tz_convert(IST)
    df["timestamp_iso"] = df["timestamp_ist"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    df["date"] = df["timestamp_ist"].dt.date.astype(str)
    df["hour"] = df["timestamp_ist"].dt.floor("H")
    return df
def _aggregations(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby(["sensor_id", "reading_type", "date"])["value_corrected"]
          .mean()
          .rename("daily_avg")
          .reset_index()
    )
    df = df.merge(daily, on=["sensor_id","reading_type","date"], how="left")

    daily_series = (
        df.drop_duplicates(["sensor_id","reading_type","date"])
          .sort_values(["sensor_id","reading_type","date"])
          .copy()
    )
    daily_series["date"] = pd.to_datetime(daily_series["date"])

    daily_series["rolling_7d"] = (
        daily_series.groupby(["sensor_id", "reading_type"])["daily_avg"]
        .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )

    daily_series["date"] = daily_series["date"].dt.date.astype(str)

    df = df.merge(
        daily_series[["sensor_id","reading_type","date","rolling_7d"]],
        on=["sensor_id","reading_type","date"], how="left"
    )
    return df

def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or len(raw_df)==0:
        return raw_df.copy() if raw_df is not None else pd.DataFrame()
    df = raw_df.copy()
    df = df.drop_duplicates(subset=["sensor_id","timestamp","reading_type"])
    df = df.dropna(subset=["sensor_id","timestamp","reading_type","value"])
    if "battery_level" in df.columns:
        if df["battery_level"].isna().any():
            df["battery_level"] = df["battery_level"].fillna(method="ffill").fillna(method="bfill").fillna(df["battery_level"].mean())
    df = _apply_calibration(df)
    df = _zscore_outliers(df)
    df = _flag_anomaly(df)
    df = _timestamp_processing(df)
    df = _aggregations(df)
    return df
