from __future__ import annotations
import os
import pandas as pd

def store_data(df: pd.DataFrame, processed_dir: str, partition_by_sensor: bool=True) -> None:
    if df is None or len(df)==0:
        return
    for (date, sensor_id), g in df.groupby(["date","sensor_id"]):
        base = os.path.join(processed_dir, f"{date}(Processed)")
        if partition_by_sensor:
            base = os.path.join(base, f"sensor_id={sensor_id}")
        os.makedirs(base, exist_ok=True)
        for rt, gg in g.groupby("reading_type"):
            out = os.path.join(base, f"{rt}.parquet")
            gg.to_parquet(out, index=False, compression="snappy")
