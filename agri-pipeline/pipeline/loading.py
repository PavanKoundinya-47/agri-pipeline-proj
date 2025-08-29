from __future__ import annotations
import os
import pandas as pd
from .utils import get_logger

logger = get_logger("ingestion")

def store_data(df: pd.DataFrame, processed_dir: str, partition_by_sensor: bool=True) -> None:
    """
    Save processed sensor data into partitioned Parquet files.

    - Groups data by `date` and `sensor_id`.
    - Creates directory structure under `processed_dir`:
        <date>(Processed)/[sensor_id=<id>/]<reading_type>.parquet
    - Optionally partitions by sensor (default: True).
    - Uses Snappy compression.

    Args:
        df (pd.DataFrame): Transformed dataset with `date`, `sensor_id`, and `reading_type`.
        processed_dir (str): Base directory for processed outputs.
        partition_by_sensor (bool, optional): If True, creates sensor-level subfolders.
                                              Defaults to True.

    Returns:
        None. Writes Parquet files to disk.
    """
    if df is None or len(df)==0:
        return
    for (date, sensor_id), g in df.groupby(["date","sensor_id"]):
        base = os.path.join(processed_dir, f"{date}(Processed)")
        if partition_by_sensor:
            base = os.path.join(base, f"sensor_id={sensor_id}")
        os.makedirs(base, exist_ok=True)
        os.chmod(base, 0o777)
        for rt, gg in g.groupby("reading_type"):
            out = os.path.join(base, f"{rt}.parquet")
            gg.to_parquet(out, index=False, compression="snappy")
            os.chmod(out, 0o666) 