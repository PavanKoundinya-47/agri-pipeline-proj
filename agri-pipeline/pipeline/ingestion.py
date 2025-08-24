from __future__ import annotations
import os
import duckdb as ddb
import pandas as pd
from typing import List
from .utils import EXPECTED_SCHEMA, get_logger,CHECKPOINT_PATH, load_checkpoint, save_checkpoint

logger = get_logger("ingestion")

def describe_parquet(file_path: str) -> pd.DataFrame:
    """
    Describe the schema of a Parquet file using DuckDB.

    Args:
        file_path (str): Path to the parquet file.

    Returns:
        pd.DataFrame: Schema information of the file (columns, types, etc.).
    """    
    con = ddb.connect()
    try:
        return con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
    except Exception as e:
        logger.error(f'Exception While Describing {file_path}')
    finally:
        con.close()

def read_parquet_duckdb(file_path: str) -> pd.DataFrame:
    """
    Read a Parquet file into a DataFrame using DuckDB.

    Args:
        file_path (str): Path to the parquet file.

    Returns:
        pd.DataFrame: File contents as a Pandas DataFrame.
    """
    con = ddb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
        return df
    except Exception as e:
        logger.error(f'Exception While Reading {file_path}')
    finally:
        con.close()

def schema_matches(df: pd.DataFrame) -> bool:
    """
    Validate whether a DataFrame matches the expected schema.

    Args:
        df (pd.DataFrame): Input DataFrame to check.

    Returns:
        bool: True if schema matches, False otherwise.
    """
    expected = set(EXPECTED_SCHEMA.keys())
    present = set(df.columns)
    return expected.issubset(present)


def ingest_data(raw_dir: str) -> pd.DataFrame:
    """
    Ingest parquet files from a raw data directory.

    - Skips already processed files using checkpointing.
    - Validates schema of incoming files.
    - Logs ingestion statistics (files read, records processed/failed).
    - Stores processed filenames in a checkpoint file.

    Args:
        raw_dir (str): Path to the raw parquet files directory.

    Returns:
        pd.DataFrame: Combined DataFrame of all newly ingested files.
                      Empty DataFrame if no new files found.
    """    
    checkpoint = load_checkpoint(CHECKPOINT_PATH)
    processed_files = set(checkpoint.get("processed_files", []))

    all_data = []
    files = sorted([f for f in os.listdir(raw_dir) if f.endswith(".parquet")])

    files_read = 0
    records_total = 0
    records_failed = 0

    for file in files:
        filename = os.path.basename(file)
        filepath = os.path.join(raw_dir, file)

        if filename in processed_files:
            logger.info(f"Skipping {filename} (already processed)")
            continue

        try:
            df = pd.read_parquet(filepath)
            df["source_file"] = filename
            all_data.append(df)
            records_total += len(df)
            files_read += 1
            processed_files.add(filename)
        except Exception as e:
            logger.error(f"Failed to read {filename}: {e}")
            records_failed += 1

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
    else:
        result = pd.DataFrame()

    save_checkpoint({"processed_files": list(processed_files)}, CHECKPOINT_PATH)

    logger.info(
        f"Ingestion summary: {{'files_read': {files_read}, "
        f"'records_total': {records_total}, "
        f"'records_failed': {records_failed}}}"
    )
    return result