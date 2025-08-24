from __future__ import annotations
import os
import duckdb as ddb
import pandas as pd
from typing import List
from .utils import EXPECTED_SCHEMA, get_logger,CHECKPOINT_PATH, load_checkpoint, save_checkpoint

logger = get_logger("ingestion")

def describe_parquet(file_path: str) -> pd.DataFrame:
    con = ddb.connect()
    try:
        return con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
    except Exception as e:
        logger.error(f'Exception While Describing {file_path}')
    finally:
        con.close()

def read_parquet_duckdb(file_path: str) -> pd.DataFrame:
    con = ddb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
        return df
    except Exception as e:
        logger.error(f'Exception While Reading {file_path}')
    finally:
        con.close()

def schema_matches(df: pd.DataFrame) -> bool:
    expected = set(EXPECTED_SCHEMA.keys())
    present = set(df.columns)
    return expected.issubset(present)

# def ingest_data(raw_dir: str) -> pd.DataFrame:
    os.makedirs(raw_dir, exist_ok=True)
    files = sorted([os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".parquet")])
    state = load_checkpoint()
    new_files = [f for f in files if f not in state.get("processed_files", [])]

    stats = {"files_read": 0, "records_total": 0, "records_failed": 0}
    batches: List[pd.DataFrame] = []

    for fp in new_files:
        try:
            _ = describe_parquet(fp)
            df = read_parquet_duckdb(fp)
            if not schema_matches(df):
                logger.error(f"Schema mismatch in {fp}")
                stats["records_failed"] += len(df)
                continue
            df = df.dropna(subset=["sensor_id", "timestamp", "reading_type", "value"])
            df["source_file"] = os.path.basename(fp)
            batches.append(df)
            stats["files_read"] += 1
            stats["records_total"] += len(df)
            logger.info(f"Ingested {len(df)} records from {fp}")
        except Exception as e:
            logger.exception(f"Failed to ingest {fp}: {e}")
            continue

    if batches:
        full = pd.concat(batches, ignore_index=True)
    else:
        full = pd.DataFrame(columns=list(EXPECTED_SCHEMA.keys()) + ["source_file"])

    state["processed_files"] = state.get("processed_files", []) + new_files
    save_checkpoint(state)
    logger.info(f"Ingestion summary: {stats}")
    return full



def ingest_data(raw_dir: str) -> pd.DataFrame:
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