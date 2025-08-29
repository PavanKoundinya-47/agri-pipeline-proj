from __future__ import annotations
import os
import duckdb as ddb
import pandas as pd
from typing import List
from .utils import EXPECTED_SCHEMA, get_logger,CHECKPOINT_PATH, load_checkpoint, save_checkpoint

logger = get_logger("ingestion")


def ingest_data(raw_dir: str) -> pd.DataFrame:
    """
    Ingest parquet files from a raw data directory.

    - Skips already processed files using checkpointing.
    - Uses DuckDB to inspect schemas and run validation queries.
    - Logs ingestion statistics (files read, records processed/failed).
    - Handles corrupt/unreadable files and schema mismatches.
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

    con = ddb.connect()

    for file in files:
        filename = os.path.basename(file)
        filepath = os.path.join(raw_dir, file)

        if filename in processed_files:
            logger.info(f"Skipping {filename} (already processed)")
            continue

        try:
            # Inspect schema using DuckDB
            schema_df = con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{filepath}')").fetchdf()
            actual_cols = set(schema_df["column_name"].str.lower())
            expected_cols = set(EXPECTED_SCHEMA.keys())

            # Schema validation
            if actual_cols != expected_cols:
                logger.error(f"Schema mismatch in {filename}. Expected {expected_cols}, got {actual_cols}")
                records_failed += 1
                continue

            # Run validation queries inside DuckDB
            stats = con.execute(f"""
                SELECT 
                    COUNT(*) AS total,
                    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_values,
                    SUM(CASE WHEN battery_level IS NULL THEN 1 ELSE 0 END) AS null_battery
                FROM parquet_scan('{filepath}')
            """).fetchdf().iloc[0].to_dict()

            logger.info(f"Validation for {filename}: {stats}")

            # Load into pandas
            df = pd.read_parquet(filepath)
            df["source_file"] = filename

            all_data.append(df)
            records_total += len(df)
            files_read += 1
            processed_files.add(filename)

        except Exception as e:
            logger.error(f"Failed to read {filename}: {e}")
            records_failed += 1

    con.close()

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
    else:
        result = pd.DataFrame()

    save_checkpoint({"processed_files": list(processed_files)}, CHECKPOINT_PATH)

    # Summary with DuckDB SQL aggregation
    if not result.empty:
        con = ddb.connect()
        con.register("df", result)
        summary = con.execute("""
            SELECT COUNT(DISTINCT source_file) AS files,
                   COUNT(*) AS records,
                   SUM(CASE WHEN value IS NULL OR battery_level IS NULL THEN 1 ELSE 0 END) AS invalid_rows
            FROM df
        """).fetchdf().iloc[0].to_dict()
        con.close()
    else:
        summary = {"files": 0, "records": 0, "invalid_rows": 0}

    logger.info(
        f"Ingestion summary: {{'files_read': {files_read}, "
        f"'records_total': {records_total}, "
        f"'records_failed': {records_failed}, "
        f"'duckdb_summary': {summary}}}"
    )
    
    return result