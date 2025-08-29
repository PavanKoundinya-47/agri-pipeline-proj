import duckdb
import pandas as pd

def validate_data(df: pd.DataFrame, report_path: str):
    # Nothing to validate → return empty DataFrame
    if df is None or df.empty:
        print("⚠️ No data to validate. Skipping validation.")
        return pd.DataFrame()

    con = duckdb.connect()
    con.register("df", df)

    # --- Type checks ---
    # 1) value must be numeric
    # 2) timestamp column storage type must be VARCHAR or TIMESTAMP (incl. TIMESTAMPTZ)
    # 3) every row must be parsable as TIMESTAMP (NULL or unparseable counted as invalid)
    type_checks = con.execute("""
        SELECT
          SUM(CASE
                WHEN typeof(value) IN ('DOUBLE','FLOAT','DECIMAL','INTEGER','HUGEINT')
                THEN 0 ELSE 1
              END) AS invalid_value_type,
          SUM(CASE
                WHEN typeof(timestamp) IN ('VARCHAR','TIMESTAMP','TIMESTAMPTZ')
                THEN 0 ELSE 1
              END) AS invalid_timestamp_storage_type,
          SUM(CASE
                WHEN timestamp IS NULL OR try_cast(timestamp AS TIMESTAMP) IS NULL
                THEN 1 ELSE 0
              END) AS unparsable_timestamp_rows
        FROM df
    """).fetchdf()

    # --- Range checks (quick profile of observed ranges) ---
    range_checks = con.execute("""
        SELECT reading_type,
               MIN(value) AS min_value,
               MAX(value) AS max_value
        FROM df
        GROUP BY reading_type
    """).fetchdf()

    # --- Missing values per reading_type ---
    missing = con.execute("""
        SELECT reading_type,
               COUNT(*) AS total,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_values
        FROM df
        GROUP BY reading_type
    """).fetchdf()
    # --- Gaps in hourly data (robust to random minutes) ---
    gaps = con.execute("""
        WITH bounds AS (
          SELECT
            sensor_id,
            reading_type,
            date_trunc('hour', MIN(try_cast(timestamp AS TIMESTAMP))) AS min_h,
            date_trunc('hour', MAX(try_cast(timestamp AS TIMESTAMP))) AS max_h
          FROM df
          GROUP BY sensor_id, reading_type
        ),
        expected AS (
          SELECT
            b.sensor_id,
            b.reading_type,
            gs.ts AS hour
          FROM bounds b
          CROSS JOIN generate_series(b.min_h, b.max_h, INTERVAL 1 HOUR) AS gs(ts)
        ),
        actual AS (
          SELECT
            sensor_id,
            reading_type,
            date_trunc('hour', try_cast(timestamp AS TIMESTAMP)) AS hour
          FROM df
          WHERE try_cast(timestamp AS TIMESTAMP) IS NOT NULL
          GROUP BY 1,2,3
        )
        SELECT
          e.sensor_id,
          e.reading_type,
          COUNT(*) AS expected_hours,
          COUNT(a.hour) AS actual_hours,
          (COUNT(*) - COUNT(a.hour)) AS missing_hours
        FROM expected e
        LEFT JOIN actual a
          ON e.sensor_id = a.sensor_id
         AND e.reading_type = a.reading_type
         AND e.hour = a.hour
        GROUP BY e.sensor_id, e.reading_type
        ORDER BY e.sensor_id, e.reading_type
    """).fetchdf()


    # --- Simple profile (per type) ---
    profile = con.execute("""
        SELECT reading_type,
               COUNT(*) AS total,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_values
        FROM df
        GROUP BY reading_type
        ORDER BY reading_type
    """).fetchdf()

    # Save sections into one CSV with headers
    report = {
        "type_checks": type_checks,
        "range_checks": range_checks,
        "missing": missing,
        "gaps": gaps,
        "profile": profile
    }

    with open(report_path, "w") as f:
        for section, data in report.items():
            f.write(f"## {section}\n")
            data.to_csv(f, index=False)
            f.write("\n\n")

    con.close()
    print(f"✅ Data quality report saved at {report_path}")
