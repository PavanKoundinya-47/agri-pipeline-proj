
import duckdb
import pandas as pd

def validate_data(df: pd.DataFrame, report_path: str):

    if df.empty:
        print("⚠️ No data to validate. Skipping validation.")
        return pd.DataFrame()

    con = duckdb.connect()

    con.register("df", df)


    # --- Type checks ---
    type_checks = con.execute("""
        SELECT
          SUM(CASE WHEN typeof(value) IN ('DOUBLE','FLOAT','DECIMAL') THEN 0 ELSE 1 END) AS invalid_value_type,
          SUM(CASE WHEN typeof(timestamp) != 'VARCHAR' THEN 1 ELSE 0 END) AS invalid_timestamp_type
        FROM df
    """).fetchdf()

    # --- Range checks (example ranges) ---
    range_checks = con.execute("""
        SELECT reading_type,
               MIN(value) AS min_value,
               MAX(value) AS max_value
        FROM df
        GROUP BY reading_type
    """).fetchdf()

    # --- Missing values ---
    missing = con.execute("""
        SELECT reading_type,
               COUNT(*) AS total,
               SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_values
        FROM df
        GROUP BY reading_type
    """).fetchdf()

    # --- Gaps in hourly data ---
    gaps = con.execute("""
        WITH bounds AS (
          SELECT sensor_id, reading_type,
                MIN(CAST(timestamp AS TIMESTAMP)) AS min_ts,
                MAX(CAST(timestamp AS TIMESTAMP)) AS max_ts
          FROM df
          GROUP BY sensor_id, reading_type
        ),
        expected AS (
          SELECT b.sensor_id, b.reading_type, t.ts
          FROM bounds b,
              UNNEST(generate_series(b.min_ts, b.max_ts, INTERVAL 1 HOUR)) AS t(ts)
        ),
        actual AS (
          SELECT sensor_id, reading_type, CAST(timestamp AS TIMESTAMP) AS ts
          FROM df
        )
        SELECT e.sensor_id, e.reading_type,
              COUNT(*) AS expected_hours,
              COUNT(a.ts) AS actual_hours,
              (COUNT(*) - COUNT(a.ts)) AS missing_hours
        FROM expected e
        LEFT JOIN actual a
          ON e.sensor_id = a.sensor_id
        AND e.reading_type = a.reading_type
        AND e.ts = a.ts
        GROUP BY e.sensor_id, e.reading_type
        ORDER BY e.sensor_id, e.reading_type
    """).fetchdf()


    # --- Profile ---
    profile = con.execute("""
          SELECT reading_type,
                COUNT(*) AS total,
                SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_values
          FROM df
          GROUP BY reading_type
    """).fetchdf()

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
