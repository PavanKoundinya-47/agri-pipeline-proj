# 🌱 Agricultural Sensor Data Pipeline

## 📌 Overview
This project implements a **production-grade data pipeline** for agricultural IoT sensor data.  
It ingests raw sensor data, transforms & enriches it, validates data quality, and stores it in an optimized format for downstream analytics.

### Data Sources
Sensors collect:
- 🌡 Temperature  
- 💧 Humidity  
- 🌱 Soil Moisture  
- ☀️ Light Intensity  
- 🔋 Battery Levels  

Raw files are provided daily in **Parquet format** (e.g., `data/raw/2025-06-01.parquet`).

---

## ⚙️ Pipeline Architecture

1. **Ingestion (`pipeline/ingestion.py`)**
   - Reads daily `.parquet` files from `data/raw/`
   - Uses **DuckDB** to inspect schema & run validations
   - Logs ingestion summary: files read, records processed/skipped
   - Supports **checkpointing** (avoids re-processing the same file)

2. **Transformation (`pipeline/transform.py`)**
   - Data cleaning:
     - Drops duplicates
     - Fills missing values (`ffill`, `bfill`, mean for `battery_level`)
     - Detects & flags outliers (`z-score > 3`)
   - Derived fields:
     - Daily averages per sensor & type
     - 7-day rolling averages
     - Anomaly flag if value is out of expected range
   - Calibration: applies correction formula per sensor type  
     ```
     corrected_value = raw_value * multiplier + offset
     ```
   - Timestamp normalization:
     - Converts to ISO 8601
     - Converts to **IST (UTC+5:30)**

3. **Validation (`pipeline/validation.py`)**
   - Type checks (e.g., numeric values, valid timestamps)
   - Range checks (expected ranges per sensor type)
   - Missing data statistics
   - Gap detection: missing hourly data (DuckDB `generate_series`)
   - Profiling (% anomalies, % missing)
   - Saves report as `reports/data_quality_report.csv`

4. **Loading (`pipeline/loading.py`)**
   - Stores cleaned data in `data/processed/`
   - **Partitioned** by date (and optionally by sensor_id)
   - Compressed **Parquet** format optimized for analytics

4. **Synthetic Data Generator (`agri-pipeline/sample_data_generator.py`)**
   - Creates sample raw sensor data in .parquet format under `data/raw/`
   - **Configurable** 

        Number of sensors (NUM_SENSORS)

        Number of days (DAYS)

        Reading types & expected ranges
   - Generates files like:

        data/raw/2025-07-01.parquet

        data/raw/2025-07-02.parquet


---

## 🛠 🐳 Docker Setup [Build & run]

### Clone & Install
```bash
git clone https://github.com/PavanKoundinya-47/agri-pipeline-proj.git
```

### Build the image
```bash
cd agri-pipeline-proj
sudo docker build -t agri-pipeline .
```

### Run Tests
Unit test coverage:
```bash
sudo docker run --rm agri-pipeline pytest -v --cov=pipeline tests/
```
Unit tests:
```bash
sudo docker run --rm agri-pipeline pytest -v tests/
```

### Run the pipeline
```bash
sudo docker run -v $(pwd)/data:/app/data -v $(pwd)/reports:/app/reports agri-pipeline
```




### 📊 Calibration & Anomaly Logic
Calibration:

    Each reading type has a multiplier & offset applied to raw values:

    | Reading Type    | Multiplier | Offset |
    |-----------------|------------|--------|
    | Temperature     | 1.01       | -0.2   |
    | Humidity        | 1.00       | 0.0    |
    | Soil Moisture   | 0.98       | 0.5    |
    | Light Intensity | 1.00       | 0.0    |
    
    Example:
    raw temperature = 25.0
    corrected = 25.0 * 1.01 - 0.2 = 25.05
    
Anomaly Detection:

    A record is flagged as anomalous if value is outside expected range:

    |   Reading Type  | Min |  Max |
    |:---------------:|:---:|:----:|
    | Temperature     | -10 | 60   |
    | Humidity        | 0   | 100  |
    | Soil Moisture   | 0   | 1.0  |
    | Light Intensity | 0   | 2000 |
    | Battery Level   | 0   | 100  |

### 📑 Example Data Quality Report
## range_checks
| reading_type    | min_value | max_value |
|-----------------|-----------|-----------|
| temperature     | -50.0     | 120.0     |
| soil_moisture   | -5.0      | 200.0     |
| light_intensity | -100.0    | 5000.0    |
| humidity        | -10.0     | 150.0     |
| battery_level   | -20.0     | 150.0     |
## missing
| reading_type    | total | missing_values |
|-----------------|-------|----------------|
| humidity        | 1133  | 0.0            |
| battery_level   | 1141  | 0.0            |
| soil_moisture   | 1134  | 0.0            |
| temperature     | 1146  | 0.0            |
| light_intensity | 1137  | 0.0            |

## gaps
| sensor_id | reading_type    | expected_hours | actual_hours | missing_hours |
|-----------|-----------------|----------------|--------------|---------------|
| sensor_1  | battery_level   | 240            | 4            | 236           |
| sensor_2  | battery_level   | 240            | 4            | 236           |
| sensor_3  | battery_level   | 240            | 3            | 237           |
| sensor_4  | battery_level   | 240            | 4            | 236           |
| sensor_5  | battery_level   | 240            | 8            | 232           |

## profile
| reading_type    | total | null_values |
|-----------------|-------|-------------|
| light_intensity | 1137  | 0.0         |
| humidity        | 1133  | 0.0         |
| battery_level   | 1141  | 0.0         |
| temperature     | 1146  | 0.0         |
| soil_moisture   | 1134  | 0.0         |



