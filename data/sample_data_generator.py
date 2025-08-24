import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

OUTPUT_DIR = "data/raw"
# OUTPUT_DIR = "/home/pavan_azista_lap/Downloads/Django-Coursera/data_pipeline/data/raw"

NUM_SENSORS = 5
DAYS = 5
START_DATE = datetime(2025, 7, 1)

READING_TYPES = {
    "temperature": {"min": 15, "max": 40},
    "humidity": {"min": 20, "max": 90},
    "soil_moisture": {"min": 0, "max": 100},
    "light_intensity": {"min": 100, "max": 1000},
    "battery_level": {"min": 10, "max": 100}
}

np.random.seed(0)

def generate_day_data(date: datetime):
    records = []
    for sensor in range(1, NUM_SENSORS + 1):
        for hour in range(24):
            for reading_type, bounds in READING_TYPES.items():
                timestamp = date + timedelta(hours=hour, minutes=np.random.randint(0, 60))

                value = np.random.uniform(bounds["min"], bounds["max"])

                if np.random.rand() < 0.05:
                    value = None

                # Introduce occasional erroneous values
                elif np.random.rand() < 0.05:
                    # extreme temp
                    if reading_type == "temperature":
                        value = np.random.choice([-50, 120])  
                    # invalid %
                    elif reading_type == "humidity":
                        value = np.random.choice([-10, 150])  
                    # impossible
                    elif reading_type == "soil_moisture":
                        value = np.random.choice([-5, 200])  
                    # bad reading
                    elif reading_type == "light_intensity":
                        value = np.random.choice([-100, 5000])  
                    # invalid
                    elif reading_type == "battery_level":
                        value = np.random.choice([-20, 150])  

                battery_level = np.random.uniform(20, 100)
                 # missing battery reading
                if np.random.rand() < 0.05:
                    battery_level = None 

                records.append({
                    "sensor_id": f"sensor_{sensor}",
                    "timestamp": timestamp,
                    "reading_type": reading_type,
                    "value": value,
                    "battery_level": battery_level
                })
    return pd.DataFrame(records)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i in range(DAYS):
        date = START_DATE + timedelta(days=i)
        df = generate_day_data(date)
        filename = os.path.join(OUTPUT_DIR, f"{date.strftime('%Y-%m-%d')}.parquet")
        df.to_parquet(filename, index=False)
        print(f"Generated {filename} with {len(df)} rows")

if __name__ == "__main__":
    main()
