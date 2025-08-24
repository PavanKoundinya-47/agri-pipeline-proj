import os, json, logging
from typing import Dict, Any

EXPECTED_SCHEMA = {
    "sensor_id": "string",
    "timestamp": "string",
    "reading_type": "string",
    "value": "double",
    "battery_level": "double"
}

CALIBRATION = {
    "temperature": {"multiplier": 1.01, "offset": -0.2},
    "humidity": {"multiplier": 1.00, "offset": 0.0},
    "soil_moisture": {"multiplier": 0.98, "offset": 0.5},
    "light_intensity": {"multiplier": 1.00, "offset": 0.0},
}

EXPECTED_RANGES = {
    "temperature": {"min": -10.0, "max": 60.0},
    "humidity": {"min": 0.0, "max": 100.0},
    "soil_moisture": {"min": 0.0, "max": 1.0},
    "light_intensity": {"min": 0.0, "max": 2000.0},
    "battery_level": {"min": 0.0, "max": 100.0}
}

CHECKPOINT_PATH = "data/.checkpoint.json"

def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
        ch = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        fh.setFormatter(fmt); ch.setFormatter(fmt)
        logger.addHandler(fh); logger.addHandler(ch)
    return logger

def load_checkpoint(path: str = CHECKPOINT_PATH) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"processed_files": []}

def save_checkpoint(state: Dict[str, Any], path: str = CHECKPOINT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
