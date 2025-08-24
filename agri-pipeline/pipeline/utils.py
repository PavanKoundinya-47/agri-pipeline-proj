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
    """
    Create and configure a logger with both file and console handlers.

    - Ensures the log directory exists.
    - Configures the logger only once per name (avoids duplicate handlers).
    - Writes logs to both:
        1. A log file in the given directory.
        2. The console (stdout).
    - Log format includes timestamp, level, logger name, and message.

    Args:
        name (str): Name of the logger (also used for the log filename).
        log_dir (str, optional): Directory where log files will be stored.
                                 Defaults to "logs".

    Returns:
        logging.Logger: Configured logger instance.

    Example:
        >>> logger = get_logger("ingestion")
        >>> logger.info("Ingestion started")
        # Logs to both console and logs/ingestion.log
    """
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
    """
    Load pipeline checkpoint state from a JSON file.

    - If the checkpoint file exists, it is loaded and returned.
    - If no file is found, a default state with an empty list of processed files is returned.

    Args:
        path (str, optional): Path to the checkpoint file. 
                              Defaults to CHECKPOINT_PATH.

    Returns:
        Dict[str, Any]: Checkpoint state with at least:
            {
                "processed_files": List[str]  # List of filenames already processed
            }

    Example:
        >>> state = load_checkpoint("data/.checkpoint.json")
        >>> isinstance(state, dict)
        True
    """    
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"processed_files": []}

def save_checkpoint(state: Dict[str, Any], path: str = CHECKPOINT_PATH) -> None:
    """
    Save pipeline checkpoint state to a JSON file.

    - Creates parent directories if they don’t exist.
    - Serializes the state dictionary into JSON with indentation.

    Args:
        state (Dict[str, Any]): Checkpoint state to save. 
                                Example:
                                {
                                    "processed_files": ["2025-05-01.parquet", "2025-05-02.parquet"]
                                }
        path (str, optional): Path to save the checkpoint file. 
                              Defaults to CHECKPOINT_PATH.

    Returns:
        None

    Example:
        >>> save_checkpoint({"processed_files": ["2025-05-01.parquet"]}, "data/.checkpoint.json")
        # Creates/updates checkpoint file on disk
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    os.chmod(path, 0o666)
