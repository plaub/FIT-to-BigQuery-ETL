"""
Configuration management for FIT to BigQuery ETL pipeline.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project Root
PROJECT_ROOT = Path(__file__).parent.parent

# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'fitness_data')
SESSIONS_TABLE = 'sessions'
DETAILS_TABLE = 'details'
METRICS_TABLE = 'metrics'

# Directory Configuration
INPUT_DIR = PROJECT_ROOT / os.getenv('INPUT_DIR', 'files')
PROCESSED_DIR = PROJECT_ROOT / os.getenv('PROCESSED_DIR', 'processed')
FAILED_DIR = PROJECT_ROOT / os.getenv('FAILED_DIR', 'failed')
LOG_DIR = PROJECT_ROOT / os.getenv('LOG_DIR', 'logs')

# Processing Configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# BigQuery Schema Definitions
SESSIONS_SCHEMA = [
    {"name": "file_hash", "type": "STRING", "mode": "REQUIRED"},
    {"name": "filename", "type": "STRING", "mode": "REQUIRED"},
    {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "start_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "manufacturer", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product", "type": "STRING", "mode": "NULLABLE"},
    {"name": "serial_number", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "sport", "type": "STRING", "mode": "NULLABLE"},
    {"name": "sub_sport", "type": "STRING", "mode": "NULLABLE"},
    {"name": "total_elapsed_time", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_timer_time", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "avg_speed", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "max_speed", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "avg_cadence", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "max_cadence", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "min_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "avg_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "max_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "avg_power", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "max_power", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "normalized_power", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "threshold_power", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "total_work", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "total_calories", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "min_altitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "avg_altitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "max_altitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_ascent", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "total_descent", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "avg_grade", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "max_pos_grade", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "max_neg_grade", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "avg_temperature", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "max_temperature", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "training_stress_score", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "intensity_factor", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "num_laps", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

DETAILS_SCHEMA = [
    {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "file_hash", "type": "STRING", "mode": "REQUIRED"},
    {"name": "record_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "position_lat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "position_long", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "gps_accuracy", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "altitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "enhanced_altitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "grade", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cadence", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "power", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "speed", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "enhanced_speed", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "temperature", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "calories", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "battery_soc", "type": "FLOAT", "mode": "NULLABLE"},
]

METRICS_SCHEMA = [
    {"name": "file_hash", "type": "STRING", "mode": "REQUIRED"},
    {"name": "filename", "type": "STRING", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "body_battery_min", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "body_battery_max", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "body_battery_avg", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "pulse", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "sleep_hours", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "stress_level_max", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "stress_level_avg", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "time_awake", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "time_in_deep_sleep", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "time_in_light_sleep", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "time_in_rem_sleep", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "weight_kilograms", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "resting_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "max_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "min_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "avg_heart_rate", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "hrv_avg", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]



def validate_config():
    """Validate required configuration parameters."""
    errors = []
    
    if not GOOGLE_APPLICATION_CREDENTIALS:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS not set")
    
    if not BIGQUERY_PROJECT_ID:
        errors.append("BIGQUERY_PROJECT_ID not set")
    
    if errors:
        raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    # Ensure directories exist
    INPUT_DIR.mkdir(exist_ok=True)
    PROCESSED_DIR.mkdir(exist_ok=True)
    FAILED_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)
