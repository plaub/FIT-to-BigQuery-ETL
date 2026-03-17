import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def is_metrics_json(file_path: Path) -> bool:
    """Check if the JSON file has the expected metrics format (Coros format)."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            data = json.load(f)
            if "data" in data and "dayList" in data["data"]:
                return True
            return False
    except Exception as e:
        logger.error(f"Error checking JSON format for {file_path}: {e}")
        return False

def parse_metrics_json(file_path: Path, file_hash: str, allowed_fields: set = None) -> List[Dict[str, Any]]:
    """
    Parse the metrics JSON file (Coros format).
    Returns a list of records ready for BigQuery upload.
    
    Args:
        file_path: Path to the JSON file
        file_hash: Hash of the file
        allowed_fields: Optional set of field names allowed in the target table
    """
    records = []
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            data = json.load(f)
            
        day_list = data.get("data", {}).get("dayList", [])
        
        for day in day_list:
            timestamp_sec = day.get("timestamp")
            avg_sleep_hrv = day.get("avgSleepHrv")
            
            if timestamp_sec is None:
                continue
                
            # Convert timestamp to datetime
            ts_dt = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
            # Remove timezone info to match BigQuery TIMESTAMP expectation
            ts_dt = ts_dt.replace(tzinfo=None)
            
            record = {
                "file_hash": file_hash,
                "filename": file_path.name,
                "timestamp": ts_dt,
                "created_at": datetime.utcnow()
            }
            
            if avg_sleep_hrv is not None:
                if allowed_fields and "hrv_avg" not in allowed_fields:
                    pass
                else:
                    record["hrv_avg"] = float(avg_sleep_hrv)
            
            if "hrv_avg" in record:
                records.append(record)
                
        return records
        
    except Exception as e:
        logger.error(f"Error parsing JSON {file_path}: {e}")
        raise
