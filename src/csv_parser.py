import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def clean_column_name(name: str) -> str:
    """Clean column name: lowercase, spaces to underscores, remove special chars."""
    clean = name.lower().strip()
    clean = clean.replace(' ', '_')
    # Remove any non-alphanumeric except underscores
    clean = ''.join(c for c in clean if c.isalnum() or c == '_')
    return clean

def is_metrics_csv(file_path: Path) -> bool:
    """Check if the CSV file has the expected metrics headers."""
    required_headers = {"Timestamp", "Type", "Value"}
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return False
            return required_headers.issubset(set(header))
    except Exception as e:
        logger.error(f"Error checking CSV headers for {file_path}: {e}")
        return False

def parse_metrics_csv(file_path: Path, file_hash: str, allowed_fields: set = None) -> List[Dict[str, Any]]:
    """
    Parse the metrics CSV file and pivot the data based on timestamp.
    Returns a list of records ready for BigQuery upload.
    
    Args:
        file_path: Path to the CSV file
        file_hash: Hash of the file
        allowed_fields: Optional set of field names allowed in the target table
    """
    records_by_timestamp = {}
    unknown_fields = set()
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts_str = row['Timestamp']
                metric_type = row['Type']
                value_str = row['Value']
                
                if not ts_str or not metric_type:
                    continue
                    
                if ts_str not in records_by_timestamp:
                    try:
                        ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        ts_dt = ts_str  # Fallback to string if format varies
                        
                    records_by_timestamp[ts_str] = {
                        "file_hash": file_hash,
                        "filename": file_path.name,
                        "timestamp": ts_dt,
                        "created_at": datetime.utcnow()
                    }
                
                record = records_by_timestamp[ts_str]
                
                # Clean metric type
                clean_base_type = clean_column_name(metric_type)
                
                # Check for multi-value strings like "Min : 18 / Max : 48 / Avg : 29"
                if " / " in value_str:
                    parts = value_str.split(" / ")
                    for part in parts:
                        if " : " in part:
                            key, val = part.split(" : ", 1)
                            clean_key = clean_column_name(key)
                            col_name = f"{clean_base_type}_{clean_key}"
                            
                            if allowed_fields and col_name not in allowed_fields:
                                if col_name not in unknown_fields:
                                    logger.warning(f"Unknown metric field '{col_name}' found in {file_path.name} (not in BigQuery schema).")
                                    unknown_fields.add(col_name)
                                continue

                            try:
                                # Try to convert to float/int
                                if '.' in val:
                                    record[col_name] = float(val)
                                else:
                                    record[col_name] = int(val)
                            except ValueError:
                                record[col_name] = val.strip()
                else:
                    # Single value
                    col_name = clean_base_type
                    
                    if allowed_fields and col_name not in allowed_fields:
                        if col_name not in unknown_fields:
                            logger.warning(f"Unknown metric field '{col_name}' found in {file_path.name} (not in BigQuery schema).")
                            unknown_fields.add(col_name)
                    else:
                        try:
                            if '.' in value_str:
                                record[col_name] = float(value_str)
                            else:
                                record[col_name] = int(value_str)
                        except ValueError:
                            record[col_name] = value_str.strip()
                        
        if unknown_fields:
            logger.info(f"Skipped {len(unknown_fields)} unknown fields in {file_path.name}: {', '.join(sorted(unknown_fields))}")
            
        return list(records_by_timestamp.values())
        
    except Exception as e:
        logger.error(f"Error parsing CSV {file_path}: {e}")
        raise
