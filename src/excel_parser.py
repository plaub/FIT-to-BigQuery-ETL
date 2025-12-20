import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def is_metrics_xlsx(file_path: Path) -> bool:
    """Check if the Excel file has the expected metrics headers."""
    try:
        # Read only headers
        df = pd.read_excel(file_path, nrows=0)
        required_headers = {"User ID", "Date", "Resting HR"}
        return required_headers.issubset(set(df.columns))
    except Exception as e:
        logger.error(f"Error checking Excel headers for {file_path}: {e}")
        return False

def parse_metrics_xlsx(file_path: Path, file_hash: str) -> List[Dict[str, Any]]:
    """
    Parse the metrics Excel file and map columns to BigQuery schema.
    Returns a list of records ready for BigQuery upload.
    
    Args:
        file_path: Path to the Excel file
        file_hash: Hash of the file
    """
    try:
        df = pd.read_excel(file_path)
        
        # Mapping definition: Excel Column -> BQ Field
        # Minutes to Hours for sleep metrics to match schema expectations
        mapping = {
            'Date': 'timestamp',
            'Resting HR': 'resting_heart_rate',
            'Max HR': 'max_heart_rate',
            'Total sleep time(min)': 'sleep_hours',
            'Deep': 'time_in_deep_sleep',
            'Light': 'time_in_light_sleep',
            'Awake': 'time_awake',
            'Min HR': 'min_heart_rate',
            'Avg HR': 'avg_heart_rate',
            'Ave. HRV/ms': 'hrv_avg'
        }
        
        records = []
        for _, row in df.iterrows():
            record = {
                "file_hash": file_hash,
                "filename": file_path.name,
                "created_at": datetime.utcnow()
            }

            # Map Avg HR to pulse as well for compatibility
            if 'Avg HR' in row and pd.notna(row['Avg HR']):
                record['pulse'] = int(row['Avg HR'])
            
            # Map columns
            for excel_col, bq_field in mapping.items():
                if excel_col in df.columns and pd.notna(row[excel_col]):
                    val = row[excel_col]
                    
                    # Convert minutes to hours for sleep fields
                    if excel_col in ['Total sleep time(min)', 'Deep', 'Light', 'Awake']:
                        record[bq_field] = round(float(val) / 60.0, 2)
                    elif bq_field == 'timestamp':
                        if isinstance(val, pd.Timestamp):
                            record[bq_field] = val.to_pydatetime()
                        else:
                            # Try to parse string timestamp if not already a Timestamp object
                            try:
                                record[bq_field] = pd.to_datetime(val).to_pydatetime()
                            except:
                                record[bq_field] = val
                    else:
                        # Standard type conversion
                        try:
                            if isinstance(val, (int, float, complex)):
                                if bq_field in ['resting_heart_rate', 'max_heart_rate', 'min_heart_rate', 'avg_heart_rate']:
                                    record[bq_field] = int(val)
                                else:
                                    record[bq_field] = float(val)
                            else:
                                record[bq_field] = val
                        except (ValueError, TypeError):
                            record[bq_field] = val

            if 'timestamp' in record:
                records.append(record)
                
        return records
        
    except Exception as e:
        logger.error(f"Error parsing Excel {file_path}: {e}")
        raise
