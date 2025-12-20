from pathlib import Path
from src.csv_parser import parse_metrics_csv, is_metrics_csv
import json
from datetime import datetime

def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

csv_path = Path(r"c:\Users\pierr\source\repos\python-fit\test\metrics.csv")
file_hash = "test_hash"

if not is_metrics_csv(csv_path):
    print("Error: The file is not a valid metrics CSV!")
else:
    try:
        # Testing with some allowed fields, 'pulse' is in there, but let's assume 'unknown_metric' is not
        allowed = {'file_hash', 'filename', 'timestamp', 'created_at', 'body_battery_min', 'body_battery_max', 'body_battery_avg', 'pulse', 'weight_kilograms'}
        data = parse_metrics_csv(csv_path, file_hash, allowed_fields=allowed)
        print(f"Parsed {len(data)} rows.")
        if data:
            print("Sample row:")
            print(json.dumps(data[0], indent=2, default=serialize))
            print("Another sample row (with weight):")
            # Find row with weight
            for row in data:
                if 'weight_kilograms' in row:
                    print(json.dumps(row, indent=2, default=serialize))
                    break
    except Exception as e:
        print(f"Error: {e}")
