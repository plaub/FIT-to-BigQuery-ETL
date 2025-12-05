# Quick Start Guide

## 🚀 Getting Started

### 1. Installation

All dependencies have already been installed! If you need to reinstall them:

```powershell
pip install --user -r requirements.txt
```

### 2. Configuration

Run the setup script:

```powershell
python setup.py
```

Or manually create a `.env` file (see `.env.example`):

```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
BIGQUERY_PROJECT_ID=your-project-id
BIGQUERY_DATASET=fitness_data
INPUT_DIR=files
PROCESSED_DIR=processed
FAILED_DIR=failed
LOG_DIR=logs
BATCH_SIZE=1000
LOG_LEVEL=INFO
```

### 3. Service Account Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com)
2. Select your project
3. Navigate to "IAM & Admin" → "Service Accounts"
4. Create a new Service Account
5. Add the "BigQuery Admin" role
6. Create a JSON key
7. Save the key and set the path in `.env`

### 4. Run Test

Test the parser with a file (without BigQuery upload):

```powershell
python test_parser.py "files\2024-05-09-095135-ELEMNT BOLT 3FFA-146-0.fit"
```

### 5. Run ETL Pipeline

```powershell
python run_etl.py
```

## 📊 What happens during execution?

1. **EXTRACT**

   - Scans the `files/` directory for FIT files
   - Generates SHA-256 hash for each file
   - Checks against BigQuery to see which files have already been processed
   - Displays the number of new files

2. **TRANSFORM**

   - Parses each new FIT file with `fitparse`
   - Extracts session data (summary)
   - Extracts record data (time series)
   - Converts GPS coordinates from Semicircles to Decimal Degrees

3. **LOAD**

   - Creates BigQuery dataset and tables (if not already existing)
   - Uploads details (many rows) in batches first
   - Then uploads the session (one row)
   - On error: Rollback, file remains in the input folder

4. **ARCHIVE**
   - Successful files → `processed/`
   - Failed files → `failed/`
   - Logs → `logs/etl_YYYYMMDD_HHMMSS.log`

## 🔍 BigQuery Tables

### Sessions Table

Partitioned by `start_time` (daily), clustered by `manufacturer` and `sport`.

**Important Fields:**

- `file_hash` - SHA-256 Hash (Primary Key)
- `session_id` - UUID for this session
- `start_time` - Start timestamp of the activity
- `total_distance` - Total distance in meters
- `avg_heart_rate` - Average heart rate
- `avg_power` - Average power in Watts
- `total_calories` - Total calories
- `sport` / `sub_sport` - Type of sport

### Details Table

Partitioned by `timestamp` (daily), clustered by `session_id` and `file_hash`.

**Important Fields:**

- `session_id` - Link to the sessions table
- `timestamp` - Timestamp of the data point
- `position_lat` / `position_long` - GPS coordinates (Decimal Degrees)
- `heart_rate` - Heart rate
- `power` - Power in Watts
- `distance` - Cumulative distance

## 💡 Example Queries

### Show all sessions

```sql
SELECT
  session_id,
  filename,
  start_time,
  sport,
  total_distance / 1000 AS distance_km,
  total_timer_time / 3600 AS duration_hours,
  avg_heart_rate,
  avg_power
FROM `your-project.fitness_data.sessions`
ORDER BY start_time DESC
LIMIT 10
```

### Combine session with details

```sql
SELECT
  s.filename,
  s.start_time,
  s.sport,
  COUNT(d.record_id) AS total_records,
  AVG(d.heart_rate) AS avg_hr,
  MAX(d.power) AS max_power
FROM `your-project.fitness_data.sessions` s
LEFT JOIN `your-project.fitness_data.details` d
  ON s.session_id = d.session_id
GROUP BY s.filename, s.start_time, s.sport
```

### GPS track of a session

```sql
SELECT
  timestamp,
  position_lat,
  position_long,
  heart_rate,
  power,
  speed
FROM `your-project.fitness_data.details`
WHERE session_id = 'YOUR-SESSION-ID'
ORDER BY timestamp
```

## 🛠️ Troubleshooting

### Import Error

```powershell
pip install --user -r requirements.txt
```

### "GOOGLE_APPLICATION_CREDENTIALS not set"

Check your `.env` file and ensure the path to the JSON key is correct.

### "Permission denied" during Pip Install

Use the `--user` flag:

```powershell
pip install --user <package>
```

### No new files found

- Check if FIT files are in the `files/` folder
- Check if files have already been moved to `processed/`
- Check BigQuery to see if the file_hash already exists

### BigQuery Error

- Verify Service Account permissions (BigQuery Admin or Editor)
- Check Project ID and Dataset Name in `.env`
- Check logs in `logs/` for details

## 📁 Files in Project

- `run_etl.py` - Main script to run the pipeline
- `setup.py` - Interactive configuration
- `test_parser.py` - Test parser without BigQuery
- `src/config.py` - Configuration management
- `src/hash_manager.py` - Duplicate detection
- `src/fit_parser.py` - FIT file parser
- `src/bigquery_client.py` - BigQuery operations
- `src/etl_pipeline.py` - ETL orchestration

## 🎯 Next Steps

1. **Perform Setup**: `python setup.py`
2. **Test Parser**: `python test_parser.py "files\<filename>.fit"`
3. **Run Pipeline**: `python run_etl.py`
4. **Check BigQuery**: Open the BigQuery Console
5. **Check Logs**: Look in `logs/` for details

## 🔄 Workflow for Regular Updates

1. Copy new FIT files to `files/`
2. Run `python run_etl.py`
3. Pipeline processes only new files (Hash check)
4. Check logs for success/failure
5. Archived files remain in `processed/`

**Important:** Never modify files in `processed/`, as this changes the hash!
