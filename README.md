# FIT to BigQuery ETL Pipeline

A Python ETL pipeline for processing FIT files (from Wahoo, Garmin, Zwift) and uploading them to Google BigQuery.

## Features

- **Hash-based duplicate detection**: SHA-256 hashing prevents duplicate processing
- **Batch upload**: Efficient uploading in configurable batches
- **Transactional processing**: Details first, then session data
- **Automatic archiving**: Successfully processed files are moved to `processed/`
- **Error handling**: Faulty files are moved to `failed/`
- **Comprehensive logging**: Detailed logs in `logs/` directory
- **BigQuery Optimization**: Partitioning by Timestamp, Clustering by Manufacturer/Sport

## Prerequisites

- Python 3.8+
- Google Cloud Service Account with BigQuery permissions
- FIT files from fitness devices

## Installation

1. **Clone or download repository**

2. **Create virtual environment (recommended)**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

3. **Install dependencies**

```powershell
pip install -r requirements.txt
```

4. **Set up configuration**

Create a `.env` file in the project directory (template: `.env.example`):

```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/your-service-account-key.json
BIGQUERY_PROJECT_ID=your-project-id
BIGQUERY_DATASET=fitness_data
INPUT_DIR=files
PROCESSED_DIR=processed
FAILED_DIR=failed
LOG_DIR=logs
BATCH_SIZE=1000
LOG_LEVEL=INFO
```

## BigQuery Schema

### Sessions Table

Stores summaries of training sessions:

- Metadata (File Hash, Filename, Session ID)
- Time information (Start, Duration)
- Device info (Manufacturer, Product, Serial Number)
- Metrics (Distance, Speed, Cadence, Heart Rate, Power)
- Altitude data (Min/Avg/Max Altitude, Ascent/Descent)
- Calories and Training metrics (TSS, Intensity Factor)

### Details Table

Stores time-series data points (second by second):

- Session association (Session ID, File Hash)
- GPS coordinates (Latitude/Longitude in decimal degrees)
- Vitals (Heart Rate, Cadence, Power)
- Environment data (Temperature, Altitude, Grade)
- Movement data (Speed, Distance)

## Usage

### Run Pipeline

```powershell
python -m src.etl_pipeline
```

### Workflow

1. **Extract**: Scans `files/` directory for FIT files
2. **Hash-Check**: Generates SHA-256 hash and checks against BigQuery
3. **Transform**: Parses FIT files with `fitparse` and extracts data
4. **Load**:
   - Uploads details data in batches
   - Uploads session data
5. **Archive**: Moves successfully processed files to `processed/`
6. **Error Handling**: Moves faulty files to `failed/`

## Project Structure

```
python-fit/
├── src/
│   ├── __init__.py
│   ├── config.py              # Configuration management
│   ├── hash_manager.py        # Duplicate detection
│   ├── fit_parser.py          # FIT file parser
│   ├── bigquery_client.py     # BigQuery operations
│   └── etl_pipeline.py        # Main pipeline
├── files/                     # Input: FIT files
├── processed/                 # Successfully processed files
├── failed/                    # Failed files
├── logs/                      # Log files
├── requirements.txt           # Python dependencies
├── .env.example              # Configuration template
└── README.md                 # This file
```

## Logging

Logs are stored in:

- **File**: `logs/etl_YYYYMMDD_HHMMSS.log`
- **Console**: Simultaneous output to screen

Log level configurable via `.env` (DEBUG, INFO, WARNING, ERROR)

## Error Handling

- **Parsing Error**: File is moved to `failed/`
- **BigQuery Error**: Transaction is aborted, file remains in `files/`
- **Network Error**: Pipeline aborts, file can be processed again in the next run

## GPS Coordinates Conversion

FIT files store GPS coordinates in Semicircles (Integer).
The pipeline automatically converts them to Decimal Degrees:

```
Degrees = Semicircles × (180 / 2^31)
```

## Example Output

```
================================================================================
FIT to BigQuery ETL Pipeline Started
================================================================================
INFO - Validating configuration...
INFO - Initializing BigQuery client...
INFO - Setting up sessions table...
INFO - Setting up details table...
INFO - BigQuery setup complete
================================================================================
EXTRACT: Finding unprocessed files...
================================================================================
INFO - Scanning for FIT files in files
INFO - Found 50 FIT files
INFO - Found 5 unprocessed files
================================================================================
TRANSFORM & LOAD: Processing files...
================================================================================
--------------------------------------------------------------------------------
Processing: 2024-05-09-095135-ELEMNT BOLT 3FFA-146-0.fit
Hash: a3b2c1d4e5f6...
INFO - Parsing FIT file...
INFO - Extracted session and 15 records
INFO - Uploading to BigQuery...
INFO - Successfully inserted 1542 rows into details
INFO - Successfully inserted 1 rows into sessions
✓ Successfully processed 2024-05-09-095135-ELEMNT BOLT 3FFA-146-0.fit
INFO - Moved file to processed/
================================================================================
ETL Pipeline Complete
================================================================================
Successfully processed: 5
Failed: 0
Total: 5
```

## Performance Optimization

- **Batch Upload**: Default 1000 rows per batch (configurable)
- **Partitioning**: By Timestamp for fast time-range queries
- **Clustering**: By Manufacturer and Sport for optimized filtering
- **Streaming Insert**: For real-time data availability

## Troubleshooting

### "Import could not be resolved"

```powershell
pip install -r requirements.txt
```

### "GOOGLE_APPLICATION_CREDENTIALS not set"

Check your `.env` file and ensure the path to the Service Account Key is correct.

### "Not found: Table"

Tables are created automatically on the first run. Ensure your Service Account has permission to create tables.

## License

Private Project

## Author

Pierre

