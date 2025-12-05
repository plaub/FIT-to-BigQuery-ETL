"""
ETL Pipeline orchestrator.
Coordinates the entire ETL process: Extract, Transform, Load.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path

from src.config import (
    INPUT_DIR, PROCESSED_DIR, FAILED_DIR, LOG_DIR, LOG_LEVEL,
    BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, SESSIONS_TABLE, DETAILS_TABLE,
    SESSIONS_SCHEMA, DETAILS_SCHEMA, BATCH_SIZE, validate_config
)
from src.bigquery_client import BigQueryClient
from src.hash_manager import find_unprocessed_files
from src.fit_parser import parse_fit_file
from src.archive_extractor import extract_archives


def setup_logging():
    """Configure logging for the pipeline."""
    log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("FIT to BigQuery ETL Pipeline Started")
    logger.info("=" * 80)
    return logger


def initialize_bigquery(logger):
    """
    Initialize BigQuery client and ensure dataset/tables exist.
    
    This function MUST be called before find_unprocessed_files() to ensure
    the sessions table exists for hash checking.
    
    Returns:
        BigQueryClient instance with verified dataset/tables
    """
    logger.info("Initializing BigQuery client...")
    
    bq_client = BigQueryClient(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET)
    
    # Ensure dataset exists (creates if needed)
    logger.info("Verifying dataset exists...")
    bq_client.ensure_dataset_exists()
    
    # Ensure sessions table exists with partitioning and clustering
    # (This table is used for hash checking in find_unprocessed_files)
    logger.info("Setting up sessions table...")
    bq_client.ensure_table_exists(
        SESSIONS_TABLE,
        SESSIONS_SCHEMA,
        partition_field='start_time',
        clustering_fields=['manufacturer', 'sport']
    )
    
    # Ensure details table exists with partitioning and clustering
    logger.info("Setting up details table...")
    bq_client.ensure_table_exists(
        DETAILS_TABLE,
        DETAILS_SCHEMA,
        partition_field='timestamp',
        clustering_fields=['session_id', 'file_hash']
    )
    
    logger.info("[OK] BigQuery setup complete - dataset and tables ready")
    return bq_client


def process_file(file_path: Path, file_hash: str, bq_client: BigQueryClient, logger):
    """
    Process a single FIT file: parse and upload to BigQuery.
    
    Args:
        file_path: Path to FIT file
        file_hash: SHA-256 hash of the file
        bq_client: BigQuery client instance
        logger: Logger instance
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("-" * 80)
    logger.info(f"Processing: {file_path.name}")
    logger.info(f"Hash: {file_hash}")
    
    # Verify file exists before processing
    if not file_path.exists():
        logger.error(f"File not found (may have been moved): {file_path}")
        return False
    
    try:
        # Step 1: Parse FIT file
        logger.info("Parsing FIT file...")
        session_data, records_data = parse_fit_file(file_path, file_hash)
        
        if not records_data:
            logger.warning(f"No records found in {file_path.name}, skipping")
            return False
        
        # Step 2: Upload to BigQuery
        logger.info("Uploading to BigQuery...")
        bq_client.upload_session_and_records(
            session_data,
            records_data,
            SESSIONS_TABLE,
            DETAILS_TABLE,
            BATCH_SIZE
        )
        
        logger.info(f"[OK] Successfully processed {file_path.name}")
        return True
    
    except Exception as e:
        logger.error(f"[FAILED] Failed to process {file_path.name}: {e}", exc_info=True)
        return False


def move_file(file_path: Path, destination_dir: Path, logger):
    """
    Move file to destination directory, preserving the filename.
    
    Args:
        file_path: Source file path
        destination_dir: Destination directory
        logger: Logger instance
    """
    # Check if source file exists
    if not file_path.exists():
        logger.warning(f"Cannot move {file_path.name}: file not found (may have been moved already)")
        return
    
    try:
        destination = destination_dir / file_path.name
        
        # Handle name collision
        if destination.exists():
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            stem = file_path.stem
            suffix = file_path.suffix
            destination = destination_dir / f"{stem}_{timestamp}{suffix}"
        
        shutil.move(str(file_path), str(destination))
        logger.info(f"Moved {file_path.name} to {destination_dir.name}/")
    
    except Exception as e:
        logger.error(f"Error moving {file_path.name}: {e}")


def run_etl_pipeline():
    """Main ETL pipeline execution."""
    logger = setup_logging()
    
    try:
        # Validate configuration
        logger.info("Validating configuration...")
        validate_config()
        
        # Initialize BigQuery (creates dataset and tables if they don't exist)
        logger.info("=" * 80)
        logger.info("SETUP: Initializing BigQuery...")
        logger.info("=" * 80)
        bq_client = initialize_bigquery(logger)
        
        # Step 0: Pre-process archives
        logger.info("=" * 80)
        logger.info("PRE-PROCESS: Checking for archives...")
        logger.info("=" * 80)
        extract_archives(INPUT_DIR, PROCESSED_DIR, FAILED_DIR)

        # Step 1: EXTRACT - Find unprocessed files
        # (This queries the sessions table that was just created/verified above)
        logger.info("=" * 80)
        logger.info("EXTRACT: Finding unprocessed files...")
        logger.info("=" * 80)
        
        unprocessed_files = find_unprocessed_files(INPUT_DIR, bq_client.client)
        
        if not unprocessed_files:
            logger.info("No unprocessed files found. Pipeline complete.")
            return
        
        logger.info(f"Found {len(unprocessed_files)} files to process")
        
        # Step 2 & 3: TRANSFORM & LOAD - Process each file
        logger.info("=" * 80)
        logger.info("TRANSFORM & LOAD: Processing files...")
        logger.info("=" * 80)
        
        success_count = 0
        failed_count = 0
        
        for file_path, file_hash in unprocessed_files:
            success = process_file(file_path, file_hash, bq_client, logger)
            
            if success:
                success_count += 1
                move_file(file_path, PROCESSED_DIR, logger)
            else:
                failed_count += 1
                move_file(file_path, FAILED_DIR, logger)
        
        # Summary
        logger.info("=" * 80)
        logger.info("ETL Pipeline Complete")
        logger.info("=" * 80)
        logger.info(f"Successfully processed: {success_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Total: {len(unprocessed_files)}")
    
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_etl_pipeline()
