"""
Script to backfill map images for previously processed FIT files.
"""

import argparse
import logging
from pathlib import Path
from google.cloud import bigquery

from src.config import (
    BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, SESSIONS_TABLE, PROCESSED_DIR,
    validate_config
)
from src.bigquery_client import BigQueryClient
from src.fit_parser import parse_fit_file
from src.map_generator import generate_route_maps_base64


def setup_backfill_logging():
    """Configure simpler logging for the backfill script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    return logging.getLogger("backfill")


def run_backfill(limit: int):
    """
    Finds sessions without map images, parses the corresponding processed file,
    and updates BigQuery with the new Base64 strings.
    """
    logger = setup_backfill_logging()
    
    logger.info("=" * 80)
    logger.info(f"Starting Map Image Backfill (Limit: {limit})")
    logger.info("=" * 80)
    
    validate_config()
    bq_client = BigQueryClient(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET)
    
    # Query sessions that are missing maps
    # Use order by start_time DESC to process the newest sessions first
    query = f"""
        SELECT session_id, file_hash, filename 
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{SESSIONS_TABLE}`
        WHERE map_mini_preview_base64 IS NULL
          AND filename LIKE '%.fit'
        ORDER BY start_time DESC
        LIMIT {limit}
    """
    
    try:
        logger.info("Querying BigQuery for sessions missing map images...")
        query_job = bq_client.client.query(query)
        rows = list(query_job.result())
        
        if not rows:
            logger.info("🎉 All sessions already have map images! Nothing to backfill.")
            return
            
        logger.info(f"Found {len(rows)} sessions without map images to process.")
        
        success_count = 0
        failure_count = 0
        skip_count = 0
        
        for row in rows:
            session_id = row.session_id
            file_hash = row.file_hash
            original_filename = row.filename
            
            logger.info("-" * 40)
            logger.info(f"Processing session: {session_id}")
            logger.info(f"Original file: {original_filename}")
            
            # 1. Find the file in PROCESSED_DIR
            # Since files might be renamed on collision (e.g. filename_20240101_120000.fit),
            # we should look for files matching the stem.
            # But the best is to just check the exact name first.
            file_path = PROCESSED_DIR / original_filename
            
            if not file_path.exists():
                # Attempt to find it by stem if it was renamed (collision handling)
                stem = Path(original_filename).stem
                candidates = list(PROCESSED_DIR.glob(f"{stem}*.fit"))
                if candidates:
                    file_path = candidates[0]
                else:
                    logger.warning(f"❌ Could not find processed file '{original_filename}' in {PROCESSED_DIR}. Skipping.")
                    failure_count += 1
                    continue
                    
            logger.info(f"Found file: {file_path.name}. Parsing...")
            
            # 2. Parse the FIT file
            try:
                # We only need the records to generate the maps
                _, records_data = parse_fit_file(file_path, file_hash)
            except Exception as e:
                logger.error(f"❌ Error parsing {file_path.name}: {e}")
                failure_count += 1
                continue
                
            if not records_data:
                logger.info("⚠️ File has no records. Skipping map generation.")
                skip_count += 1
                continue
                
            # 3. Generate Maps
            logger.info(f"Generating maps for {len(records_data)} data points...")
            mini_b64, large_b64 = generate_route_maps_base64(records_data)
            
            if not mini_b64 or not large_b64:
                logger.info("⚠️ Not enough GPS data to generate maps. Skipping.")
                skip_count += 1
                continue
                
            # 4. Update BigQuery
            logger.info("Executing UPDATE against BigQuery...")
            update_query = f"""
                UPDATE `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{SESSIONS_TABLE}`
                SET map_mini_preview_base64 = @mini_b64,
                    map_large_base64 = @large_b64
                WHERE session_id = @session_id
            """
            
            update_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("mini_b64", "STRING", mini_b64),
                    bigquery.ScalarQueryParameter("large_b64", "STRING", large_b64),
                    bigquery.ScalarQueryParameter("session_id", "STRING", session_id)
                ]
            )
            
            try:
                bq_client.client.query(update_query, job_config=update_job_config).result()
                logger.info(f"✅ Successfully updated maps for session {session_id}")
                success_count += 1
            except Exception as e:
                logger.error(f"❌ Error updating BigQuery for session {session_id}: {e}")
                failure_count += 1
                
        logger.info("=" * 80)
        logger.info("Backfill Complete")
        logger.info(f"Successfully updated: {success_count}")
        logger.info(f"Skipped (no GPS):     {skip_count}")
        logger.info(f"Failed:             {failure_count}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Critical error during backfill: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill map images for previously processed FIT files.")
    parser.add_argument(
        "-l", "--limit", 
        type=int, 
        default=10, 
        help="Maximum number of sessions to backfill in one run (default: 10)"
    )
    args = parser.parse_args()
    
    run_backfill(args.limit)
