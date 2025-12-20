"""
Hash management for duplicate detection.
Generates SHA-256 hashes from FIT files and checks against BigQuery.
"""

import hashlib
import logging
from pathlib import Path
from typing import List, Set

logger = logging.getLogger(__name__)


def generate_file_hash(file_path: Path) -> str:
    """
    Generate SHA-256 hash from file content.
    
    Args:
        file_path: Path to the file
        
    Returns:
        SHA-256 hash as hex string
    """
    sha256_hash = hashlib.sha256()
    
    try:
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        
        file_hash = sha256_hash.hexdigest()
        logger.debug(f"Generated hash {file_hash} for {file_path.name}")
        return file_hash
    
    except Exception as e:
        logger.error(f"Error generating hash for {file_path}: {e}")
        raise


def get_processed_hashes(bigquery_client) -> Set[str]:
    """
    Query BigQuery to get all processed file hashes from all target tables.
    """
    from src.config import BIGQUERY_DATASET, SESSIONS_TABLE, METRICS_TABLE
    
    hashes = set()
    
    # Check sessions table
    try:
        query_sessions = f"""
            SELECT DISTINCT file_hash
            FROM `{bigquery_client.project}.{BIGQUERY_DATASET}.{SESSIONS_TABLE}`
        """
        query_job = bigquery_client.query(query_sessions)
        results = query_job.result()
        hashes.update({row.file_hash for row in results})
    except Exception as e:
        if "Not found: Table" not in str(e):
            logger.error(f"Error querying sessions hashes: {e}")
            
    # Check metrics table
    try:
        query_metrics = f"""
            SELECT DISTINCT file_hash
            FROM `{bigquery_client.project}.{BIGQUERY_DATASET}.{METRICS_TABLE}`
        """
        query_job = bigquery_client.query(query_metrics)
        results = query_job.result()
        hashes.update({row.file_hash for row in results})
    except Exception as e:
        if "Not found: Table" not in str(e):
            logger.error(f"Error querying metrics hashes: {e}")
            
    if len(hashes) == 0:
        logger.info("No previously processed files found in BigQuery")
    else:
        logger.info(f"Found {len(hashes)} previously processed files in BigQuery")
        
    return hashes


def find_unprocessed_files(input_dir: Path, bigquery_client) -> List[tuple]:
    """
    Find all unprocessed FIT and CSV files in input directory.
    
    Args:
        input_dir: Directory containing files
        bigquery_client: BigQuery client instance
        
    Returns:
        List of tuples (file_path, file_hash) for unprocessed files
    """
    logger.info(f"Scanning for files in {input_dir}")
    
    # Get all FIT and CSV files
    extensions = ["*.fit", "*.FIT", "*.csv", "*.CSV", "*.xlsx", "*.XLSX"]
    all_files = []
    for ext in extensions:
        all_files.extend(input_dir.rglob(ext))
    
    # Remove duplicates (case-insensitive)
    file_map = {f.resolve(): f for f in all_files}
    files = list(file_map.values())
    
    logger.info(f"Found {len(files)} candidate files")
    
    # Get already processed hashes
    processed_hashes = get_processed_hashes(bigquery_client)
    
    # Check each file
    unprocessed = []
    for file_path in files:
        if not file_path.exists():
            continue
            
        try:
            file_hash = generate_file_hash(file_path)
            
            if file_hash not in processed_hashes:
                unprocessed.append((file_path, file_hash))
                logger.info(f"New file: {file_path.name}")
            else:
                logger.debug(f"Already processed: {file_path.name}")
        
        except Exception as e:
            logger.error(f"Error checking {file_path.name}: {e}")
            continue
    
    logger.info(f"Found {len(unprocessed)} unprocessed files")
    return unprocessed
