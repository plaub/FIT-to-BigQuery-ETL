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
    Query BigQuery to get all processed file hashes.
    
    Note: This function is called AFTER initialize_bigquery() ensures tables exist.
    The table should always exist at this point.
    
    Args:
        bigquery_client: BigQuery client instance
        
    Returns:
        Set of file hashes already in BigQuery
    """
    from src.config import BIGQUERY_DATASET, SESSIONS_TABLE
    
    query = f"""
        SELECT file_hash
        FROM `{bigquery_client.project}.{BIGQUERY_DATASET}.{SESSIONS_TABLE}`
    """
    
    try:
        query_job = bigquery_client.query(query)
        results = query_job.result()
        
        hashes = {row.file_hash for row in results}
        
        if len(hashes) == 0:
            logger.info("No previously processed files found in BigQuery (first run or empty table)")
        else:
            logger.info(f"Found {len(hashes)} previously processed files in BigQuery")
        
        return hashes
    
    except Exception as e:
        # If table doesn't exist (shouldn't happen after initialize_bigquery)
        if "Not found: Table" in str(e) or "Not found: Dataset" in str(e):
            logger.warning(f"Sessions table not found - tables should have been created by initialize_bigquery()")
            logger.warning(f"Assuming first run, treating all files as new")
            return set()
        else:
            logger.error(f"Error querying processed hashes: {e}")
            raise


def find_unprocessed_files(input_dir: Path, bigquery_client) -> List[tuple]:
    """
    Find all unprocessed FIT files in input directory.
    
    Args:
        input_dir: Directory containing FIT files
        bigquery_client: BigQuery client instance
        
    Returns:
        List of tuples (file_path, file_hash) for unprocessed files
    """
    logger.info(f"Scanning for FIT files in {input_dir}")
    
    # Get all FIT files (use set to avoid duplicates on case-insensitive filesystems like Windows)
    fit_files_lower = set(input_dir.rglob("*.fit"))
    fit_files_upper = set(input_dir.rglob("*.FIT"))
    fit_files = list(fit_files_lower | fit_files_upper)  # Union removes duplicates
    logger.info(f"Found {len(fit_files)} FIT files")
    
    # Get already processed hashes
    processed_hashes = get_processed_hashes(bigquery_client)
    
    # Check each file
    unprocessed = []
    for file_path in fit_files:
        # First check if file actually exists (it might have been moved)
        if not file_path.exists():
            logger.warning(f"File listed but not found (may have been moved): {file_path.name}")
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
