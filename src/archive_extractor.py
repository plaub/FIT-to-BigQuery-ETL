import shutil
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def extract_archives(input_dir: Path, processed_dir: Path, failed_dir: Path):
    """
    Finds and extracts archive files in the input directory.
    Moves processed archives to processed_dir/archives.
    
    Args:
        input_dir: Directory containing archives and files
        processed_dir: Directory to move processed archives to
        failed_dir: Directory to move failed archives to
    """
    # Create archives subdirectories
    processed_archives_dir = processed_dir / "archives"
    processed_archives_dir.mkdir(parents=True, exist_ok=True)
    
    failed_archives_dir = failed_dir / "archives"
    failed_archives_dir.mkdir(parents=True, exist_ok=True)

    # Common archive extensions supported by shutil
    # simple extensions
    extensions = ['*.zip', '*.tar', '*.gztar', '*.bztar', '*.xztar']
    # Start with basic glob for specific common extensions to avoid matching everything if user has weird files
    # shutil.get_unpack_formats() returns list of (name, extensions, description)
    # usually: zip, tar, gztar, bztar, xztar -> .zip, .tar, .tar.gz, .tar.bz2, .tar.xz
    
    archives = []
    # explicit list matches shutil defaults commonly used
    for ext in ['*.zip', '*.tar', '*.tar.gz', '*.tgz', '*.tar.bz2', '*.tbz2', '*.tar.xz', '*.txz']:
        archives.extend(input_dir.glob(ext))
    
    # Deduplicate in case patterns overlap (rare with glob, but good practice)
    archives = list(set(archives))
    
    if not archives:
        return

    logger.info(f"Found {len(archives)} archives to extract")

    for archive_path in archives:
        try:
            logger.info(f"Extracting {archive_path.name}...")
            
            # Extract to input_dir
            # This puts files directly into input_dir, potentially creating subfolders
            shutil.unpack_archive(str(archive_path), str(input_dir))
            
            # Move to processed
            destination = processed_archives_dir / archive_path.name
            if destination.exists():
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                # careful with multi-dot extensions like .tar.gz
                # pathlib .suffix only gives last suffix. .suffixes gives all.
                # easiest is to just insert before the first dot of name, or append to end
                name = archive_path.name
                destination = processed_archives_dir / f"{timestamp}_{name}"
            
            shutil.move(str(archive_path), str(destination))
            logger.info(f"Extracted and moved {archive_path.name}")
            
        except Exception as e:
            logger.error(f"Failed to extract {archive_path.name}: {e}")
            # Move to failed
            destination = failed_archives_dir / archive_path.name
            if destination.exists():
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                name = archive_path.name
                destination = failed_archives_dir / f"{timestamp}_{name}"
            
            try:
                shutil.move(str(archive_path), str(destination))
                logger.info(f"Moved failed archive to {destination}")
            except Exception as move_error:
                logger.error(f"Failed to move failed archive {archive_path.name}: {move_error}")
