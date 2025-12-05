import shutil
import logging
import gzip
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
    
    # Initialize search for archives
    iteration = 0
    max_iterations = 10  # Safety breaking point
    
    while iteration < max_iterations:
        # Find archives in current state of input_dir
        archives = []
        # Recursive glob to look into extracted subfolders too
        for ext in ['*.zip', '*.tar', '*.tar.gz', '*.tgz', '*.tar.bz2', '*.tbz2', '*.tar.xz', '*.txz']:
            archives.extend(input_dir.rglob(ext))
        
        archives = list(set(archives))
        
        if not archives:
            # no more archives to process
            break
            
        logger.info(f"Iteration {iteration+1}: Found {len(archives)} archives to extract")
        
        # If we found archives, process them
        processed_count = 0
        
        for archive_path in archives:
            try:
                # Skip if file somehow disappeared (moved by previous iteration or race)
                if not archive_path.exists():
                    continue

                logger.info(f"Extracting {archive_path.name}...")
                
                # Extract to the directory CONTAINING the archive
                extract_path = archive_path.parent
                
                # Special handling for single compressed files that shutil doesn't handle natively as archives
                if archive_path.suffix.lower() == '.gz' and not archive_path.name.lower().endswith('.tar.gz'):
                    # It's likely a single file compressed (e.g. .fit.gz)
                    # output name: drop the .gz
                    output_file = extract_path / archive_path.stem
                    
                    with gzip.open(archive_path, 'rb') as f_in:
                        with open(output_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                            
                else:
                    # Use shutil for standard archives (zip, tar, tar.gz, etc)
                    shutil.unpack_archive(str(archive_path), str(extract_path))
                
                # Move to processed
                destination = processed_archives_dir / archive_path.name
                if destination.exists():
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    name = archive_path.name
                    destination = processed_archives_dir / f"{timestamp}_{name}"
                
                shutil.move(str(archive_path), str(destination))
                logger.info(f"Extracted and moved {archive_path.name}")
                processed_count += 1
                
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
        
        iteration += 1
        
        if processed_count == 0:
            # We found archives but failed to process any of them?
            # Avoid infinite loop if persistent failures
            break
            
    if iteration >= max_iterations:
        logger.warning("Reached maximum recursion depth for archive extraction. Some nested archives may remain.")
