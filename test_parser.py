"""
Test script to verify FIT file parsing without uploading to BigQuery.
Useful for testing the parser on your local files.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.fit_parser import parse_fit_file
from src.hash_manager import generate_file_hash


def test_fit_file(file_path: str):
    """Test parsing a single FIT file."""
    print("=" * 80)
    print(f"Testing FIT Parser")
    print("=" * 80)
    print()
    
    fit_path = Path(file_path)
    
    if not fit_path.exists():
        print(f"❌ File not found: {file_path}")
        return
    
    if not fit_path.suffix.lower() == '.fit':
        print(f"❌ Not a FIT file: {file_path}")
        return
    
    print(f"📁 File: {fit_path.name}")
    print(f"📏 Size: {fit_path.stat().st_size:,} bytes")
    print()
    
    try:
        # Generate hash
        print("🔒 Generating file hash...")
        file_hash = generate_file_hash(fit_path)
        print(f"   Hash: {file_hash}")
        print()
        
        # Parse file
        print("📖 Parsing FIT file...")
        session_data, records_data = parse_fit_file(fit_path, file_hash)
        print(f"   ✅ Parsed successfully!")
        print()
        
        # Display session info
        print("=" * 80)
        print("Session Information")
        print("=" * 80)
        print(f"Session ID: {session_data.get('session_id')}")
        print(f"Manufacturer: {session_data.get('manufacturer')}")
        print(f"Product: {session_data.get('product')}")
        print(f"Sport: {session_data.get('sport')}")
        print(f"Sub-Sport: {session_data.get('sub_sport')}")
        print(f"Start Time: {session_data.get('start_time')}")
        print()
        
        # Display metrics
        print("Metrics:")
        print(f"  Distance: {session_data.get('total_distance')} m")
        print(f"  Duration: {session_data.get('total_timer_time')} s")
        print(f"  Avg Speed: {session_data.get('avg_speed')} m/s")
        print(f"  Avg Heart Rate: {session_data.get('avg_heart_rate')} bpm")
        print(f"  Avg Power: {session_data.get('avg_power')} W")
        print(f"  Total Calories: {session_data.get('total_calories')} kcal")
        print(f"  Total Ascent: {session_data.get('total_ascent')} m")
        print()
        
        # Display records info
        print("=" * 80)
        print("Records Information")
        print("=" * 80)
        print(f"Total Records: {len(records_data)}")
        print()
        
        if records_data:
            print("First Record:")
            first = records_data[0]
            print(f"  Timestamp: {first.get('timestamp')}")
            print(f"  Position: {first.get('position_lat')}, {first.get('position_long')}")
            print(f"  Altitude: {first.get('enhanced_altitude') or first.get('altitude')} m")
            print(f"  Heart Rate: {first.get('heart_rate')} bpm")
            print(f"  Power: {first.get('power')} W")
            print(f"  Speed: {first.get('enhanced_speed') or first.get('speed')} m/s")
            print()
            
            print("Last Record:")
            last = records_data[-1]
            print(f"  Timestamp: {last.get('timestamp')}")
            print(f"  Position: {last.get('position_lat')}, {last.get('position_long')}")
            print(f"  Distance: {last.get('distance')} m")
            print()
        
        print("=" * 80)
        print("✅ Test Complete!")
        print("=" * 80)
        print()
        print(f"Summary:")
        print(f"  • Session data fields: {len(session_data)}")
        print(f"  • Total records: {len(records_data)}")
        print(f"  • Ready for BigQuery upload: ✓")
        print()
    
    except Exception as e:
        print(f"❌ Error parsing file: {e}")
        import traceback
        traceback.print_exc()


import shutil
import tempfile
import atexit
import gzip

def cleanup_temp_dir(temp_dir):
    """Cleanup temporary directory."""
    if temp_dir and Path(temp_dir).exists():
        shutil.rmtree(temp_dir)



def process_path(path_str: str):
    """Process a path (file, directory, or archive)."""
    path = Path(path_str)
    
    if not path.exists():
        print(f"❌ Path not found: {path}")
        return

    # Check if archive
    suffixes = path.suffixes
    is_archive = False
    if any(s.lower() in ['.zip', '.tar', '.gz', '.tgz', '.bz2', '.xz'] for s in suffixes):
        is_archive = True
        
    if is_archive:
        print(f"📦 Detected archive: {path.name}")
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"   Extracting to temporary directory...")
            try:
                shutil.unpack_archive(str(path), temp_dir)
                # Process contents of temp dir
                process_directory(Path(temp_dir))
            except Exception as e:
                print(f"❌ Failed to extract archive: {e}")
                
    elif path.is_dir():
        print(f"📂 Processing directory: {path.name}")
        process_directory(path)
        
    elif path.suffix.lower() == '.fit':
        test_fit_file(path)
        
    else:
        print(f"❌ Unsupported file type: {path.name}")

def process_directory(directory: Path):
    """Recursively find and process FIT files and nested archives in directory."""
    
    # 1. Look for and extract nested archives first
    archive_extensions = {'.zip', '.tar', '.gz', '.tgz', '.bz2', '.xz'}
    found_archives = []
    # Use rglob to find all archives. Iterate copy to allow processing.
    for path in directory.rglob("*"):
        if path.is_file() and any(path.name.endswith(ext) for ext in archive_extensions):
            found_archives.append(path)
            
    for archive_path in found_archives:
        try:
            # Create a specific directory for this archive's contents to avoid collisions
            extract_dir = archive_path.parent / f"{archive_path.stem}_extracted"
            
            # Skip if already extracted (simple heuristic if dir exists)
            if not extract_dir.exists():
                extract_dir.mkdir(exist_ok=True)
                print(f"📦 Extracting nested archive: {archive_path.name}")
                
                if archive_path.suffix.lower() == '.gz' and not archive_path.name.lower().endswith('.tar.gz'):
                    # Handle single compressed file .gz
                    output_file = extract_dir / archive_path.stem
                    with gzip.open(archive_path, 'rb') as f_in:
                        with open(output_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                else:
                    shutil.unpack_archive(str(archive_path), str(extract_dir))
                
                # Recurse into the new directory to handle triple nesting, etc.
                process_directory(extract_dir)
        except Exception as e:
            # Don't fail the whole run, just warn
            print(f"⚠️ Failed to extract nested archive {archive_path.name}: {e}")

    # 2. Find and process FIT files
    # rglob again because new files might have appeared from extraction
    fit_files = list(directory.rglob("*.fit")) + list(directory.rglob("*.FIT"))
    fit_files = list(set(fit_files)) # dedupe
    
    if not fit_files:
        # Check if we at least found archives, otherwise warn
        if not found_archives:
           print(f"⚠️ No FIT files or archives found in {directory.name}")
        return
        
    print(f"Found {len(fit_files)} FIT files in {directory.name} (including extracted)...")
    
    for fit_file in fit_files:
        test_fit_file(fit_file)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_parser.py <path_to_fit_file_or_archive_or_directory>")
        print()
        print("Examples:")
        print("  python test_parser.py files/my_run.fit")
        print("  python test_parser.py files/my_archive.zip")
        print("  python test_parser.py files/")
        sys.exit(1)
    
    process_path(sys.argv[1])
