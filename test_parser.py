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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_parser.py <path_to_fit_file>")
        print()
        print("Example:")
        print("  python test_parser.py files/2024-05-09-095135-ELEMNT BOLT 3FFA-146-0.fit")
        sys.exit(1)
    
    test_fit_file(sys.argv[1])
