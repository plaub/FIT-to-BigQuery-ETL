"""
Test script to verify the map generator functionality.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.map_generator import generate_route_maps_base64


def test_generator():
    """Test generating map images with synthetic data."""
    print("=" * 80)
    print("Testing Map Generator")
    print("=" * 80)
    print()

    # Create some dummy points (approximating a small square route)
    dummy_records = [
        {'position_lat': 52.5200, 'position_long': 13.4050},  # Berlin center
        {'position_lat': 52.5200, 'position_long': 13.4150},
        {'position_lat': 52.5100, 'position_long': 13.4150},
        {'position_lat': 52.5100, 'position_long': 13.4050},
        {'position_lat': 52.5200, 'position_long': 13.4050},  # Back to start
    ]

    print(f"Testing with {len(dummy_records)} mock coordinate points...")
    
    try:
        mini_b64, large_b64 = generate_route_maps_base64(dummy_records)
        
        if mini_b64 and large_b64:
            print("✅ Successfully generated Base64 map images!")
            print()
            print(f"Mini preview format check:")
            print(f"  Prefix correct: {mini_b64.startswith('data:image/png;base64,')}")
            print(f"  Length: {len(mini_b64)} characters")
            print()
            print(f"Large preview format check:")
            print(f"  Prefix correct: {large_b64.startswith('data:image/png;base64,')}")
            print(f"  Length: {len(large_b64)} characters")
        else:
            print("❌ Generator returned None values instead of Base64 strings.")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error during map generation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print()
    print("=" * 80)
    print("Testing map generator with insufficient data (1 point)")
    print("=" * 80)
    print()
    
    try:
        mini_b64, large_b64 = generate_route_maps_base64([dummy_records[0]])
        if mini_b64 is None and large_b64 is None:
            print("✅ Successfully skipped map generation for insufficient points (returned None, None).")
        else:
            print("❌ Generator should have returned None, None for 1 point.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error during empty map generation: {e}")
        sys.exit(1)

    print()
    print("=" * 80)
    print("✅ All Tests Passed!")
    print("=" * 80)


if __name__ == "__main__":
    test_generator()
