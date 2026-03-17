"""
Map generation module for GPS routes.
Converts GPS point sequences into Base64 encoded static map images.
"""

import io
import base64
import logging
from typing import List, Dict, Any, Tuple, Optional
from staticmap import StaticMap, Line

logger = logging.getLogger(__name__)

def generate_route_maps_base64(records: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate static map images from a sequence of records containing GPS coordinates.
    Generates two images:
    1. A mini preview (300x300 px) for list views.
    2. A large image (800x450 px, 16:9 ratio) for web view.
    
    Args:
        records: List of dictionaries containing 'position_lat' and 'position_long'.
                 Assumed to be sorted by time (as they come from the FIT file).
                 
    Returns:
        A tuple of (mini_preview_base64, large_base64).
        If not enough valid points are found, returns (None, None).
    """
    # Extract valid coordinates
    coordinates = []
    for record in records:
        lat = record.get('position_lat')
        lon = record.get('position_long')
        
        # Only include valid coordinates (excluding None and 0.0/0.0 which often means no fix)
        if lat is not None and lon is not None:
            # Note: staticmap expects (lon, lat) tuple order
            coordinates.append((lon, lat))
            
    # We need at least 2 points to draw a line
    if len(coordinates) < 2:
        logger.info(f"Not enough valid GPS points for a map (found {len(coordinates)}). Skipping map generation.")
        return None, None
        
    logger.info(f"Generating maps for route with {len(coordinates)} GPS points.")
    
    try:
        # Create the route line
        # We use a distinct color (e.g., #FC4C02 - Strava Orange, or #FF0000 - Red)
        # and a line width of 3 pixels
        route_line_mini = Line(coordinates, color='#FF3366', width=3)
        route_line_large = Line(coordinates, color='#FF3366', width=4)
        
        # Generate Mini Preview (Square 1:1)
        mini_map = StaticMap(300, 300)
        mini_map.add_line(route_line_mini)
        img_mini = mini_map.render()
        
        # Generate Large View (Widescreen 16:9)
        large_map = StaticMap(800, 450)
        large_map.add_line(route_line_large)
        img_large = large_map.render()
        
        # Convert to Base64
        mini_base64 = _image_to_base64(img_mini)
        large_base64 = _image_to_base64(img_large)
        
        return mini_base64, large_base64
        
    except Exception as e:
        logger.error(f"Error generating map images: {e}", exc_info=True)
        return None, None

def _image_to_base64(img) -> str:
    """Helper formatting Pillow Image to a Base64 string for HTML use."""
    buffer = io.BytesIO()
    # Save the Pillow image to the buffer as PNG
    img.save(buffer, format='PNG')
    # Encode buffer contents as base64
    b64_str = base64.b64encode(buffer.getvalue()).decode('utf-8')
    # Prefix with the data URI scheme
    return f"data:image/png;base64,{b64_str}"
