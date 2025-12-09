"""
FIT file parser.
Extracts session and record data from FIT files using fitparse library.
"""

import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from garmin_fit_sdk import Decoder, Stream

logger = logging.getLogger(__name__)


def semicircles_to_degrees(semicircles: Optional[int]) -> Optional[float]:
    """
    Convert GPS coordinates from semicircles to decimal degrees.
    
    Args:
        semicircles: GPS coordinate in semicircles (2^31 semicircles = 180 degrees)
        
    Returns:
        Coordinate in decimal degrees, or None if input is None
    """
    if semicircles is None:
        return None
    return semicircles * (180.0 / 2**31)


def safe_get_value(data_dict: Dict[str, Any], key: str, default=None) -> Any:
    """
    Safely get value from dictionary, handling None values.
    
    Args:
        data_dict: Dictionary to extract from
        key: Key to look for
        default: Default value if key not found or value is None
        
    Returns:
        Value or default
    """
    value = data_dict.get(key)
    return value if value is not None else default


class FITParser:
    """Parser for FIT files to extract sessions and records data."""
    
    def __init__(self, file_path: Path, file_hash: str):
        """
        Initialize FIT parser.
        
        Args:
            file_path: Path to FIT file
            file_hash: SHA-256 hash of the file
        """
        self.file_path = file_path
        self.file_hash = file_hash
        self.session_id = str(uuid.uuid4())
        self.messages = None
        self.errors = None
        
    def parse(self) -> tuple:
        """
        Parse FIT file and extract session and records data.
        
        Returns:
            Tuple of (session_data, records_data)
        """
        try:
            logger.info(f"Parsing {self.file_path.name}")
            
            # Use Garmin SDK to decode the FIT file
            stream = Stream.from_file(str(self.file_path))
            decoder = Decoder(stream)
            self.messages, self.errors = decoder.read()
            
            # Log any decoding errors but continue processing
            if self.errors:
                logger.warning(f"Decoding errors in {self.file_path.name}: {self.errors}")
            
            session_data = self._extract_session_data()
            records_data = self._extract_records_data()
            
            logger.info(f"Extracted session and {len(records_data)} records from {self.file_path.name}")
            return session_data, records_data
        
        except Exception as e:
            logger.error(f"Error parsing {self.file_path.name}: {e}")
            raise
    
    def _extract_session_data(self) -> Dict[str, Any]:
        """Extract session-level summary data."""
        session_data = {
            'file_hash': self.file_hash,
            'filename': self.file_path.name,
            'session_id': self.session_id,
            'created_at': datetime.utcnow(),
        }
        
        # Extract file_id data
        try:
            if 'file_id_mesgs' in self.messages and self.messages['file_id_mesgs']:
                data = self.messages['file_id_mesgs'][0]  # Use first file_id message
                session_data['timestamp'] = data.get('time_created')
                session_data['manufacturer'] = data.get('manufacturer')
                session_data['product'] = str(data.get('product', ''))
                session_data['serial_number'] = data.get('serial_number')
        except Exception as e:
            logger.warning(f"Could not read file_id message: {e}. Continuing without file metadata.")
        
        # Extract session data
        try:
            if 'session_mesgs' in self.messages and self.messages['session_mesgs']:
                data = self.messages['session_mesgs'][0]  # Use first session message
                
                session_data['start_time'] = data.get('start_time')
                session_data['sport'] = data.get('sport')
                session_data['sub_sport'] = data.get('sub_sport')
                
                # Time metrics
                session_data['total_elapsed_time'] = data.get('total_elapsed_time')
                session_data['total_timer_time'] = data.get('total_timer_time')
                
                # Distance and speed
                session_data['total_distance'] = data.get('total_distance')
                session_data['avg_speed'] = data.get('avg_speed') or data.get('enhanced_avg_speed')
                session_data['max_speed'] = data.get('max_speed') or data.get('enhanced_max_speed')
                
                # Cadence
                session_data['avg_cadence'] = data.get('avg_cadence')
                session_data['max_cadence'] = data.get('max_cadence')
                
                # Heart rate
                session_data['min_heart_rate'] = data.get('min_heart_rate')
                session_data['avg_heart_rate'] = data.get('avg_heart_rate')
                session_data['max_heart_rate'] = data.get('max_heart_rate')
                
                # Power
                session_data['avg_power'] = data.get('avg_power')
                session_data['max_power'] = data.get('max_power')
                session_data['normalized_power'] = data.get('normalized_power')
                session_data['threshold_power'] = data.get('threshold_power')
                
                # Work and calories
                session_data['total_work'] = data.get('total_work')
                session_data['total_calories'] = data.get('total_calories')
                
                # Altitude
                session_data['min_altitude'] = data.get('min_altitude') or data.get('enhanced_min_altitude')
                session_data['avg_altitude'] = data.get('avg_altitude') or data.get('enhanced_avg_altitude')
                session_data['max_altitude'] = data.get('max_altitude') or data.get('enhanced_max_altitude')
                session_data['total_ascent'] = data.get('total_ascent')
                session_data['total_descent'] = data.get('total_descent')
                
                # Grade
                session_data['avg_grade'] = data.get('avg_grade')
                session_data['max_pos_grade'] = data.get('max_pos_grade')
                session_data['max_neg_grade'] = data.get('max_neg_grade')
                
                # Temperature
                session_data['avg_temperature'] = data.get('avg_temperature')
                session_data['max_temperature'] = data.get('max_temperature')
                
                # Training metrics
                session_data['training_stress_score'] = data.get('training_stress_score')
                session_data['intensity_factor'] = data.get('intensity_factor')
                
                # Laps
                session_data['num_laps'] = data.get('num_laps')
        except Exception as e:
            logger.warning(f"Could not read session message: {e}. Session summary may be incomplete.")
        
        return session_data
    
    def _extract_records_data(self) -> List[Dict[str, Any]]:
        """Extract record-level time-series data."""
        records = []
        record_sequence = 0
        
        try:
            if 'record_mesgs' in self.messages and self.messages['record_mesgs']:
                for data in self.messages['record_mesgs']:
                    timestamp = data.get('timestamp')
                    
                    if not timestamp:
                        continue
                    
                    record_id = f"{self.file_hash}_{timestamp.isoformat()}_{record_sequence}"
                    
                    record_data = {
                        'session_id': self.session_id,
                        'file_hash': self.file_hash,
                        'record_id': record_id,
                        'timestamp': timestamp,
                    }
                    
                    # GPS coordinates - convert from semicircles to degrees
                    position_lat = data.get('position_lat')
                    position_long = data.get('position_long')
                    
                    record_data['position_lat'] = semicircles_to_degrees(position_lat)
                    record_data['position_long'] = semicircles_to_degrees(position_long)
                    record_data['gps_accuracy'] = data.get('gps_accuracy')
                    
                    # Altitude
                    record_data['altitude'] = data.get('altitude')
                    record_data['enhanced_altitude'] = data.get('enhanced_altitude')
                    record_data['grade'] = data.get('grade')
                    
                    # Distance
                    record_data['distance'] = data.get('distance')
                    
                    # Vitals
                    record_data['heart_rate'] = data.get('heart_rate')
                    record_data['cadence'] = data.get('cadence')
                    record_data['power'] = data.get('power')
                    
                    # Speed
                    record_data['speed'] = data.get('speed')
                    record_data['enhanced_speed'] = data.get('enhanced_speed')
                    
                    # Other
                    record_data['temperature'] = data.get('temperature')
                    record_data['calories'] = data.get('calories')
                    record_data['battery_soc'] = data.get('battery_soc')
                    
                    records.append(record_data)
                    record_sequence += 1
        except Exception as e:
            logger.warning(f"Stopped reading records after {record_sequence} records: {e}")
        
        return records


def parse_fit_file(file_path: Path, file_hash: str) -> tuple:
    """
    Parse a FIT file and return session and records data.
    
    Args:
        file_path: Path to FIT file
        file_hash: SHA-256 hash of the file
        
    Returns:
        Tuple of (session_data, records_data)
    """
    parser = FITParser(file_path, file_hash)
    return parser.parse()
