#!/usr/bin/env python3
"""
JSON Response Sanitizer
Comprehensive sanitization of JSON responses to prevent NaN, Infinity, and other invalid JSON values
"""

import json
import math
from typing import Any, Dict, List, Union
import numpy as np

class JSONSanitizer:
    """Sanitizes data structures to ensure valid JSON serialization."""
    
    @staticmethod
    def sanitize_value(value: Any) -> Any:
        """Sanitize a single value to ensure JSON compatibility."""
        
        # Handle numpy types
        if isinstance(value, (np.integer, np.int8, np.int16, np.int32, np.int64)):
            return int(value)
        elif isinstance(value, (np.floating, np.float16, np.float32, np.float64)):
            if np.isnan(value) or np.isinf(value):
                return 0.0
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, np.ndarray):
            return JSONSanitizer.sanitize_value(value.tolist())
        
        # Handle Python float types
        elif isinstance(value, float):
            if math.isnan(value):
                return 0.0
            elif math.isinf(value):
                return 1e10 if value > 0 else -1e10  # Large but finite number
            return value
            
        # Handle dictionaries
        elif isinstance(value, dict):
            return {key: JSONSanitizer.sanitize_value(val) for key, val in value.items()}
            
        # Handle lists/tuples
        elif isinstance(value, (list, tuple)):
            return [JSONSanitizer.sanitize_value(item) for item in value]
            
        # Handle None, strings, bools, ints - these are already JSON safe
        elif value is None or isinstance(value, (str, bool, int)):
            return value
            
        # Handle complex types by converting to string
        else:
            try:
                # Try to convert to a basic type
                if hasattr(value, '__dict__'):
                    return str(value)
                else:
                    return value
            except:
                return str(value)
    
    @staticmethod
    def sanitize_response(data: Any) -> Any:
        """Sanitize an entire response data structure."""
        return JSONSanitizer.sanitize_value(data)
    
    @staticmethod
    def validate_json_serializable(data: Any) -> bool:
        """Check if data can be safely serialized to JSON."""
        try:
            json.dumps(JSONSanitizer.sanitize_response(data))
            return True
        except (TypeError, ValueError, OverflowError):
            return False
    
    @staticmethod
    def safe_json_dumps(data: Any, **kwargs) -> str:
        """Safely serialize data to JSON with sanitization."""
        sanitized_data = JSONSanitizer.sanitize_response(data)
        return json.dumps(sanitized_data, **kwargs)

# Convenience functions for common use cases
def sanitize_training_features(features: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize training feature dictionaries."""
    sanitized = {}
    for key, value in features.items():
        if isinstance(value, (int, float)):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                sanitized[key] = 0.0
            else:
                sanitized[key] = value
        elif isinstance(value, np.ndarray):
            # Handle numpy arrays
            clean_array = np.nan_to_num(value, nan=0.0, posinf=1e10, neginf=-1e10)
            sanitized[key] = clean_array.tolist()
        else:
            sanitized[key] = JSONSanitizer.sanitize_value(value)
    return sanitized

def sanitize_ohlc_data(ohlc_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sanitize OHLC data arrays."""
    sanitized_data = []
    for record in ohlc_data:
        sanitized_record = {}
        for key, value in record.items():
            if key in ['open', 'high', 'low', 'close', 'volume', 'vwap']:
                # Financial data should never be negative (except for some edge cases)
                if isinstance(value, (int, float)):
                    if math.isnan(value) or math.isinf(value):
                        sanitized_record[key] = 0.0
                    else:
                        sanitized_record[key] = max(0.0, float(value))
                else:
                    sanitized_record[key] = 0.0
            elif key == 'timestamp':
                # Timestamps should be positive integers
                if isinstance(value, (int, float)) and not (math.isnan(value) or math.isinf(value)):
                    sanitized_record[key] = int(value)
                else:
                    sanitized_record[key] = 0
            else:
                sanitized_record[key] = JSONSanitizer.sanitize_value(value)
        sanitized_data.append(sanitized_record)
    return sanitized_data

def validate_api_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and sanitize complete API response."""
    sanitized = JSONSanitizer.sanitize_response(response_data)
    
    # Additional validation for API responses
    if isinstance(sanitized, dict):
        # Ensure required fields are present and valid
        if 'success' not in sanitized:
            sanitized['success'] = True
            
        # Sanitize common API fields
        if 'ohlc_data' in sanitized:
            sanitized['ohlc_data'] = {
                tf: sanitize_ohlc_data(data) if isinstance(data, list) else data
                for tf, data in sanitized['ohlc_data'].items()
            }
            
        if 'table_data' in sanitized and isinstance(sanitized['table_data'], list):
            sanitized_table = []
            for row in sanitized['table_data']:
                if isinstance(row, dict):
                    sanitized_table.append(sanitize_training_features(row))
                else:
                    sanitized_table.append(JSONSanitizer.sanitize_value(row))
            sanitized['table_data'] = sanitized_table
    
    return sanitized