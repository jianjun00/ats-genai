#!/usr/bin/env python3
"""
Fake data detection utility - fails fast when synthetic/mock data is detected.
Enforces CLAUDE.md principle: NO MOCK/SYNTHETIC DATA outside of unit tests.
"""
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

class FakeDataDetector:
    """Detects and rejects fake/synthetic/mock data in production code."""
    
    # Patterns that indicate fake data
    FAKE_DATA_INDICATORS = {
        # Synthetic timestamps
        'synthetic_timestamps': [
            '2025-08-01T00:00:00',  # Our test data timestamp
            '2024-01-01T00:00:00',  # Common test timestamp
            '1970-01-01T00:00:00',  # Epoch time
        ],
        
        # Fake data sources
        'synthetic_sources': [
            'synthetic_ohlc',
            'demo_data',
            'mock_data', 
            'test_data',
            'fake_data',
            'working_generator',
            'visualization_generator'
        ],
        
        # Fake dataset names
        'synthetic_dataset_names': [
            'Working_',
            'Test_',
            'Demo_',
            'Sample_', 
            'Mock_',
            'Visualization_Ready_'
        ],
        
        # Unrealistic OHLC patterns (too perfect/rounded)
        'suspicious_ohlc_patterns': [
            # Values that are too rounded (like 180.0, 250.0)
            lambda ohlc: any(abs(v - round(v, 0)) < 0.001 for v in ohlc if v),
            # All values exactly the same (no market movement)
            lambda ohlc: len(set(ohlc)) <= 1
        ]
    }
    
    @staticmethod
    def check_dataset_record(dataset_record: Dict[str, Any]) -> None:
        """Check database dataset record for fake data indicators."""
        dataset_name = dataset_record.get('dataset_name', '')
        data_sources = dataset_record.get('data_sources', [])
        created_by = dataset_record.get('created_by', '')
        
        # Check dataset name
        for pattern in FakeDataDetector.FAKE_DATA_INDICATORS['synthetic_dataset_names']:
            if pattern in dataset_name:
                raise ValueError(
                    f"FAKE DATA DETECTED: Dataset name '{dataset_name}' contains synthetic pattern '{pattern}'. "
                    f"No synthetic data allowed outside unit tests."
                )
        
        # Check data sources
        if isinstance(data_sources, list):
            sources_to_check = data_sources
        else:
            sources_to_check = [str(data_sources)]
            
        for source in sources_to_check:
            if source in FakeDataDetector.FAKE_DATA_INDICATORS['synthetic_sources']:
                raise ValueError(
                    f"FAKE DATA DETECTED: Data source '{source}' is synthetic. "
                    f"No synthetic data allowed outside unit tests."
                )
        
        # Check created_by
        synthetic_creators = ['working_generator', 'visualization_generator', 'test', 'demo']
        for creator in synthetic_creators:
            if creator in created_by.lower():
                raise ValueError(
                    f"FAKE DATA DETECTED: Creator '{created_by}' indicates synthetic data. "
                    f"No synthetic data allowed outside unit tests."
                )
    
    @staticmethod 
    def check_ohlc_data(ohlc_data: List[Dict[str, Any]]) -> None:
        """Check OHLC data for synthetic patterns."""
        if not ohlc_data:
            return
            
        sample = ohlc_data[0]
        
        # Check timestamps
        datetime_val = sample.get('datetime', '')
        for fake_timestamp in FakeDataDetector.FAKE_DATA_INDICATORS['synthetic_timestamps']:
            if fake_timestamp in str(datetime_val):
                raise ValueError(
                    f"FAKE DATA DETECTED: Timestamp '{datetime_val}' matches synthetic pattern '{fake_timestamp}'. "
                    f"No synthetic data allowed outside unit tests."
                )
        
        # Check OHLC values for suspicious patterns
        ohlc_fields = ['open', 'high', 'low', 'close']
        ohlc_values = [sample.get(field, 0) for field in ohlc_fields if sample.get(field)]
        
        if ohlc_values:
            for pattern_check in FakeDataDetector.FAKE_DATA_INDICATORS['suspicious_ohlc_patterns']:
                if pattern_check(ohlc_values):
                    raise ValueError(
                        f"FAKE DATA DETECTED: OHLC values {ohlc_values} show suspicious synthetic patterns. "
                        f"No synthetic data allowed outside unit tests."
                    )
    
    @staticmethod
    def check_api_response(api_response: Dict[str, Any]) -> None:
        """Check complete API response for fake data."""
        # Check source field
        source = api_response.get('source', '')
        if 'mock' in source.lower() or 'synthetic' in source.lower() or 'test' in source.lower():
            raise ValueError(
                f"FAKE DATA DETECTED: API response source '{source}' indicates synthetic data. "
                f"No synthetic data allowed outside unit tests."
            )
        
        # Check data content
        data = api_response.get('data', [])
        if data:
            FakeDataDetector.check_ohlc_data(data)
        
        # Check if this looks like a fallback response
        message = api_response.get('message', '').lower()
        if any(term in message for term in ['synthetic', 'demo', 'test', 'mock', 'sample']):
            raise ValueError(
                f"FAKE DATA DETECTED: API response message '{message}' indicates synthetic data. "
                f"No synthetic data allowed outside unit tests."
            )
    
    @staticmethod
    def enforce_real_data_only():
        """Add this call to ensure no fake data is ever returned."""
        pass  # This is a marker function for code clarity

def fail_on_fake_data(data: Any, context: str = "unknown") -> None:
    """
    Utility function to fail fast when fake data is detected.
    Use this throughout the codebase to enforce real data only.
    """
    try:
        if isinstance(data, dict):
            if 'data' in data and 'source' in data:
                # This looks like an API response
                FakeDataDetector.check_api_response(data)
            elif 'dataset_name' in data:
                # This looks like a dataset record
                FakeDataDetector.check_dataset_record(data)
            else:
                # Check if it's OHLC data
                if any(field in data for field in ['open', 'high', 'low', 'close']):
                    FakeDataDetector.check_ohlc_data([data])
        
        elif isinstance(data, list) and data:
            # Check list of data
            if isinstance(data[0], dict) and any(field in data[0] for field in ['open', 'high', 'low', 'close']):
                FakeDataDetector.check_ohlc_data(data)
    
    except ValueError as e:
        # Re-raise with context
        raise ValueError(f"FAKE DATA ERROR in {context}: {e}")

# Make the detector easily importable
__all__ = ['FakeDataDetector', 'fail_on_fake_data']