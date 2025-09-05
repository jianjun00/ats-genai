#!/usr/bin/env python3
"""
JSON Datetime Serialization Tests

Tests the critical fixes made for JSON serialization of datetime objects
in the training data storage system.

Based on fixes documented in PRD: ArrayRecord Training Data System (September 4, 2025)
Issue: json.dumps() cannot serialize datetime objects in training data
Solution: Custom _json_serializer method with proper datetime.isoformat() conversion
"""

import pytest
import json
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


def test_custom_json_serializer():
    """Test custom JSON serializer handles datetime objects."""
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
    except ImportError:
        pytest.skip("SequenceStorageManager not available")
    
    manager = SequenceStorageManager("/tmp", StorageConfig())
    
    test_data = {
        'timestamp': datetime(2025, 8, 1, 10, 30, 0),
        'symbol': 'TSLA',
        'price': 123.45,
        'nested_data': {
            'created_at': datetime(2025, 9, 4, 15, 20, 10)
        }
    }
    
    # Should not raise TypeError
    serialized = json.dumps(test_data, default=manager._json_serializer)
    assert '2025-08-01T10:30:00' in serialized
    assert '2025-09-04T15:20:10' in serialized
    assert 'TSLA' in serialized
    assert '123.45' in serialized
    
    # Verify round-trip works
    deserialized = json.loads(serialized)
    assert deserialized['symbol'] == 'TSLA'
    assert deserialized['price'] == 123.45
    assert deserialized['timestamp'] == '2025-08-01T10:30:00'


def test_json_serializer_with_various_types():
    """Test JSON serializer handles various object types."""
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager
    except ImportError:
        pytest.skip("SequenceStorageManager not available")
        
    manager = SequenceStorageManager("/tmp")
    
    class CustomObject:
        def __init__(self):
            self.value = 42
    
    test_data = {
        'datetime': datetime.now(),
        'string': 'test',
        'int': 123,
        'float': 123.45,
        'list': [1, 2, 3],
        'dict': {'key': 'value'},
        'custom_obj': CustomObject(),
        'none': None
    }
    
    # Should handle all types without error
    serialized = json.dumps(test_data, default=manager._json_serializer)
    assert serialized is not None
    assert len(serialized) > 0


@pytest.mark.asyncio
async def test_datetime_objects_in_training_data():
    """Test that training data with datetime objects can be serialized."""
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager
    except ImportError:
        pytest.skip("SequenceStorageManager not available")
    
    class MockExample:
        def __init__(self):
            self.symbol = "TSLA"
            self.prediction_timestamp = datetime.now()
            self.instrument_id = 12345
            self.base_features = [1.0, 2.0, 3.0]
            self.sequence_5m = [{"open": 100.0, "close": 101.0, "timestamp": datetime.now()}]
            self.sequence_15m = [{"open": 101.0, "close": 102.0}]
            self.sequence_1h = [{"open": 102.0, "close": 103.0}]
            self.sequence_1d = [{"open": 103.0, "close": 104.0}]
            self.timeframe_features = {}
            self.future_1h = []
            self.future_1d = []
            self.sequence_length = {"5m": 1}
            self.prediction_horizon = {"1h": 1}
    
    import tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = SequenceStorageManager(temp_dir)
        examples = [MockExample()]
        
        # Should complete without JSON serialization errors
        try:
            result = await manager.save_sequence_batch(examples, "test_batch")
            assert result is not None
            assert result.get('batch_id') == "test_batch"
            assert result.get('example_count') == 1
        except Exception as e:
            if "array_record" in str(e):
                pytest.skip("ArrayRecord package not available")
            else:
                raise


def test_datetime_serialization_edge_cases():
    """Test datetime serialization edge cases."""
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager
    except ImportError:
        pytest.skip("SequenceStorageManager not available")
        
    manager = SequenceStorageManager("/tmp")
    
    # Test various datetime formats
    test_cases = [
        datetime(2025, 1, 1, 0, 0, 0),  # Start of year
        datetime(2025, 12, 31, 23, 59, 59),  # End of year
        datetime(2025, 2, 29, 12, 30, 45) if datetime(2025, 1, 1).replace(year=2024).year % 4 == 0 else datetime(2025, 2, 28, 12, 30, 45),  # Leap year handling
        datetime.now(),  # Current time
    ]
    
    for dt in test_cases:
        result = manager._json_serializer(dt)
        assert isinstance(result, str)
        assert 'T' in result  # ISO format marker
        
        # Verify it's valid ISO format
        parsed_back = datetime.fromisoformat(result)
        assert parsed_back == dt


def test_json_serializer_preserves_non_datetime_objects():
    """Test that non-datetime objects are handled correctly."""
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager
    except ImportError:
        pytest.skip("SequenceStorageManager not available")
        
    manager = SequenceStorageManager("/tmp")
    
    # Test that other object types use str() fallback
    class TestObject:
        def __str__(self):
            return "custom_string_representation"
    
    test_obj = TestObject()
    result = manager._json_serializer(test_obj)
    assert result == "custom_string_representation"
    
    # Test basic types pass through
    assert manager._json_serializer("string") == "string"
    assert manager._json_serializer(123) == "123"  # str() fallback
    assert manager._json_serializer(123.45) == "123.45"  # str() fallback


def test_sequence_record_serialization():
    """Test complete sequence record serialization with datetime objects."""
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager
    except ImportError:
        pytest.skip("SequenceStorageManager not available")
        
    manager = SequenceStorageManager("/tmp")
    
    # Create a realistic sequence record with datetime
    sequence_record = {
        'example_id': 'test_001',
        'symbol': 'TSLA',
        'prediction_timestamp': datetime(2025, 8, 1, 10, 30, 0),
        'instrument_id': 12345,
        'base_features': [1.0, 2.0, 3.0],
        'sequence_5m': [
            {
                "timestamp": datetime(2025, 8, 1, 10, 25, 0),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000
            }
        ]
    }
    
    # Should serialize without error
    serialized = json.dumps(sequence_record, default=manager._json_serializer)
    assert serialized is not None
    
    # Verify datetime objects were converted
    assert '2025-08-01T10:30:00' in serialized
    assert '2025-08-01T10:25:00' in serialized
    
    # Verify structure is preserved
    deserialized = json.loads(serialized)
    assert deserialized['symbol'] == 'TSLA'
    assert deserialized['instrument_id'] == 12345
    assert len(deserialized['sequence_5m']) == 1
    assert deserialized['sequence_5m'][0]['open'] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])