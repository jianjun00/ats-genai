#!/usr/bin/env python3
"""
ArrayRecord API Compatibility Tests

Tests the critical fixes made for ArrayRecord import issues and ensures
that the correct C extension module is accessible.

Based on fixes documented in PRD: ArrayRecord Training Data System (September 4, 2025)
Issue: ArrayRecord classes located in C extension module, not main package
"""

import pytest
import tempfile
from pathlib import Path


def test_arrayrecord_import_path():
    """Test that ArrayRecord classes can be imported correctly."""
    try:
        from array_record.python.array_record_module import ArrayRecordWriter, ArrayRecordReader
        assert ArrayRecordWriter is not None
        assert ArrayRecordReader is not None
    except ImportError as e:
        pytest.fail(f"ArrayRecord import failed: {e}")


def test_arrayrecord_writer_instantiation():
    """Test that ArrayRecordWriter can be instantiated."""
    pytest.importorskip("array_record")
    
    from array_record.python.array_record_module import ArrayRecordWriter
    
    with tempfile.NamedTemporaryFile(suffix='.arrayrecord') as f:
        try:
            writer = ArrayRecordWriter(f.name, 'group_size:1')
            assert writer is not None
        except Exception as e:
            pytest.fail(f"ArrayRecordWriter instantiation failed: {e}")


def test_array_record_package_structure():
    """Verify ArrayRecord package has expected structure."""
    pytest.importorskip("array_record")
    
    import array_record
    
    # Test that main module exists but doesn't expose classes
    assert not hasattr(array_record, 'ArrayRecordWriter')
    assert not hasattr(array_record, 'ArrayRecordReader')
    
    # Test that python submodule exists
    from array_record import python
    assert python is not None
    
    # Test that C extension module is available
    from array_record.python import array_record_module
    assert hasattr(array_record_module, 'ArrayRecordWriter')
    assert hasattr(array_record_module, 'ArrayRecordReader')


def test_arrayrecord_writer_basic_functionality():
    """Test that ArrayRecordWriter can write and read basic data."""
    pytest.importorskip("array_record")
    
    from array_record.python.array_record_module import ArrayRecordWriter, ArrayRecordReader
    import json
    
    test_data = {"symbol": "TSLA", "price": 123.45}
    
    with tempfile.NamedTemporaryFile(suffix='.arrayrecord', delete=False) as f:
        file_path = f.name
    
    try:
        # Write data
        with ArrayRecordWriter(file_path, 'group_size:1') as writer:
            data_bytes = json.dumps(test_data).encode()
            writer.write_record(data_bytes)
        
        # Read data back
        with ArrayRecordReader(file_path) as reader:
            records = list(reader)
            assert len(records) == 1
            
            read_data = json.loads(records[0].decode())
            assert read_data == test_data
            
    finally:
        Path(file_path).unlink(missing_ok=True)


@pytest.mark.skipif(
    not pytest.importorskip("array_record", reason="array_record not available"),
    reason="array_record package not available"
)
def test_arrayrecord_with_datetime_serialization():
    """Test ArrayRecord with datetime objects using custom serializer."""
    from array_record.python.array_record_module import ArrayRecordWriter
    from datetime import datetime
    import json
    import tempfile
    
    def custom_json_serializer(obj):
        """Custom JSON serializer for datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)
    
    test_data = {
        "timestamp": datetime(2025, 8, 1, 10, 30, 0),
        "symbol": "TSLA",
        "price": 123.45
    }
    
    with tempfile.NamedTemporaryFile(suffix='.arrayrecord', delete=False) as f:
        file_path = f.name
    
    try:
        with ArrayRecordWriter(file_path, 'group_size:1') as writer:
            # Should not raise TypeError with custom serializer
            data_bytes = json.dumps(test_data, default=custom_json_serializer).encode()
            writer.write_record(data_bytes)
        
        # Verify file was created and has content
        assert Path(file_path).stat().st_size > 0
        
    finally:
        Path(file_path).unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])