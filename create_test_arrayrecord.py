#!/usr/bin/env python3

import json
from pathlib import Path

try:
    import array_record
    print("ArrayRecord top-level:", dir(array_record))

    # Try to find writer classes through different paths
    import pkgutil
    for importer, modname, ispkg in pkgutil.iter_modules(array_record.__path__, array_record.__name__ + "."):
        print(f"Found submodule: {modname}")
        try:
            submod = __import__(modname, fromlist=[''])
            print(f"  {modname} attributes: {[attr for attr in dir(submod) if 'Writer' in attr or 'Record' in attr]}")
        except Exception as e:
            print(f"  Error importing {modname}: {e}")

except ImportError as e:
    print(f"ArrayRecord import error: {e}")

# Investigate the python submodule more deeply
try:
    import array_record.python
    python_mod = array_record.python
    print(f"Python module path: {python_mod.__path__}")

    # Check all submodules in python
    import pkgutil
    for importer, modname, ispkg in pkgutil.iter_modules(python_mod.__path__, python_mod.__name__ + "."):
        print(f"Found python submodule: {modname}")
        try:
            submod = __import__(modname, fromlist=[''])
            attrs = [attr for attr in dir(submod) if not attr.startswith('_')]
            print(f"  {modname} public attributes: {attrs}")
        except Exception as e:
            print(f"  Error importing {modname}: {e}")
except Exception as e:
    print(f"Python submodule check error: {e}")

# Try direct imports
try:
    from array_record.python.array_record_writer import ArrayRecordWriter
    print("✅ Found ArrayRecordWriter in array_record.python.array_record_writer")
except ImportError as e:
    print(f"❌ ArrayRecordWriter import error: {e}")

try:
    from array_record.python import ArrayRecordWriter
    print("✅ Found ArrayRecordWriter in array_record.python")
except ImportError as e:
    print(f"❌ ArrayRecordWriter from python import error: {e}")

# Create test data
test_data = {
    'example_id': 'test_001',
    'symbol': 'TSLA',
    'prediction_timestamp': '2025-08-01T00:00:00',
    'instrument_id': 12345,
    'base_features': [1.0, 2.0, 3.0],
    'sequence_5m': [100.0, 101.0, 102.0],
    'sequence_15m': [100.5, 101.5, 102.5],
    'sequence_1h': [101.0, 102.0, 103.0],
    'sequence_1d': [105.0, 106.0, 107.0],
    'sequence_1w': [110.0, 115.0, 120.0]
}

# Write ArrayRecord file
output_path = Path("/data/training_data/52/TSLA/20250801_000000_20250802_000000.arrayrecord")
output_path.parent.mkdir(parents=True, exist_ok=True)

with array_record.ArrayRecordWriter(str(output_path), 'group_size:1') as writer:
    data = json.dumps(test_data, default=str).encode()
    writer.write_record(data)

print(f"✅ Created test ArrayRecord file: {output_path}")
print(f"File size: {output_path.stat().st_size} bytes")