#!/usr/bin/env python3
"""
Actually read the content of ArrayRecord files to see what's inside
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

import numpy as np
from pathlib import Path

def read_arrayrecord_with_numpy(file_path):
    """Try to read ArrayRecord file using numpy binary reading"""
    print(f"🔍 Reading ArrayRecord file: {file_path}")

    try:
        # Read as raw bytes first
        with open(file_path, 'rb') as f:
            data = f.read()

        print(f"📊 Total file size: {len(data)} bytes")

        # Try to interpret as numpy array
        try:
            # ArrayRecord might be structured binary data
            # Let's try different approaches

            # Try to read as float64 array
            float_data = np.frombuffer(data, dtype=np.float64)
            print(f"🔢 As float64: {len(float_data)} elements")
            if len(float_data) > 0:
                print(f"   First 10 values: {float_data[:10]}")
                non_zero = float_data[float_data != 0.0]
                print(f"   Non-zero values: {len(non_zero)} / {len(float_data)}")
                if len(non_zero) > 0:
                    print(f"   Non-zero sample: {non_zero[:10]}")

        except Exception as e:
            print(f"❌ Error reading as float64: {e}")

        # Try as float32
        try:
            float32_data = np.frombuffer(data, dtype=np.float32)
            print(f"🔢 As float32: {len(float32_data)} elements")
            if len(float32_data) > 0:
                print(f"   First 10 values: {float32_data[:10]}")
                non_zero = float32_data[float32_data != 0.0]
                print(f"   Non-zero values: {len(non_zero)} / {len(float32_data)}")
                if len(non_zero) > 0:
                    print(f"   Non-zero sample: {non_zero[:10]}")

        except Exception as e:
            print(f"❌ Error reading as float32: {e}")

        # Check for patterns in raw bytes
        zero_bytes = data.count(b'\x00')
        print(f"🔍 Zero bytes: {zero_bytes} / {len(data)} ({100*zero_bytes/len(data):.1f}%)")

        # Look for non-zero sections
        non_zero_positions = []
        for i, byte in enumerate(data[:1000]):  # Check first 1000 bytes
            if byte != 0:
                non_zero_positions.append(i)

        print(f"🎯 Non-zero byte positions (first 20): {non_zero_positions[:20]}")

        return True

    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False

def try_arrayrecord_library(file_path):
    """Try to use the actual ArrayRecord library if available"""
    try:
        import array_record
        print(f"✅ ArrayRecord library is available")

        # Try to read with ArrayRecord
        reader = array_record.ArrayRecordReader(str(file_path))

        record_count = 0
        for i, record in enumerate(reader):
            record_count += 1
            if i < 3:  # Show first 3 records
                print(f"📋 Record {i}:")
                print(f"   Type: {type(record)}")
                if hasattr(record, 'shape'):
                    print(f"   Shape: {record.shape}")
                if hasattr(record, 'dtype'):
                    print(f"   Dtype: {record.dtype}")

                # Try to show some actual data
                try:
                    if hasattr(record, '__len__') and len(record) > 0:
                        if isinstance(record, np.ndarray):
                            print(f"   Data sample: {record[:10]}")
                            non_zero_count = np.count_nonzero(record)
                            print(f"   Non-zero elements: {non_zero_count} / {len(record)}")
                        else:
                            print(f"   Data: {str(record)[:200]}")
                except Exception as e:
                    print(f"   Error accessing data: {e}")

            if i >= 10:  # Don't read too many records
                break

        print(f"✅ Total records found: {record_count}")
        return record_count

    except ImportError:
        print("❌ ArrayRecord library not available")
        return None
    except Exception as e:
        print(f"❌ Error using ArrayRecord library: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to read ArrayRecord files"""
    arrayrecord_files = [
        "/mnt/d/ats-data/training_data/89/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord",
        "/mnt/d/ats-data/training_data/89/TSLA_20250701_000000_20250906_000000/5m/TSLA_20250701_000000_20250906_000000.arrayrecord"
    ]

    for file_path in arrayrecord_files:
        print(f"\n{'='*60}")
        print(f"Reading: {file_path}")
        print('='*60)

        # Try ArrayRecord library first
        record_count = try_arrayrecord_library(file_path)

        # Also try numpy binary reading
        read_arrayrecord_with_numpy(file_path)

        if record_count is not None and record_count > 1:
            print(f"✅ SUCCESS: Found {record_count} records (not just 1!)")
        elif record_count == 1:
            print("⚠️  Only 1 record found - matches metadata")
        else:
            print("❌ Could not determine record count")

if __name__ == "__main__":
    main()