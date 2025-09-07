#!/usr/bin/env python3
"""
Simple ArrayRecord reader with correct API
"""
from array_record.python.array_record_module import ArrayRecordReader
import numpy as np

def read_arrayrecord_file(file_path):
    """Read ArrayRecord file and show actual content"""
    print(f"🔍 Reading: {file_path}")

    try:
        reader = ArrayRecordReader(file_path)

        record_count = 0
        for i, record in enumerate(reader):
            record_count += 1

            if i < 10:  # Show first 10 records
                print(f"\n📋 Record {i}:")
                print(f"   Type: {type(record)}")

                if isinstance(record, np.ndarray):
                    print(f"   Shape: {record.shape}")
                    print(f"   Dtype: {record.dtype}")
                    non_zero = np.count_nonzero(record)
                    print(f"   Non-zero elements: {non_zero} / {len(record)}")

                    # Show some actual values
                    print(f"   First 10 values: {record[:10]}")

                    # Look for realistic price data
                    realistic = record[(record > 0.1) & (record < 10000)]
                    if len(realistic) > 0:
                        print(f"   Realistic values (0.1-10000): {realistic[:20]}")
                else:
                    print(f"   Content: {str(record)[:200]}")

            if record_count >= 50:  # Don't read too many
                print(f"\n... (stopping after {record_count} records)")
                break

        print(f"\n✅ Total records: {record_count}")
        return record_count

    except Exception as e:
        print(f"❌ Error reading ArrayRecord: {e}")
        import traceback
        traceback.print_exc()
        return 0

# Test both files
files = [
    "/mnt/d/ats-data/training_data/89/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord",
    "/mnt/d/ats-data/training_data/89/TSLA_20250701_000000_20250906_000000/5m/TSLA_20250701_000000_20250906_000000.arrayrecord"
]

for file_path in files:
    print("\n" + "="*80)
    count = read_arrayrecord_file(file_path)
    if count > 1:
        print(f"🎉 SUCCESS: {count} records found (not just 1!)")
    else:
        print(f"❌ Only found {count} records")