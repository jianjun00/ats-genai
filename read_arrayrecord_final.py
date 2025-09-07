#!/usr/bin/env python3
"""
Read ArrayRecord files using the correct API
"""
from array_record.python.array_record_module import ArrayRecordReader
import numpy as np

def read_arrayrecord_file(file_path):
    """Read ArrayRecord file and show actual content"""
    print(f"🔍 Reading: {file_path}")

    try:
        reader = ArrayRecordReader(file_path)

        # Get total number of records
        total_records = reader.num_records()
        print(f"📊 Total records: {total_records}")

        if total_records == 0:
            print("❌ No records found")
            return 0

        # Read first few records
        for i in range(min(10, total_records)):
            reader.seek(i)
            record = reader.read()

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
                    print(f"   Realistic values (0.1-10000): {len(realistic)} found")
                    print(f"   Sample realistic values: {realistic[:10]}")

                    # Check if these could be OHLC data
                    if len(realistic) > 4:
                        print(f"   Could be OHLC data - min: {realistic.min():.2f}, max: {realistic.max():.2f}")

            elif isinstance(record, (list, tuple)):
                print(f"   Length: {len(record)}")
                print(f"   Content sample: {record[:10]}")
            else:
                print(f"   Content: {str(record)[:200]}")

        reader.close()
        return total_records

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
        print(f"\n🎉 SUCCESS: {count} records found! (The metadata was WRONG - it said only 1)")
    elif count == 1:
        print(f"\n⚠️  Only 1 record found (matches metadata)")
    else:
        print(f"\n❌ No records found")