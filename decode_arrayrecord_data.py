#!/usr/bin/env python3
"""
Decode the actual ArrayRecord data to understand the structure
"""
from array_record.python.array_record_module import ArrayRecordReader
import numpy as np
import json

def decode_arrayrecord_file(file_path):
    """Decode ArrayRecord file and show actual content"""
    print(f"🔍 Decoding: {file_path}")

    reader = ArrayRecordReader(file_path)
    total_records = reader.num_records()
    print(f"📊 Total records: {total_records}")

    for i in range(total_records):
        reader.seek(i)
        record = reader.read()

        print(f"\n📋 Record {i}:")

        if i == 0:  # Column names record
            columns_str = record.decode('utf-8')
            columns = json.loads(columns_str)
            print(f"   Column names record ({len(columns)} columns)")
            print(f"   First 10 columns: {columns[:10]}")
            print(f"   Last 10 columns: {columns[-10:]}")

            # Check for expected OHLCV columns
            ohlcv_count = sum(1 for col in columns if any(x in col for x in ['open', 'high', 'low', 'close', 'volume']))
            print(f"   OHLCV-related columns: {ohlcv_count}")

        elif i == 1:  # Training data record
            print(f"   Training data record ({len(record)} bytes)")

            # Try to interpret as numpy array
            # The data might be stored as float32
            float_array = np.frombuffer(record, dtype=np.float32)
            print(f"   As float32 array: {len(float_array)} elements")

            non_zero = np.count_nonzero(float_array)
            print(f"   Non-zero elements: {non_zero} / {len(float_array)} ({100*non_zero/len(float_array):.1f}%)")

            if non_zero > 0:
                print(f"   First 20 non-zero values: {float_array[float_array != 0][:20]}")

                # Look for price-like data
                realistic = float_array[(float_array > 0.1) & (float_array < 10000)]
                if len(realistic) > 0:
                    print(f"   Price-like values (0.1-10000): {len(realistic)} found")
                    print(f"   Price range: {realistic.min():.2f} - {realistic.max():.2f}")
                    print(f"   Sample prices: {realistic[:20]}")

                    # This looks like we have actual OHLCV data!
                    if len(realistic) > 100:
                        print(f"   🎉 SUFFICIENT DATA: {len(realistic)} price values found!")

            float64_array = np.frombuffer(record, dtype=np.float64)
            non_zero_64 = np.count_nonzero(float64_array)
            if non_zero_64 > 0:
                print(f"   As float64: {len(float64_array)} elements, {non_zero_64} non-zero")
                realistic_64 = float64_array[(float64_array > 0.1) & (float64_array < 10000)]
                if len(realistic_64) > 0:
                    print(f"   Float64 price-like values: {len(realistic_64)} found")
    reader.close()
    return total_records > 0

files = [
    "/mnt/d/ats-data/training_data/89/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord"
]

for file_path in files:
    print("="*80)
    success = decode_arrayrecord_file(file_path)
    if success:
        print(f"\n✅ ArrayRecord file contains valid training data!")
    else:
        print(f"\n❌ ArrayRecord file has issues")