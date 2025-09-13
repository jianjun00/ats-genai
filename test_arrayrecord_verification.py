#!/usr/bin/env python3
"""
Verify ArrayRecord files contain real TSLA training data.
"""

import sys
sys.path.append('src')

import array_record.python.array_record_module as array_record
import numpy as np


def verify_arrayrecord_data(file_path):
    """Verify ArrayRecord file contains real TSLA data."""
    print(f"🔍 Verifying ArrayRecord file: {file_path}")

    try:
        # Check file size first
        import os
        file_size = os.path.getsize(file_path)
        print(f"   File size: {file_size} bytes")

        if file_size == 0:
            print(f"   ⚠️ File is empty")
            return False

        # Open ArrayRecord reader
        reader = array_record.ArrayRecordReader(file_path)

        print(f"   ArrayRecord reader methods: {dir(reader)}")

        # Try to read first few records
        record_count = 0
        sample_records = []

        # Try reading records one by one
        try:
            while record_count < 10:
                try:
                    # Try to read a record by index
                    record = reader.read(record_count)
                    if record is None:
                        break

                    # Convert bytes back to numpy array
                    data_array = np.frombuffer(record, dtype=np.float32)
                    sample_records.append(data_array)
                    record_count += 1

                    # Only show first 3 for inspection
                    if record_count <= 3:
                        print(f"   Record {record_count}: {len(data_array)} features")
                        print(f"   Sample values: {data_array[:10] if len(data_array) >= 10 else data_array}")

                except Exception as e:
                    print(f"   Error reading record {record_count}: {e}")
                    break

        except Exception as e:
            print(f"   Error in reading loop: {e}")

        print(f"\n📊 ArrayRecord Summary:")
        print(f"   Total records: {record_count}")
        if sample_records:
            print(f"   Features per record: {len(sample_records[0])}")

            # Check if we have realistic stock price data (values in 200-400 range for TSLA)
            realistic_prices = []
            for record in sample_records[:5]:
                if len(record) >= 4:  # Check first few features (likely OHLC)
                    ohlc_values = record[:4]
                    # TSLA prices should be in 200-400 range in July 2025
                    if any(200 < val < 400 for val in ohlc_values):
                        realistic_prices.append(ohlc_values)

            if realistic_prices:
                print(f"   ✅ Contains realistic TSLA price data:")
                for i, prices in enumerate(realistic_prices):
                    print(f"      Record {i+1} OHLC: O={prices[0]:.2f}, H={prices[1]:.2f}, L={prices[2]:.2f}, C={prices[3]:.2f}")
            else:
                print(f"   ⚠️ Price data not in expected TSLA range")

        return True

    except Exception as e:
        print(f"❌ Failed to read ArrayRecord: {e}")
        return False


if __name__ == "__main__":
    # Test multiple timeframe files
    base_dir = "/data/training_data/dataset_20250911_220405/TSLA_2025_07"
    timeframes = ["5m", "15m", "1h", "1d"]

    print("🧪 Verifying ArrayRecord training data files...")

    for timeframe in timeframes:
        file_path = f"{base_dir}/{timeframe}/TSLA_2025_07.arrayrecord"
        print(f"\n📁 {timeframe} timeframe:")
        verify_arrayrecord_data(file_path)