#!/usr/bin/env python3
"""
Read ArrayRecord files using correct API and container paths
"""
import numpy as np
from pathlib import Path

def try_arrayrecord_library(file_path):
    """Try to use the actual ArrayRecord library"""
    try:
        from array_record.python.array_record_module import ArrayRecordReader
        print(f"✅ ArrayRecord library is available")

        print(f"📂 Reading file: {file_path}")

        # Try to read with ArrayRecord
        reader = ArrayRecordReader(str(file_path))

        record_count = 0
        total_elements = 0

        for i, record in enumerate(reader):
            record_count += 1

            if i < 5:  # Show first 5 records in detail
                print(f"\n📋 Record {i}:")
                print(f"   Type: {type(record)}")

                # Try to show some actual data
                try:
                    if isinstance(record, np.ndarray):
                        print(f"   Shape: {record.shape}")
                        print(f"   Dtype: {record.dtype}")
                        print(f"   Data sample (first 10): {record[:10]}")
                        non_zero_count = np.count_nonzero(record)
                        print(f"   Non-zero elements: {non_zero_count} / {len(record)}")
                        total_elements += len(record)

                        # Look for realistic price data
                        reasonable_values = record[(record > 0.1) & (record < 10000)]
                        if len(reasonable_values) > 0:
                            print(f"   Reasonable price-like values: {reasonable_values[:10]}")
                            print(f"   Count of reasonable values: {len(reasonable_values)}")

                    elif hasattr(record, '__len__'):
                        print(f"   Length: {len(record)}")
                        print(f"   Data: {str(record)[:200]}")
                        total_elements += len(record)
                    else:
                        print(f"   Data: {str(record)[:200]}")

                except Exception as e:
                    print(f"   Error accessing data: {e}")

            if i >= 100:  # Don't read too many records, but more than before
                print(f"   ... (stopping after {i+1} records)")
                break

        print(f"\n✅ Total records found: {record_count}")
        print(f"✅ Total elements across all records: {total_elements}")

        return record_count

    except Exception as e:
        print(f"❌ Error using ArrayRecord library: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function to read ArrayRecord files in container"""
    arrayrecord_files = [
        "/data/training_data/89/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord",
        "/data/training_data/89/TSLA_20250701_000000_20250906_000000/5m/TSLA_20250701_000000_20250906_000000.arrayrecord"
    ]

    for file_path in arrayrecord_files:
        print(f"\n{'='*80}")
        print(f"Reading: {file_path}")
        print('='*80)

        # Check if file exists
        if not Path(file_path).exists():
            print(f"❌ File does not exist: {file_path}")
            continue

        # Try ArrayRecord library
        record_count = try_arrayrecord_library(file_path)

        if record_count is not None and record_count > 1:
            print(f"\n🎉 SUCCESS: Found {record_count} records (much more than the metadata suggested!)")
        elif record_count == 1:
            print(f"\n⚠️  Only 1 record found - matches metadata")
        else:
            print(f"\n❌ Could not determine record count")

if __name__ == "__main__":
    main()