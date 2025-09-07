#!/usr/bin/env python3
"""
Debug ArrayRecord files in run 89 to identify what's wrong with the parsing
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pathlib import Path
import json

def debug_arrayrecord_file(file_path):
    """Debug a specific ArrayRecord file"""
    print(f"\n=== Debugging ArrayRecord file: {file_path} ===")

    file_path = Path(file_path)

    if not file_path.exists():
        print(f"❌ File does not exist: {file_path}")
        return

    # Check basic file info
    stat = file_path.stat()
    print(f"📊 File size: {stat.st_size} bytes ({stat.st_size / 1024:.1f} KB)")
    print(f"📅 Modified: {stat.st_mtime}")

    # Check if file is empty
    if stat.st_size == 0:
        print("❌ File is completely empty!")
        return

    # Try to read first few bytes to see if it's a valid binary file
    try:
        with open(file_path, 'rb') as f:
            first_bytes = f.read(32)
            print(f"🔍 First 32 bytes (hex): {first_bytes.hex()}")
            print(f"🔍 First 32 bytes (repr): {repr(first_bytes)}")
    except Exception as e:
        print(f"❌ Error reading file bytes: {e}")
        return

    # Try to read with ArrayRecord library
    try:
        from array_record.python.array_record_module import ArrayRecordReader

        print("📖 Attempting to read with ArrayRecordReader...")
        reader = ArrayRecordReader(str(file_path))

        print(f"✅ ArrayRecord opened successfully")

        # Try to read records
        records = []
        try:
            for i, record in enumerate(reader):
                records.append(record)
                if i >= 2:  # Just read first 3 records
                    break

            print(f"✅ Successfully read {len(records)} records")

            if records:
                first_record = records[0]
                print(f"📋 First record type: {type(first_record)}")
                if hasattr(first_record, 'shape'):
                    print(f"📋 First record shape: {first_record.shape}")
                if hasattr(first_record, 'dtype'):
                    print(f"📋 First record dtype: {first_record.dtype}")
                print(f"📋 First record size: {len(first_record) if hasattr(first_record, '__len__') else 'no len'}")
                print(f"📋 First record sample: {str(first_record)[:200]}...")

        except Exception as e:
            print(f"❌ Error reading records: {e}")
            import traceback
            traceback.print_exc()

    except ImportError:
        print("⚠️ ArrayRecord library not available, trying alternative approach")

    except Exception as e:
        print(f"❌ Error opening ArrayRecord: {e}")
        import traceback
        traceback.print_exc()

def debug_metadata_files():
    """Debug the metadata and column files associated with ArrayRecord"""
    base_path = Path("/mnt/d/ats-data/training_data/89")

    for symbol in ["AAPL", "TSLA"]:
        symbol_dir = base_path / f"{symbol}_20250701_000000_20250906_000000"

        print(f"\n=== Debugging {symbol} metadata ===")

        for timeframe in ["5m", "15m", "1h", "1d", "1w"]:
            tf_dir = symbol_dir / timeframe

            if tf_dir.exists():
                print(f"\n--- {timeframe} timeframe ---")

                # Check metadata file
                metadata_file = tf_dir / f"{symbol}_20250701_000000_20250906_000000_metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        print(f"✅ Metadata file exists")
                        print(f"   Example count: {metadata.get('example_count', 'unknown')}")
                        print(f"   Data format: {metadata.get('data_format', 'unknown')}")
                        print(f"   Total features: {metadata.get('total_features', 'unknown')}")
                        print(f"   Date range: {metadata.get('date_range', {})}")

                        # Check if there are any concerning values
                        if metadata.get('example_count', 0) <= 1:
                            print("⚠️  WARNING: Very low example count!")
                        if metadata.get('total_features', 0) == 0:
                            print("⚠️  WARNING: Zero features!")

                    except Exception as e:
                        print(f"❌ Error reading metadata: {e}")
                else:
                    print("❌ No metadata file found")

                # Check columns file
                columns_file = tf_dir / f"{symbol}_20250701_000000_20250906_000000_columns.json"
                if columns_file.exists():
                    try:
                        with open(columns_file, 'r') as f:
                            columns = json.load(f)
                        print(f"✅ Columns file exists with {len(columns)} columns")
                        print(f"   Sample columns: {list(columns.keys())[:5]}")
                    except Exception as e:
                        print(f"❌ Error reading columns: {e}")
                else:
                    print("❌ No columns file found")

                # Check ArrayRecord file
                arrayrecord_file = tf_dir / f"{symbol}_20250701_000000_20250906_000000.arrayrecord"
                if arrayrecord_file.exists():
                    print(f"✅ ArrayRecord file exists: {arrayrecord_file.stat().st_size} bytes")
                else:
                    print("❌ No ArrayRecord file found")

def main():
    """Main debug function"""
    print("🔍 Debugging ArrayRecord files in run 89...")

    # Debug metadata first
    debug_metadata_files()

    # Debug specific ArrayRecord files
    arrayrecord_files = [
        "/mnt/d/ats-data/training_data/89/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord",
        "/mnt/d/ats-data/training_data/89/TSLA_20250701_000000_20250906_000000/5m/TSLA_20250701_000000_20250906_000000.arrayrecord"
    ]

    for file_path in arrayrecord_files:
        debug_arrayrecord_file(file_path)

    print("\n🎯 Summary:")
    print("   - Check if example_count is too low")
    print("   - Check if ArrayRecord files are corrupted or incomplete")
    print("   - Check if the training data generation completed properly")

if __name__ == "__main__":
    main()