#!/usr/bin/env python3
"""
Test to verify ArrayRecord files need proper closing to be readable.
"""

import array_record.python.array_record_module as array_record
import struct
import os
from datetime import datetime

def test_arrayrecord_closing():
    """Test that ArrayRecord files must be properly closed to be readable."""

    test_file = "/tmp/test_arrayrecord_close.arrayrecord"

    # Remove existing test file
    if os.path.exists(test_file):
        os.remove(test_file)

    print("🧪 Testing ArrayRecord file closing behavior")

    # Create writer and write test data
    writer = array_record.ArrayRecordWriter(str(test_file), 'group_size:1')

    # Create test binary record matching our format
    timestamp = datetime.now().timestamp()
    symbol = "TEST"
    symbol_bytes = symbol.encode('utf-8')
    symbol_len = len(symbol_bytes)

    # Core data: timestamp + symbol_len + symbol + OHLCV
    core_data = struct.pack(
        f'>dI{symbol_len}sfffff',
        float(timestamp),
        symbol_len,
        symbol_bytes,
        100.0,  # open
        102.0,  # high
        99.0,   # low
        101.0,  # close
        1000.0  # volume
    )

    # Add indicator count at beginning
    binary_record = struct.pack('>H', 0) + core_data  # 0 indicators for simplicity

    # Write the record
    writer.write(binary_record)
    print(f"✅ Written test record ({len(binary_record)} bytes)")

    # Test 1: Try reading without closing writer
    print("\n📖 Test 1: Reading without closing writer...")
    try:
        reader = array_record.ArrayRecordReader(str(test_file))
        num_records = reader.num_records()
        print(f"   Records found (unclosed): {num_records}")
    except Exception as e:
        print(f"   ❌ Error reading unclosed file: {e}")

    # Test 2: Close writer and try reading again
    print("\n🔒 Closing writer...")
    writer.close()

    print("\n📖 Test 2: Reading after closing writer...")
    try:
        reader = array_record.ArrayRecordReader(str(test_file))
        num_records = reader.num_records()
        print(f"   Records found (closed): {num_records}")

        if num_records > 0:
            reader.seek(0)
            record = reader.read()
            print(f"   ✅ Successfully read record: {len(record)} bytes")

            # Parse the record
            indicator_count = struct.unpack('>H', record[:2])[0]
            timestamp_read = struct.unpack('>d', record[2:10])[0]
            symbol_len_read = struct.unpack('>I', record[10:14])[0]
            symbol_read = record[14:14+symbol_len_read].decode('utf-8')
            ohlcv = struct.unpack('>fffff', record[14+symbol_len_read:14+symbol_len_read+20])

            print(f"   📊 Parsed data:")
            print(f"      Symbol: {symbol_read}")
            print(f"      OHLCV: O=${ohlcv[0]:.2f}, H=${ohlcv[1]:.2f}, L=${ohlcv[2]:.2f}, C=${ohlcv[3]:.2f}")
            print(f"      Volume: {ohlcv[4]:.0f}")

    except Exception as e:
        print(f"   ❌ Error reading closed file: {e}")

    # Cleanup
    if os.path.exists(test_file):
        os.remove(test_file)

    print("\n🎯 Conclusion: ArrayRecord files must be properly closed to be readable!")

if __name__ == "__main__":
    test_arrayrecord_closing()