#!/usr/bin/env python3
"""
Simple ArrayRecord reader for testing training data files.
"""

import sys
import array_record.python.array_record_module as array_record
import struct
from datetime import datetime

def read_arrayrecord_file(file_path, limit=10):
    """Read and display ArrayRecord file contents."""

    reader = array_record.ArrayRecordReader(str(file_path))
    total_records = reader.num_records()

    print(f"ArrayRecord File: {file_path}")
    print(f"Total Records: {total_records:,}")
    print("=" * 60)

    if total_records == 0:
        print("❌ Empty ArrayRecord file")
        return

    # Show up to 'limit' records
    display_count = min(limit, total_records)

    for i in range(display_count):
        reader.seek(i)
        record = reader.read()

        print(f"\n📋 Record {i+1}:")
        print(f"   Size: {len(record)} bytes")

        # Parse our binary format
        indicator_count = struct.unpack('>H', record[:2])[0]
        timestamp = struct.unpack('>d', record[2:10])[0]
        symbol_len = struct.unpack('>I', record[10:14])[0]
        symbol = record[14:14+symbol_len].decode('utf-8')
        ohlcv_data = struct.unpack('>fffff', record[14+symbol_len:14+symbol_len+20])

        dt = datetime.fromtimestamp(timestamp)
        print(f"   📅 Time: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   📊 Symbol: {symbol}")
        print(f"   💰 OHLCV: O=${ohlcv_data[0]:.2f}, H=${ohlcv_data[1]:.2f}, L=${ohlcv_data[2]:.2f}, C=${ohlcv_data[3]:.2f}")
        print(f"   📈 Volume: {ohlcv_data[4]:,.0f}")
        print(f"   🔧 Indicators: {indicator_count}")

        # Parse all indicators
        indicator_offset = 14 + symbol_len + 20
        indicators_shown = 0
        for _ in range(indicator_count):
            if indicator_offset + 6 < len(record):
                key_len = struct.unpack('>H', record[indicator_offset:indicator_offset+2])[0]
                if indicator_offset + 2 + key_len + 4 <= len(record):
                    key = record[indicator_offset+2:indicator_offset+2+key_len].decode('utf-8')
                    value = struct.unpack('>f', record[indicator_offset+2+key_len:indicator_offset+2+key_len+4])[0]
                    print(f"        {key}: {value:.6f}")
                    indicator_offset += 2 + key_len + 4
                    indicators_shown += 1
                else:
                    break
            else:
                break

        # All indicators are now shown
        # No need to show truncation message

    if total_records > display_count:
        print(f"\n... and {total_records - display_count} more records")

    print(f"\n🎯 Summary:")
    print(f"   Total records: {total_records:,}")
    print(f"   File size: {len(record) * total_records / 1024:.1f} KB (estimated)")
    print(f"   Average record size: {len(record)} bytes")

    return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 arrayrecord_reader.py <file.arrayrecord>")
        sys.exit(1)

    file_path = sys.argv[1]
    sys.exit(read_arrayrecord_file(file_path))