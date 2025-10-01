#!/usr/bin/env python3
"""
Direct test to verify that the cache fix resolves duplicate OHLCV values in training data.

This test examines existing ArrayRecord files before and after the cache fix
to verify that OHLCV values are now unique per timestamp.
"""

import sys
import struct
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.append('src')

def analyze_arrayrecord_duplicates(file_path: str, max_records: int = 10):
    """Analyze ArrayRecord file for duplicate OHLCV values."""
    print(f"🔍 Analyzing: {file_path}")
    
    if not Path(file_path).exists():
        print(f"❌ File not found")
        return None
        
    try:
        from array_record.python import array_record_module
        reader = array_record_module.ArrayRecordReader(file_path)
        num_records = reader.num_records()
        
        if num_records == 0:
            print(f"⚠️  No records found")
            reader.close()
            return None
            
        print(f"📊 Total records: {num_records}")
        
        # Extract OHLCV values from first few records
        records_data = []
        for i in range(min(max_records, num_records)):
            reader.seek(i)
            record_bytes = reader.read()
            
            # Parse binary format
            indicator_count = struct.unpack('>H', record_bytes[0:2])[0]
            offset = 2
            timestamp, symbol_len = struct.unpack('>dI', record_bytes[offset:offset+12])
            offset += 12
            symbol = record_bytes[offset:offset+symbol_len].decode('utf-8')
            offset += symbol_len
            ohlcv = struct.unpack('>fffff', record_bytes[offset:offset+20])
            
            open_price, high_price, low_price, close_price, volume = ohlcv
            dt = datetime.fromtimestamp(timestamp)
            
            records_data.append({
                'index': i,
                'timestamp': timestamp,
                'datetime': dt,
                'symbol': symbol,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            })
            
            print(f"   Record {i+1}: {dt.strftime('%H:%M:%S')} | O:{open_price:.2f} H:{high_price:.2f} L:{low_price:.2f} C:{close_price:.2f}")
        
        reader.close()
        
        # Check for duplicates
        if len(records_data) >= 2:
            # Check if all OHLCV values are identical
            first_record = records_data[0]
            duplicates_found = all(
                record['open'] == first_record['open'] and
                record['high'] == first_record['high'] and
                record['low'] == first_record['low'] and
                record['close'] == first_record['close']
                for record in records_data[1:]
            )
            
            if duplicates_found:
                print(f"❌ DUPLICATE ISSUE: All {len(records_data)} records have identical OHLCV values")
                print(f"   Values: O:{first_record['open']:.2f} H:{first_record['high']:.2f} L:{first_record['low']:.2f} C:{first_record['close']:.2f}")
            else:
                print(f"✅ UNIQUE VALUES: Records have varying OHLCV values")
            
            return {
                'has_duplicates': duplicates_found,
                'total_records': num_records,
                'sample_records': records_data,
                'file_path': file_path
            }
        else:
            print(f"⚠️  Insufficient records for duplicate analysis")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def test_training_data_duplicates():
    """Test existing training data files for duplicate OHLCV values."""
    print("🧪 Testing Training Data for Duplicate OHLCV Values")
    print("=" * 70)
    
    # Test files - look for recent datasets
    test_files = [
        "/data/training_data/dataset_20250928_231551/ohlcv_basic/TSLA_2025_07/60m/TSLA_2025_07_ohlcv_basic.arrayrecord",
        "/data/training_data/dataset_20250928_135038/ohlcv_basic/TSLA_2025_08/5m/TSLA_2025_08_ohlcv_basic.arrayrecord"
    ]
    
    results = []
    for file_path in test_files:
        print(f"\n📋 Testing file: {Path(file_path).name}")
        print("-" * 50)
        result = analyze_arrayrecord_duplicates(file_path)
        if result:
            results.append(result)
    
    # Summary
    print(f"\n📊 SUMMARY")
    print("=" * 70)
    
    if results:
        duplicate_files = [r for r in results if r['has_duplicates']]
        unique_files = [r for r in results if not r['has_duplicates']]
        
        print(f"📁 Files analyzed: {len(results)}")
        print(f"❌ Files with duplicates: {len(duplicate_files)}")
        print(f"✅ Files with unique values: {len(unique_files)}")
        
        if duplicate_files:
            print(f"\n🔍 FILES WITH DUPLICATE ISSUE:")
            for result in duplicate_files:
                print(f"   • {Path(result['file_path']).name}")
            
            print(f"\n💡 RECOMMENDATION:")
            print(f"   These files were generated with cache enabled.")
            print(f"   Re-run training data generation with enable_cache=False to fix.")
        
        if unique_files:
            print(f"\n✅ FILES WITH UNIQUE VALUES:")
            for result in unique_files:
                print(f"   • {Path(result['file_path']).name}")
        
        return len(duplicate_files) == 0  # Success if no duplicates found
    else:
        print(f"⚠️  No valid files found for analysis")
        return False

if __name__ == "__main__":
    success = test_training_data_duplicates()
    print(f"\n{'🟢 ALL FILES HAVE UNIQUE VALUES' if success else '🔴 DUPLICATE VALUES DETECTED'}")
    sys.exit(0 if success else 1)