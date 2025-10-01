#!/usr/bin/env python3
"""
Test to reproduce OHLCV zero issue in ArrayRecord files.

This test demonstrates that:
1. ohlcv_basic files contain real OHLCV data
2. technical_momentum files contain zero OHLCV but real indicators
3. The feature service should only use ohlcv_basic for OHLCV extraction
"""

import sys
import struct
from pathlib import Path

try:
    from array_record.python import array_record_module
    ARRAYRECORD_AVAILABLE = True
except ImportError:
    ARRAYRECORD_AVAILABLE = False
    print("❌ ArrayRecord not available")
    sys.exit(1)

def analyze_arrayrecord_file(file_path: str, feature_group: str):
    """Analyze an ArrayRecord file and extract OHLCV data."""
    print(f"\n🔍 Analyzing {feature_group}: {file_path}")
    
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return None
        
    try:
        reader = array_record_module.ArrayRecordReader(file_path)
        num_records = reader.num_records()
        print(f"📊 Total records: {num_records}")
        
        if num_records == 0:
            print("⚠️  No records found")
            reader.close()
            return None
            
        # Read first record
        reader.seek(0)
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
        
        print(f"   📅 First record timestamp: {timestamp}")
        print(f"   📊 Symbol: {symbol}")
        print(f"   💰 OHLCV: O:{open_price:.2f} H:{high_price:.2f} L:{low_price:.2f} C:{close_price:.2f}")
        print(f"   📈 Volume: {volume:.0f}")
        print(f"   🔧 Indicators: {indicator_count}")
        
        reader.close()
        
        return {
            'feature_group': feature_group,
            'has_real_ohlcv': any(val != 0.0 for val in ohlcv[:4]),  # Check if O/H/L/C are non-zero
            'ohlcv': ohlcv,
            'indicator_count': indicator_count,
            'record_count': num_records
        }
        
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return None

def test_ohlcv_data_issue():
    """Test that reproduces the OHLCV zero issue."""
    print("🧪 Testing OHLCV Data Issue in ArrayRecord Files")
    print("=" * 60)
    
    # Test files from dataset_20250928_231551
    base_path = "/data/training_data/dataset_20250928_231551"
    symbol_period = "TSLA_2025_07"
    timeframe = "60m"
    
    test_files = [
        {
            'path': f"{base_path}/ohlcv_basic/{symbol_period}/{timeframe}/{symbol_period}_ohlcv_basic.arrayrecord",
            'feature_group': 'ohlcv_basic'
        },
        {
            'path': f"{base_path}/technical_momentum/{symbol_period}/{timeframe}/{symbol_period}_technical_momentum.arrayrecord", 
            'feature_group': 'technical_momentum'
        },
        {
            'path': f"{base_path}/technical_volatility/{symbol_period}/{timeframe}/{symbol_period}_technical_volatility.arrayrecord",
            'feature_group': 'technical_volatility'
        }
    ]
    
    results = []
    for test_file in test_files:
        result = analyze_arrayrecord_file(test_file['path'], test_file['feature_group'])
        if result:
            results.append(result)
    
    print(f"\n📋 Test Results Summary:")
    print("=" * 60)
    
    for result in results:
        status = "✅ HAS REAL OHLCV" if result['has_real_ohlcv'] else "❌ ZERO OHLCV"
        print(f"{result['feature_group']:20} | {status:15} | Records: {result['record_count']:3} | Indicators: {result['indicator_count']:2}")
    
    # Validation
    ohlcv_basic_has_data = any(r['feature_group'] == 'ohlcv_basic' and r['has_real_ohlcv'] for r in results)
    technical_has_zero = any(r['feature_group'] in ['technical_momentum', 'technical_volatility'] and not r['has_real_ohlcv'] for r in results)
    
    print(f"\n🎯 Issue Reproduction:")
    if ohlcv_basic_has_data and technical_has_zero:
        print("✅ REPRODUCED: ohlcv_basic has real data, technical files have zero OHLCV")
        print("💡 Solution: Feature service should only use ohlcv_basic for OHLCV extraction")
        return True
    else:
        print("❌ Could not reproduce the issue")
        return False

if __name__ == "__main__":
    success = test_ohlcv_data_issue()
    sys.exit(0 if success else 1)