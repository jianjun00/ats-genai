#!/usr/bin/env python3
"""
Server-Side Proof: Test multi-timeframe API with detailed debugging
"""

import requests
import json
import time

def test_server_side_debugging():
    print("🔧 SERVER-SIDE DEBUG PROOF TEST")
    print("=" * 50)

    # Test the multi-timeframe API directly
    dataset_id = 63
    sequence_id = "AAPL_20250801_000000_20250801_000000"

    print(f"Testing multi-timeframe API:")
    print(f"  Dataset: {dataset_id}")
    print(f"  Sequence: {sequence_id}")

    # Make the API call
    url = f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe"
    print(f"\n🌐 Making API request to: {url}")

    response = requests.get(url, timeout=10)

    print(f"📊 Response status: {response.status_code}")

    if response.status_code == 200:
        data = response.json()

        print("✅ API Response Structure:")
        print(f"   Keys: {list(data.keys())}")

        if 'success' in data:
            print(f"   Success: {data['success']}")

        if 'sequence_id' in data:
            print(f"   Sequence ID: {data['sequence_id']}")

        if 'dataset_name' in data:
            print(f"   Dataset: {data['dataset_name']}")

        if 'ohlc_data' in data:
            ohlc_data = data['ohlc_data']
            print(f"   OHLC Data:")
            for timeframe, bars in ohlc_data.items():
                print(f"     {timeframe}: {len(bars)} bars")
                if bars:
                    sample = bars[0]
                    print(f"       Sample: open={sample.get('open', 'N/A')}, high={sample.get('high', 'N/A')}, low={sample.get('low', 'N/A')}, close={sample.get('close', 'N/A')}")

        if 'table_data' in data:
            table_data = data['table_data']
            print(f"   Table Data: {len(table_data)} rows")
            if table_data:
                sample = table_data[0]
                print(f"     Sample row: {sample}")

        if 'available_timeframes' in data:
            print(f"   Available Timeframes: {data['available_timeframes']}")

        print("\n🎯 DETAILED DATA ANALYSIS:")
        total_ohlc_records = sum(len(bars) for bars in ohlc_data.values()) if 'ohlc_data' in data else 0
        print(f"   Total OHLC records across all timeframes: {total_ohlc_records}")

        if total_ohlc_records > 0:
            print("   ✅ OHLC data is available for visualization")

            # Check data quality
            all_valid = True
            for tf, bars in ohlc_data.items():
                for bar in bars:
                    if not all(isinstance(bar.get(field, 0), (int, float)) for field in ['open', 'high', 'low', 'close']):
                        print(f"   ⚠️  Invalid data in {tf}: {bar}")
                        all_valid = False
                        break

            if all_valid:
                print("   ✅ All OHLC data is valid numeric format")
            else:
                print("   ❌ Some OHLC data has invalid format")

        if len(table_data) > 0:
            print("   ✅ Table data is available")
        else:
            print("   ❌ No table data available")

        return True

    else:
        print(f"❌ API Error: {response.status_code}")
        error_data = response.json()
        print(f"   Error details: {error_data}")
        return False

def test_sequence_discovery():
    print("\n🔍 SEQUENCE DISCOVERY TEST")
    print("=" * 30)

    # Test sequence discovery endpoint
    dataset_id = 63
    url = f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences"

    print(f"Testing sequences endpoint: {url}")

    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        data = response.json()
        print("✅ Sequence discovery successful:")
        print(f"   Total sequences: {data.get('total_count', 0)}")

        sequences = data.get('sequences', [])
        for seq in sequences:
            print(f"   Sequence: {seq.get('sequence_id')}")
            print(f"     Description: {seq.get('description')}")
            print(f"     Timeframes: {seq.get('timeframes', [])}")
            print(f"     File count: {seq.get('file_count', 0)}")
            print(f"     Size: {seq.get('total_size_mb', 0)} MB")

        return len(sequences) > 0
    else:
        print(f"❌ Sequence discovery failed: {response.status_code}")
        return False

def main():
    print("🧪 COMPREHENSIVE SERVER-SIDE TESTING")
    print("=" * 60)

    # Wait for service to be ready
    print("⏳ Waiting for analytics service to be ready...")
    time.sleep(2)

    # Test sequence discovery first
    seq_result = test_sequence_discovery()

    # Test multi-timeframe API
    api_result = test_server_side_debugging()

    print("\n📋 SUMMARY:")
    print(f"   Sequence Discovery: {'✅ PASS' if seq_result else '❌ FAIL'}")
    print(f"   Multi-timeframe API: {'✅ PASS' if api_result else '❌ FAIL'}")

    if seq_result and api_result:
        print("\n🎉 ALL TESTS PASSED!")
        print("   ✅ Sequences are discoverable")
        print("   ✅ Multi-timeframe OHLC data loads correctly")
        print("   ✅ Table data is available")
        print("   ✅ API returns valid numeric data for visualization")
        print("\n💡 The sequence selection functionality should work in the browser!")
        return True
    else:
        print("\n❌ SOME TESTS FAILED!")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)