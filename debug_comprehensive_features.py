#!/usr/bin/env python3
"""
Debug Comprehensive Features
Check that comprehensive features are still available in the response
"""

import requests
import json

def debug_comprehensive_features():
    """Debug that comprehensive features are still available."""
    print("🔍 Debugging Comprehensive Features Availability")
    print("="*60)

    try:
        # Test the multi-timeframe endpoint
        api_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        params = {"row_index": 10}

        response = requests.get(api_url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()

            # Check both table_data and comprehensive_features
            table_data = data.get('table_data', [])
            comprehensive_features = data.get('comprehensive_features', [])

            print(f"📊 Data Structure Analysis:")
            print(f"   table_data: {len(table_data)} rows")
            if table_data:
                first_table_row = table_data[0]
                print(f"     - Fields per row: {len(first_table_row) if isinstance(first_table_row, dict) else 'not dict'}")
                if isinstance(first_table_row, dict):
                    basic_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    found_fields = [f for f in basic_fields if f in first_table_row]
                    print(f"     - Basic OHLCV fields: {found_fields}")
                    print(f"     - Sample values: open=${first_table_row.get('open', 'N/A')}, close=${first_table_row.get('close', 'N/A')}")

            print(f"   comprehensive_features: {len(comprehensive_features)} rows")
            if comprehensive_features:
                first_comp_row = comprehensive_features[0]
                print(f"     - Fields per row: {len(first_comp_row) if isinstance(first_comp_row, dict) else 'not dict'}")
                if isinstance(first_comp_row, dict):
                    # Check for multi-timeframe features
                    timeframe_counts = {}
                    for key in first_comp_row.keys():
                        for tf in ['5m_', '15m_', '1h_', '1d_', '1w_']:
                            if key.startswith(tf):
                                timeframe_counts[tf] = timeframe_counts.get(tf, 0) + 1
                                break

                    print(f"     - Timeframe feature breakdown:")
                    for tf, count in timeframe_counts.items():
                        print(f"       * {tf}: {count} features")

                    # Show sample comprehensive features
                    sample_features = list(first_comp_row.items())[:10]
                    print(f"     - Sample features:")
                    for key, value in sample_features:
                        print(f"       * {key}: {value}")

            print(f"\n🎯 Summary:")
            print(f"   ✅ Table data suitable for UI display: {len(table_data)} rows with basic OHLCV")
            print(f"   ✅ Comprehensive features available: {len(comprehensive_features)} rows with {len(comprehensive_features[0]) if comprehensive_features else 0} features")

            # Verify no NaN values
            response_text = response.text
            if 'NaN' in response_text:
                print(f"   ❌ WARNING: Found NaN in response")
            else:
                print(f"   ✅ No NaN values found in JSON response")

        else:
            print(f"❌ API request failed: {response.status_code}")

    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_comprehensive_features()