#!/usr/bin/env python3
"""
Debug Table Data Content
Check exactly what's in the table_data response and how it should be displayed
"""

import requests
import json

def debug_table_data():
    """Debug the table data content in detail."""
    print("🔍 Debugging Table Data Content")
    print("="*50)
    
    try:
        # Test the multi-timeframe endpoint
        api_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        params = {"row_index": 10}
        
        response = requests.get(api_url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Focus on table_data
            table_data = data.get('table_data', [])
            print(f"📊 Table Data Analysis:")
            print(f"   Rows: {len(table_data)}")
            
            if table_data and len(table_data) > 0:
                first_row = table_data[0]
                print(f"   Type of first row: {type(first_row)}")
                print(f"   Features in first row: {len(first_row) if isinstance(first_row, dict) else 'not dict'}")
                
                if isinstance(first_row, dict):
                    # Check for basic OHLCV fields that the table might be looking for
                    basic_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    print(f"\n🔍 Basic OHLCV Fields Check:")
                    
                    for field in basic_fields:
                        if field in first_row:
                            value = first_row[field]
                            print(f"   ✅ {field}: {value} (type: {type(value)})")
                        else:
                            print(f"   ❌ {field}: NOT FOUND")
                    
                    # Check for fields containing "open", "high", etc.
                    print(f"\n🔍 Fields Containing OHLCV Terms:")
                    ohlcv_terms = ['open', 'high', 'low', 'close', 'volume']
                    for term in ohlcv_terms:
                        matching_fields = [k for k in first_row.keys() if term in k.lower()]
                        print(f"   {term}: {len(matching_fields)} fields")
                        if matching_fields:
                            # Show first few matching fields with values
                            for field in matching_fields[:3]:
                                value = first_row[field]
                                print(f"     - {field}: {value}")
                    
                    # Check for timestamp-like fields
                    print(f"\n🔍 Timestamp-like Fields:")
                    timestamp_fields = [k for k in first_row.keys() if 'time' in k.lower()]
                    for field in timestamp_fields:
                        value = first_row[field]
                        print(f"   - {field}: {value} (type: {type(value)})")
                    
                    # Show first 20 fields for debugging
                    print(f"\n📋 First 20 Fields (for debugging):")
                    field_items = list(first_row.items())[:20]
                    for i, (key, value) in enumerate(field_items):
                        print(f"   {i+1:2d}. {key}: {value} (type: {type(value).__name__})")
                
            else:
                print("   ❌ No table data rows found")
                
            # Also check OHLC data structure
            ohlc_data = data.get('ohlc_data', {})
            print(f"\n📊 OHLC Data Analysis:")
            print(f"   Timeframes: {list(ohlc_data.keys())}")
            
            if '1h' in ohlc_data and ohlc_data['1h']:
                sample_ohlc = ohlc_data['1h'][0]
                print(f"   Sample 1h OHLC record:")
                for key, value in sample_ohlc.items():
                    print(f"     - {key}: {value} (type: {type(value).__name__})")
                    
        else:
            print(f"❌ API request failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_table_data()