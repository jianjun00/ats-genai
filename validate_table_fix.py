#!/usr/bin/env python3
"""
Direct validation of table data fix
Validate that the table now shows real OHLCV data instead of N/A
"""

import requests
import json

def validate_table_fix():
    """Validate the table data fix directly."""
    print("🔍 Validating Table Data Fix")
    print("="*40)

    try:
        # Test the fixed endpoint
        api_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        params = {"row_index": 10}

        response = requests.get(api_url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()

            # Validate table_data structure
            table_data = data.get('table_data', [])
            comprehensive_features = data.get('comprehensive_features', [])

            print(f"📊 Data Validation:")
            print(f"   Table rows: {len(table_data)}")
            print(f"   Comprehensive feature rows: {len(comprehensive_features)}")

            # Test Issue Resolution
            print(f"\n🎯 Issue Resolution Check:")

            # Issue 1: Only one row showing
            if len(table_data) > 1:
                print(f"   ✅ FIXED: Multiple rows ({len(table_data)}) instead of just 1")
            else:
                print(f"   ❌ Still showing only {len(table_data)} row")

            # Issue 2: All values showing as N/A
            if table_data and len(table_data) > 0:
                first_row = table_data[0]
                basic_fields = ['open', 'high', 'low', 'close', 'volume']

                valid_values = []
                invalid_values = []

                for field in basic_fields:
                    if field in first_row:
                        value = first_row[field]
                        if isinstance(value, (int, float)) and value > 0:
                            valid_values.append(f"{field}=${value}")
                        else:
                            invalid_values.append(f"{field}={value}")

                print(f"   Field Analysis:")
                if valid_values:
                    print(f"   ✅ FIXED: Real values found - {', '.join(valid_values[:3])}")
                else:
                    print(f"   ❌ Still no valid values")

                if invalid_values:
                    print(f"   ❌ Invalid values: {', '.join(invalid_values)}")

            # Issue 3: Comprehensive features still available
            if comprehensive_features and len(comprehensive_features) > 0:
                comp_features_count = len(comprehensive_features[0])
                print(f"   ✅ MAINTAINED: {comp_features_count} comprehensive features still available")
            else:
                print(f"   ❌ Comprehensive features missing")

            # Issue 4: No NaN in JSON
            response_text = response.text
            if 'NaN' in response_text:
                print(f"   ❌ Still contains NaN in JSON response")
            else:
                print(f"   ✅ FIXED: No NaN values in JSON response")

            print(f"\n💡 Expected vs Actual:")
            print(f"   Expected table format: Multiple rows with open, high, low, close, volume")
            print(f"   Actual: {len(table_data)} rows with {len(table_data[0]) if table_data else 0} fields each")

            if table_data:
                sample_row = table_data[0]
                print(f"   Sample row fields: {list(sample_row.keys())}")
                print(f"   Sample values: {dict(list(sample_row.items())[:5])}")

        else:
            print(f"❌ API request failed: {response.status_code}")

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()

def test_browser_display_simulation():
    """Simulate how browser would display the table data."""
    print(f"\n🌐 Browser Display Simulation:")
    print("="*40)

    try:
        api_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        response = requests.get(api_url, params={"row_index": 10})
        data = response.json()

        table_data = data.get('table_data', [])

        if table_data:
            print("Browser would display table like this:")
            print("-" * 80)
            print(f"{'Timestamp':<20} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10} {'Volume':<12}")
            print("-" * 80)

            for i, row in enumerate(table_data[:5]):  # Show first 5 rows
                timestamp = row.get('timestamp', 'N/A')
                open_price = row.get('open', 'N/A')
                high_price = row.get('high', 'N/A')
                low_price = row.get('low', 'N/A')
                close_price = row.get('close', 'N/A')
                volume = row.get('volume', 'N/A')

                # Format timestamp as date
                if timestamp != 'N/A':
                    try:
                        from datetime import datetime
                        dt = datetime.fromtimestamp(timestamp)
                        timestamp_str = dt.strftime('%m/%d/%Y, %I:%M:%S %p')[:19]
                    except:
                        timestamp_str = str(timestamp)[:19]
                else:
                    timestamp_str = 'N/A'

                # Format prices
                def format_price(price):
                    if price != 'N/A' and isinstance(price, (int, float)):
                        return f"${price:.2f}"
                    return str(price)

                def format_volume(vol):
                    if vol != 'N/A' and isinstance(vol, (int, float)):
                        return f"{vol:,.0f}"
                    return str(vol)

                print(f"{timestamp_str:<20} {format_price(open_price):<10} {format_price(high_price):<10} {format_price(low_price):<10} {format_price(close_price):<10} {format_volume(volume):<12}")

            if len(table_data) > 5:
                print(f"... and {len(table_data) - 5} more rows")
        else:
            print("No table data to display")

    except Exception as e:
        print(f"❌ Simulation failed: {e}")

if __name__ == "__main__":
    validate_table_fix()
    test_browser_display_simulation()