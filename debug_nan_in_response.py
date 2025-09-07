#!/usr/bin/env python3
"""
Debug NaN values in API response
Find exactly where NaN is occurring in the JSON response
"""

import requests
import json
import re

def debug_nan_in_response():
    """Debug where NaN values are appearing in the response."""
    print("🔍 Debugging NaN in API Response")
    print("="*50)

    try:
        # Test the multi-timeframe endpoint
        api_url = "http://localhost:3000/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe"
        params = {"row_index": 10}

        response = requests.get(api_url, params=params, timeout=30)

        print(f"📡 Response status: {response.status_code}")

        if response.status_code == 200:
            response_text = response.text

            # Look for NaN patterns in the raw response text
            nan_pattern = r'"[^"]*":\s*NaN'
            nan_matches = re.findall(nan_pattern, response_text)

            if nan_matches:
                print(f"❌ Found {len(nan_matches)} NaN values in JSON response:")
                for i, match in enumerate(nan_matches[:10]):  # Show first 10
                    print(f"   {i+1}. {match}")

                # Look for the context around NaN values
                nan_contexts = []
                for match in re.finditer(nan_pattern, response_text):
                    start = max(0, match.start() - 100)
                    end = min(len(response_text), match.end() + 100)
                    context = response_text[start:end]
                    nan_contexts.append(context)

                print(f"\n🔍 Context around NaN values:")
                for i, context in enumerate(nan_contexts[:3]):  # Show first 3 contexts
                    print(f"   Context {i+1}:")
                    print(f"   {context}")
                    print()

            else:
                print("✅ No NaN patterns found in response text")

            # Check for other invalid JSON patterns
            inf_pattern = r'"[^"]*":\s*(Infinity|-Infinity)'
            inf_matches = re.findall(inf_pattern, response_text)

            if inf_matches:
                print(f"❌ Found {len(inf_matches)} Infinity values:")
                for match in inf_matches[:5]:
                    print(f"   - {match}")
            else:
                print("✅ No Infinity values found")

            # Try to parse as JSON to see if it's actually invalid
            try:
                data = response.json()
                print("✅ Response is valid JSON (can be parsed)")

                # Recursively search for NaN values in the parsed data
                nan_paths = find_nan_in_data(data)
                if nan_paths:
                    print(f"❌ Found NaN values in parsed data at:")
                    for path in nan_paths[:10]:
                        print(f"   - {path}")
                else:
                    print("✅ No NaN values found in parsed data structure")

            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed: {e}")
                # Show the area around the JSON error
                if hasattr(e, 'pos'):
                    error_pos = e.pos
                    start = max(0, error_pos - 100)
                    end = min(len(response_text), error_pos + 100)
                    error_context = response_text[start:end]
                    print(f"Error context: {error_context}")

        else:
            print(f"❌ API request failed: {response.status_code}")

    except Exception as e:
        print(f"❌ Test failed: {e}")

def find_nan_in_data(obj, path=""):
    """Recursively find NaN values in data structure."""
    nan_paths = []

    if isinstance(obj, dict):
        for key, value in obj.items():
            new_path = f"{path}.{key}" if path else key
            nan_paths.extend(find_nan_in_data(value, new_path))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            new_path = f"{path}[{i}]"
            nan_paths.extend(find_nan_in_data(item, new_path))
    elif isinstance(obj, float):
        import math
        if math.isnan(obj):
            nan_paths.append(path)

    return nan_paths

if __name__ == "__main__":
    debug_nan_in_response()