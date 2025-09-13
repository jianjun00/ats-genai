#!/usr/bin/env python3
"""
Direct test to verify TSLA can be found in the specific frontend request.
"""
import requests

def test_tsla_direct():
    """Test the exact API call the frontend makes."""
    print("🔬 Direct TSLA Accessibility Test")
    print("=" * 40)

    # Test the exact frontend request
    try:
        # This matches what the frontend requests for symbol columns
        response = requests.get("http://localhost:4000/api/eda/datasets/intg_daily_price_tiingo/columns/symbol/values?limit=1000", timeout=10)
        if response.status_code == 200:
            data = response.json()
            symbols = [v['value'] for v in data.get('values', [])]

            print(f"✅ API returned {len(symbols)} symbols")
            print(f"✅ TSLA in API response: {'TSLA' in symbols}")

            if 'TSLA' in symbols:
                tsla_pos = symbols.index('TSLA') + 1
                print(f"✅ TSLA position: {tsla_pos}")

                # Check if TSLA would be in first 100 (what frontend displays)
                tsla_in_first_100 = tsla_pos <= 100
                print(f"✅ TSLA in first 100: {tsla_in_first_100}")

                if tsla_in_first_100:
                    print("🎉 TSLA SHOULD BE VISIBLE IN FRONTEND!")
                else:
                    print(f"❌ TSLA at position {tsla_pos} - not in first 100 displayed")
                    print("   Frontend displays positions 1-100, TSLA needs to move up")

                    # Show what's taking up the first 100 positions
                    print(f"   Position 90-100: {symbols[89:100]}")

            else:
                print("❌ TSLA not found in API response")

        else:
            print(f"❌ API call failed: {response.status_code}")

    except Exception as e:
        print(f"❌ Test failed: {e}")

    # Additional test: Check if the priority query is working by looking at first 30
    print(f"\n🔍 Checking priority effectiveness...")
    try:
        response = requests.get("http://localhost:4000/api/eda/datasets/intg_daily_price_tiingo/columns/symbol/values?limit=30", timeout=10)
        if response.status_code == 200:
            data = response.json()
            first_30 = [v['value'] for v in data.get('values', [])]

            popular_in_db = ['AAPL', 'AMZN', 'GOOGL', 'TSLA']  # Only ones that exist
            popular_found = [s for s in first_30 if s in popular_in_db]

            print(f"✅ Popular symbols in first 30: {popular_found}")
            print(f"✅ Priority query working: {len(popular_found) == 4}")

    except Exception as e:
        print(f"❌ Priority test failed: {e}")

if __name__ == "__main__":
    test_tsla_direct()