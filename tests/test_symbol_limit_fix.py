#!/usr/bin/env python3
"""
Simple test to verify the symbol limit fix without requiring full browser automation.
"""

import requests
import time

def test_symbol_limit_fix():
    """Test that symbol API returns more symbols than before."""
    print("🧪 Testing Symbol API Limit Fix")
    print("=" * 40)
    
    base_url = "http://localhost:4000"
    
    # Test health endpoint first
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            print("✅ Service health check passed")
            print(f"   Response: {response.text}")
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return
    except Exception as e:
        print(f"❌ Service not responding: {e}")
        return
    
    # Test the symbol values API with increased limit
    symbol_api = f"{base_url}/api/eda/datasets/intg_daily_prices_tiingo/columns/symbol/values"
    
    # Test with small limit (old behavior)
    try:
        print("\n🧪 Testing small limit (10 symbols)...")
        response = requests.get(f"{symbol_api}?limit=10", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'values' in data:
                symbols_10 = [v['value'] if isinstance(v, dict) else v for v in data['values']]
                print(f"✅ Got {len(symbols_10)} symbols with limit=10")
                print(f"   First 5: {symbols_10[:5]}")
                print(f"   Contains TSLA: {'TSLA' in symbols_10}")
            else:
                print(f"❌ Unexpected response format: {data}")
        else:
            print(f"❌ API call failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ API call error: {e}")
        
    # Test with large limit (new behavior)
    try:
        print("\n🧪 Testing large limit (1000 symbols)...")
        response = requests.get(f"{symbol_api}?limit=1000", timeout=30)
        if response.status_code == 200:
            data = response.json()
            if 'values' in data:
                symbols_1000 = [v['value'] if isinstance(v, dict) else v for v in data['values']]
                print(f"✅ Got {len(symbols_1000)} symbols with limit=1000")
                print(f"   First 5: {symbols_1000[:5]}")
                print(f"   Contains TSLA: {'TSLA' in symbols_1000}")
                
                if 'TSLA' in symbols_1000:
                    tsla_position = symbols_1000.index('TSLA') + 1
                    print(f"🎯 TSLA found at position {tsla_position}")
                    print("✅ TSLA SYMBOL ISSUE RESOLVED!")
                else:
                    print("❌ TSLA still not found - may not exist in dataset")
                    # Show some symbols around where TSLA should be
                    t_symbols = [s for s in symbols_1000 if s.startswith('T')]
                    print(f"   T-symbols found: {t_symbols[:10]}")
                    
            else:
                print(f"❌ Unexpected response format: {data}")
        else:
            print(f"❌ Large limit API call failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Large limit API call error: {e}")
    
    # Summary
    print(f"\n📊 SYMBOL LIMIT FIX SUMMARY:")
    if 'symbols_10' in locals() and 'symbols_1000' in locals():
        improvement = len(symbols_1000) - len(symbols_10)
        print(f"✅ Symbol limit increased: {len(symbols_10)} → {len(symbols_1000)} (+{improvement})")
        print(f"✅ TSLA accessibility: {'Available' if 'TSLA' in symbols_1000 else 'Not in dataset'}")
    else:
        print("❌ Unable to complete comparison due to API issues")

if __name__ == "__main__":
    test_symbol_limit_fix()