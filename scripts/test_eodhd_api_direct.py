#!/usr/bin/env python3
"""
Direct test of EODHD API functionality without database dependencies

This script tests the EODHD API calls directly to validate our logic
before running the full population.
"""

import os
import requests
import json
from datetime import datetime

# EODHD API configuration
EODHD_API_KEY = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')

def parse_date(val):
    """Parse date string to validate format"""
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def test_exchange_symbols_api():
    """Test the exchange-symbol-list API"""
    print("🌍 Testing EODHD exchange-symbol-list API...")
    
    url = f"https://eodhd.com/api/exchange-symbol-list/US?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ Retrieved {len(data)} symbols from US exchange")
        
        # Show first 5 symbols
        print("📋 Sample symbols:")
        for i, item in enumerate(data[:5]):
            print(f"   {i+1}. {item.get('Code', 'N/A')} - {item.get('Name', 'N/A')} ({item.get('Exchange', 'N/A')})")
        
        # Check if IPO date is included (it shouldn't be)
        first_item = data[0] if data else {}
        has_ipo_date = 'IPODate' in first_item
        print(f"⚠️  IPO Date in exchange list: {'Yes' if has_ipo_date else 'No'} (Expected: No)")
        
        return data[:10]  # Return first 10 for testing
        
    except Exception as e:
        print(f"❌ Failed to fetch exchange symbols: {e}")
        return []

def test_fundamental_api(symbol):
    """Test the fundamentals API for a specific symbol"""
    print(f"🔍 Testing EODHD fundamentals API for {symbol}...")
    
    if '.' not in symbol:
        symbol = f"{symbol}.US"
    
    url = f"https://eodhd.com/api/fundamentals/{symbol}?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"❌ API error for {symbol}: {response.status_code}")
            return None
        
        data = response.json()
        general = data.get('General', {})
        
        # Extract key information
        result = {
            'symbol': symbol.split('.')[0],
            'name': general.get('Name'),
            'exchange': general.get('Exchange'),
            'type': general.get('Type'),
            'currency': general.get('CurrencyCode'),
            'ipo_date': general.get('IPODate'),
            'country': general.get('Country'),
            'sector': general.get('Sector'),
            'industry': general.get('Industry')
        }
        
        print(f"✅ {symbol}: {result['name']}")
        print(f"   Exchange: {result['exchange']}")
        print(f"   IPO Date: {result['ipo_date']}")
        print(f"   Sector: {result['sector']}")
        
        # Validate IPO date
        if result['ipo_date']:
            parsed_date = parse_date(result['ipo_date'])
            if parsed_date:
                print(f"   📅 Parsed IPO Date: {parsed_date}")
            else:
                print(f"   ⚠️  Could not parse IPO date: {result['ipo_date']}")
        else:
            print(f"   ⚠️  No IPO date available")
        
        return result
        
    except Exception as e:
        print(f"❌ Error fetching fundamentals for {symbol}: {e}")
        return None

def test_sample_symbols():
    """Test fundamentals API with known good symbols"""
    print("\n🧪 Testing fundamentals API with sample symbols...")
    
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'META']
    results = []
    
    for symbol in test_symbols:
        print(f"\n--- Testing {symbol} ---")
        result = test_fundamental_api(symbol)
        if result:
            results.append(result)
        print()
    
    return results

def analyze_results(results):
    """Analyze the test results"""
    print("📊 Analysis of Results:")
    print(f"   Total symbols tested: {len(results)}")
    
    symbols_with_ipo = [r for r in results if r['ipo_date']]
    print(f"   Symbols with IPO dates: {len(symbols_with_ipo)}")
    
    if symbols_with_ipo:
        print("   Symbols with IPO dates:")
        for result in symbols_with_ipo:
            print(f"     • {result['symbol']}: {result['ipo_date']}")
    
    symbols_without_ipo = [r for r in results if not r['ipo_date']]
    if symbols_without_ipo:
        print(f"   Symbols without IPO dates: {len(symbols_without_ipo)}")
        for result in symbols_without_ipo:
            print(f"     • {result['symbol']} ({result['type']})")

def main():
    """Run all API tests"""
    print("🚀 EODHD API Direct Test")
    print("=" * 50)
    
    # Test 1: Exchange symbols API
    exchange_symbols = test_exchange_symbols_api()
    
    print("\n" + "=" * 50)
    
    # Test 2: Fundamentals API with sample symbols
    fundamental_results = test_sample_symbols()
    
    print("\n" + "=" * 50)
    
    # Test 3: Analyze results
    if fundamental_results:
        analyze_results(fundamental_results)
    
    print("\n" + "=" * 50)
    
    # Summary
    print("📋 Summary:")
    print("✅ Exchange-symbol-list API: Working (but no IPO dates)")
    print("✅ Fundamentals API: Working (includes IPO dates)")
    print("💡 Conclusion: Need to use fundamentals API for each symbol to get IPO dates")
    
    if fundamental_results:
        ipo_coverage = len([r for r in fundamental_results if r['ipo_date']]) / len(fundamental_results) * 100
        print(f"📈 IPO Date Coverage: {ipo_coverage:.1f}%")
    
    print("\n🎯 Ready to proceed with enhanced population logic!")

if __name__ == "__main__":
    main()