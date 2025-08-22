#!/usr/bin/env python3
"""
Basic test of Alpha Vantage daily price ingestion
"""

import asyncio
import os
from datetime import date, timedelta

# Set up environment
os.environ['PYTHONPATH'] = 'src'
os.environ['ENVIRONMENT'] = 'dev'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PORT'] = '5433'
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'postgres'
os.environ['DB_NAME'] = 'dev_db'

from config.environment import Environment
from dao.daily_prices_alphavantage_dao import DailyPricesAlphaVantageDAO
from dao.instrument_xrefs_dao import InstrumentXrefsDAO

async def test_alphavantage_dao():
    """Test basic Alpha Vantage DAO functionality"""
    print("=== Testing Alpha Vantage DAO ===")
    
    env = Environment()
    dao = DailyPricesAlphaVantageDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    
    # Test instrument resolution
    test_symbol = 'AAPL'
    instrument_id = await xrefs_dao.resolve_instrument_id(test_symbol)
    
    if not instrument_id:
        print(f"❌ Could not resolve instrument_id for {test_symbol}")
        return False
    
    print(f"✅ Resolved {test_symbol} to instrument_id: {instrument_id}")
    
    # Test inserting a sample price record
    test_date = date.today() - timedelta(days=1)
    test_price = {
        'date': test_date,
        'instrument_id': instrument_id,
        'open_price': 100.0,
        'high_price': 105.0,
        'low_price': 99.0,
        'close': 103.0,
        'adj_close': 103.0,
        'volume': 1000000
    }
    
    try:
        await dao.batch_insert_prices([test_price])
        print(f"✅ Successfully inserted test price for {test_symbol}")
        
        # Verify the insert
        retrieved = await dao.get_price(test_date, instrument_id)
        if retrieved:
            print(f"✅ Successfully retrieved price: {retrieved['close']}")
            return True
        else:
            print(f"❌ Could not retrieve inserted price")
            return False
            
    except Exception as e:
        print(f"❌ Error testing DAO: {e}")
        return False

async def test_api_key():
    """Check if Alpha Vantage API key is available"""
    print("\n=== Testing Alpha Vantage API Key ===")
    
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        print("❌ ALPHA_VANTAGE_API_KEY not set")
        print("   Set your Alpha Vantage API key:")
        print("   export ALPHA_VANTAGE_API_KEY=your_key_here")
        return False
    
    print(f"✅ Alpha Vantage API key available: {api_key[:8]}...")
    return True

async def main():
    """Run basic tests"""
    print("Running Alpha Vantage integration tests...\n")
    
    # Test API key
    api_ok = await test_api_key()
    
    # Test DAO
    dao_ok = await test_alphavantage_dao()
    
    print(f"\n=== Test Results ===")
    print(f"API Key: {'✅' if api_ok else '❌'}")
    print(f"DAO Test: {'✅' if dao_ok else '❌'}")
    
    if api_ok and dao_ok:
        print("\n🎉 All tests passed! Alpha Vantage integration is ready.")
        print("\nNext steps:")
        print("1. Run actual ingestion with limited symbols:")
        print("   PYTHONPATH=src python src/market_data/eod/daily_price_alphavantage.py --tickers AAPL,MSFT --start_date 2024-01-01 --end_date 2024-01-10 --debug")
        return True
    else:
        print("\n❌ Some tests failed. Please fix issues before proceeding.")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)