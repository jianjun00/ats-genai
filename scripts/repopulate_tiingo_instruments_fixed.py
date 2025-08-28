#!/usr/bin/env python3
"""
Repopulate Tiingo Instruments with Corrected End Date Logic

CRITICAL FIX:
- Tiingo 'endDate' represents data feed availability, NOT stock delisting
- Recent endDate (within 7 days) = active data feed -> store as NULL
- Old endDate (> 7 days ago) = actual delisting -> preserve date
- Apply fix during population to prevent future data quality issues

TARGETS:
- Repopulate all major US stocks with corrected logic
- Validate against known market facts during population
- Ensure >70% active rate with proper end_date interpretation
"""

import sys
import os
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

# Tiingo configuration
TIINGO_API_KEY = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily"

def parse_date(date_str: str) -> Optional[date]:
    """Parse date string to date object"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def fix_tiingo_end_date(raw_end_date: Optional[date]) -> Optional[date]:
    """
    Apply Tiingo end_date interpretation fix
    
    Logic:
    - NULL end_date -> NULL (active)
    - Recent end_date (within 7 days) -> NULL (active data feed)
    - Old end_date (> 7 days ago) -> preserve (actual delisting)
    """
    if raw_end_date is None:
        return None
    
    cutoff_date = datetime.now().date() - timedelta(days=7)
    
    if raw_end_date > cutoff_date:
        # Recent date = active data feed, should be NULL
        return None
    else:
        # Old date = actual delisting, preserve
        return raw_end_date

def get_major_us_symbols() -> List[str]:
    """Get list of major US stocks to repopulate"""
    return [
        # Major indices components
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'TSLA', 'META',
        'BRK.B', 'UNH', 'JNJ', 'XOM', 'V', 'PG', 'MA', 'HD', 'JPM', 'CVX',
        'LLY', 'ABBV', 'PFE', 'AVGO', 'KO', 'TMO', 'COST', 'BAC', 'WMT', 'NFLX',
        'CRM', 'ACN', 'DIS', 'AMD', 'LIN', 'ADBE', 'VZ', 'CMCSA', 'DHR', 'NKE',
        
        # Test cases for validation
        'GM',     # Should be active (2010 IPO)
        'DELL',   # Should be active (2018 return)
        'F',      # Should be active (long-term)
        'BBBYQ',  # Should be delisted (2023-09-29)
        
        # Other major stocks
        'T', 'INTC', 'WFC', 'IBM', 'ORCL', 'C', 'GS', 'MS', 'AXP', 'CAT',
        'BA', 'MMM', 'HON', 'RTX', 'UPS', 'UNP', 'LOW', 'SPGI', 'DE', 'SBUX'
    ]

async def fetch_tiingo_instrument(symbol: str) -> Optional[Dict]:
    """Fetch instrument data from Tiingo API"""
    url = f"{TIINGO_BASE_URL}/{symbol}?token={TIINGO_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"   📊 {symbol}: startDate={data.get('startDate')}, endDate={data.get('endDate')}")
            return data
        elif response.status_code == 404:
            print(f"   ⚠️ {symbol}: Not found on Tiingo")
            return None
        else:
            print(f"   ❌ {symbol}: API error {response.status_code}")
            return None
    except Exception as e:
        print(f"   💥 {symbol}: Request failed - {e}")
        return None

async def upsert_instrument(conn, instrument_data: Dict) -> bool:
    """Insert/update instrument with corrected end_date logic"""
    
    symbol = instrument_data.get('ticker')
    name = instrument_data.get('name')
    exchange = instrument_data.get('exchangeCode')
    start_date = parse_date(instrument_data.get('startDate'))
    raw_end_date = parse_date(instrument_data.get('endDate'))
    
    # Apply the fix: interpret end_date correctly
    corrected_end_date = fix_tiingo_end_date(raw_end_date)
    
    # Log the correction if applied
    if raw_end_date != corrected_end_date:
        if corrected_end_date is None:
            print(f"   🔧 {symbol}: Corrected end_date {raw_end_date} -> NULL (active)")
        else:
            print(f"   ✅ {symbol}: Preserved end_date {corrected_end_date} (delisted)")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
        INSERT INTO dev_instrument_tiingo (symbol, name, exchange, asset_type, currency, start_date, end_date, raw, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol) DO UPDATE SET
            name = EXCLUDED.name,
            exchange = EXCLUDED.exchange,
            asset_type = EXCLUDED.asset_type,
            currency = EXCLUDED.currency,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            raw = EXCLUDED.raw,
            updated_at = CURRENT_TIMESTAMP
        """, (
            symbol,
            name,
            exchange,
            'stock',
            'USD',
            start_date,
            corrected_end_date,  # Use corrected date
            json.dumps(instrument_data)
        ))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"   ❌ {symbol}: Database error - {e}")
        conn.rollback()
        return False

async def validate_repopulation(conn) -> bool:
    """Validate the repopulation results"""
    
    print(f"\n✅ Validation: Checking repopulation quality")
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Test critical symbols
    test_cases = [
        ('GM', None, 'General Motors should be active'),
        ('DELL', None, 'Dell Technologies should be active'),
        ('F', None, 'Ford should be active'),
        ('BBBYQ', date(2023, 9, 29), 'Bed Bath & Beyond should be delisted on 2023-09-29')
    ]
    
    validation_passed = True
    
    for symbol, expected_end_date, description in test_cases:
        cur.execute("SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = %s", (symbol,))
        result = cur.fetchone()
        
        if result:
            actual_end_date = result['end_date']
            if actual_end_date == expected_end_date:
                print(f"   ✅ {symbol}: {description} - CORRECT")
            else:
                print(f"   ❌ {symbol}: Expected {expected_end_date}, got {actual_end_date}")
                validation_passed = False
        else:
            print(f"   ⚠️ {symbol}: Not found in database")
    
    # Check overall metrics
    cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN end_date IS NULL THEN 1 END) as active,
        COUNT(CASE WHEN end_date > CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_dates,
        ROUND(COUNT(CASE WHEN end_date IS NULL THEN 1 END)::numeric / COUNT(*)::numeric * 100, 2) as active_pct
    FROM dev_instrument_tiingo
    """)
    
    metrics = cur.fetchone()
    print(f"   📊 Active rate: {metrics['active']:,}/{metrics['total']:,} ({metrics['active_pct']}%)")
    print(f"   📊 Recent end_dates (should be 0): {metrics['recent_dates']}")
    
    if metrics['active_pct'] < 70:
        print(f"   ❌ Active percentage too low: {metrics['active_pct']}%")
        validation_passed = False
    
    if metrics['recent_dates'] > 0:
        print(f"   ❌ Found {metrics['recent_dates']} recent end_dates")
        validation_passed = False
    
    return validation_passed

async def main():
    """Main repopulation function"""
    
    print("🔄 Repopulating Tiingo Instruments with Corrected End Date Logic")
    print("=" * 70)
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Get symbols to repopulate
    symbols = get_major_us_symbols()
    print(f"📋 Repopulating {len(symbols)} major US instruments")
    
    successful = 0
    failed = 0
    
    try:
        for i, symbol in enumerate(symbols, 1):
            print(f"\n📊 [{i:3d}/{len(symbols)}] Processing {symbol}")
            
            # Fetch from Tiingo API
            instrument_data = await fetch_tiingo_instrument(symbol)
            if not instrument_data:
                failed += 1
                continue
                
            # Upsert with corrected logic
            if await upsert_instrument(conn, instrument_data):
                successful += 1
            else:
                failed += 1
                
            # Rate limiting
            await asyncio.sleep(0.2)  # 5 requests per second max
            
        print(f"\n📊 Repopulation Results:")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📈 Success rate: {successful/(successful+failed)*100:.1f}%")
        
        # Validate results
        if await validate_repopulation(conn):
            print(f"\n🎉 SUCCESS: Tiingo instruments repopulated with corrected logic!")
            print(f"   • End_date interpretation fix applied during population")
            print(f"   • Historical market facts validated")
            print(f"   • Data quality metrics within acceptable ranges")
        else:
            print(f"\n❌ VALIDATION FAILED: Issues found in repopulated data")
            
    except Exception as e:
        print(f"💥 Repopulation error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        conn.close()
        print("🔌 Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())