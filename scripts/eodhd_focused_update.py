#!/usr/bin/env python3
"""
EODHD Focused Update

This script focuses on updating regular stock symbols (1-5 characters)
that are more likely to have IPO dates, avoiding complex fund symbols.
"""

import os
import requests
import time
from datetime import datetime
import re

# EODHD API configuration
EODHD_API_KEY = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')

def is_regular_stock_symbol(symbol):
    """Check if symbol looks like a regular stock (1-5 letters, maybe ending with digit)"""
    if not symbol:
        return False
    # Regular stocks: 1-5 characters, mostly letters
    return bool(re.match(r'^[A-Z]{1,4}[A-Z0-9]?$', symbol))

def get_focused_symbols(limit=500):
    """Get focused list of regular stock symbols"""
    print("🌍 Fetching focused stock symbols from EODHD...")
    
    url = f"https://eodhd.com/api/exchange-symbol-list/US?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Filter for regular stocks and common stocks
        regular_stocks = []
        for item in data:
            code = item.get('Code', '')
            stock_type = item.get('Type', '')
            
            if (is_regular_stock_symbol(code) and 
                ('Common Stock' in stock_type or 'Stock' in stock_type) and
                'Fund' not in stock_type and 'ETF' not in stock_type):
                regular_stocks.append({
                    'symbol': code,
                    'name': item.get('Name', ''),
                    'exchange': item.get('Exchange', ''),
                    'type': stock_type,
                    'currency': item.get('Currency', '')
                })
        
        # Sort by symbol length (shorter symbols first) then alphabetically
        regular_stocks.sort(key=lambda x: (len(x['symbol']), x['symbol']))
        
        if limit:
            regular_stocks = regular_stocks[:limit]
            
        print(f"✅ Retrieved {len(regular_stocks)} regular stock symbols")
        print(f"📋 Sample symbols: {', '.join([s['symbol'] for s in regular_stocks[:10]])}")
        return regular_stocks
        
    except Exception as e:
        print(f"❌ Failed to fetch exchange symbols: {e}")
        return []

def fetch_fundamental_data(symbol):
    """Fetch fundamental data including IPO date for a symbol"""
    if '.' not in symbol:
        symbol = f"{symbol}.US"
    
    url = f"https://eodhd.com/api/fundamentals/{symbol}?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None
        
        data = response.json()
        general = data.get('General', {})
        
        return {
            'symbol': symbol.split('.')[0],
            'name': general.get('Name', ''),
            'exchange': general.get('Exchange', ''),
            'type': general.get('Type', ''),
            'currency': general.get('CurrencyCode', ''),
            'ipo_date': general.get('IPODate')
        }
        
    except Exception as e:
        return None

def main():
    """Main execution"""
    print("🚀 EODHD Focused Stock Update")
    print("=" * 60)
    
    # Get focused symbols (regular stocks only)
    symbols = get_focused_symbols(limit=200)  # Start with 200 regular stocks
    
    if not symbols:
        print("❌ No symbols retrieved. Exiting.")
        return
    
    print(f"\n🎯 Processing {len(symbols)} regular stock symbols")
    print(f"⏱️  Estimated time: {len(symbols) * 2 / 60:.1f} minutes (2 sec per symbol)")
    
    # Process symbols
    sql_statements = []
    successful_ipo = 0
    total_processed = 0
    
    for i, symbol_info in enumerate(symbols):
        symbol = symbol_info['symbol']
        print(f"🔍 {i+1:3d}/{len(symbols)}: {symbol:<6}", end=" ")
        
        # Fetch fundamental data
        fundamental_data = fetch_fundamental_data(symbol)
        total_processed += 1
        
        if not fundamental_data:
            print("❌ No data")
            continue
        
        ipo_date = fundamental_data['ipo_date']
        if ipo_date:
            successful_ipo += 1
            print(f"✅ IPO: {ipo_date}")
            
            # Generate SQL with IPO date
            sql = f"""UPDATE dev_instrument_eodhd SET 
    name = '{(fundamental_data['name'] or '').replace("'", "''")}',
    exchange = '{fundamental_data['exchange'] or ''}',
    asset_type = '{fundamental_data['type'] or ''}',
    currency = '{fundamental_data['currency'] or ''}',
    ipo_date = '{ipo_date}',
    updated_at = NOW()
WHERE symbol = '{symbol}';"""
        else:
            print("⚠️  No IPO")
            # Still update basic info
            sql = f"""UPDATE dev_instrument_eodhd SET 
    name = '{(fundamental_data['name'] or '').replace("'", "''")}',
    exchange = '{fundamental_data['exchange'] or ''}',
    asset_type = '{fundamental_data['type'] or ''}',
    currency = '{fundamental_data['currency'] or ''}',
    updated_at = NOW()
WHERE symbol = '{symbol}';"""
        
        sql_statements.append(sql)
        
        # Progress update every 25 symbols
        if (i + 1) % 25 == 0:
            progress = (i + 1) / len(symbols) * 100
            ipo_rate = successful_ipo / (i + 1) * 100
            print(f"📊 Progress: {progress:.1f}% | IPO rate: {ipo_rate:.1f}% ({successful_ipo}/{i+1})")
        
        # Rate limiting - 2 seconds for focused approach
        time.sleep(2)
    
    # Write SQL file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_file = f"eodhd_focused_update_{timestamp}.sql"
    
    with open(sql_file, 'w') as f:
        f.write(f"-- EODHD Focused Stock Update with IPO Dates\n")
        f.write(f"-- Generated on {datetime.now().isoformat()}\n")
        f.write(f"-- Processed {total_processed} regular stock symbols\n")
        f.write(f"-- Found {successful_ipo} IPO dates ({successful_ipo/total_processed*100:.1f}%)\n\n")
        
        for sql in sql_statements:
            f.write(sql + "\n\n")
        
        # Add verification query
        f.write("-- Verification query\n")
        f.write("SELECT symbol, name, ipo_date FROM dev_instrument_eodhd WHERE ipo_date IS NOT NULL ORDER BY ipo_date DESC LIMIT 20;\n")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 FOCUSED UPDATE SUMMARY")
    print("=" * 60)
    print(f"✅ Regular stocks processed: {total_processed}")
    print(f"✅ IPO dates found: {successful_ipo}")
    print(f"✅ Success rate: {successful_ipo/total_processed*100:.1f}%")
    print(f"💾 SQL file: {sql_file}")
    
    # Calculate expected improvement
    current_count = 12
    expected_count = current_count + successful_ipo
    current_total = 50772
    
    print(f"\n📈 Expected database improvement:")
    print(f"   Before: {current_count}/{current_total} ({current_count/current_total*100:.3f}%)")
    print(f"   After: {expected_count}/{current_total} ({expected_count/current_total*100:.3f}%)")
    print(f"   Improvement: +{successful_ipo/current_total*100:.3f} percentage points")
    
    print(f"\n🔧 To apply updates:")
    print(f"   PYTHONPATH=src python3 scripts/run_dev.py query --query \"$(cat {sql_file})\"")
    
    print(f"\n🔍 To verify:")
    print(f"   PYTHONPATH=src python3 scripts/run_dev.py query --query \"SELECT COUNT(*) as total_with_ipo FROM dev_instrument_eodhd WHERE ipo_date IS NOT NULL\"")

if __name__ == "__main__":
    main()