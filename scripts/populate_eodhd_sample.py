#!/usr/bin/env python3
"""
Simple EODHD sample population using direct database approach

This script demonstrates the enhanced EODHD population logic by
updating a sample of instruments with proper IPO dates using
the fundamentals API.
"""

import os
import requests
import json
from datetime import datetime

# EODHD API configuration
EODHD_API_KEY = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')

def parse_date(val):
    """Parse date string"""
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def fetch_fundamental_data(symbol):
    """Fetch fundamental data including IPO date for a symbol"""
    if '.' not in symbol:
        symbol = f"{symbol}.US"
    
    url = f"https://eodhd.com/api/fundamentals/{symbol}?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"⚠️  API error for {symbol}: {response.status_code}")
            return None
        
        data = response.json()
        general = data.get('General', {})
        
        return {
            'symbol': symbol.split('.')[0],
            'name': general.get('Name'),
            'exchange': general.get('Exchange'),
            'type': general.get('Type'),
            'currency': general.get('CurrencyCode'),
            'ipo_date': general.get('IPODate'),
            'country': general.get('Country'),
            'sector': general.get('Sector'),
            'industry': general.get('Industry'),
            'full_response': data
        }
        
    except Exception as e:
        print(f"❌ Error fetching fundamentals for {symbol}: {e}")
        return None

def generate_sql_updates(symbols):
    """Generate SQL UPDATE statements for the symbols"""
    print("📝 Generating SQL UPDATE statements...")
    
    sql_statements = []
    successful_lookups = 0
    
    for symbol in symbols:
        print(f"🔍 Looking up {symbol}...")
        
        fundamental_data = fetch_fundamental_data(symbol)
        if not fundamental_data:
            continue
        
        successful_lookups += 1
        
        # Parse the IPO date
        ipo_date_str = fundamental_data['ipo_date']
        ipo_date = parse_date(ipo_date_str) if ipo_date_str else None
        
        print(f"✅ {symbol}: {fundamental_data['name']}, IPO: {ipo_date_str}")
        
        # Generate SQL UPDATE statement
        sql = f"""UPDATE dev_instrument_eodhd SET 
    name = '{fundamental_data['name'].replace("'", "''")}',
    exchange = '{fundamental_data['exchange'] or ''}',
    asset_type = '{fundamental_data['type'] or ''}',
    currency = '{fundamental_data['currency'] or ''}',
    ipo_date = {'NULL' if not ipo_date else f"'{ipo_date}'"},
    country = '{fundamental_data['country'] or ''}',
    sector = '{fundamental_data['sector'] or ''}',
    industry = '{fundamental_data['industry'] or ''}',
    raw = '{json.dumps(fundamental_data['full_response']).replace("'", "''")}',
    updated_at = NOW()
WHERE symbol = '{symbol}';"""
        
        sql_statements.append(sql)
        
        # Rate limiting
        import time
        time.sleep(1)
    
    print(f"✅ Successfully looked up {successful_lookups}/{len(symbols)} symbols")
    return sql_statements

def main():
    """Main execution"""
    print("🚀 EODHD Sample Population")
    print("=" * 60)
    
    # Sample symbols to test with
    test_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'TSLA', 'META', 
        'NVDA', 'AMZN', 'NFLX', 'AMD', 'CRM'
    ]
    
    print(f"📋 Testing with {len(test_symbols)} symbols:")
    print(f"   {', '.join(test_symbols)}")
    print()
    
    # Generate SQL statements
    sql_statements = generate_sql_updates(test_symbols)
    
    print("\n" + "=" * 60)
    print("📜 Generated SQL Statements:")
    print("=" * 60)
    
    # Write SQL to file
    sql_file = "update_eodhd_sample.sql"
    with open(sql_file, 'w') as f:
        f.write("-- EODHD Sample Data Update with IPO Dates\n")
        f.write(f"-- Generated on {datetime.now().isoformat()}\n")
        f.write(f"-- Updated {len(sql_statements)} instruments\n\n")
        
        for sql in sql_statements:
            f.write(sql + "\n\n")
        
        # Add verification query
        f.write("-- Verification query\n")
        symbols_list = "', '".join([stmt.split("WHERE symbol = '")[1].split("'")[0] for stmt in sql_statements])
        f.write(f"SELECT symbol, name, ipo_date, sector FROM dev_instrument_eodhd WHERE symbol IN ('{symbols_list}') ORDER BY symbol;\n")
    
    print(f"💾 SQL statements saved to: {sql_file}")
    print("\n📋 Summary:")
    print(f"   ✅ Symbols processed: {len(sql_statements)}")
    print(f"   📝 SQL file: {sql_file}")
    print()
    print("🔧 To apply these updates:")
    print(f"   PYTHONPATH=src python3 scripts/run_dev.py query --query \"$(cat {sql_file})\"")
    print()
    print("🔍 To verify the updates:")
    print(f"   PYTHONPATH=src python3 scripts/run_dev.py query --query \"SELECT symbol, name, ipo_date FROM dev_instrument_eodhd WHERE symbol IN ('{symbols_list}') ORDER BY symbol\"")

if __name__ == "__main__":
    main()