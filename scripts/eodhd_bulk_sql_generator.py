#!/usr/bin/env python3
"""
EODHD Bulk SQL Generator

This script generates SQL UPDATE statements in batches to populate
EODHD instruments with IPO dates using the fundamentals API.
Works without database dependencies.
"""

import os
import requests
import json
import time
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

def get_exchange_symbols(limit=None):
    """Get symbols from EODHD exchange API"""
    print("🌍 Fetching US exchange symbols from EODHD...")
    
    url = f"https://eodhd.com/api/exchange-symbol-list/US?api_token={EODHD_API_KEY}&fmt=json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        symbols = []
        for item in data:
            code = item.get('Code')
            if code and len(code) <= 10:  # Filter out overly long symbols
                symbols.append({
                    'symbol': code,
                    'name': item.get('Name', ''),
                    'exchange': item.get('Exchange', ''),
                    'type': item.get('Type', ''),
                    'currency': item.get('Currency', '')
                })
        
        if limit:
            symbols = symbols[:limit]
            
        print(f"✅ Retrieved {len(symbols)} symbols")
        return symbols
        
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
            'ipo_date': general.get('IPODate'),
            'country': general.get('Country', ''),
            'sector': general.get('Sector', ''),
            'industry': general.get('Industry', '')
        }
        
    except Exception as e:
        return None

def generate_sql_batch(symbols, start_idx, batch_size):
    """Generate SQL updates for a batch of symbols"""
    print(f"\n📝 Processing batch {start_idx//batch_size + 1} (symbols {start_idx+1}-{min(start_idx+batch_size, len(symbols))})")
    
    sql_statements = []
    processed = 0
    successful = 0
    
    for i in range(start_idx, min(start_idx + batch_size, len(symbols))):
        symbol_info = symbols[i]
        symbol = symbol_info['symbol']
        
        print(f"🔍 {processed+1:3d}/{batch_size}: {symbol}...", end=" ")
        
        # Fetch fundamental data
        fundamental_data = fetch_fundamental_data(symbol)
        processed += 1
        
        if not fundamental_data:
            print("❌ No data")
            continue
        
        ipo_date = fundamental_data['ipo_date']
        if ipo_date:
            successful += 1
            print(f"✅ IPO: {ipo_date}")
            
            # Generate SQL UPDATE statement with IPO date
            sql = f"""UPDATE dev_instrument_eodhd SET 
    name = '{(fundamental_data['name'] or '').replace("'", "''")}',
    exchange = '{fundamental_data['exchange'] or ''}',
    asset_type = '{fundamental_data['type'] or ''}',
    currency = '{fundamental_data['currency'] or ''}',
    ipo_date = '{ipo_date}',
    updated_at = NOW()
WHERE symbol = '{symbol}';"""
        else:
            print(f"⚠️  No IPO date")
            # Still update with better name/exchange data
            sql = f"""UPDATE dev_instrument_eodhd SET 
    name = '{(fundamental_data['name'] or '').replace("'", "''")}',
    exchange = '{fundamental_data['exchange'] or ''}',
    asset_type = '{fundamental_data['type'] or ''}',
    currency = '{fundamental_data['currency'] or ''}',
    updated_at = NOW()
WHERE symbol = '{symbol}';"""
        
        sql_statements.append(sql)
        
        # Rate limiting - 3 seconds between API calls
        time.sleep(3)
    
    print(f"📊 Batch summary: {successful}/{processed} with IPO dates ({successful/processed*100:.1f}%)")
    return sql_statements

def main():
    """Main execution"""
    print("🚀 EODHD Bulk SQL Generator")
    print("=" * 60)
    
    # Configuration
    BATCH_SIZE = 50  # Process 50 symbols per batch
    MAX_SYMBOLS = 1000  # Limit for initial run (can increase later)
    
    print(f"📋 Configuration:")
    print(f"   Batch size: {BATCH_SIZE} symbols")
    print(f"   Max symbols: {MAX_SYMBOLS}")
    print(f"   Rate limit: 3 seconds per symbol")
    print(f"   Estimated time: {MAX_SYMBOLS * 3 / 60:.1f} minutes")
    
    # Get symbols
    symbols = get_exchange_symbols(limit=MAX_SYMBOLS)
    
    if not symbols:
        print("❌ No symbols retrieved. Exiting.")
        return
    
    print(f"\n🎯 Processing {len(symbols)} symbols in batches of {BATCH_SIZE}")
    
    # Process in batches
    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
    all_sql_statements = []
    overall_successful = 0
    overall_processed = 0
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * BATCH_SIZE
        print(f"\n🔄 Batch {batch_idx + 1}/{total_batches}")
        
        batch_sql = generate_sql_batch(symbols, start_idx, BATCH_SIZE)
        all_sql_statements.extend(batch_sql)
        
        # Count successful IPO dates in this batch
        batch_ipo_count = sum(1 for sql in batch_sql if 'ipo_date =' in sql and "ipo_date = ''" not in sql)
        overall_successful += batch_ipo_count
        overall_processed += len(batch_sql)
        
        print(f"✅ Batch {batch_idx + 1} completed: {batch_ipo_count} IPO dates found")
    
    # Write all SQL statements to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_file = f"eodhd_bulk_update_{timestamp}.sql"
    
    with open(sql_file, 'w') as f:
        f.write(f"-- EODHD Bulk Data Update with IPO Dates\n")
        f.write(f"-- Generated on {datetime.now().isoformat()}\n")
        f.write(f"-- Processed {overall_processed} symbols\n")
        f.write(f"-- Found {overall_successful} IPO dates ({overall_successful/overall_processed*100:.1f}%)\n\n")
        
        for sql in all_sql_statements:
            f.write(sql + "\n\n")
        
        # Add verification query
        updated_symbols = [sql.split("WHERE symbol = '")[1].split("'")[0] for sql in all_sql_statements]
        symbols_list = "', '".join(updated_symbols[:100])  # Limit to first 100 for verification
        f.write("-- Verification query (first 100 symbols)\n")
        f.write(f"SELECT symbol, name, ipo_date FROM dev_instrument_eodhd WHERE symbol IN ('{symbols_list}') AND ipo_date IS NOT NULL ORDER BY symbol;\n")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 BULK PROCESSING SUMMARY")
    print("=" * 60)
    print(f"✅ Symbols processed: {overall_processed}")
    print(f"✅ IPO dates found: {overall_successful}")
    print(f"✅ Success rate: {overall_successful/overall_processed*100:.1f}%")
    print(f"💾 SQL file: {sql_file}")
    print(f"📏 File size: {os.path.getsize(sql_file) / 1024:.1f} KB")
    
    print(f"\n🔧 To apply these updates:")
    print(f"   PYTHONPATH=src python3 scripts/run_dev.py query --query \"$(cat {sql_file})\"")
    
    print(f"\n🔍 To verify the updates:")
    print(f"   PYTHONPATH=src python3 scripts/run_dev.py query --query \"SELECT COUNT(*) as with_ipo FROM dev_instrument_eodhd WHERE ipo_date IS NOT NULL\"")
    
    print(f"\n📈 Expected improvement:")
    current_count = 12  # Current IPO date count
    expected_count = current_count + overall_successful
    current_total = 50772  # Total instruments
    print(f"   Before: {current_count}/{current_total} ({current_count/current_total*100:.2f}%)")
    print(f"   After: {expected_count}/{current_total} ({expected_count/current_total*100:.2f}%)")
    print(f"   Improvement: {overall_successful/current_total*100:.2f} percentage points")

if __name__ == "__main__":
    main()