#!/usr/bin/env python3
"""
Daily Price Refresh Job for ATS-INTG Environment

Refreshes daily prices from all configured vendors for the integration environment.
Designed to run as a scheduled daily job.
"""

import sys
import os
import subprocess
import time
from datetime import datetime, timedelta
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add ATS source path
sys.path.append('/workspace/src')

# Configuration
API_KEYS = {
    'POLYGON_API_KEY': 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',
    'FMP_API_KEY': 'Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr',
    'TIINGO_API_KEY': '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5',
    'ALPHA_VANTAGE_API_KEY': '9GI0NZ3V4VNFX271'
}

VENDORS = ['polygon', 'fmp', 'tiingo', 'alpha_vantage']
MAX_WORKERS = 3
RATE_LIMIT_DELAY = 0.5  # seconds between requests

# Threading for progress tracking
stats = {
    'total_symbols': 0,
    'processed_symbols': 0,
    'successful_vendors': 0,
    'failed_vendors': 0,
    'total_records': 0
}
stats_lock = threading.Lock()

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - INFO - {message}")

def run_intg_query(query: str) -> str:
    """Execute database query using run_intg infrastructure."""
    try:
        result = subprocess.run(
            ['python3', 'scripts/run_intg.py', 'query', '--query', query],
            capture_output=True,
            text=True,
            cwd='/workspace'
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            log_info(f"❌ Database query failed: {result.stderr}")
            return ""
    except Exception as e:
        log_info(f"❌ Error executing database query: {e}")
        return ""

def get_active_symbols(limit: int = 1000) -> list:
    """Get list of active symbols from intg database."""
    log_info(f"📋 Fetching active symbols (limit: {limit})")
    
    query = f"""
    SELECT DISTINCT symbol 
    FROM intg_instruments 
    WHERE active = true 
    AND symbol ~ '^[A-Z]{{1,5}}$'
    ORDER BY symbol 
    LIMIT {limit}
    """
    
    result = run_intg_query(query)
    symbols = []
    
    for line in result.split('\n'):
        line = line.strip()
        if line and line not in ['symbol', '--------', '(', 'rows)'] and 'row' not in line:
            symbols.append(line)
    
    log_info(f"📊 Found {len(symbols)} active symbols for refresh")
    return symbols

def create_checkpoint_table():
    """Create checkpoint table for tracking daily price refresh progress."""
    log_info("🔧 Setting up checkpoint tracking...")
    
    query = """
    CREATE TABLE IF NOT EXISTS intg_daily_price_checkpoint (
        id SERIAL PRIMARY KEY,
        job_date DATE NOT NULL,
        vendor VARCHAR(50) NOT NULL,
        symbols_processed INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        last_symbol VARCHAR(20),
        status VARCHAR(20) DEFAULT 'running',
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT,
        UNIQUE(job_date, vendor)
    );
    """
    
    run_intg_query(query)
    
    # Initialize today's checkpoints
    today = datetime.now().date()
    for vendor in VENDORS:
        init_query = f"""
        INSERT INTO intg_daily_price_checkpoint (job_date, vendor, status)
        VALUES ('{today}', '{vendor}', 'running')
        ON CONFLICT (job_date, vendor) DO NOTHING
        """
        run_intg_query(init_query)

def update_checkpoint(vendor: str, symbols_processed: int, records_inserted: int, 
                     last_symbol: str = None, status: str = 'running', error: str = None):
    """Update checkpoint for a vendor."""
    today = datetime.now().date()
    
    query = f"""
    UPDATE intg_daily_price_checkpoint 
    SET symbols_processed = {symbols_processed},
        records_inserted = {records_inserted},
        last_symbol = {f"'{last_symbol}'" if last_symbol else 'NULL'},
        status = '{status}',
        {"completed_at = CURRENT_TIMESTAMP," if status == 'completed' else ""}
        error_message = {f"'{error}'" if error else 'NULL'}
    WHERE job_date = '{today}' AND vendor = '{vendor}'
    """
    
    run_intg_query(query)

def fetch_polygon_daily_prices(symbol: str, date: str) -> dict:
    """Fetch daily prices from Polygon API."""
    import requests
    
    try:
        url = f"https://api.polygon.io/v1/open-close/{symbol}/{date}"
        params = {'apikey': API_KEYS['POLYGON_API_KEY']}
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'OK':
                return {
                    'symbol': symbol,
                    'date': date,
                    'open': data.get('open'),
                    'high': data.get('high'), 
                    'low': data.get('low'),
                    'close': data.get('close'),
                    'volume': data.get('volume'),
                    'vendor': 'polygon'
                }
        return None
    except Exception as e:
        log_info(f"⚠️ Polygon API error for {symbol}: {e}")
        return None

def fetch_fmp_daily_prices(symbol: str, date: str) -> dict:
    """Fetch daily prices from FMP API."""
    import requests
    
    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
        params = {
            'apikey': API_KEYS['FMP_API_KEY'],
            'from': date,
            'to': date
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            historical = data.get('historical', [])
            if historical:
                price_data = historical[0]
                return {
                    'symbol': symbol,
                    'date': date,
                    'open': price_data.get('open'),
                    'high': price_data.get('high'),
                    'low': price_data.get('low'),
                    'close': price_data.get('close'),
                    'volume': price_data.get('volume'),
                    'vendor': 'fmp'
                }
        return None
    except Exception as e:
        log_info(f"⚠️ FMP API error for {symbol}: {e}")
        return None

def fetch_tiingo_daily_prices(symbol: str, date: str) -> dict:
    """Fetch daily prices from Tiingo API."""
    import requests
    
    try:
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        headers = {'Authorization': f'Token {API_KEYS["TIINGO_API_KEY"]}'}
        params = {
            'startDate': date,
            'endDate': date
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data:
                price_data = data[0]
                return {
                    'symbol': symbol,
                    'date': date,
                    'open': price_data.get('open'),
                    'high': price_data.get('high'),
                    'low': price_data.get('low'),
                    'close': price_data.get('close'),
                    'volume': price_data.get('volume'),
                    'vendor': 'tiingo'
                }
        return None
    except Exception as e:
        log_info(f"⚠️ Tiingo API error for {symbol}: {e}")
        return None

def insert_daily_price(price_data: dict) -> bool:
    """Insert daily price data into intg database."""
    if not price_data:
        return False
    
    query = f"""
    INSERT INTO intg_daily_prices 
    (symbol, date, vendor, open_price, high_price, low_price, close_price, volume)
    VALUES (
        '{price_data['symbol']}',
        '{price_data['date']}',
        '{price_data['vendor']}',
        {price_data['open'] or 'NULL'},
        {price_data['high'] or 'NULL'},
        {price_data['low'] or 'NULL'},
        {price_data['close'] or 'NULL'},
        {price_data['volume'] or 'NULL'}
    )
    ON CONFLICT (symbol, date, vendor) 
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        updated_at = CURRENT_TIMESTAMP
    """
    
    result = run_intg_query(query)
    return 'INSERT' in result or 'UPDATE' in result

def process_symbol_vendor(symbol: str, vendor: str, target_date: str) -> bool:
    """Process a single symbol for a specific vendor."""
    try:
        # Fetch data based on vendor
        if vendor == 'polygon':
            price_data = fetch_polygon_daily_prices(symbol, target_date)
        elif vendor == 'fmp':
            price_data = fetch_fmp_daily_prices(symbol, target_date)
        elif vendor == 'tiingo':
            price_data = fetch_tiingo_daily_prices(symbol, target_date)
        else:
            return False
        
        # Insert data if available
        if price_data:
            success = insert_daily_price(price_data)
            if success:
                with stats_lock:
                    stats['total_records'] += 1
                return True
        
        return False
        
    except Exception as e:
        log_info(f"❌ Error processing {symbol} with {vendor}: {e}")
        return False
    finally:
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

def process_vendor_batch(vendor: str, symbols: list, target_date: str) -> dict:
    """Process a batch of symbols for a specific vendor."""
    log_info(f"🚀 Starting {vendor.upper()} batch: {len(symbols)} symbols")
    
    vendor_stats = {
        'processed': 0,
        'successful': 0,
        'records': 0
    }
    
    with ThreadPoolExecutor(max_workers=2) as executor:  # Conservative threading per vendor
        futures = {
            executor.submit(process_symbol_vendor, symbol, vendor, target_date): symbol 
            for symbol in symbols
        }
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                success = future.result()
                vendor_stats['processed'] += 1
                
                if success:
                    vendor_stats['successful'] += 1
                    vendor_stats['records'] += 1
                
                # Update checkpoint periodically
                if vendor_stats['processed'] % 50 == 0:
                    update_checkpoint(
                        vendor, 
                        vendor_stats['processed'], 
                        vendor_stats['records'],
                        symbol
                    )
                    log_info(f"📊 {vendor.upper()}: {vendor_stats['processed']}/{len(symbols)} symbols, {vendor_stats['records']} records")
                
            except Exception as e:
                log_info(f"❌ Future error for {symbol}: {e}")
                vendor_stats['processed'] += 1
    
    # Final checkpoint update
    status = 'completed' if vendor_stats['processed'] == len(symbols) else 'partial'
    update_checkpoint(vendor, vendor_stats['processed'], vendor_stats['records'], status=status)
    
    log_info(f"✅ {vendor.upper()} completed: {vendor_stats['successful']}/{len(symbols)} successful, {vendor_stats['records']} records")
    return vendor_stats

def main():
    """Main daily price refresh job."""
    log_info("🚀 Starting Daily Price Refresh Job for ATS-INTG")
    
    # Determine target date (yesterday for daily refresh)
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    log_info(f"📅 Target date: {target_date}")
    
    # Setup checkpoint tracking
    create_checkpoint_table()
    
    # Get active symbols
    symbols = get_active_symbols(limit=500)  # Conservative limit for daily refresh
    if not symbols:
        log_info("❌ No active symbols found for refresh")
        return False
    
    with stats_lock:
        stats['total_symbols'] = len(symbols)
    
    log_info(f"📋 Processing {len(symbols)} symbols across {len(VENDORS)} vendors")
    
    # Process each vendor sequentially to avoid rate limiting conflicts
    vendor_results = {}
    
    for vendor in VENDORS:
        log_info(f"🔧 Starting vendor: {vendor.upper()}")
        
        try:
            vendor_stats = process_vendor_batch(vendor, symbols, target_date)
            vendor_results[vendor] = vendor_stats
            
            with stats_lock:
                if vendor_stats['successful'] > 0:
                    stats['successful_vendors'] += 1
                else:
                    stats['failed_vendors'] += 1
            
            # Brief pause between vendors
            time.sleep(2)
            
        except Exception as e:
            log_info(f"❌ Vendor {vendor} failed: {e}")
            update_checkpoint(vendor, 0, 0, status='failed', error=str(e))
            with stats_lock:
                stats['failed_vendors'] += 1
    
    # Final summary
    log_info("🎉 Daily Price Refresh Job Complete!")
    log_info("=" * 60)
    log_info(f"📊 Total symbols: {stats['total_symbols']}")
    log_info(f"✅ Successful vendors: {stats['successful_vendors']}")
    log_info(f"❌ Failed vendors: {stats['failed_vendors']}")
    log_info(f"📈 Total records inserted: {stats['total_records']}")
    
    # Vendor breakdown
    for vendor, result in vendor_results.items():
        log_info(f"   {vendor.upper()}: {result['successful']}/{result['processed']} symbols, {result['records']} records")
    
    # Check final database status
    count_query = f"SELECT COUNT(*) as total FROM intg_daily_prices WHERE date = '{target_date}'"
    result = run_intg_query(count_query)
    log_info(f"🗄️ Database total for {target_date}: {result}")
    
    log_info("=" * 60)
    return stats['successful_vendors'] > 0

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)