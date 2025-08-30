#!/usr/bin/env python3
"""
Daily Fundamentals Refresh Job for ATS-INTG Environment

Refreshes fundamental data from all configured vendors for the integration environment.
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
    'ALPHA_VANTAGE_API_KEY': '9GI0NZ3V4VNFX271'
}

VENDORS = ['polygon', 'fmp', 'alpha_vantage']
MAX_WORKERS = 2  # Conservative for API rate limits
RATE_LIMIT_DELAY = 1.0  # seconds between requests for fundamentals

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

def get_symbols_needing_refresh(limit: int = 200) -> list:
    """Get list of symbols needing fundamental data refresh."""
    log_info(f"📋 Fetching symbols needing fundamentals refresh (limit: {limit})")
    
    # Get symbols that either have no recent fundamentals data or need updates
    query = f"""
    SELECT DISTINCT i.symbol 
    FROM intg_instruments i
    LEFT JOIN intg_fundamentals_comprehensive f ON i.symbol = f.symbol 
        AND f.date >= CURRENT_DATE - INTERVAL '90 days'
    WHERE i.active = true 
    AND i.symbol ~ '^[A-Z]{{1,5}}$'
    AND (f.symbol IS NULL OR COUNT(f.symbol) < 3)
    GROUP BY i.symbol
    ORDER BY i.symbol 
    LIMIT {limit}
    """
    
    result = run_intg_query(query)
    symbols = []
    
    for line in result.split('\n'):
        line = line.strip()
        if line and line not in ['symbol', '--------', '(', 'rows)'] and 'row' not in line:
            symbols.append(line)
    
    log_info(f"📊 Found {len(symbols)} symbols needing fundamentals refresh")
    return symbols

def create_fundamentals_checkpoint_table():
    """Create checkpoint table for tracking fundamentals refresh progress."""
    log_info("🔧 Setting up fundamentals checkpoint tracking...")
    
    query = """
    CREATE TABLE IF NOT EXISTS intg_fundamentals_checkpoint (
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
        INSERT INTO intg_fundamentals_checkpoint (job_date, vendor, status)
        VALUES ('{today}', '{vendor}', 'running')
        ON CONFLICT (job_date, vendor) DO NOTHING
        """
        run_intg_query(init_query)

def update_fundamentals_checkpoint(vendor: str, symbols_processed: int, records_inserted: int, 
                                 last_symbol: str = None, status: str = 'running', error: str = None):
    """Update checkpoint for a vendor."""
    today = datetime.now().date()
    
    query = f"""
    UPDATE intg_fundamentals_checkpoint 
    SET symbols_processed = {symbols_processed},
        records_inserted = {records_inserted},
        last_symbol = {f"'{last_symbol}'" if last_symbol else 'NULL'},
        status = '{status}',
        {"completed_at = CURRENT_TIMESTAMP," if status == 'completed' else ""}
        error_message = {f"'{error}'" if error else 'NULL'}
    WHERE job_date = '{today}' AND vendor = '{vendor}'
    """
    
    run_intg_query(query)

def fetch_fmp_fundamentals(symbol: str) -> list:
    """Fetch fundamental data from FMP API."""
    import requests
    
    try:
        fundamentals_data = []
        
        # Fetch income statement
        income_url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}"
        income_params = {"limit": 4, "apikey": API_KEYS['FMP_API_KEY']}
        
        income_response = requests.get(income_url, params=income_params, timeout=30)
        if income_response.status_code == 200:
            income_data = income_response.json()
            for item in income_data[:2]:  # Latest 2 quarters
                fundamentals_data.append({
                    'symbol': symbol,
                    'date': item.get('date'),
                    'vendor': 'fmp',
                    'fiscal_period': item.get('period', 'FY'),
                    'revenue': item.get('revenue'),
                    'gross_profit': item.get('grossProfit'),
                    'operating_income': item.get('operatingIncome'),
                    'net_income': item.get('netIncome'),
                    'ebitda': item.get('ebitda'),
                    'eps': item.get('eps')
                })
        
        time.sleep(0.3)  # Rate limiting
        
        # Fetch balance sheet for same periods
        balance_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}"
        balance_params = {"limit": 2, "apikey": API_KEYS['FMP_API_KEY']}
        
        balance_response = requests.get(balance_url, params=balance_params, timeout=30)
        if balance_response.status_code == 200:
            balance_data = balance_response.json()
            
            # Match balance sheet data with income statement data by date
            balance_by_date = {item.get('date'): item for item in balance_data}
            
            for fund_item in fundamentals_data:
                date_key = fund_item['date']
                if date_key in balance_by_date:
                    balance_item = balance_by_date[date_key]
                    fund_item.update({
                        'total_assets': balance_item.get('totalAssets'),
                        'total_liabilities': balance_item.get('totalLiabilities'),
                        'shareholders_equity': balance_item.get('totalStockholdersEquity'),
                        'current_assets': balance_item.get('totalCurrentAssets'),
                        'current_liabilities': balance_item.get('totalCurrentLiabilities'),
                        'total_debt': balance_item.get('totalDebt'),
                        'cash_and_equivalents': balance_item.get('cashAndCashEquivalents')
                    })
        
        return fundamentals_data
        
    except Exception as e:
        log_info(f"⚠️ FMP API error for {symbol}: {e}")
        return []

def fetch_polygon_fundamentals(symbol: str) -> list:
    """Fetch fundamental data from Polygon API."""
    import requests
    
    try:
        url = f"https://api.polygon.io/vX/reference/financials"
        params = {"ticker": symbol, "apikey": API_KEYS['POLYGON_API_KEY'], "limit": 4}
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            financials = data.get("results", [])
            
            fundamentals_data = []
            for item in financials[:2]:  # Latest 2 periods
                if not item.get("end_date"):
                    continue
                
                fin_data = item.get("financials", {})
                income = fin_data.get("income_statement", {})
                balance = fin_data.get("balance_sheet", {})
                cash_flow = fin_data.get("cash_flow_statement", {})
                
                fundamentals_data.append({
                    'symbol': symbol,
                    'date': item["end_date"],
                    'vendor': 'polygon',
                    'fiscal_period': 'FY',
                    'revenue': income.get("revenues", {}).get("value"),
                    'net_income': income.get("net_income_loss", {}).get("value"),
                    'total_assets': balance.get("assets", {}).get("value"),
                    'operating_cash_flow': cash_flow.get("net_cash_flow_from_operating_activities", {}).get("value")
                })
            
            return fundamentals_data
        
        return []
        
    except Exception as e:
        log_info(f"⚠️ Polygon API error for {symbol}: {e}")
        return []

def fetch_alpha_vantage_fundamentals(symbol: str) -> list:
    """Fetch fundamental data from Alpha Vantage API."""
    import requests
    
    try:
        # Alpha Vantage has fundamental data in their income statement endpoint
        url = f"https://www.alphavantage.co/query"
        params = {
            "function": "INCOME_STATEMENT",
            "symbol": symbol,
            "apikey": API_KEYS['ALPHA_VANTAGE_API_KEY']
        }
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            annual_reports = data.get("annualReports", [])
            
            fundamentals_data = []
            for item in annual_reports[:2]:  # Latest 2 annual reports
                fundamentals_data.append({
                    'symbol': symbol,
                    'date': item.get('fiscalDateEnding'),
                    'vendor': 'alpha_vantage',
                    'fiscal_period': 'FY',
                    'revenue': item.get('totalRevenue'),
                    'gross_profit': item.get('grossProfit'),
                    'operating_income': item.get('operatingIncome'),
                    'net_income': item.get('netIncome'),
                    'ebitda': item.get('ebitda')
                })
            
            return fundamentals_data
        
        return []
        
    except Exception as e:
        log_info(f"⚠️ Alpha Vantage API error for {symbol}: {e}")
        return []

def insert_fundamentals_data(fundamentals_list: list) -> int:
    """Insert fundamental data into intg database."""
    if not fundamentals_list:
        return 0
    
    records_inserted = 0
    
    for fund_data in fundamentals_list:
        if not fund_data.get('date'):
            continue
        
        # Build the insert query dynamically based on available data
        columns = ['symbol', 'date', 'vendor', 'fiscal_period']
        values = [
            f"'{fund_data['symbol']}'",
            f"'{fund_data['date']}'", 
            f"'{fund_data['vendor']}'",
            f"'{fund_data.get('fiscal_period', 'FY')}'"
        ]
        
        # Add financial metrics if available
        metrics = {
            'revenue': fund_data.get('revenue'),
            'gross_profit': fund_data.get('gross_profit'),
            'operating_income': fund_data.get('operating_income'),
            'net_income': fund_data.get('net_income'),
            'ebitda': fund_data.get('ebitda'),
            'eps': fund_data.get('eps'),
            'total_assets': fund_data.get('total_assets'),
            'total_liabilities': fund_data.get('total_liabilities'),
            'shareholders_equity': fund_data.get('shareholders_equity'),
            'current_assets': fund_data.get('current_assets'),
            'current_liabilities': fund_data.get('current_liabilities'),
            'total_debt': fund_data.get('total_debt'),
            'cash_and_equivalents': fund_data.get('cash_and_equivalents'),
            'operating_cash_flow': fund_data.get('operating_cash_flow')
        }
        
        for metric, value in metrics.items():
            if value is not None:
                columns.append(metric)
                values.append(str(value))
        
        query = f"""
        INSERT INTO intg_fundamentals_comprehensive 
        ({', '.join(columns)})
        VALUES ({', '.join(values)})
        ON CONFLICT (symbol, date, vendor, fiscal_period) 
        DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns[4:]])},
            updated_at = CURRENT_TIMESTAMP
        """
        
        result = run_intg_query(query)
        if 'INSERT' in result or 'UPDATE' in result:
            records_inserted += 1
    
    return records_inserted

def process_symbol_vendor_fundamentals(symbol: str, vendor: str) -> int:
    """Process a single symbol for a specific vendor."""
    try:
        # Fetch data based on vendor
        if vendor == 'fmp':
            fundamentals_data = fetch_fmp_fundamentals(symbol)
        elif vendor == 'polygon':
            fundamentals_data = fetch_polygon_fundamentals(symbol)
        elif vendor == 'alpha_vantage':
            fundamentals_data = fetch_alpha_vantage_fundamentals(symbol)
        else:
            return 0
        
        # Insert data if available
        if fundamentals_data:
            records_count = insert_fundamentals_data(fundamentals_data)
            if records_count > 0:
                with stats_lock:
                    stats['total_records'] += records_count
                return records_count
        
        return 0
        
    except Exception as e:
        log_info(f"❌ Error processing {symbol} with {vendor}: {e}")
        return 0
    finally:
        # Rate limiting for fundamentals
        time.sleep(RATE_LIMIT_DELAY)

def process_vendor_fundamentals_batch(vendor: str, symbols: list) -> dict:
    """Process a batch of symbols for a specific vendor."""
    log_info(f"🚀 Starting {vendor.upper()} fundamentals batch: {len(symbols)} symbols")
    
    vendor_stats = {
        'processed': 0,
        'successful': 0,
        'records': 0
    }
    
    with ThreadPoolExecutor(max_workers=1) as executor:  # Single thread per vendor for rate limiting
        futures = {
            executor.submit(process_symbol_vendor_fundamentals, symbol, vendor): symbol 
            for symbol in symbols
        }
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                records_count = future.result()
                vendor_stats['processed'] += 1
                
                if records_count > 0:
                    vendor_stats['successful'] += 1
                    vendor_stats['records'] += records_count
                    log_info(f"✅ {symbol}: {records_count} records from {vendor}")
                
                # Update checkpoint periodically
                if vendor_stats['processed'] % 20 == 0:
                    update_fundamentals_checkpoint(
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
    update_fundamentals_checkpoint(vendor, vendor_stats['processed'], vendor_stats['records'], status=status)
    
    log_info(f"✅ {vendor.upper()} completed: {vendor_stats['successful']}/{len(symbols)} successful, {vendor_stats['records']} records")
    return vendor_stats

def main():
    """Main daily fundamentals refresh job."""
    log_info("🚀 Starting Daily Fundamentals Refresh Job for ATS-INTG")
    
    # Setup checkpoint tracking
    create_fundamentals_checkpoint_table()
    
    # Get symbols needing refresh
    symbols = get_symbols_needing_refresh(limit=100)  # Conservative limit for daily refresh
    if not symbols:
        log_info("✅ No symbols need fundamentals refresh today")
        return True
    
    with stats_lock:
        stats['total_symbols'] = len(symbols)
    
    log_info(f"📋 Processing {len(symbols)} symbols across {len(VENDORS)} vendors")
    
    # Process each vendor sequentially to avoid rate limiting conflicts
    vendor_results = {}
    
    for vendor in VENDORS:
        log_info(f"🔧 Starting vendor: {vendor.upper()}")
        
        try:
            vendor_stats = process_vendor_fundamentals_batch(vendor, symbols)
            vendor_results[vendor] = vendor_stats
            
            with stats_lock:
                if vendor_stats['successful'] > 0:
                    stats['successful_vendors'] += 1
                else:
                    stats['failed_vendors'] += 1
            
            # Longer pause between vendors for fundamentals
            time.sleep(5)
            
        except Exception as e:
            log_info(f"❌ Vendor {vendor} failed: {e}")
            update_fundamentals_checkpoint(vendor, 0, 0, status='failed', error=str(e))
            with stats_lock:
                stats['failed_vendors'] += 1
    
    # Final summary
    log_info("🎉 Daily Fundamentals Refresh Job Complete!")
    log_info("=" * 60)
    log_info(f"📊 Total symbols: {stats['total_symbols']}")
    log_info(f"✅ Successful vendors: {stats['successful_vendors']}")
    log_info(f"❌ Failed vendors: {stats['failed_vendors']}")
    log_info(f"📈 Total records inserted: {stats['total_records']}")
    
    # Vendor breakdown
    for vendor, result in vendor_results.items():
        log_info(f"   {vendor.upper()}: {result['successful']}/{result['processed']} symbols, {result['records']} records")
    
    # Check final database status
    today = datetime.now().date()
    count_query = f"SELECT COUNT(*) as total FROM intg_fundamentals_comprehensive WHERE date >= '{today - timedelta(days=7)}'"
    result = run_intg_query(count_query)
    log_info(f"🗄️ Recent fundamentals in database: {result}")
    
    log_info("=" * 60)
    return stats['successful_vendors'] > 0

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)