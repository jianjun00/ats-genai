#!/usr/bin/env python3
"""
Batch Fundamentals Processor
Processes fundamental data in manageable batches using proven infrastructure.
Works with existing run_dev database connection patterns.
"""

import requests
import time
import json
import subprocess
from datetime import datetime
from typing import Dict, List, Optional

# Configuration
FMP_API_KEY = "Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"  # From .env.test
POLYGON_API_KEY = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"  # From .env.test
BATCH_SIZE = 30  # Process in batches for checkpointing
MAX_SYMBOLS = 500  # Process more symbols per session

# Get symbols dynamically from database instead of hardcoded list
def get_expanded_symbol_universe():
    """Get expanded universe of symbols from database."""
    query = """
    SELECT DISTINCT symbol 
    FROM (
        SELECT symbol FROM dev_instrument_tiingo WHERE symbol IS NOT NULL AND LENGTH(symbol) BETWEEN 1 AND 5
        UNION 
        SELECT symbol FROM dev_instrument_eodhd WHERE symbol IS NOT NULL AND LENGTH(symbol) BETWEEN 1 AND 5  
        UNION
        SELECT symbol FROM dev_instruments WHERE symbol IS NOT NULL AND LENGTH(symbol) BETWEEN 1 AND 5
    ) combined
    WHERE symbol ~ '^[A-Z]+$'  -- Only alphabetic symbols
    ORDER BY symbol
    LIMIT 1000
    """
    result = run_db_query(query)
    if result and '|' not in result.split('\n')[0]:  # Simple list format
        symbols = []
        lines = result.split('\n')
        for line in lines:
            line = line.strip()
            if line and line != 'symbol' and line != '--------' and line != '(' and 'row' not in line:
                symbols.append(line)
        return symbols[:500]  # Limit to 500 for this session
    return []

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - INFO - {message}")

def run_db_query(query: str) -> str:
    """Execute database query using run_dev infrastructure."""
    try:
        result = subprocess.run(
            ['python3', 'scripts/run_dev.py', 'query', '--query', query],
            capture_output=True,
            text=True,
            cwd='/home/jianjun/ats-genai-data'
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            log_info(f"❌ Database query failed: {result.stderr}")
            return ""
    except Exception as e:
        log_info(f"❌ Error executing database query: {e}")
        return ""

def get_checkpoint_status():
    """Get current checkpoint status."""
    query = """
    SELECT last_symbol, symbols_processed, records_inserted 
    FROM dev_fundamentals_checkpoint 
    WHERE vendor = 'fmp' AND job_type = 'fundamentals'
    """
    result = run_db_query(query)
    if result and '|' in result:
        # Parse the result
        lines = result.split('\n')
        if len(lines) >= 3:  # Header, separator, data
            data_line = lines[2].strip()
            parts = [p.strip() for p in data_line.split('|')]
            if len(parts) >= 3:
                try:
                    symbols_processed = int(parts[1]) if parts[1] != '' else 0
                    records_inserted = int(parts[2]) if parts[2] != '' else 0
                    return parts[0] if parts[0] != '' else None, symbols_processed, records_inserted
                except:
                    pass
    return None, 0, 0

def update_checkpoint(last_symbol: str, symbols_processed: int, records_inserted: int):
    """Update checkpoint in database."""
    query = f"""
    INSERT INTO dev_fundamentals_checkpoint 
    (vendor, job_type, last_symbol, symbols_processed, records_inserted, start_time, last_update)
    VALUES ('fmp', 'fundamentals', '{last_symbol}', {symbols_processed}, {records_inserted}, 
            COALESCE((SELECT start_time FROM dev_fundamentals_checkpoint WHERE vendor = 'fmp' AND job_type = 'fundamentals'), CURRENT_TIMESTAMP),
            CURRENT_TIMESTAMP)
    ON CONFLICT (vendor, job_type) DO UPDATE SET
        last_symbol = EXCLUDED.last_symbol,
        symbols_processed = EXCLUDED.symbols_processed,
        records_inserted = EXCLUDED.records_inserted,
        last_update = CURRENT_TIMESTAMP
    """
    run_db_query(query)

def fetch_fmp_financial_statements(symbol: str, statement_type: str) -> List[Dict]:
    """Fetch financial statements from FMP API."""
    url = f"https://financialmodelingprep.com/api/v3/{statement_type}/{symbol}"
    params = {"limit": 40, "apikey": FMP_API_KEY}  # 10 years of quarterly data
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            log_info(f"⏳ Rate limit hit for {symbol}, waiting 60 seconds...")
            time.sleep(60)
            return fetch_fmp_financial_statements(symbol, statement_type)
        
        if response.status_code != 200:
            log_info(f"⚠️ FMP API error for {symbol} {statement_type}: {response.status_code}")
            return []
        
        data = response.json()
        return data if isinstance(data, list) else []
        
    except Exception as e:
        log_info(f"❌ Error fetching {symbol} {statement_type}: {e}")
        return []

def safe_value(value, default=None):
    """Safely convert value to database-safe format."""
    if value is None or value == '':
        return 'NULL'
    if isinstance(value, (int, float)):
        return str(int(value)) if isinstance(value, float) and value.is_integer() else str(value)
    return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'"

def insert_fundamental_records(symbol: str, records: List[Dict]) -> int:
    """Insert fundamental records using batch INSERT."""
    if not records:
        return 0
    
    # Prepare batch insert values
    values_list = []
    for record in records:
        values = f"""(
            '{symbol}', 
            '{record['date']}', 
            'fmp', 
            '{record.get('period', 'FY')}',
            {safe_value(record.get('revenue'))},
            {safe_value(record.get('grossProfit'))},
            {safe_value(record.get('operatingIncome'))},
            {safe_value(record.get('netIncome'))},
            {safe_value(record.get('ebitda'))},
            {safe_value(record.get('eps'))},
            {safe_value(record.get('totalAssets'))},
            {safe_value(record.get('totalLiabilities'))},
            {safe_value(record.get('totalStockholdersEquity'))},
            {safe_value(record.get('totalCurrentAssets'))},
            {safe_value(record.get('totalCurrentLiabilities'))},
            {safe_value(record.get('totalDebt'))},
            {safe_value(record.get('cashAndCashEquivalents'))},
            {safe_value(record.get('operatingCashFlow'))},
            {safe_value(record.get('netCashUsedProvidedByInvestingActivities'))},
            {safe_value(record.get('netCashUsedProvidedByFinancingActivities'))},
            {safe_value(record.get('freeCashFlow'))}
        )"""
        values_list.append(values)
    
    if not values_list:
        return 0
    
    # Construct insert query
    query = f"""
    INSERT INTO dev_fundamentals_comprehensive 
    (symbol, date, vendor, fiscal_period, revenue, gross_profit, operating_income, 
     net_income, ebitda, eps, total_assets, total_liabilities, shareholders_equity,
     current_assets, current_liabilities, total_debt, cash_and_equivalents,
     operating_cash_flow, investing_cash_flow, financing_cash_flow, free_cash_flow)
    VALUES {', '.join(values_list)}
    ON CONFLICT (symbol, date, vendor, fiscal_period) DO UPDATE SET
        revenue = EXCLUDED.revenue,
        gross_profit = EXCLUDED.gross_profit,
        operating_income = EXCLUDED.operating_income,
        net_income = EXCLUDED.net_income,
        ebitda = EXCLUDED.ebitda,
        eps = EXCLUDED.eps,
        total_assets = EXCLUDED.total_assets,
        total_liabilities = EXCLUDED.total_liabilities,
        shareholders_equity = EXCLUDED.shareholders_equity,
        current_assets = EXCLUDED.current_assets,
        current_liabilities = EXCLUDED.current_liabilities,
        total_debt = EXCLUDED.total_debt,
        cash_and_equivalents = EXCLUDED.cash_and_equivalents,
        operating_cash_flow = EXCLUDED.operating_cash_flow,
        investing_cash_flow = EXCLUDED.investing_cash_flow,
        financing_cash_flow = EXCLUDED.financing_cash_flow,
        free_cash_flow = EXCLUDED.free_cash_flow,
        updated_at = CURRENT_TIMESTAMP
    """
    
    result = run_db_query(query)
    return len(records) if 'INSERT' in result or 'UPDATE' in result else 0

def process_symbol(symbol: str) -> Dict:
    """Process fundamental data for a single symbol."""
    log_info(f"🔍 Processing {symbol}...")
    
    try:
        # Fetch financial statements
        income_data = fetch_fmp_financial_statements(symbol, "income-statement")
        balance_data = fetch_fmp_financial_statements(symbol, "balance-sheet-statement")
        cashflow_data = fetch_fmp_financial_statements(symbol, "cash-flow-statement")
        
        # Merge data by date
        merged_records = {}
        
        # Process income data
        for item in income_data:
            if 'date' in item and item['date']:
                date_key = item['date']
                if date_key not in merged_records:
                    merged_records[date_key] = {'date': date_key}
                merged_records[date_key].update({
                    'period': item.get('period', 'FY'),
                    'revenue': item.get('revenue'),
                    'grossProfit': item.get('grossProfit'),
                    'operatingIncome': item.get('operatingIncome'),
                    'netIncome': item.get('netIncome'),
                    'ebitda': item.get('ebitda'),
                    'eps': item.get('eps')
                })
        
        # Process balance data
        balance_by_date = {item.get('date'): item for item in balance_data if item.get('date')}
        for date_key, balance_item in balance_by_date.items():
            if date_key not in merged_records:
                merged_records[date_key] = {'date': date_key, 'period': 'FY'}
            merged_records[date_key].update({
                'totalAssets': balance_item.get('totalAssets'),
                'totalLiabilities': balance_item.get('totalLiabilities'),
                'totalStockholdersEquity': balance_item.get('totalStockholdersEquity'),
                'totalCurrentAssets': balance_item.get('totalCurrentAssets'),
                'totalCurrentLiabilities': balance_item.get('totalCurrentLiabilities'),
                'totalDebt': balance_item.get('totalDebt'),
                'cashAndCashEquivalents': balance_item.get('cashAndCashEquivalents')
            })
        
        # Process cashflow data
        cashflow_by_date = {item.get('date'): item for item in cashflow_data if item.get('date')}
        for date_key, cashflow_item in cashflow_by_date.items():
            if date_key not in merged_records:
                merged_records[date_key] = {'date': date_key, 'period': 'FY'}
            merged_records[date_key].update({
                'operatingCashFlow': cashflow_item.get('operatingCashFlow'),
                'netCashUsedProvidedByInvestingActivities': cashflow_item.get('netCashUsedProvidedByInvestingActivities'),
                'netCashUsedProvidedByFinancingActivities': cashflow_item.get('netCashUsedProvidedByFinancingActivities'),
                'freeCashFlow': cashflow_item.get('freeCashFlow')
            })
        
        # Convert to list and sort by date (newest first)
        records_list = list(merged_records.values())
        records_list.sort(key=lambda x: x['date'], reverse=True)
        
        # Insert records into database
        records_inserted = insert_fundamental_records(symbol, records_list)
        
        if records_inserted > 0:
            log_info(f"✅ {symbol}: {records_inserted} records inserted (from {len(income_data)}I + {len(balance_data)}B + {len(cashflow_data)}C)")
            return {
                'symbol': symbol,
                'records_inserted': records_inserted,
                'periods_processed': len(records_list),
                'success': True
            }
        else:
            log_info(f"⚠️ {symbol}: No records inserted")
            return {
                'symbol': symbol,
                'records_inserted': 0,
                'success': False,
                'error': 'No data or insert failed'
            }
    
    except Exception as e:
        log_info(f"❌ {symbol}: Error - {e}")
        return {
            'symbol': symbol,
            'records_inserted': 0,
            'success': False,
            'error': str(e)
        }

def main():
    """Main batch processing function."""
    log_info("🚀 Starting expanded fundamentals processing")
    log_info("📋 Fetching symbols from database...")
    
    # Get expanded symbol universe
    all_symbols = get_expanded_symbol_universe()
    if not all_symbols:
        log_info("❌ No symbols found to process")
        return False
    
    log_info(f"📊 Found {len(all_symbols)} symbols for processing")
    log_info(f"📋 Sample symbols: {', '.join(all_symbols[:10])}...")
    
    # Get current checkpoint to determine where to start
    last_symbol, symbols_processed, records_inserted = get_checkpoint_status()
    log_info(f"📍 Checkpoint: {symbols_processed} symbols processed, {records_inserted} records inserted")
    
    # Find starting position based on checkpoint
    start_index = 0
    if last_symbol and last_symbol in all_symbols:
        start_index = all_symbols.index(last_symbol) + 1
        log_info(f"🔄 Resuming from symbol: {last_symbol} (index {start_index})")
    
    # Select symbols to process (limit per session)
    symbols_to_process = all_symbols[start_index:start_index + MAX_SYMBOLS]
    log_info(f"📊 Processing {len(symbols_to_process)} symbols in this session")
    
    # Process symbols in batches
    total_processed = symbols_processed
    total_records = records_inserted
    successful = 0
    failed = 0
    
    for batch_start in range(0, len(symbols_to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(symbols_to_process))
        batch_symbols = symbols_to_process[batch_start:batch_end]
        
        log_info(f"🔄 Processing batch {batch_start//BATCH_SIZE + 1}: {len(batch_symbols)} symbols")
        
        batch_records = 0
        batch_successful = 0
        
        for i, symbol in enumerate(batch_symbols):
            result = process_symbol(symbol)
            
            if result['success']:
                batch_successful += 1
                batch_records += result['records_inserted']
            else:
                failed += 1
            
            total_processed += 1
            
            # Rate limiting - FMP free tier allows 250 requests/minute
            time.sleep(0.5)  # 2 requests per second = 120/minute (safe buffer)
        
        # Update totals
        successful += batch_successful
        total_records += batch_records
        
        # Update checkpoint after each batch
        if batch_symbols:
            update_checkpoint(batch_symbols[-1], total_processed, total_records)
            log_info(f"💾 Checkpoint updated: {total_processed} symbols, {total_records} records")
        
        log_info(f"✅ Batch complete: {batch_successful}/{len(batch_symbols)} successful, {batch_records} records")
    
    # Final summary
    log_info("🎉 Batch processing completed!")
    log_info("=" * 80)
    log_info("📊 FINAL RESULTS:")
    log_info(f"   ✅ Symbols processed this session: {successful}")
    log_info(f"   ❌ Failed this session: {failed}")
    log_info(f"   📈 Records added this session: {total_records - records_inserted}")
    log_info(f"   📊 Total cumulative symbols: {total_processed}")
    log_info(f"   🗂️ Total cumulative records: {total_records:,}")
    
    # Verify data in database
    result = run_db_query("SELECT COUNT(*) as total, COUNT(DISTINCT symbol) as symbols FROM dev_fundamentals_comprehensive WHERE vendor = 'fmp'")
    if result:
        lines = result.split('\n')
        if len(lines) >= 3:
            data_line = lines[2].strip()
            parts = [p.strip() for p in data_line.split('|')]
            if len(parts) >= 2:
                log_info(f"   🗄️ Database verification: {parts[0]} records, {parts[1]} unique symbols")
    
    # Show progress towards full universe
    progress_pct = (total_processed / 70000) * 100
    log_info(f"   📈 Progress: {total_processed}/70,000 symbols ({progress_pct:.2f}%)")
    
    if start_index + len(symbols_to_process) < len(all_symbols):
        remaining = len(all_symbols) - (start_index + len(symbols_to_process))
        log_info(f"   🔄 Remaining in current batch: {remaining} symbols")
        log_info("   💡 Run again to continue processing...")
    
    log_info("=" * 80)
    log_info("🚀 Expanded fundamentals backfill infrastructure operational!")
    
    return successful > 0

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)