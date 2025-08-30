#!/usr/bin/env python3
"""
Production Fundamentals Backfill Starter
Starts comprehensive 30-year fundamental data backfill for all 70K+ instruments
using existing ATS infrastructure with real API integration and database storage.
"""

import sys
sys.path.append('/workspace/src')

import os
import requests
import time
import json
import psycopg2
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional

# Configuration
FMP_API_KEY = "Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"  # From .env.test
BATCH_SIZE = 50  # Process in batches for checkpointing
MAX_SYMBOLS = 1000  # Start with first 1000 symbols, can resume later

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - INFO - {message}")

def get_db_connection():
    """Get database connection using host networking."""
    return psycopg2.connect(
        host="localhost",
        port="5433", 
        user="postgres",
        password="postgres",
        database="dev_db"
    )

def initialize_checkpoint_system():
    """Initialize checkpoint tracking."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if checkpoint exists
        cursor.execute("""
            SELECT last_symbol, symbols_processed, records_inserted 
            FROM dev_fundamentals_checkpoint 
            WHERE vendor = 'fmp' AND job_type = 'fundamentals'
        """)
        result = cursor.fetchone()
        
        if result:
            log_info(f"📍 Found existing checkpoint: {result[1]} symbols processed, {result[2]} records inserted")
            return result[0], result[1], result[2]
        else:
            # Create initial checkpoint
            cursor.execute("""
                INSERT INTO dev_fundamentals_checkpoint 
                (vendor, job_type, last_symbol, symbols_processed, records_inserted, start_time)
                VALUES ('fmp', 'fundamentals', NULL, 0, 0, CURRENT_TIMESTAMP)
            """)
            conn.commit()
            log_info("📍 Created initial checkpoint")
            return None, 0, 0
    finally:
        conn.close()

def save_checkpoint(last_symbol: str, symbols_processed: int, records_inserted: int):
    """Save checkpoint progress."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE dev_fundamentals_checkpoint 
            SET last_symbol = %s, symbols_processed = %s, records_inserted = %s, last_update = CURRENT_TIMESTAMP
            WHERE vendor = 'fmp' AND job_type = 'fundamentals'
        """, (last_symbol, symbols_processed, records_inserted))
        conn.commit()
        log_info(f"💾 Checkpoint saved: {symbols_processed} symbols, {records_inserted} records")
    finally:
        conn.close()

def get_all_symbols():
    """Get all unique symbols from instrument tables."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Get symbols from all sources, prioritizing active and liquid stocks
        cursor.execute("""
            SELECT DISTINCT symbol 
            FROM (
                SELECT symbol FROM dev_instrument_tiingo WHERE symbol IS NOT NULL AND LENGTH(symbol) BETWEEN 1 AND 10
                UNION 
                SELECT symbol FROM dev_instrument_eodhd WHERE symbol IS NOT NULL AND LENGTH(symbol) BETWEEN 1 AND 10
                UNION
                SELECT symbol FROM dev_instruments WHERE symbol IS NOT NULL AND LENGTH(symbol) BETWEEN 1 AND 10
            ) combined
            ORDER BY symbol
        """)
        
        symbols = [row[0] for row in cursor.fetchall()]
        log_info(f"📊 Found {len(symbols)} total symbols for processing")
        return symbols
        
    finally:
        conn.close()

def fetch_fmp_financial_statements(symbol: str, statement_type: str) -> List[Dict]:
    """Fetch financial statements from FMP API."""
    url = f"https://financialmodelingprep.com/api/v3/{statement_type}/{symbol}"
    params = {"limit": 120, "apikey": FMP_API_KEY}  # 30 years of quarterly data
    
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

def insert_fundamental_data(symbol: str, income_data: List[Dict], 
                          balance_data: List[Dict], cashflow_data: List[Dict]) -> int:
    """Insert fundamental data into comprehensive table."""
    if not any([income_data, balance_data, cashflow_data]):
        return 0
    
    conn = get_db_connection()
    records_inserted = 0
    
    try:
        cursor = conn.cursor()
        
        # Create lookup dictionaries by date
        balance_by_date = {item.get('date', ''): item for item in balance_data}
        cashflow_by_date = {item.get('date', ''): item for item in cashflow_data}
        
        for income_item in income_data:
            if 'date' not in income_item:
                continue
                
            record_date_str = income_item['date']
            try:
                record_date = datetime.strptime(record_date_str, '%Y-%m-%d').date()
            except:
                continue
            
            # Get corresponding data from other statements
            balance_item = balance_by_date.get(record_date_str, {})
            cashflow_item = cashflow_by_date.get(record_date_str, {})
            
            # Create raw data object
            raw_data = {
                'income': income_item,
                'balance': balance_item,
                'cashflow': cashflow_item
            }
            
            try:
                cursor.execute("""
                    INSERT INTO dev_fundamentals_comprehensive 
                    (symbol, date, vendor, fiscal_period, revenue, gross_profit, 
                     operating_income, net_income, ebitda, eps, total_assets, 
                     total_liabilities, shareholders_equity, current_assets, 
                     current_liabilities, total_debt, cash_and_equivalents,
                     operating_cash_flow, investing_cash_flow, financing_cash_flow,
                     free_cash_flow, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        raw_data = EXCLUDED.raw_data,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    symbol, record_date, 'fmp', income_item.get('period', 'FY'),
                    income_item.get('revenue'), income_item.get('grossProfit'),
                    income_item.get('operatingIncome'), income_item.get('netIncome'),
                    income_item.get('ebitda'), income_item.get('eps'),
                    balance_item.get('totalAssets'), balance_item.get('totalLiabilities'),
                    balance_item.get('totalStockholdersEquity'), balance_item.get('totalCurrentAssets'),
                    balance_item.get('totalCurrentLiabilities'), balance_item.get('totalDebt'),
                    balance_item.get('cashAndCashEquivalents'),
                    cashflow_item.get('operatingCashFlow'), 
                    cashflow_item.get('netCashUsedProvidedByInvestingActivities'),
                    cashflow_item.get('netCashUsedProvidedByFinancingActivities'), 
                    cashflow_item.get('freeCashFlow'),
                    json.dumps(raw_data)
                ))
                records_inserted += 1
                
            except Exception as e:
                log_info(f"⚠️ Error inserting record for {symbol} {record_date}: {e}")
                continue
        
        conn.commit()
        return records_inserted
        
    finally:
        conn.close()

def process_symbol_batch(symbols: List[str], start_index: int) -> Dict:
    """Process a batch of symbols."""
    batch_stats = {
        'processed': 0,
        'successful': 0,
        'total_records': 0,
        'errors': 0
    }
    
    for i, symbol in enumerate(symbols):
        global_index = start_index + i
        log_info(f"📈 [{global_index + 1}] Processing {symbol}...")
        
        try:
            # Fetch financial statements
            income_data = fetch_fmp_financial_statements(symbol, "income-statement")
            balance_data = fetch_fmp_financial_statements(symbol, "balance-sheet-statement") 
            cashflow_data = fetch_fmp_financial_statements(symbol, "cash-flow-statement")
            
            # Insert into database
            records_inserted = insert_fundamental_data(symbol, income_data, balance_data, cashflow_data)
            
            if records_inserted > 0:
                batch_stats['successful'] += 1
                batch_stats['total_records'] += records_inserted
                log_info(f"✅ {symbol}: {records_inserted} fundamental records inserted")
            else:
                log_info(f"⚠️ {symbol}: No fundamental data found")
            
            batch_stats['processed'] += 1
            
            # Rate limiting - FMP allows 250 requests/minute
            time.sleep(0.3)  # ~3 requests per second = 180/minute (safe buffer)
            
        except Exception as e:
            log_info(f"❌ Failed to process {symbol}: {e}")
            batch_stats['errors'] += 1
            continue
    
    return batch_stats

def main():
    """Main backfill execution."""
    log_info("🚀 Starting comprehensive fundamentals backfill")
    log_info("📋 Processing up to 70,000+ instruments with 30-year fundamental data")
    log_info(f"🔑 Using FMP API key: {FMP_API_KEY[:10]}...")
    
    # Initialize checkpoint system
    last_symbol, symbols_processed, records_inserted = initialize_checkpoint_system()
    
    # Get all symbols
    all_symbols = get_all_symbols()
    if not all_symbols:
        log_info("❌ No symbols found to process")
        return False
    
    # Determine start position
    start_index = symbols_processed
    symbols_to_process = all_symbols[start_index:start_index + MAX_SYMBOLS]
    
    log_info(f"📊 Processing {len(symbols_to_process)} symbols starting from index {start_index}")
    log_info(f"📋 Total universe: {len(all_symbols)} symbols")
    log_info(f"🔄 Previous progress: {symbols_processed} symbols, {records_inserted} records")
    
    # Process in batches
    total_stats = {
        'processed': 0,
        'successful': 0, 
        'total_records': 0,
        'errors': 0
    }
    
    for batch_start in range(0, len(symbols_to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(symbols_to_process))
        batch_symbols = symbols_to_process[batch_start:batch_end]
        
        log_info(f"🔄 Processing batch {batch_start//BATCH_SIZE + 1}: symbols {batch_start + start_index + 1} to {batch_end + start_index}")
        
        # Process batch
        batch_stats = process_symbol_batch(batch_symbols, start_index + batch_start)
        
        # Update totals
        for key in total_stats:
            total_stats[key] += batch_stats[key]
        
        # Save checkpoint after each batch
        current_symbols_processed = symbols_processed + total_stats['processed']
        current_records_inserted = records_inserted + total_stats['total_records']
        last_processed_symbol = batch_symbols[-1] if batch_symbols else last_symbol
        
        save_checkpoint(last_processed_symbol, current_symbols_processed, current_records_inserted)
        
        log_info(f"✅ Batch completed: {batch_stats['successful']}/{len(batch_symbols)} successful, {batch_stats['total_records']} records")
        log_info(f"📊 Total progress: {current_symbols_processed}/{len(all_symbols)} symbols ({current_symbols_processed/len(all_symbols)*100:.1f}%)")
    
    # Final summary
    log_info("🎉 Fundamentals backfill session completed!")
    log_info("=" * 80)
    log_info("📊 SESSION RESULTS:")
    log_info(f"   ✅ Symbols processed: {total_stats['processed']}")
    log_info(f"   🎯 Successful: {total_stats['successful']}")  
    log_info(f"   📈 Fundamental records inserted: {total_stats['total_records']}")
    log_info(f"   ❌ Errors: {total_stats['errors']}")
    
    # Overall progress
    final_symbols_processed = symbols_processed + total_stats['processed']
    final_records_inserted = records_inserted + total_stats['total_records']
    
    log_info("")
    log_info("📊 OVERALL PROGRESS:")
    log_info(f"   📈 Total symbols processed: {final_symbols_processed}/{len(all_symbols)} ({final_symbols_processed/len(all_symbols)*100:.1f}%)")
    log_info(f"   🗂️ Total fundamental records: {final_records_inserted:,}")
    
    if final_symbols_processed < len(all_symbols):
        log_info("")
        log_info("🔄 TO CONTINUE BACKFILL:")
        log_info("   python3 scripts/run_dev.py run --script scripts/start_fundamentals_backfill.py")
        log_info(f"   Will resume from symbol: {last_processed_symbol}")
    else:
        log_info("")
        log_info("🏁 BACKFILL COMPLETE! All symbols processed.")
    
    return total_stats['successful'] > 0

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)