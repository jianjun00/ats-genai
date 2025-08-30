#!/usr/bin/env python3
"""
Continue Fundamentals Backfill
Simple continuation script that processes the next batch of instruments.
"""

import requests
import time
import subprocess
from datetime import datetime

# Configuration
FMP_API_KEY = "Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"
BATCH_SIZE = 20

# Next batch of quality symbols to process
NEXT_BATCH = [
    'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'CRM', 'ORCL', 'ADBE',
    'PYPL', 'SHOP', 'SQ', 'ROKU', 'ZOOM', 'DOCU', 'JPM', 'BAC', 'WFC', 'C',
    'V', 'MA', 'KO', 'PEP', 'WMT', 'HD', 'NKE', 'SBUX', 'DIS', 'UNH',
    'PFE', 'ABBV', 'MRK', 'XOM', 'CVX', 'T', 'VZ', 'IBM', 'GE', 'BA',
    'CAT', 'MMM', 'HON', 'LMT', 'RTX', 'COST', 'AVGO', 'CSCO', 'QCOM', 'TXN'
]

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

def fetch_fmp_data(symbol: str) -> dict:
    """Fetch comprehensive fundamental data for a symbol from FMP."""
    log_info(f"🔍 Fetching {symbol} data...")
    
    data = {
        'income': [],
        'balance': [],
        'cashflow': []
    }
    
    # Fetch income statement
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}"
        params = {"limit": 20, "apikey": FMP_API_KEY}
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data['income'] = response.json()
            log_info(f"📊 {symbol}: {len(data['income'])} income periods")
        time.sleep(0.2)
    except Exception as e:
        log_info(f"⚠️ {symbol} income error: {e}")
    
    # Fetch balance sheet
    try:
        url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}"
        params = {"limit": 20, "apikey": FMP_API_KEY}
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data['balance'] = response.json()
            log_info(f"💰 {symbol}: {len(data['balance'])} balance periods")
        time.sleep(0.2)
    except Exception as e:
        log_info(f"⚠️ {symbol} balance error: {e}")
    
    # Fetch cash flow
    try:
        url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}"
        params = {"limit": 20, "apikey": FMP_API_KEY}
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data['cashflow'] = response.json()
            log_info(f"💸 {symbol}: {len(data['cashflow'])} cashflow periods")
        time.sleep(0.2)
    except Exception as e:
        log_info(f"⚠️ {symbol} cashflow error: {e}")
    
    return data

def insert_symbol_data(symbol: str, data: dict) -> int:
    """Insert fundamental data for a symbol."""
    records_inserted = 0
    
    # Process income statement data
    for item in data['income']:
        if not item.get('date'):
            continue
            
        try:
            query = f"""
            INSERT INTO dev_fundamentals_comprehensive 
            (symbol, date, vendor, fiscal_period, revenue, gross_profit, operating_income, 
             net_income, ebitda, eps)
            VALUES (
                '{symbol}', 
                '{item['date']}', 
                'fmp', 
                '{item.get('period', 'FY')}',
                {item.get('revenue') or 'NULL'},
                {item.get('grossProfit') or 'NULL'},
                {item.get('operatingIncome') or 'NULL'},
                {item.get('netIncome') or 'NULL'},
                {item.get('ebitda') or 'NULL'},
                {item.get('eps') or 'NULL'}
            )
            ON CONFLICT (symbol, date, vendor, fiscal_period) 
            DO UPDATE SET
                revenue = EXCLUDED.revenue,
                gross_profit = EXCLUDED.gross_profit,
                operating_income = EXCLUDED.operating_income,
                net_income = EXCLUDED.net_income,
                ebitda = EXCLUDED.ebitda,
                eps = EXCLUDED.eps,
                updated_at = CURRENT_TIMESTAMP
            """
            result = run_db_query(query)
            if 'INSERT' in result or 'UPDATE' in result:
                records_inserted += 1
        except Exception as e:
            log_info(f"⚠️ Error inserting {symbol} income data: {e}")
    
    # Process balance sheet data
    balance_by_date = {item.get('date'): item for item in data['balance']}
    for date_key, item in balance_by_date.items():
        if not date_key:
            continue
            
        try:
            query = f"""
            INSERT INTO dev_fundamentals_comprehensive 
            (symbol, date, vendor, fiscal_period, total_assets, total_liabilities, 
             shareholders_equity, current_assets, current_liabilities, total_debt, cash_and_equivalents)
            VALUES (
                '{symbol}', 
                '{date_key}', 
                'fmp', 
                'FY',
                {item.get('totalAssets') or 'NULL'},
                {item.get('totalLiabilities') or 'NULL'},
                {item.get('totalStockholdersEquity') or 'NULL'},
                {item.get('totalCurrentAssets') or 'NULL'},
                {item.get('totalCurrentLiabilities') or 'NULL'},
                {item.get('totalDebt') or 'NULL'},
                {item.get('cashAndCashEquivalents') or 'NULL'}
            )
            ON CONFLICT (symbol, date, vendor, fiscal_period) 
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_liabilities = EXCLUDED.total_liabilities,
                shareholders_equity = EXCLUDED.shareholders_equity,
                current_assets = EXCLUDED.current_assets,
                current_liabilities = EXCLUDED.current_liabilities,
                total_debt = EXCLUDED.total_debt,
                cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                updated_at = CURRENT_TIMESTAMP
            """
            result = run_db_query(query)
        except Exception as e:
            log_info(f"⚠️ Error inserting {symbol} balance data: {e}")
    
    # Process cash flow data  
    cashflow_by_date = {item.get('date'): item for item in data['cashflow']}
    for date_key, item in cashflow_by_date.items():
        if not date_key:
            continue
            
        try:
            query = f"""
            INSERT INTO dev_fundamentals_comprehensive 
            (symbol, date, vendor, fiscal_period, operating_cash_flow, investing_cash_flow, 
             financing_cash_flow, free_cash_flow)
            VALUES (
                '{symbol}', 
                '{date_key}', 
                'fmp', 
                'FY',
                {item.get('operatingCashFlow') or 'NULL'},
                {item.get('netCashUsedProvidedByInvestingActivities') or 'NULL'},
                {item.get('netCashUsedProvidedByFinancingActivities') or 'NULL'},
                {item.get('freeCashFlow') or 'NULL'}
            )
            ON CONFLICT (symbol, date, vendor, fiscal_period) 
            DO UPDATE SET
                operating_cash_flow = EXCLUDED.operating_cash_flow,
                investing_cash_flow = EXCLUDED.investing_cash_flow,
                financing_cash_flow = EXCLUDED.financing_cash_flow,
                free_cash_flow = EXCLUDED.free_cash_flow,
                updated_at = CURRENT_TIMESTAMP
            """
            result = run_db_query(query)
        except Exception as e:
            log_info(f"⚠️ Error inserting {symbol} cashflow data: {e}")
    
    return records_inserted

def main():
    """Main processing function."""
    log_info("🚀 Continuing fundamentals backfill")
    log_info(f"📊 Processing {len(NEXT_BATCH)} additional symbols")
    
    # Check current status
    result = run_db_query("SELECT COUNT(*) as total, COUNT(DISTINCT symbol) as symbols FROM dev_fundamentals_comprehensive WHERE vendor = 'fmp'")
    log_info(f"📋 Current database status: {result}")
    
    total_processed = 0
    total_records = 0
    successful = 0
    
    # Process each symbol
    for i, symbol in enumerate(NEXT_BATCH, 1):
        log_info(f"📈 [{i}/{len(NEXT_BATCH)}] Processing {symbol}...")
        
        try:
            # Fetch data
            data = fetch_fmp_data(symbol)
            
            # Check if we got any data
            total_periods = len(data['income']) + len(data['balance']) + len(data['cashflow'])
            if total_periods == 0:
                log_info(f"⚠️ {symbol}: No data found")
                continue
            
            # Insert data
            records = insert_symbol_data(symbol, data)
            
            if records > 0:
                successful += 1
                total_records += records
                log_info(f"✅ {symbol}: {records} records inserted")
            else:
                log_info(f"⚠️ {symbol}: No records inserted")
            
            total_processed += 1
            
            # Update checkpoint
            try:
                checkpoint_query = f"""
                UPDATE dev_fundamentals_checkpoint 
                SET last_symbol = '{symbol}', 
                    symbols_processed = symbols_processed + 1,
                    records_inserted = records_inserted + {records},
                    last_update = CURRENT_TIMESTAMP
                WHERE vendor = 'fmp' AND job_type = 'fundamentals'
                """
                run_db_query(checkpoint_query)
            except Exception as e:
                log_info(f"⚠️ Checkpoint update error: {e}")
            
            # Rate limiting
            time.sleep(1.0)  # 1 second between symbols for stability
            
        except Exception as e:
            log_info(f"❌ Failed to process {symbol}: {e}")
            continue
    
    # Final summary
    log_info("🎉 Batch processing completed!")
    log_info("=" * 60)
    log_info(f"✅ Symbols processed: {total_processed}")
    log_info(f"🎯 Successful: {successful}")
    log_info(f"📈 Records added: {total_records}")
    
    # Check final database status
    result = run_db_query("SELECT COUNT(*) as total, COUNT(DISTINCT symbol) as symbols FROM dev_fundamentals_comprehensive WHERE vendor = 'fmp'")
    log_info(f"🗄️ Final database status: {result}")
    
    log_info("=" * 60)
    log_info("🚀 Fundamentals backfill continued successfully!")
    
    return successful > 0

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)