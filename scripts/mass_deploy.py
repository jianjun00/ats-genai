#!/usr/bin/env python3

import requests
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

POLYGON_API_KEY = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
stats = {"processed": 0, "records": 0}
lock = threading.Lock()

def get_batch_symbols(offset=0):
    sql = f"""SELECT symbol FROM dev_instrument_tiingo 
              WHERE symbol ~ '^[A-Z]{{1,4}}$' 
              AND symbol NOT IN (SELECT DISTINCT symbol FROM dev_fundamentals_comprehensive) 
              ORDER BY symbol LIMIT 500 OFFSET {offset}"""
    
    result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', sql], 
                          capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')
    
    symbols = []
    for line in result.stdout.split('\n'):
        line = line.strip()
        if line and line not in ['symbol', '--------'] and '(' not in line and 'row' not in line:
            symbols.append(line)
    return symbols

def insert_batch(symbol, financials):
    if not financials:
        return 0
        
    values = []
    for item in financials:
        if not item.get("end_date"):
            continue
            
        fin = item.get("financials", {})
        income = fin.get("income_statement", {})
        balance = fin.get("balance_sheet", {})
        
        rev = income.get("revenues", {}).get("value")
        ni = income.get("net_income_loss", {}).get("value") 
        assets = balance.get("assets", {}).get("value")
        
        if any([rev, ni, assets]):
            values.append(f"('{symbol}', '{item['end_date']}', 'polygon', 'FY', {rev or 'NULL'}, {ni or 'NULL'}, {assets or 'NULL'})")
    
    if not values:
        return 0
        
    sql = f"""INSERT INTO dev_fundamentals_comprehensive 
              (symbol, date, vendor, fiscal_period, revenue, net_income, total_assets)
              VALUES {','.join(values)}
              ON CONFLICT (symbol, date, vendor, fiscal_period) DO NOTHING"""
    
    result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', sql], 
                          capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')
    return len(values) if result.returncode == 0 else 0

def process_symbol(symbol):
    try:
        response = requests.get(f"https://api.polygon.io/vX/reference/financials", 
                               params={"ticker": symbol, "apiKey": POLYGON_API_KEY, "limit": 8}, 
                               timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            financials = data.get("results", [])
            records = insert_batch(symbol, financials)
            
            with lock:
                stats["processed"] += 1
                stats["records"] += records
                if stats["processed"] % 50 == 0:
                    print(f"Progress: {stats['processed']} symbols, {stats['records']} records")
            
            return records > 0
        return False
    except:
        return False

# Process multiple batches
total_symbols = 0
total_successful = 0

for batch_num in range(5):  # Process 5 batches
    symbols = get_batch_symbols(batch_num * 500)
    if not symbols:
        break
        
    print(f"Batch {batch_num + 1}: {len(symbols)} symbols")
    total_symbols += len(symbols)
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
        
        for future in as_completed(futures):
            if future.result():
                total_successful += 1
    
    time.sleep(2)  # Brief pause between batches

print(f"Deployment complete: {total_successful}/{total_symbols} symbols successful")
print(f"Total records: {stats['records']}")

# Final database count
result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', 
                        "SELECT COUNT(*) as records, COUNT(DISTINCT symbol) as symbols FROM dev_fundamentals_comprehensive"], 
                        capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')

for line in result.stdout.split('\n'):
    if '|' in line and 'records' not in line and '---' not in line:
        print(f"Database total: {line.strip()}")