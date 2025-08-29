#!/usr/bin/env python3

import requests
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor
import threading

POLYGON_API_KEY = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
processed_count = 0
total_records = 0
lock = threading.Lock()

def get_more_symbols():
    result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', 
        "SELECT symbol FROM dev_instrument_tiingo WHERE symbol ~ '^[A-Z]{1,4}$' AND symbol NOT IN (SELECT DISTINCT symbol FROM dev_fundamentals_comprehensive) ORDER BY symbol LIMIT 200"],
        capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')
    
    symbols = []
    for line in result.stdout.split('\n'):
        line = line.strip()
        if line and line != 'symbol' and '---' not in line and 'row' not in line and '(' not in line:
            symbols.append(line)
    return symbols[:100]

def query(sql):
    result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', sql], 
                          capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')
    return result.returncode == 0

def process_symbol(symbol):
    global processed_count, total_records
    
    try:
        url = f"https://api.polygon.io/vX/reference/financials"
        params = {"ticker": symbol, "apiKey": POLYGON_API_KEY, "limit": 8}
        
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            return False
            
        data = response.json()
        financials = data.get("results", [])
        if not financials:
            return False
        
        records_inserted = 0
        for item in financials:
            if not item.get("end_date"):
                continue
                
            fin_data = item.get("financials", {})
            income = fin_data.get("income_statement", {})
            balance = fin_data.get("balance_sheet", {})
            
            revenue = income.get("revenues", {}).get("value")
            net_income = income.get("net_income_loss", {}).get("value")
            assets = balance.get("assets", {}).get("value")
            
            if any([revenue, net_income, assets]):
                sql = f"""INSERT INTO dev_fundamentals_comprehensive 
                (symbol, date, vendor, revenue, net_income, total_assets)
                VALUES ('{symbol}', '{item["end_date"]}', 'polygon', {revenue or 'NULL'}, {net_income or 'NULL'}, {assets or 'NULL'})
                ON CONFLICT (symbol, date, vendor, fiscal_period) DO NOTHING"""
                
                if query(sql):
                    records_inserted += 1
        
        with lock:
            processed_count += 1
            total_records += records_inserted
            if processed_count % 10 == 0:
                print(f"Processed: {processed_count}, Records: {total_records}")
        
        return records_inserted > 0
        
    except Exception:
        return False

symbols = get_more_symbols()
print(f"Processing {len(symbols)} symbols")

with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(process_symbol, symbols))

successful = sum(1 for r in results if r)
print(f"Complete: {successful}/{len(symbols)} symbols, {total_records} records")

# Final count
result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', 
    "SELECT COUNT(*) as total, COUNT(DISTINCT symbol) as symbols FROM dev_fundamentals_comprehensive"],
    capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')
print("Database:", result.stdout.split('\n')[2].strip() if len(result.stdout.split('\n')) > 2 else "")