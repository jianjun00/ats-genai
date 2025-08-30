#!/usr/bin/env python3

import requests
import time
import subprocess
from datetime import datetime

# Use Polygon for deployment since FMP is rate limited
POLYGON_API_KEY = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"

symbols = ["A", "AA", "AAA", "AAAA", "AAAB", "AAAG", "AAAU", "AABA", "AABB", "AABC", "AAC", "AACB", "AACC", "AACE", "AACG", "AACH", "AACI", "AACS", "AACT", "AADR", "AADV", "AAGC", "AAGH", "AAGR", "AAL", "AAM", "AAMC", "AAME", "AAMI", "AAN", "AANB", "AANC", "AANR", "AAOI", "AAON", "AAP", "AAPB", "AAPC", "AAPD", "AAPG", "AAPH", "AAPI", "AAPJ", "AAPL", "AAPR", "AAPT", "AAPU", "AAPW", "AAPX", "AAPY", "AAQC", "AAQL", "AAR", "AASL", "AASP", "AAST", "AAT", "AATC", "AATI", "AATT", "AATV", "AAU", "AAUC", "AAUS", "AAV", "AAVL", "AAVM", "AAWH", "AAWW", "AAXJ", "AAXT"]

def log(msg):
    print(f"{datetime.now():%H:%M:%S} {msg}")

def query(sql):
    result = subprocess.run(['python3', 'scripts/run_dev.py', 'query', '--query', sql], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-data')
    return result.returncode == 0

def get_polygon_financials(symbol):
    url = f"https://api.polygon.io/vX/reference/financials"
    params = {"ticker": symbol, "apiKey": POLYGON_API_KEY, "limit": 10}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        return []
    except:
        return []

def insert_polygon_data(symbol, financials):
    inserted = 0
    for item in financials:
        try:
            if not item.get("end_date"):
                continue
                
            financials_data = item.get("financials", {})
            income = financials_data.get("income_statement", {})
            balance = financials_data.get("balance_sheet", {})
            cash_flow = financials_data.get("cash_flow_statement", {})
            
            revenue = income.get("revenues", {}).get("value")
            net_income = income.get("net_income_loss", {}).get("value") 
            total_assets = balance.get("assets", {}).get("value")
            operating_cf = cash_flow.get("net_cash_flow_from_operating_activities", {}).get("value")
            
            if any([revenue, net_income, total_assets, operating_cf]):
                sql = f"""
                INSERT INTO dev_fundamentals_comprehensive 
                (symbol, date, vendor, fiscal_period, revenue, net_income, total_assets, operating_cash_flow)
                VALUES ('{symbol}', '{item["end_date"]}', 'polygon', 'FY', 
                        {revenue or 'NULL'}, {net_income or 'NULL'}, 
                        {total_assets or 'NULL'}, {operating_cf or 'NULL'})
                ON CONFLICT (symbol, date, vendor, fiscal_period) DO UPDATE SET
                    revenue = EXCLUDED.revenue,
                    net_income = EXCLUDED.net_income,
                    total_assets = EXCLUDED.total_assets,
                    operating_cash_flow = EXCLUDED.operating_cash_flow,
                    updated_at = CURRENT_TIMESTAMP
                """
                if query(sql):
                    inserted += 1
        except Exception as e:
            log(f"Error inserting {symbol}: {e}")
            continue
    return inserted

processed = 0
total_records = 0

log(f"Processing {len(symbols)} symbols with Polygon API")

for i, symbol in enumerate(symbols, 1):
    log(f"[{i}/{len(symbols)}] {symbol}")
    
    financials = get_polygon_financials(symbol)
    if financials:
        records = insert_polygon_data(symbol, financials)
        if records > 0:
            total_records += records
            processed += 1
            log(f"✓ {symbol}: {records} records")
        else:
            log(f"- {symbol}: no data")
    else:
        log(f"- {symbol}: API failed")
    
    # Update checkpoint
    query(f"""
    UPDATE dev_fundamentals_checkpoint 
    SET last_symbol = '{symbol}', symbols_processed = symbols_processed + 1,
        records_inserted = records_inserted + {records if financials else 0}
    WHERE vendor = 'fmp' AND job_type = 'fundamentals'
    """)
    
    time.sleep(0.2)  # Rate limiting

log(f"Complete: {processed}/{len(symbols)} symbols, {total_records} records")