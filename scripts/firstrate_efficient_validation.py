#!/usr/bin/env python3
"""
Efficient FirstRate Validation

Fast validation using directory scanning and statistical sampling:
1. Complete instrument universe check (directory-based)
2. Sample-based trading day analysis for performance
3. Missing data identification
"""

import os
import sys
import asyncio
import asyncpg
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd
from collections import defaultdict
import random

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def get_recent_trading_days(num_days: int = 90) -> list:
    """Get recent trading days"""
    trading_days = []
    current_date = date.today()
    
    days_checked = 0
    while len(trading_days) < num_days and days_checked < 150:
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
        days_checked += 1
    
    return sorted(trading_days, reverse=True)

async def get_database_universe():
    """Get expected instruments from database"""
    try:
        conn = await asyncpg.connect("postgresql://postgres:dev_password@localhost:3432/dev_db")
        
        query = """
        SELECT DISTINCT symbol 
        FROM dev_instruments 
        WHERE active = true 
          AND symbol IS NOT NULL 
          AND symbol != ''
          AND symbol ~ '^[A-Z]{1,5}$'
        ORDER BY symbol
        """
        
        result = await conn.fetch(query)
        expected = {row['symbol'] for row in result}
        await conn.close()
        
        print(f"📊 Database universe: {len(expected):,} expected instruments")
        return expected
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return set()

def scan_firstrate_instruments():
    """Fast directory scan to get all FirstRate instruments"""
    firstrate_path = "/mnt/d/ats-data/minute-bars/firstrate"
    instruments = set()
    instruments_by_letter = defaultdict(int)
    
    if not os.path.exists(firstrate_path):
        print(f"❌ FirstRate path not found: {firstrate_path}")
        return instruments, instruments_by_letter
    
    print(f"📁 Scanning FirstRate directory structure...")
    
    for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        letter_path = os.path.join(firstrate_path, letter)
        if os.path.exists(letter_path):
            symbols = [d for d in os.listdir(letter_path) 
                      if os.path.isdir(os.path.join(letter_path, d)) and d.isalpha() and len(d) <= 5]
            instruments.update(symbols)
            instruments_by_letter[letter] = len(symbols)
            
    print(f"📊 FirstRate instruments found: {len(instruments):,}")
    return instruments, dict(instruments_by_letter)

def analyze_missing_instruments(expected, existing):
    """Analyze missing vs existing instruments"""
    missing = expected - existing
    unexpected = existing - expected
    found = expected & existing
    
    return {
        'expected_count': len(expected),
        'existing_count': len(existing),
        'found_count': len(found),
        'missing_count': len(missing),
        'unexpected_count': len(unexpected),
        'missing_symbols': sorted(list(missing)),
        'coverage_pct': (len(found) / len(expected) * 100) if expected else 0
    }

def sample_trading_day_analysis(instruments, trading_days, sample_size=1000):
    """Sample-based trading day analysis for performance"""
    print(f"📊 Sampling {sample_size} instruments for trading day analysis...")
    
    # Stratified sampling across letters
    sample_instruments = []
    instruments_list = list(instruments)
    
    if len(instruments_list) <= sample_size:
        sample_instruments = instruments_list
    else:
        # Random sample
        sample_instruments = random.sample(instruments_list, sample_size)
    
    firstrate_path = "/mnt/d/ats-data/minute-bars/firstrate"
    recent_months = [('2025', '09'), ('2025', '08'), ('2025', '07')]
    
    coverage_data = {
        'total_sampled': len(sample_instruments),
        'with_recent_data': 0,
        'coverage_by_day': defaultdict(int),
        'symbols_with_excellent_coverage': [],
        'symbols_with_no_recent_data': [],
        'file_counts': defaultdict(int)
    }
    
    processed = 0
    for symbol in sample_instruments:
        letter = symbol[0]
        symbol_path = os.path.join(firstrate_path, letter, symbol)
        
        if not os.path.exists(symbol_path):
            coverage_data['symbols_with_no_recent_data'].append(symbol)
            continue
        
        symbol_dates = set()
        file_count = 0
        
        # Check recent months
        for year, month in recent_months:
            file_path = os.path.join(symbol_path, year, month, f'{symbol}_{year}_{month}.parquet')
            if os.path.exists(file_path):
                file_count += 1
                try:
                    df = pd.read_parquet(file_path)
                    if not df.empty:
                        df['date'] = pd.to_datetime(df['timestamp']).dt.date
                        file_dates = set(df['date'].unique())
                        symbol_dates.update(file_dates)
                except Exception:
                    continue
        
        coverage_data['file_counts'][symbol] = file_count
        
        if symbol_dates:
            coverage_data['with_recent_data'] += 1
            
            # Check coverage for recent trading days
            recent_10_days = trading_days[:10]
            coverage_count = sum(1 for day in recent_10_days if day in symbol_dates)
            coverage_pct = coverage_count / len(recent_10_days) * 100
            
            if coverage_pct >= 80:  # Excellent coverage
                coverage_data['symbols_with_excellent_coverage'].append(symbol)
            
            # Count coverage by specific days
            for day in recent_10_days:
                if day in symbol_dates:
                    coverage_data['coverage_by_day'][day] += 1
        else:
            coverage_data['symbols_with_no_recent_data'].append(symbol)
        
        processed += 1
        if processed % 100 == 0:
            print(f"   Processed {processed}/{len(sample_instruments)} samples...")
    
    return coverage_data

def estimate_full_scale(sample_data, total_instruments):
    """Extrapolate sample results to full scale"""
    sample_size = sample_data['total_sampled']
    if sample_size == 0:
        return {}
    
    scale_factor = total_instruments / sample_size
    
    return {
        'estimated_with_recent_data': int(sample_data['with_recent_data'] * scale_factor),
        'estimated_excellent_coverage': int(len(sample_data['symbols_with_excellent_coverage']) * scale_factor),
        'estimated_no_data': int(len(sample_data['symbols_with_no_recent_data']) * scale_factor),
        'avg_files_per_symbol': sum(sample_data['file_counts'].values()) / len(sample_data['file_counts']) if sample_data['file_counts'] else 0
    }

async def main():
    print("🚀 FirstRate Efficient Validation - ALL Instruments Analysis")
    
    # 1. Get universe comparison
    print("\n1️⃣ INSTRUMENT UNIVERSE ANALYSIS")
    expected_instruments = await get_database_universe()
    existing_instruments, instruments_by_letter = scan_firstrate_instruments()
    
    missing_analysis = analyze_missing_instruments(expected_instruments, existing_instruments)
    
    print(f"📊 Expected instruments: {missing_analysis['expected_count']:,}")
    print(f"📁 FirstRate instruments: {missing_analysis['existing_count']:,}")
    print(f"✅ Found instruments: {missing_analysis['found_count']:,}")
    print(f"❌ Missing instruments: {missing_analysis['missing_count']:,}")
    print(f"➕ Unexpected instruments: {missing_analysis['unexpected_count']:,}")
    print(f"📈 Coverage: {missing_analysis['coverage_pct']:.1f}%")
    
    # Show distribution by letter
    print(f"\n🔤 Instruments by letter (top 10):")
    sorted_letters = sorted(instruments_by_letter.items(), key=lambda x: x[1], reverse=True)
    for letter, count in sorted_letters[:10]:
        print(f"   {letter}: {count:,} instruments")
    
    # 2. Trading day analysis
    print(f"\n2️⃣ TRADING DAY COVERAGE ANALYSIS")
    trading_days = get_recent_trading_days(90)
    print(f"📅 Analyzing {len(trading_days)} trading days: {trading_days[-1]} to {trading_days[0]}")
    
    # Sample analysis
    sample_data = sample_trading_day_analysis(existing_instruments, trading_days, sample_size=1000)
    full_estimates = estimate_full_scale(sample_data, len(existing_instruments))
    
    print(f"📊 Sample Results (1,000 instruments):")
    print(f"   • With recent data: {sample_data['with_recent_data']}/1,000 ({sample_data['with_recent_data']/10:.1f}%)")
    print(f"   • Excellent coverage: {len(sample_data['symbols_with_excellent_coverage'])}/1,000")
    print(f"   • No recent data: {len(sample_data['symbols_with_no_recent_data'])}/1,000")
    
    print(f"📊 Full Scale Estimates:")
    print(f"   • Est. instruments with recent data: {full_estimates['estimated_with_recent_data']:,}")
    print(f"   • Est. excellent coverage: {full_estimates['estimated_excellent_coverage']:,}")
    print(f"   • Est. missing recent data: {full_estimates['estimated_no_data']:,}")
    print(f"   • Avg files per symbol: {full_estimates['avg_files_per_symbol']:.1f}")
    
    # Recent trading day breakdown
    if sample_data['coverage_by_day']:
        print(f"\n📅 Recent Trading Day Coverage (sample of 1,000):")
        recent_days = sorted(sample_data['coverage_by_day'].keys(), reverse=True)[:5]
        for day in recent_days:
            count = sample_data['coverage_by_day'][day]
            pct = (count / sample_data['total_sampled']) * 100
            estimated_full = int(count * (len(existing_instruments) / sample_data['total_sampled']))
            print(f"   {day}: {count}/1,000 ({pct:.1f}%) → Est. {estimated_full:,} total")
    
    # 3. Missing data examples
    print(f"\n3️⃣ MISSING DATA ANALYSIS")
    
    if missing_analysis['missing_symbols']:
        print(f"🔍 Sample Missing Instruments (first 20):")
        print(f"   {', '.join(missing_analysis['missing_symbols'][:20])}")
        
        if len(missing_analysis['missing_symbols']) > 100:
            print(f"   ... and {len(missing_analysis['missing_symbols']) - 20:,} more")
    
    if sample_data['symbols_with_no_recent_data']:
        print(f"🔍 Sample Instruments with No Recent Data (first 20):")
        print(f"   {', '.join(sample_data['symbols_with_no_recent_data'][:20])}")
    
    # 4. SUMMARY
    print(f"\n📋 FIRSTRATE VALIDATION SUMMARY")
    print(f"=" * 50)
    
    # Missing instruments status
    missing_pct = (missing_analysis['missing_count'] / missing_analysis['expected_count'] * 100) if missing_analysis['expected_count'] else 0
    if missing_pct < 5:
        missing_status = "🟢 EXCELLENT"
    elif missing_pct < 15:
        missing_status = "🟡 GOOD" 
    elif missing_pct < 30:
        missing_status = "🟠 FAIR"
    else:
        missing_status = "🔴 POOR"
    
    # Recent data coverage
    recent_data_pct = (sample_data['with_recent_data'] / sample_data['total_sampled'] * 100) if sample_data['total_sampled'] else 0
    if recent_data_pct > 80:
        data_status = "🟢 EXCELLENT"
    elif recent_data_pct > 60:
        data_status = "🟡 GOOD"
    elif recent_data_pct > 40:
        data_status = "🟠 FAIR"
    else:
        data_status = "🔴 POOR"
    
    print(f"📊 MISSING INSTRUMENTS: {missing_status}")
    print(f"   {missing_analysis['missing_count']:,} missing out of {missing_analysis['expected_count']:,} ({missing_pct:.1f}%)")
    
    print(f"📅 RECENT DATA COVERAGE: {data_status}")
    print(f"   Est. {full_estimates['estimated_no_data']:,} instruments missing recent data")
    print(f"   Est. {full_estimates['estimated_excellent_coverage']:,} with excellent coverage")
    
    print(f"📁 TOTAL SCALE: {missing_analysis['existing_count']:,} instruments across 26 letter directories")

if __name__ == "__main__":
    asyncio.run(main())