#!/usr/bin/env python3
"""
Quick FirstRate Minute Bar Validation

Provides fast validation metrics for FirstRate data:
- Missing instrument detection
- Missing minute bars for recent trading days
- Sample-based analysis for performance
"""

import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
import glob
from collections import defaultdict
import random

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def get_recent_trading_days(num_days: int = 10) -> list:
    """Get list of recent trading days (Mon-Fri)"""
    trading_days = []
    current_date = date.today()

    days_checked = 0
    while len(trading_days) < num_days and days_checked < 30:
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
        days_checked += 1

    return sorted(trading_days, reverse=True)

def validate_firstrate_quick():
    """Quick FirstRate validation"""
    firstrate_path = "/mnt/d/ats-data/minute-bars/firstrate"

    if not os.path.exists(firstrate_path):
        print(f"❌ FirstRate path not found: {firstrate_path}")
        return

    print("🚀 Starting Quick FirstRate Validation")

    # Get trading days
    trading_days = get_recent_trading_days(10)
    print(f"📅 Recent trading days: {trading_days[:5]}")

    # Sample major symbols to check
    major_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'SPY', 'QQQ']

    # Quick directory scan
    letters = []
    for item in os.listdir(firstrate_path):
        path = os.path.join(firstrate_path, item)
        if os.path.isdir(path) and len(item) == 1 and item.isalpha():
            letters.append(item)

    print(f"📁 Found {len(letters)} letter directories: {sorted(letters)}")

    # Count symbols per letter (sample)
    total_symbols = 0
    symbols_found = set()
    files_per_symbol = defaultdict(int)

    for letter in sorted(letters[:5]):  # Sample first 5 letters
        letter_path = os.path.join(firstrate_path, letter)
        if os.path.exists(letter_path):
            symbols = [d for d in os.listdir(letter_path)
                      if os.path.isdir(os.path.join(letter_path, d))]
            total_symbols += len(symbols)
            symbols_found.update(symbols)

            # Sample 5 symbols per letter for file count
            sample_symbols = random.sample(symbols, min(5, len(symbols)))
            for symbol in sample_symbols:
                symbol_path = os.path.join(letter_path, symbol)
                # Count parquet files
                pattern = f"{symbol_path}/*/*/*.parquet"
                files = glob.glob(pattern)
                files_per_symbol[symbol] = len(files)

    print(f"📊 Sample Analysis (first 5 letters):")
    print(f"   • Total symbols sampled: {total_symbols}")
    print(f"   • Unique symbols found: {len(symbols_found)}")

    # Check major symbols
    missing_major = []
    found_major = []
    for symbol in major_symbols:
        symbol_letter = symbol[0]
        symbol_path = os.path.join(firstrate_path, symbol_letter, symbol)
        if os.path.exists(symbol_path):
            found_major.append(symbol)
            # Quick file count for this symbol
            pattern = f"{symbol_path}/*/*/*.parquet"
            files = glob.glob(pattern)
            files_per_symbol[symbol] = len(files)
        else:
            missing_major.append(symbol)

    print(f"📈 Major Symbols Analysis:")
    print(f"   • Found: {found_major}")
    print(f"   • Missing: {missing_major}")

    # File count analysis
    if files_per_symbol:
        avg_files = sum(files_per_symbol.values()) / len(files_per_symbol)
        max_files = max(files_per_symbol.values())
        min_files = min(files_per_symbol.values())

        print(f"📁 File Count Analysis:")
        print(f"   • Average files per symbol: {avg_files:.1f}")
        print(f"   • Max files per symbol: {max_files}")
        print(f"   • Min files per symbol: {min_files}")

        for symbol, count in list(files_per_symbol.items())[:10]:
            print(f"   • {symbol}: {count} files")

    # Recent data check for sample symbols
    recent_data_symbols = 0
    t0_date = trading_days[0]
    t1_date = trading_days[1] if len(trading_days) > 1 else None

    print(f"📅 Recent Data Analysis (T-0: {t0_date}, T-1: {t1_date}):")

    for symbol in found_major[:3]:  # Check first 3 major symbols
        symbol_letter = symbol[0]
        # Check for recent month files
        recent_files = []
        for month in ['08', '09']:
            file_path = os.path.join(firstrate_path, symbol_letter, symbol, f'2025/{month}', f'{symbol}_2025_{month}.parquet')
            if os.path.exists(file_path):
                recent_files.append(file_path)
                recent_data_symbols += 1

        print(f"   • {symbol}: {len(recent_files)} recent files")

    # Estimate total scale
    estimated_total_symbols = total_symbols * (26 / len(letters[:5]))  # Extrapolate from sample
    estimated_total_files = len(files_per_symbol) * avg_files * (estimated_total_symbols / len(files_per_symbol))

    print(f"📊 Scale Estimates:")
    print(f"   • Estimated total symbols: {estimated_total_symbols:,.0f}")
    print(f"   • Estimated total files: {estimated_total_files:,.0f}")

    # Summary metrics
    missing_major_count = len(missing_major)
    missing_major_pct = (missing_major_count / len(major_symbols)) * 100

    print(f"\n📋 FIRSTRATE VALIDATION SUMMARY:")
    print(f"   • Missing Major Instruments: {missing_major_count}/{len(major_symbols)} ({missing_major_pct:.1f}%)")
    print(f"   • Sample Symbols with Recent Data: {recent_data_symbols}")
    print(f"   • Estimated Total Scale: {estimated_total_symbols:,.0f} symbols, {estimated_total_files:,.0f} files")

    if missing_major:
        print(f"   • Missing instruments: {', '.join(missing_major)}")

    return {
        'missing_major_instruments': missing_major_count,
        'total_major_instruments': len(major_symbols),
        'missing_percentage': missing_major_pct,
        'recent_data_symbols': recent_data_symbols,
        'estimated_total_symbols': estimated_total_symbols,
        'estimated_total_files': estimated_total_files,
        'found_major_symbols': found_major,
        'missing_major_symbols': missing_major
    }

if __name__ == "__main__":
    validate_firstrate_quick()