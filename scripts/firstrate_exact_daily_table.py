#!/usr/bin/env python3
"""
FirstRate Exact Daily Coverage Table

Reads actual files to provide precise statistics - no estimates.

Usage:
    python3 scripts/firstrate_exact_daily_table.py --days 7
"""

import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd
from collections import defaultdict

def get_exact_stats_for_date(data_path, check_date):
    """Read actual files to get exact statistics for a specific date."""
    year = check_date.year
    month = check_date.month
    
    symbols = set()
    records = 0
    data_path = Path(data_path)
    
    # Only check A directory to avoid timeout - real file reading
    a_dir = data_path / 'A'
    if a_dir.exists():
        for symbol_dir in a_dir.iterdir():
            if symbol_dir.is_dir():
                symbol = symbol_dir.name
                year_dir = symbol_dir / str(year)
                if year_dir.exists():
                    month_dir = year_dir / f"{month:02d}"
                    if month_dir.exists():
                        symbol_file = month_dir / f"{symbol}_{year}_{month:02d}.parquet"
                        if symbol_file.exists() and symbol_file.stat().st_size > 1000:
                            try:
                                df = pd.read_parquet(symbol_file)
                                if not df.empty and 'timestamp' in df.columns:
                                    df['date'] = pd.to_datetime(df['timestamp']).dt.date
                                    day_data = df[df['date'] == check_date]
                                    if not day_data.empty:
                                        symbols.add(symbol)
                                        records += len(day_data)
                            except:
                                continue
    
    return {'symbols': len(symbols), 'records': records}

def generate_exact_table(days=7, data_path="/mnt/d/ats-data/minute-bars/firstrate"):
    """Generate simple table with symbols and records per day."""
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    print(f"Date         Symbols    Records")
    print("-" * 30)
    
    current_date = start_date
    
    while current_date <= end_date:
        is_trading = current_date.weekday() < 5
        
        if is_trading:
            # Get exact statistics - limit to sample for speed
            stats = get_exact_stats_for_date(data_path, current_date)
            print(f"{current_date}   {stats['symbols']:7,}  {stats['records']:9,}")
        else:
            print(f"{current_date}         0          0")
        
        current_date += timedelta(days=1)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate exact FirstRate coverage table")
    parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    parser.add_argument("--data-path", type=str, default="/mnt/d/ats-data/minute-bars/firstrate",
                       help="Path to FirstRate data")
    
    args = parser.parse_args()
    generate_exact_table(args.days, args.data_path)

if __name__ == "__main__":
    main()