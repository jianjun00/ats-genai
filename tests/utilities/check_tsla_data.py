#!/usr/bin/env python3
"""
Check TSLA data availability and listing information
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

print("🔍 Checking TSLA data availability...")

# Check in backup data locations (where we found AAPL)
data_paths = [
    "/data/backup/minute-files/comprehensive-sync-20250823_214935/eodhd",
    "/data/backup/minute-files/comprehensive-sync-20250823_214935/polygon",
    "/data/backup/minute-files/comprehensive-sync-20250823_214029/eodhd",
    "/data/minute-bars",
]

for data_path in data_paths:
    tsla_path = Path(data_path) / "TSLA"
    if tsla_path.exists():
        print(f"✅ Found TSLA data at: {tsla_path}")

        # Check what years are available
        years = []
        for year_dir in tsla_path.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                years.append(int(year_dir.name))

        if years:
            years.sort()
            print(f"  📅 Available years: {years[0]} - {years[-1]} ({len(years)} years)")

            # Check first year for file count
            first_year = min(years)
            first_year_path = tsla_path / str(first_year)

            total_files = 0
            for month_dir in first_year_path.iterdir():
                if month_dir.is_dir():
                    parquet_files = list(month_dir.glob("*.parquet"))
                    total_files += len(parquet_files)

            print(f"  📊 Sample year {first_year}: {total_files} monthly files")

            # Check a sample file to see date range
            sample_file = None
            for year_dir in sorted(tsla_path.iterdir()):
                if year_dir.is_dir() and year_dir.name.isdigit():
                    for month_dir in sorted(year_dir.iterdir()):
                        if month_dir.is_dir():
                            parquet_files = list(month_dir.glob("*.parquet"))
                            if parquet_files:
                                sample_file = parquet_files[0]
                                break
                    if sample_file:
                        break

            if sample_file:
                try:
                    import pandas as pd
                    df = pd.read_parquet(sample_file)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    earliest = df['timestamp'].min()
                    latest = df['timestamp'].max()
                    print(f"  📈 Sample data range: {earliest} to {latest}")
                    print(f"  📊 Sample records: {len(df)}")
                except Exception as e:
                    print(f"  ❌ Error reading sample file: {e}")
        else:
            print(f"  📅 No year directories found")
    else:
        print(f"❌ No TSLA data at: {data_path}")

# TSLA went public on June 29, 2010
print(f"\n📅 TSLA IPO Date: June 29, 2010")
print(f"📅 Expected data range: 2010-06-29 to {datetime.now().strftime('%Y-%m-%d')}")