#!/usr/bin/env python3
"""
Check AAPL minute bar data for gaps and missing months
"""

from pathlib import Path
from datetime import date

def main():
    print("🔍 Checking AAPL minute bar data completeness...")

    # AAPL data directory
    aapl_path = Path("/data/minute-bars/firstrate/AAPL")

    if not aapl_path.exists():
        print("❌ AAPL directory not found!")
        return

    # Get all existing parquet files
    existing_files = []
    for parquet_file in aapl_path.glob("**/*.parquet"):
        parts = parquet_file.stem.split("_")  # AAPL_2000_01.parquet
        if len(parts) == 3:
            year = int(parts[1])
            month = int(parts[2])
            existing_files.append((year, month))

    existing_files.sort()

    if not existing_files:
        print("❌ No AAPL parquet files found!")
        return

    print(f"📊 Found {len(existing_files)} AAPL parquet files")
    print(f"📅 Date range: {existing_files[0][0]}-{existing_files[0][1]:02d} to {existing_files[-1][0]}-{existing_files[-1][1]:02d}")

    # Check for gaps
    missing_months = []
    start_year, start_month = existing_files[0]
    end_year, end_month = existing_files[-1]

    current_year = start_year
    current_month = start_month

    while (current_year, current_month) <= (end_year, end_month):
        if (current_year, current_month) not in existing_files:
            missing_months.append((current_year, current_month))

        # Move to next month
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    if missing_months:
        print(f"\n❌ Found {len(missing_months)} missing months:")
        for year, month in missing_months[:20]:  # Show first 20
            print(f"   Missing: {year}-{month:02d}")
        if len(missing_months) > 20:
            print(f"   ... and {len(missing_months) - 20} more")
    else:
        print("\n✅ No gaps found - AAPL data is complete!")

    # Check recent months (last 12 months)
    today = date.today()
    recent_missing = []

    for i in range(12):
        check_month = today.month - i
        check_year = today.year

        if check_month <= 0:
            check_month += 12
            check_year -= 1

        if (check_year, check_month) not in existing_files:
            recent_missing.append((check_year, check_month))

    if recent_missing:
        print(f"\n⚠️  Recent missing months (last 12 months):")
        for year, month in sorted(recent_missing):
            print(f"   Missing: {year}-{month:02d}")

    # File size analysis
    print(f"\n📁 Sample file sizes:")
    sample_files = existing_files[-5:]  # Last 5 files
    for year, month in sample_files:
        file_path = aapl_path / str(year) / f"{month:02d}" / f"AAPL_{year}_{month:02d}.parquet"
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"   {year}-{month:02d}: {size_mb:.1f} MB")

if __name__ == "__main__":
    main()