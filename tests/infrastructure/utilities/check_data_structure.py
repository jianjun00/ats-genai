#!/usr/bin/env python3
from pathlib import Path

print("🔍 Detailed data structure check:")

data_path = Path("/data")
if data_path.exists():
    print(f"✅ Found /data directory")

    # Look for minute-bars
    minute_bars_path = data_path / "minute-bars"
    if minute_bars_path.exists():
        print(f"✅ Found minute-bars at: {minute_bars_path}")

        # Check for AAPL
        aapl_path = minute_bars_path / "AAPL"
        if aapl_path.exists():
            print(f"✅ Found AAPL data at: {aapl_path}")

            # Check years
            for year_dir in aapl_path.iterdir():
                if year_dir.is_dir():
                    print(f"  📅 Year: {year_dir.name}")

                    # Check months
                    for month_dir in year_dir.iterdir():
                        if month_dir.is_dir():
                            print(f"    📆 Month: {month_dir.name}")

                            # Check files
                            files = list(month_dir.glob("*.parquet"))
                            if files:
                                file_sizes = [f.stat().st_size for f in files]
                                total_size = sum(file_sizes)
                                print(f"      📊 {len(files)} parquet files, {total_size:,} bytes total")
                            break  # Just check first month
                    break  # Just check first year
        else:
            print(f"❌ No AAPL directory found")
            # List what symbols are available
            symbols = [d.name for d in minute_bars_path.iterdir() if d.is_dir()]
            print(f"Available symbols: {symbols[:10]}...")  # First 10
        print(f"❌ No minute-bars directory found")
        contents = [d.name for d in data_path.iterdir()]
        print(f"Contents of /data: {contents}")
    print(f"❌ /data directory not found")