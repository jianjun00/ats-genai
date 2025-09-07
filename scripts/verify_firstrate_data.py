#!/usr/bin/env python3
"""
Verify FirstRate minute bar data quality
"""

import pandas as pd
from pathlib import Path

def main():
    print("🔍 Verifying FirstRate minute bar data quality...")

    # Check all major symbols that should now have data
    major_symbols = {
        'B': ['BRK.A', 'BRK.B'],
        'C': ['CVX', 'CRM'],
        'K': ['KO'],
        'L': ['LLY'],
        'M': ['MSFT', 'META', 'MA', 'MCD', 'MRK'],
        'N': ['NVDA', 'NFLX', 'NKE'],
        'O': ['ORCL'],
        'P': ['PFE', 'PG'],
        'U': ['UNH'],
        'V': ['V'],
        'W': ['WMT', 'WFC'],
        'X': ['XOM']
    }

    base_path = Path("/data/minute-bars/firstrate")

    for letter, symbols in major_symbols.items():
        print(f"\n📊 Checking letter {letter}:")
        for symbol in symbols:
            symbol_dir = base_path / letter / symbol
            if symbol_dir.exists():
                parquet_files = list(symbol_dir.glob("*.parquet"))
                if parquet_files:
                    # Read the first file to verify data quality
                    df = pd.read_parquet(parquet_files[0])
                    print(f"✅ {symbol}: {len(df)} records, columns: {list(df.columns)}")
                    if len(df) > 0:
                        print(f"   Sample: {df.iloc[0]['timestamp']} - O:{df.iloc[0]['open']:.2f} H:{df.iloc[0]['high']:.2f} L:{df.iloc[0]['low']:.2f} C:{df.iloc[0]['close']:.2f} V:{df.iloc[0]['volume']}")
                        print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                else:
                    print(f"❌ {symbol}: Directory exists but no parquet files")
            else:
                print(f"❌ {symbol}: Directory does not exist")

if __name__ == "__main__":
    main()