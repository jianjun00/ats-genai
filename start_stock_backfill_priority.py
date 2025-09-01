#!/usr/bin/env python3
"""
Start Stock Backfill for Priority Symbols
Uses the same Docker-based approach as the working ETF and AAPL backfills
"""

import subprocess
import sys

def main():
    """Start priority stock backfill using Docker approach"""
    
    print("🚀 Starting FirstRate Stock Backfill - Priority Symbols")
    print("📊 Processing high-value stocks using proven Docker approach")
    print()
    
    # High-priority symbols for first batch
    priority_symbols = [
        'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'ADBE',
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'JNJ', 'PFE', 'ABT', 'MRK',
        'PG', 'KO', 'PEP', 'WMT', 'HD', 'NKE', 'XOM', 'CVX', 'COP', 'GE',
        'V', 'MA', 'DIS', 'NFLX', 'PYPL', 'COST', 'TMUS', 'AVGO', 'UNH'
    ]
    
    symbols_str = ",".join(priority_symbols)
    checkpoint_file = "stock_backfill_priority_batch.json"
    
    cmd = [
        'python3', 
        'scripts/populate_firstrate_minute_bars.py',
        '--asset-type', 'stock',
        '--symbols', symbols_str,
        '--checkpoint-file', checkpoint_file,
        '--debug'
    ]
    
    print(f"🎯 Symbols: {len(priority_symbols)} high-priority stocks")
    print(f"📋 First 10: {', '.join(priority_symbols[:10])}...")
    print(f"💾 Checkpoint: {checkpoint_file}")
    print(f"🐳 Using Docker execution for proper environment")
    print()
    
    print("🚀 Executing command:")
    print(f"   {' '.join(cmd)}")
    print()
    
    try:
        # Execute the command and let it run
        result = subprocess.run(cmd)
        
        if result.returncode == 0:
            print("✅ Stock backfill completed successfully!")
        else:
            print(f"❌ Stock backfill failed with exit code: {result.returncode}")
            
    except KeyboardInterrupt:
        print("⚠️  Stock backfill interrupted by user")
    except Exception as e:
        print(f"❌ Error running stock backfill: {e}")


if __name__ == "__main__":
    main()