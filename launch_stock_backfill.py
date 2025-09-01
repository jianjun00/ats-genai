#!/usr/bin/env python3
"""
Launch FirstRate Stock Backfill
Direct approach using proven populate_firstrate_minute_bars.py script

Starts processing major stock symbols from FirstRate data using the 
same approach that successfully processed ETFs and AAPL.
"""

import subprocess
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def launch_stock_backfill():
    """Launch stock backfill using proven approach"""
    
    print("🚀 Launching FirstRate Stock Backfill")
    print("📊 Using proven populate_firstrate_minute_bars.py approach")
    print("⚠️  This will process thousands of stock symbols (6,827 remaining)")
    print("⏰ Estimated time: 200+ hours (8-10 days continuous)")
    print()
    
    # Start with high-priority symbols first
    priority_symbols = [
        'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'ADBE',
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'JNJ', 'PFE', 'ABT', 'MRK',
        'PG', 'KO', 'PEP', 'WMT', 'HD', 'NKE', 'XOM', 'CVX', 'COP', 'GE',
        'V', 'MA', 'DIS', 'NFLX', 'PYPL', 'COST', 'TMUS', 'AVGO', 'UNH'
    ]
    
    # Create command for high-priority batch
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
    
    print(f"🎯 Starting with {len(priority_symbols)} high-priority symbols:")
    print(f"   {', '.join(priority_symbols[:10])}...")
    print(f"   Checkpoint: {checkpoint_file}")
    print(f"   Command: {' '.join(cmd)}")
    print()
    
    # Confirm before starting
    response = input("Start stock backfill? This will run for HOURS [y/N]: ")
    if response.lower() != 'y':
        print("❌ Cancelled by user")
        return
    
    print(f"🚀 Starting stock backfill at {datetime.now()}")
    print("⚡ Running in background - use 'ps aux | grep populate_firstrate' to monitor")
    print()
    
    try:
        # Execute the command - let it run in background of this terminal
        result = subprocess.Popen(cmd, cwd='/home/jianjun/ats-genai-data')
        
        print(f"✅ Stock backfill launched (PID: {result.pid})")
        print(f"📋 Processing {len(priority_symbols)} high-priority symbols")
        print(f"💾 Progress saved to: {checkpoint_file}")
        print()
        print("🔍 Monitor progress:")
        print(f"   ps -p {result.pid}")
        print(f"   tail -f {checkpoint_file}")
        print(f"   ls /mnt/d/ats-data/minute-bars/firstrate/")
        print()
        print("⚠️  This is just the first batch of high-priority symbols!")
        print("   After completion, run full backfill for all 6,827 symbols")
        
        return result.pid
        
    except Exception as e:
        print(f"❌ Error launching stock backfill: {e}")
        return None


def main():
    """Main launcher"""
    
    print("=" * 60)
    print("FirstRate Stock Backfill Launcher")
    print("=" * 60)
    print()
    
    # Show current status first
    print("📊 Current Processing Status:")
    
    try:
        # Check existing processes
        result = subprocess.run([
            'ps', 'aux'
        ], capture_output=True, text=True)
        
        if 'populate_firstrate' in result.stdout:
            print("⚡ FirstRate processes already running:")
            lines = result.stdout.split('\n')
            for line in lines:
                if 'populate_firstrate' in line:
                    print(f"   {line}")
            print()
        else:
            print("✅ No FirstRate processes currently running")
            print()
        
    except Exception as e:
        print(f"⚠️  Could not check process status: {e}")
        print()
    
    # Launch new backfill
    pid = launch_stock_backfill()
    
    if pid:
        print("🎉 Stock backfill successfully launched!")
        print(f"📋 Process ID: {pid}")
    else:
        print("❌ Failed to launch stock backfill")


if __name__ == "__main__":
    main()