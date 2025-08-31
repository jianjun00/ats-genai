#!/usr/bin/env python3
"""
Monitor FirstRate backfill progress
"""

import time
import subprocess
from pathlib import Path
from datetime import datetime

def monitor_progress():
    """Monitor ongoing FirstRate backfill processes"""
    
    while True:
        print(f"\n🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - FirstRate Backfill Status")
        print("=" * 70)
        
        # Check running processes
        result = subprocess.run(
            "ps aux | grep 'populate_firstrate' | grep -v grep | wc -l", 
            shell=True, capture_output=True, text=True
        )
        process_count = int(result.stdout.strip()) if result.stdout.strip() else 0
        
        print(f"🔄 Active processes: {process_count}")
        
        if process_count == 0:
            print("❌ No FirstRate backfill processes running!")
            print("💡 To restart: PYTHONPATH=src python3 scripts/run_dev.py run --script scripts/populate_firstrate_minute_bars.py")
            break
        
        # Count processed symbols (directories)
        output_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
        if output_path.exists():
            symbol_dirs = [d for d in output_path.iterdir() 
                          if d.is_dir() and not d.name.startswith('.')]
            symbol_count = len(symbol_dirs)
            
            # Count total parquet files
            parquet_files = list(output_path.rglob("*.parquet"))
            parquet_count = len(parquet_files)
            
            print(f"✅ Symbols processed: {symbol_count}")
            print(f"📄 Parquet files: {parquet_count:,}")
            
            # Show recent symbols (last 10)
            if symbol_dirs:
                recent_symbols = sorted([d.name for d in symbol_dirs])[-10:]
                print(f"🔸 Recent symbols: {', '.join(recent_symbols)}")
        
        # Check log progress
        for log_file in ["/tmp/firstrate_fixed2.log", "/tmp/firstrate_full_backfill.log"]:
            if Path(log_file).exists():
                try:
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        if lines:
                            last_line = lines[-1].strip()
                            if "Processing inventory for" in last_line:
                                zip_name = last_line.split("for ")[-1]
                                print(f"📦 {Path(log_file).name}: {zip_name}")
                            elif "Processing symbol:" in last_line:
                                symbol = last_line.split("Processing symbol: ")[-1]
                                print(f"🎯 {Path(log_file).name}: Processing {symbol}")
                except:
                    pass
        
        print("\n💤 Sleeping 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    try:
        monitor_progress()
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")