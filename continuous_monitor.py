#!/usr/bin/env python3
"""
Continuous monitoring of FirstRate backfill with detailed progress tracking
"""

import time
import subprocess
from pathlib import Path
from datetime import datetime
import re

def get_process_info():
    """Get process information"""
    try:
        result = subprocess.run(
            "ps aux | grep 'populate_firstrate' | grep -v grep",
            shell=True, capture_output=True, text=True
        )
        lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
        
        processes = []
        for line in lines:
            if 'python scripts/populate_firstrate_minute_bars.py' in line:
                parts = line.split()
                if len(parts) >= 11:
                    processes.append({
                        'pid': parts[1],
                        'cpu': parts[2],
                        'mem': parts[3],
                        'time': parts[9]
                    })
        return processes
    except:
        return []

def get_inventory_progress():
    """Track inventory building progress"""
    log_file = Path("/tmp/firstrate_corrected_backfill.log")
    if not log_file.exists():
        return None, None
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        # Find all inventory processing lines
        inventory_lines = [line for line in lines if "Processing inventory for stock_" in line]
        
        if inventory_lines:
            last_line = inventory_lines[-1]
            # Extract stock letter from filename like "stock_A_full_1min_adjsplitdiv_fz3yij8.zip"
            match = re.search(r'stock_([A-Z])_full_1min', last_line)
            if match:
                current_letter = match.group(1)
                progress = ord(current_letter) - ord('A') + 1  # A=1, B=2, etc.
                return current_letter, progress
    except:
        pass
    
    return None, None

def get_symbol_processing_progress():
    """Check if we've moved to actual symbol processing"""
    log_file = Path("/tmp/firstrate_corrected_backfill.log")
    if not log_file.exists():
        return None, None
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Look for symbol processing indicators
        if "Processing symbol:" in content:
            lines = content.split('\n')
            symbol_lines = [line for line in lines if "Processing symbol:" in line]
            if symbol_lines:
                last_symbol_line = symbol_lines[-1]
                match = re.search(r'Processing symbol: (\w+)', last_symbol_line)
                if match:
                    return match.group(1), len(symbol_lines)
    except:
        pass
    
    return None, None

def monitor_continuously():
    """Monitor FirstRate backfill continuously"""
    
    print("🚀 Starting continuous FirstRate backfill monitoring...")
    print("=" * 80)
    
    last_letter = None
    last_symbol = None
    start_time = datetime.now()
    
    while True:
        current_time = datetime.now()
        elapsed = current_time - start_time
        
        print(f"\n🕒 {current_time.strftime('%Y-%m-%d %H:%M:%S')} (Runtime: {elapsed})")
        print("-" * 60)
        
        # Check processes
        processes = get_process_info()
        if not processes:
            print("❌ No FirstRate backfill processes running!")
            print("💡 Process may have completed or failed")
            break
        
        for i, proc in enumerate(processes, 1):
            print(f"🔄 Process {i}: PID={proc['pid']}, CPU={proc['cpu']}%, MEM={proc['mem']}%, Time={proc['time']}")
        
        # Check inventory progress
        current_letter, letter_progress = get_inventory_progress()
        if current_letter:
            if current_letter != last_letter:
                print(f"📦 Inventory Progress: Processing ZIP file for letter '{current_letter}' ({letter_progress}/26 ZIP files)")
                last_letter = current_letter
            else:
                print(f"📦 Still processing '{current_letter}' ZIP file ({letter_progress}/26)")
        
        # Check symbol processing
        current_symbol, symbol_count = get_symbol_processing_progress()
        if current_symbol:
            if current_symbol != last_symbol:
                print(f"🎯 Symbol Processing: Currently processing '{current_symbol}' (Total processed: {symbol_count})")
                last_symbol = current_symbol
            else:
                print(f"🎯 Still processing '{current_symbol}' (Total symbols processed: {symbol_count})")
        
        # Count current output
        output_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
        if output_path.exists():
            symbol_dirs = [d for d in output_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
            parquet_files = list(output_path.rglob("*.parquet"))
            
            print(f"📊 Current Output: {len(symbol_dirs)} symbols, {len(parquet_files):,} parquet files")
            
            # Check for new files
            recent_files = [f for f in parquet_files if (current_time.timestamp() - f.stat().st_mtime) < 3600]  # Last hour
            if recent_files:
                print(f"✨ New files in last hour: {len(recent_files)}")
        
        print("\n💤 Sleeping 120 seconds...")
        time.sleep(120)  # Check every 2 minutes

if __name__ == "__main__":
    try:
        monitor_continuously()
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")