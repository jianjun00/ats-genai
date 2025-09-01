#!/usr/bin/env python3
"""
Simple Parallel FirstRate Backfill Launcher
Launches multiple background processes using the proven approach

This uses the exact same command pattern as the working processes:
- Direct calls to populate_firstrate_minute_bars.py
- Background execution with nohup
- Separate log files for each worker
- Simple and reliable approach
"""

import subprocess
import time
import os
from pathlib import Path
from datetime import datetime
import json

def launch_parallel_backfill():
    """Launch multiple parallel backfill processes"""
    
    print("🚀 Simple Parallel FirstRate Backfill Launcher")
    print("=" * 60)
    
    # Load remaining symbols
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    if analysis_file.exists():
        with open(analysis_file, 'r') as f:
            analysis = json.load(f)
        all_symbols = analysis.get('remaining_symbols', [])[:200]  # First 200 for testing
    else:
        # High-priority symbols if no analysis file
        all_symbols = [
            'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'ADBE',
            'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'JNJ', 'PFE', 'ABT', 'MRK',
            'PG', 'KO', 'PEP', 'WMT', 'HD', 'NKE', 'XOM', 'CVX', 'COP', 'GE',
            'V', 'MA', 'DIS', 'NFLX', 'PYPL', 'COST', 'TMUS', 'AVGO', 'UNH'
        ]
    
    # Split into 4 parallel batches
    num_workers = 4
    batch_size = len(all_symbols) // num_workers
    batches = []
    
    for i in range(num_workers):
        start_idx = i * batch_size
        if i == num_workers - 1:
            # Last batch gets remaining symbols
            end_idx = len(all_symbols)
        else:
            end_idx = (i + 1) * batch_size
        
        batch = all_symbols[start_idx:end_idx]
        if batch:
            batches.append(batch)
    
    print(f"📊 Processing Plan:")
    print(f"   Total symbols: {len(all_symbols)}")
    print(f"   Parallel workers: {len(batches)}")
    
    for i, batch in enumerate(batches):
        print(f"   Worker {i}: {len(batch)} symbols ({batch[0]} to {batch[-1]})")
    
    print(f"\n⚡ Expected speedup: ~{len(batches)}x")
    print(f"📈 Est. sequential time: {len(all_symbols) * 2 / 60:.1f} minutes")
    print(f"📈 Est. parallel time: {len(all_symbols) * 2 / len(batches) / 60:.1f} minutes")
    
    # Confirm launch
    response = input(f"\nLaunch {len(batches)} parallel workers? [y/N]: ")
    if response.lower() != 'y':
        print("Cancelled by user")
        return
    
    # Launch workers
    launched_processes = []
    
    for i, batch in enumerate(batches):
        symbols_str = ",".join(batch)
        checkpoint_file = f"parallel_worker_{i}_checkpoint.json"
        log_file = f"/tmp/parallel_worker_{i}.log"
        
        # Use the exact command pattern that works
        cmd = [
            'nohup', 'python3', 'scripts/populate_firstrate_minute_bars.py',
            '--asset-type', 'stock',
            '--symbols', symbols_str,
            '--checkpoint-file', checkpoint_file,
            '--debug'
        ]
        
        print(f"🚀 Launching Worker {i}...")
        print(f"   Symbols: {len(batch)} ({batch[0]} to {batch[-1]})")
        print(f"   Checkpoint: {checkpoint_file}")
        print(f"   Log: {log_file}")
        
        try:
            # Launch process
            with open(log_file, 'w') as log_f:
                process = subprocess.Popen(
                    cmd,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    cwd='/home/jianjun/ats-genai-data'
                )
            
            launched_processes.append({
                "worker_id": i,
                "pid": process.pid,
                "symbols": batch,
                "checkpoint_file": checkpoint_file,
                "log_file": log_file,
                "process": process
            })
            
            print(f"   ✅ Started (PID: {process.pid})")
            
            # Brief delay between launches
            time.sleep(2)
            
        except Exception as e:
            print(f"   ❌ Failed to launch: {e}")
    
    print(f"\n🎉 Successfully launched {len(launched_processes)} parallel workers!")
    print("\n📋 Monitoring Commands:")
    
    for proc_info in launched_processes:
        print(f"\nWorker {proc_info['worker_id']} (PID {proc_info['pid']}):")
        print(f"   Status: ps -p {proc_info['pid']}")
        print(f"   Log: tail -f {proc_info['log_file']}")
        print(f"   Progress: ls /mnt/d/ats-data/minute-bars/firstrate/{proc_info['symbols'][0]}/")
    
    print(f"\n🔍 Overall Monitoring:")
    print(f"   ps aux | grep populate_firstrate")
    print(f"   ls /tmp/parallel_worker_*.log")
    
    # Save launch info
    launch_info = {
        "launched_at": datetime.now().isoformat(),
        "num_workers": len(launched_processes),
        "total_symbols": len(all_symbols),
        "workers": [
            {
                "worker_id": proc["worker_id"],
                "pid": proc["pid"],
                "symbols_count": len(proc["symbols"]),
                "symbols": proc["symbols"],
                "checkpoint_file": proc["checkpoint_file"],
                "log_file": proc["log_file"]
            }
            for proc in launched_processes
        ]
    }
    
    with open("parallel_launch_info.json", "w") as f:
        json.dump(launch_info, f, indent=2)
    
    print(f"\n💾 Launch info saved to: parallel_launch_info.json")
    
    return launched_processes

if __name__ == "__main__":
    launch_parallel_backfill()