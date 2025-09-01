#!/usr/bin/env python3
"""
Docker-Based Parallel FirstRate Backfill Launcher
Uses run_dev.py to ensure proper Docker environment with all dependencies
"""

import subprocess
import time
import os
from pathlib import Path
from datetime import datetime
import json

def launch_docker_parallel_backfill():
    """Launch parallel backfill processes using Docker via run_dev.py"""
    
    print("🐳 Docker-Based Parallel FirstRate Backfill Launcher")
    print("=" * 60)
    
    # Load remaining symbols
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    if analysis_file.exists():
        with open(analysis_file, 'r') as f:
            analysis = json.load(f)
        # Use first 40 symbols for testing (instead of 200)
        all_symbols = analysis.get('remaining_symbols', [])[:40]
    else:
        # Small test batch if no analysis file
        all_symbols = [
            'IBM', 'INTC', 'CSCO', 'ORCL', 'CRM', 'QCOM', 'TXN', 'AMAT',
            'MRVL', 'LRCX', 'KLAC', 'AMAT', 'MU', 'WDC', 'STX', 'NTAP'
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
    print(f"📈 Est. parallel time: {len(all_symbols) * 2 / len(batches) / 60:.1f} minutes")
    
    # Auto-confirm for testing (remove input prompt)
    print(f"\n🚀 Auto-launching {len(batches)} Docker-based parallel workers...")
    
    # Launch workers using run_dev.py
    launched_processes = []
    
    for i, batch in enumerate(batches):
        symbols_str = ",".join(batch)
        checkpoint_file = f"docker_parallel_worker_{i}_checkpoint.json"
        log_file = f"/tmp/docker_parallel_worker_{i}.log"
        
        # Create a temporary script file for this worker
        script_content = f'''#!/usr/bin/env python3
"""
Temporary worker script for Docker parallel backfill
Worker {i}: {len(batch)} symbols
"""
import sys
import os
sys.path.append('/workspace/src')
os.chdir('/workspace')

from scripts.populate_firstrate_minute_bars import main
import argparse

if __name__ == "__main__":
    # Mock the command line arguments
    import sys
    sys.argv = [
        'populate_firstrate_minute_bars.py',
        '--asset-type', 'stock',
        '--symbols', '{symbols_str}',
        '--checkpoint-file', '{checkpoint_file}',
        '--debug'
    ]
    main()
'''
        
        worker_script = f"/tmp/docker_worker_{i}.py"
        with open(worker_script, 'w') as f:
            f.write(script_content)
        
        # Copy worker script to container accessible location
        cmd = [
            'python3', 'scripts/run_dev.py', 'run', 
            '--script', f'docker_worker_{i}.py'
        ]
        
        print(f"\n🚀 Launching Docker Worker {i}...")
        print(f"   Symbols: {len(batch)} ({batch[0]} to {batch[-1]})")
        print(f"   Script: {worker_script}")
        print(f"   Log: {log_file}")
        
        try:
            # Copy script to working directory first
            subprocess.run(['cp', worker_script, f'docker_worker_{i}.py'], check=True)
            
            # Launch process in background
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
                "script_file": worker_script,
                "process": process
            })
            
            print(f"   ✅ Started (PID: {process.pid})")
            
            # Brief delay between launches
            time.sleep(3)
            
        except Exception as e:
            print(f"   ❌ Failed to launch: {e}")
    
    print(f"\n🎉 Successfully launched {len(launched_processes)} Docker parallel workers!")
    print("\n📋 Monitoring Commands:")
    
    for proc_info in launched_processes:
        print(f"\nDocker Worker {proc_info['worker_id']} (PID {proc_info['pid']}):")
        print(f"   Status: ps -p {proc_info['pid']}")
        print(f"   Log: tail -f {proc_info['log_file']}")
        print(f"   Progress: ls /mnt/d/ats-data/minute-bars/firstrate/{proc_info['symbols'][0]}/")
    
    print(f"\n🔍 Overall Monitoring:")
    print(f"   ps aux | grep run_dev")
    print(f"   ls /tmp/docker_parallel_worker_*.log")
    
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
    
    with open("docker_parallel_launch_info.json", "w") as f:
        json.dump(launch_info, f, indent=2)
    
    print(f"\n💾 Launch info saved to: docker_parallel_launch_info.json")
    
    return launched_processes

if __name__ == "__main__":
    launch_docker_parallel_backfill()