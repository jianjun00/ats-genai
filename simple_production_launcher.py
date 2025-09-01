#!/usr/bin/env python3
"""
Simple Production FirstRate Backfill Launcher
Uses built-in tools and conservative resource management
"""

import subprocess
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

def get_system_load():
    """Get basic system load information using built-in tools"""
    try:
        # Get load average
        load_result = subprocess.run(['uptime'], capture_output=True, text=True)
        load_line = load_result.stdout.strip()
        # Extract load average (last 3 numbers in uptime output)
        load_avg = float(load_line.split()[-3].replace(',', ''))
        
        # Get memory info
        mem_result = subprocess.run(['free', '-g'], capture_output=True, text=True)
        mem_lines = mem_result.stdout.strip().split('\n')
        mem_available = int(mem_lines[1].split()[-1])  # Available memory in GB
        
        return {'load_average': load_avg, 'memory_available_gb': mem_available}
    except:
        return {'load_average': 2.0, 'memory_available_gb': 10}  # Safe defaults

def launch_simple_production_backfill():
    """Launch production backfill with simple resource checks"""
    
    print("🏭 Simple Production FirstRate Backfill Launcher")
    print("=" * 60)
    
    # Basic system check
    system_info = get_system_load()
    print(f"🖥️  System Status:")
    print(f"   Load Average: {system_info['load_average']:.2f}")
    print(f"   Available Memory: {system_info['memory_available_gb']} GB")
    
    # Check existing FirstRate processes
    existing_result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
    existing_count = existing_result.stdout.count('populate_firstrate')
    print(f"   Active FirstRate Processes: {existing_count}")
    
    # Load remaining symbols
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    if not analysis_file.exists():
        print(f"❌ Error: {analysis_file} not found!")
        return
    
    with open(analysis_file, 'r') as f:
        analysis = json.load(f)
    
    all_remaining = analysis.get('remaining_symbols', [])
    
    # Conservative filtering - avoid conflicts with existing processes
    priority_symbols = ['MSFT','GOOGL','GOOG','AMZN','META','TSLA','NVDA','ADBE','JPM','BAC','WFC','GS','MS','C','JNJ','PFE','ABT','MRK','PG','KO','PEP','WMT','HD','NKE','XOM','CVX','COP','GE','V','MA','DIS','NFLX','PYPL','COST','TMUS','AVGO','UNH']
    
    # Skip first ~100 symbols to avoid conflicts with test batches
    production_symbols = [s for s in all_remaining[100:] if s not in priority_symbols]
    
    total_count = len(production_symbols)
    print(f"\n📊 Production Scope:")
    print(f"   Total available: {len(all_remaining):,} symbols")
    print(f"   Production batch: {total_count:,} symbols")
    print(f"   Starting from: {production_symbols[0] if production_symbols else 'N/A'}")
    
    if total_count < 50:
        print("ℹ️  Small batch remaining - may not justify parallel launch")
        if total_count == 0:
            print("✅ No symbols need processing!")
            return
    
    # Conservative worker calculation
    if system_info['load_average'] > 4.0 or existing_count > 2:
        workers = 2  # Very conservative
        print("⚠️  High system load - using minimal workers")
    elif total_count > 1000:
        workers = 4  # Good for large batches
    else:
        workers = 3  # Medium batch
        
    print(f"\n⚡ Launch Configuration:")
    print(f"   Workers: {workers}")
    print(f"   Symbols per worker: ~{total_count // workers:,}")
    print(f"   Estimated time: {total_count * 2 / workers / 60:.1f} hours")
    
    # Safety check
    if system_info['load_average'] > 6.0:
        print("\n❌ System load too high for additional workers")
        print("   Wait for existing processes to complete")
        return
        
    # Take smaller batch for testing if total is very large
    if total_count > 2000:
        production_symbols = production_symbols[:1000]  # Limit to 1000 for testing
        total_count = len(production_symbols)
        print(f"\n📝 Limited to first 1,000 symbols for testing")
        print(f"   Processing: {total_count:,} symbols")
    
    print(f"\n🚀 Auto-launching {workers} production workers for {total_count:,} symbols...")
    print("   (Non-interactive mode enabled)")
    
    # Create batches
    batch_size = total_count // workers
    batches = []
    
    for i in range(workers):
        start_idx = i * batch_size
        if i == workers - 1:
            end_idx = total_count
        else:
            end_idx = (i + 1) * batch_size
        
        batch = production_symbols[start_idx:end_idx]
        if batch:
            batches.append(batch)
    
    # Launch workers
    print(f"\n🚀 Launching Production Workers:")
    launched_processes = []
    start_time = datetime.now()
    
    for i, batch in enumerate(batches):
        if len(batch) == 0:
            continue
            
        symbols_str = ",".join(batch)
        checkpoint_file = f"simple_production_worker_{i}_checkpoint.json"
        log_file = f"/tmp/simple_production_worker_{i}.log"
        
        # Create worker script
        script_content = f'''#!/usr/bin/env python3
"""
Simple Production Worker {i}
Launched: {datetime.now().strftime('%H:%M:%S')}
Symbols: {len(batch)} ({batch[0]} to {batch[-1]})
"""
import sys
import os
sys.path.append('/workspace/src')
os.chdir('/workspace')

from scripts.populate_firstrate_minute_bars import main

if __name__ == "__main__":
    sys.argv = [
        'populate_firstrate_minute_bars.py',
        '--asset-type', 'stock',
        '--symbols', '{symbols_str}',
        '--checkpoint-file', '{checkpoint_file}',
        '--debug'
    ]
    main()
'''
        
        worker_script = f"simple_production_worker_{i}.py"
        with open(worker_script, 'w') as f:
            f.write(script_content)
        
        cmd = [
            'python3', 'scripts/run_dev.py', 'run',
            '--script', worker_script
        ]
        
        print(f"  Worker {i}: {len(batch):,} symbols ({batch[0]} → {batch[-1]})")
        
        try:
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
                "symbols_count": len(batch),
                "checkpoint_file": checkpoint_file,
                "log_file": log_file
            })
            
            print(f"    ✅ Started (PID: {process.pid})")
            time.sleep(4)  # Staggered launch
            
        except Exception as e:
            print(f"    ❌ Failed: {e}")
    
    # Save launch info
    launch_info = {
        "launched_at": start_time.isoformat(),
        "launch_type": "simple_production",
        "system_load": system_info['load_average'],
        "num_workers": len(launched_processes),
        "total_symbols": sum(len(p['symbols']) for p in launched_processes),
        "estimated_completion": (start_time + timedelta(hours=total_count * 2 / len(batches) / 60)).isoformat(),
        "workers": [
            {
                "worker_id": proc["worker_id"],
                "pid": proc["pid"],
                "symbols_count": proc["symbols_count"],
                "symbols_range": f"{proc['symbols'][0]} → {proc['symbols'][-1]}",
                "checkpoint_file": proc["checkpoint_file"],
                "log_file": proc["log_file"]
            }
            for proc in launched_processes
        ]
    }
    
    with open("simple_production_launch_info.json", "w") as f:
        json.dump(launch_info, f, indent=2)
    
    print(f"\n🎉 Production Launch Complete!")
    print(f"   Workers launched: {len(launched_processes)}")
    print(f"   Total symbols: {sum(len(p['symbols']) for p in launched_processes):,}")
    print(f"   Monitor: python3 parallel_backfill_status.py")
    print(f"   Launch info: simple_production_launch_info.json")
    
    return launched_processes

if __name__ == "__main__":
    launch_simple_production_backfill()