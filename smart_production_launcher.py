#!/usr/bin/env python3
"""
Smart Production FirstRate Backfill Launcher
Optimized approach considering current system load and active processes
"""

import subprocess
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import psutil

def get_system_resources():
    """Get current system resource utilization"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/mnt/d')
    load_avg = os.getloadavg()
    
    return {
        'cpu_percent': cpu_percent,
        'memory_available_gb': memory.available / (1024**3),
        'memory_percent': memory.percent,
        'disk_available_tb': disk.free / (1024**4),
        'disk_percent': (disk.used / disk.total) * 100,
        'load_average': load_avg[0]
    }

def calculate_optimal_workers(resources, existing_load=1):
    """Calculate optimal number of workers based on system resources"""
    # Conservative approach: account for existing priority process
    cpu_cores = psutil.cpu_count()
    available_cores = max(2, cpu_cores - 2)  # Reserve 2 cores for system + existing process
    
    # Memory-based limitation (each worker ~500MB peak)
    memory_workers = int(resources['memory_available_gb'] / 0.5)
    
    # Load-based limitation 
    load_workers = max(2, int((cpu_cores * 0.8) - resources['load_average']))
    
    # Take conservative minimum
    optimal_workers = min(available_cores, memory_workers, load_workers, 6)  # Cap at 6
    
    return max(2, optimal_workers)  # Minimum 2 workers

def launch_smart_production_backfill():
    """Launch production backfill with smart resource management"""
    
    print("🧠 Smart Production FirstRate Backfill Launcher")
    print("=" * 60)
    
    # Check system resources
    resources = get_system_resources()
    print(f"🖥️  System Resources:")
    print(f"   CPU Usage: {resources['cpu_percent']:.1f}%")
    print(f"   Memory Available: {resources['memory_available_gb']:.1f} GB")
    print(f"   Load Average: {resources['load_average']:.2f}")
    print(f"   Disk Available: {resources['disk_available_tb']:.1f} TB")
    
    # Check for existing processes
    existing_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['cmdline'] and any('populate_firstrate' in cmd for cmd in proc.info['cmdline']):
                existing_processes.append(proc.info['pid'])
        except:
            pass
    
    print(f"\n📊 Active FirstRate Processes: {len(existing_processes)}")
    if existing_processes:
        print(f"   PIDs: {existing_processes}")
        print(f"   Note: Will coordinate with existing processes")
    
    # Load remaining symbols
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    if not analysis_file.exists():
        print(f"❌ Error: {analysis_file} not found!")
        return
    
    with open(analysis_file, 'r') as f:
        analysis = json.load(f)
    
    # Smart symbol selection - avoid priority symbols already being processed
    all_remaining = analysis.get('remaining_symbols', [])
    
    # Remove symbols likely being processed by priority batch
    priority_symbols = ['MSFT','GOOGL','GOOG','AMZN','META','TSLA','NVDA','ADBE','JPM','BAC','WFC','GS','MS','C','JNJ','PFE','ABT','MRK','PG','KO','PEP','WMT','HD','NKE','XOM','CVX','COP','GE','V','MA','DIS','NFLX','PYPL','COST','TMUS','AVGO','UNH']
    
    # Filter out priority symbols and symbols already processed by Docker workers
    docker_test_symbols = ['A','AACB','AACBR','AACBU','AACI','AACIU','AAGRW','AAM','AAMI','AAPG','AARD','AAUC','ABAT','ABI','ABL','ABLLL','ABLLW','ABLV','ABLVW','ABP','ABPWW','ABTS','ABVE','ABVEW','ABVX','AC','ACCS','ACFN','ACGLN','ACGLO','ACHR','ACHV','ACI','ACIC','ACIU','ACIW','ACLS','ACLX','ACM','ACMR']
    
    excluded_symbols = set(priority_symbols + docker_test_symbols)
    production_symbols = [s for s in all_remaining if s not in excluded_symbols]
    
    total_count = len(production_symbols)
    print(f"\n📈 Production Scope:")
    print(f"   Total remaining: {len(all_remaining):,} symbols")
    print(f"   Excluded (priority): {len(priority_symbols)} symbols")
    print(f"   Excluded (test batch): {len(docker_test_symbols)} symbols")  
    print(f"   Production batch: {total_count:,} symbols")
    
    if total_count == 0:
        print("✅ No additional symbols need processing!")
        return
    
    # Calculate optimal workers
    optimal_workers = calculate_optimal_workers(resources, len(existing_processes))
    print(f"\n⚡ Optimal Configuration:")
    print(f"   Recommended workers: {optimal_workers}")
    print(f"   Symbols per worker: ~{total_count // optimal_workers:,}")
    print(f"   Estimated time: {total_count * 2 / optimal_workers / 60:.1f} hours")
    print(f"   Expected speedup: ~{optimal_workers}x")
    
    # System safety check
    safety_ok = (
        resources['cpu_percent'] < 90 and
        resources['memory_available_gb'] > 2 and
        resources['load_average'] < 4.0 and
        len(existing_processes) < 3
    )
    
    if not safety_ok:
        print(f"\n⚠️  SYSTEM SAFETY CHECK FAILED:")
        print(f"   Current load too high for production-scale launch")
        print(f"   Consider waiting for existing processes to complete")
        print(f"   Or launch with fewer workers")
        return
    
    print(f"\n✅ System Safety Check: PASSED")
    print(f"   Safe to launch {optimal_workers} additional workers")
    
    # Confirm launch
    response = input(f"\nLaunch {optimal_workers} smart production workers for {total_count:,} symbols? [y/N]: ")
    if response.lower() != 'y':
        print("Smart production launch cancelled")
        return
    
    # Create batches
    batch_size = total_count // optimal_workers
    batches = []
    
    for i in range(optimal_workers):
        start_idx = i * batch_size
        if i == optimal_workers - 1:
            end_idx = total_count
        else:
            end_idx = (i + 1) * batch_size
        
        batch = production_symbols[start_idx:end_idx]
        if batch:
            batches.append(batch)
    
    print(f"\n🚀 Launching Smart Production Workers:")
    launched_processes = []
    start_time = datetime.now()
    
    for i, batch in enumerate(batches):
        symbols_str = ",".join(batch)
        checkpoint_file = f"smart_production_worker_{i}_checkpoint.json"
        log_file = f"/tmp/smart_production_worker_{i}.log"
        
        # Create optimized worker script
        script_content = f'''#!/usr/bin/env python3
"""
Smart Production Worker {i}
Processing {len(batch)} symbols: {batch[0]} to {batch[-1]}
Launched: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
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
        
        worker_script = f"smart_production_worker_{i}.py"
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
            time.sleep(3)  # Staggered launch
            
        except Exception as e:
            print(f"    ❌ Failed: {e}")
    
    # Save launch info
    launch_info = {
        "launched_at": start_time.isoformat(),
        "launch_type": "smart_production",
        "system_resources": resources,
        "num_workers": len(launched_processes),
        "total_symbols": sum(len(p['symbols']) for p in launched_processes),
        "excluded_symbols": len(excluded_symbols),
        "estimated_completion": (start_time + timedelta(hours=total_count * 2 / optimal_workers / 60)).isoformat(),
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
    
    with open("smart_production_launch_info.json", "w") as f:
        json.dump(launch_info, f, indent=2)
    
    print(f"\n🎉 Smart Production Launch Complete!")
    print(f"   Workers: {len(launched_processes)}")
    print(f"   Symbols: {sum(len(p['symbols']) for p in launched_processes):,}")
    print(f"   Estimated completion: {launch_info['estimated_completion']}")
    print(f"   Monitor with: python3 parallel_backfill_status.py")
    print(f"   Launch info: smart_production_launch_info.json")
    
    return launched_processes

if __name__ == "__main__":
    launch_smart_production_backfill()