#!/usr/bin/env python3
"""
Production Parallel FirstRate Backfill Launcher
Launches comprehensive parallel processing for remaining 6,827 stock symbols
Uses proven Docker-based approach with dynamic load balancing
"""

import subprocess
import time
import os
from pathlib import Path
from datetime import datetime
import json

def launch_production_parallel_backfill():
    """Launch comprehensive parallel backfill for all remaining symbols"""
    
    print("🏭 Production Parallel FirstRate Backfill Launcher")
    print("=" * 60)
    
    # Load complete remaining symbols from analysis
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    if not analysis_file.exists():
        print(f"❌ Error: {analysis_file} not found!")
        print("   Run stock universe analysis first to identify remaining symbols")
        return
    
    with open(analysis_file, 'r') as f:
        analysis = json.load(f)
    
    all_symbols = analysis.get('remaining_symbols', [])
    total_count = len(all_symbols)
    
    if total_count == 0:
        print("✅ No remaining symbols to process!")
        return
    
    print(f"📊 Production Scale Analysis:")
    print(f"   Total remaining symbols: {total_count:,}")
    print(f"   Estimated data size: ~{total_count * 30 / 1000:.1f} GB")
    print(f"   Sequential time estimate: {total_count * 2 / 60:.0f} hours")
    
    # Calculate optimal worker count based on system resources
    # Conservative approach: start with 8 workers to avoid overwhelming system
    num_workers = min(8, max(4, total_count // 200))  # 1 worker per ~200-500 symbols
    
    print(f"   Parallel workers: {num_workers}")
    print(f"   Symbols per worker: ~{total_count // num_workers}")
    print(f"   Expected parallel time: {total_count * 2 / num_workers / 60:.1f} hours")
    print(f"   Expected speedup: ~{num_workers}x")
    
    # Ask for confirmation due to scale
    print(f"\n🚨 PRODUCTION SCALE WARNING:")
    print(f"   This will process {total_count:,} symbols")
    print(f"   Estimated runtime: {total_count * 2 / num_workers / 60:.1f} hours")
    print(f"   System load: {num_workers} parallel Docker processes")
    print(f"   Ensure system has adequate resources and monitoring")
    
    response = input(f"\nProceed with production-scale parallel backfill? [y/N]: ")
    if response.lower() != 'y':
        print("Production backfill cancelled by user")
        return
    
    # Split symbols into batches
    batch_size = total_count // num_workers
    batches = []
    
    for i in range(num_workers):
        start_idx = i * batch_size
        if i == num_workers - 1:
            # Last batch gets remaining symbols
            end_idx = total_count
        else:
            end_idx = (i + 1) * batch_size
        
        batch = all_symbols[start_idx:end_idx]
        if batch:
            batches.append(batch)
    
    print(f"\n📋 Batch Configuration:")
    for i, batch in enumerate(batches):
        print(f"   Worker {i}: {len(batch):,} symbols ({batch[0]} → {batch[-1]})")
    
    # Launch production workers
    launched_processes = []
    start_time = datetime.now()
    
    print(f"\n🚀 Launching production workers...")
    
    for i, batch in enumerate(batches):
        symbols_str = ",".join(batch)
        checkpoint_file = f"production_worker_{i}_checkpoint.json"
        log_file = f"/tmp/production_worker_{i}.log"
        
        # Create worker script
        script_content = f'''#!/usr/bin/env python3
"""
Production Worker {i} - FirstRate Backfill
Processing {len(batch)} symbols: {batch[0]} to {batch[-1]}
"""
import sys
import os
sys.path.append('/workspace/src')
os.chdir('/workspace')

from scripts.populate_firstrate_minute_bars import main
import argparse

if __name__ == "__main__":
    # Mock command line arguments for this worker
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
        
        worker_script = f"/tmp/production_worker_{i}.py"
        with open(worker_script, 'w') as f:
            f.write(script_content)
        
        # Copy to working directory and launch
        cmd = [
            'python3', 'scripts/run_dev.py', 'run', 
            '--script', f'production_worker_{i}.py'
        ]
        
        print(f"\n  Worker {i}:")
        print(f"    Symbols: {len(batch):,} ({batch[0]} → {batch[-1]})")
        print(f"    Checkpoint: {checkpoint_file}")
        print(f"    Log: {log_file}")
        
        try:
            # Copy script and launch
            subprocess.run(['cp', worker_script, f'production_worker_{i}.py'], check=True)
            
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
                "log_file": log_file,
                "script_file": worker_script,
                "process": process
            })
            
            print(f"    ✅ Started (PID: {process.pid})")
            
            # Staggered launch to avoid overwhelming system
            time.sleep(5)
            
        except Exception as e:
            print(f"    ❌ Failed: {e}")
    
    print(f"\n🎉 Production Launch Summary:")
    print(f"   Workers launched: {len(launched_processes)}")
    print(f"   Total symbols: {sum(len(p['symbols']) for p in launched_processes):,}")
    print(f"   Launch completed: {datetime.now().strftime('%H:%M:%S')}")
    
    # Save comprehensive launch info
    launch_info = {
        "launched_at": start_time.isoformat(),
        "launch_type": "production",
        "num_workers": len(launched_processes),
        "total_symbols": sum(len(p['symbols']) for p in launched_processes),
        "estimated_runtime_hours": total_count * 2 / num_workers / 60,
        "expected_speedup": num_workers,
        "workers": [
            {
                "worker_id": proc["worker_id"],
                "pid": proc["pid"],
                "symbols_count": len(proc["symbols"]),
                "symbols_range": f"{proc['symbols'][0]} → {proc['symbols'][-1]}",
                "checkpoint_file": proc["checkpoint_file"],
                "log_file": proc["log_file"]
            }
            for proc in launched_processes
        ]
    }
    
    with open("production_parallel_launch_info.json", "w") as f:
        json.dump(launch_info, f, indent=2)
    
    print(f"\n📋 Monitoring Commands:")
    print(f"   Overall status: python3 parallel_backfill_status.py")
    print(f"   System load: htop or top")
    print(f"   Process status: ps aux | grep run_dev")
    
    print(f"\n📁 Individual Worker Monitoring:")
    for proc in launched_processes[:3]:  # Show first 3 workers
        print(f"   Worker {proc['worker_id']}: tail -f {proc['log_file']}")
    if len(launched_processes) > 3:
        print(f"   ... and {len(launched_processes) - 3} more workers")
    
    print(f"\n💾 Complete launch info: production_parallel_launch_info.json")
    print(f"\n⚠️  IMPORTANT: Monitor system resources and worker progress")
    print(f"   Expected completion: {(start_time + timedelta(hours=total_count * 2 / num_workers / 60)).strftime('%Y-%m-%d %H:%M')}")
    
    return launched_processes

if __name__ == "__main__":
    from datetime import timedelta
    launch_production_parallel_backfill()