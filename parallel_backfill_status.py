#!/usr/bin/env python3
"""
Comprehensive Parallel FirstRate Backfill Status Monitor
Shows status of all running FirstRate backfill operations
"""

import subprocess
import json
from pathlib import Path
from datetime import datetime, timedelta
import time

def get_process_info(pid):
    """Get CPU usage and runtime for a process"""
    try:
        # Get process info using ps
        result = subprocess.run(['ps', '-o', 'pid,pcpu,etime,command', '-p', str(pid)], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) >= 2:  # Header + process line
                parts = lines[1].split(None, 3)  # Split into max 4 parts
                if len(parts) >= 3:
                    cpu = parts[1]
                    etime = parts[2]
                    return {'cpu': cpu, 'runtime': etime, 'status': 'running'}
        return {'cpu': '0.0', 'runtime': 'unknown', 'status': 'not_found'}
    except:
        return {'cpu': '0.0', 'runtime': 'unknown', 'status': 'error'}

def check_data_output(symbol, base_path="/mnt/d/ats-data/minute-bars/firstrate"):
    """Check if symbol has output data"""
    symbol_path = Path(base_path) / symbol
    if symbol_path.exists():
        files = list(symbol_path.glob("*.parquet"))
        return len(files)
    return 0

def main():
    print("🔍 Comprehensive FirstRate Backfill Status Monitor")
    print("=" * 70)
    print(f"⏰ Status as of: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 1. Check original long-running processes
    print("📊 ORIGINAL BACKFILL PROCESSES:")
    print("-" * 40)
    
    original_processes = [
        {'pid': 303766, 'name': 'AAPL Gap Fill', 'symbols': ['AAPL'], 'checkpoint': 'firstrate_aapl_missing.json'},
        {'pid': 405359, 'name': 'Priority Stocks', 'symbols': ['MSFT','GOOGL','GOOG','AMZN','META','TSLA','NVDA','ADBE','JPM','BAC','WFC','GS','MS','C','JNJ','PFE','ABT','MRK','PG','KO','PEP','WMT','HD','NKE','XOM','CVX','COP','GE','V','MA','DIS','NFLX','PYPL','COST','TMUS','AVGO','UNH'], 'checkpoint': 'stock_backfill_priority_batch.json'}
    ]
    
    for proc in original_processes:
        info = get_process_info(proc['pid'])
        print(f"  {proc['name']} (PID {proc['pid']}):")
        print(f"    Status: {info['status']}")
        print(f"    CPU: {info['cpu']}%")
        print(f"    Runtime: {info['runtime']}")
        print(f"    Symbols: {len(proc['symbols'])} ({proc['symbols'][0]} to {proc['symbols'][-1]})")
        print(f"    Checkpoint: {proc['checkpoint']}")
        
        # Check data output for first symbol
        if proc['symbols']:
            files = check_data_output(proc['symbols'][0])
            print(f"    Data files: {files} parquet files for {proc['symbols'][0]}")
        print()

    # 2. Check Docker parallel workers
    print("🐳 DOCKER PARALLEL WORKERS:")
    print("-" * 40)
    
    launch_info_file = Path("docker_parallel_launch_info.json")
    if launch_info_file.exists():
        with open(launch_info_file, 'r') as f:
            launch_info = json.load(f)
        
        launched_at = datetime.fromisoformat(launch_info['launched_at'])
        total_runtime = datetime.now() - launched_at
        
        print(f"  Launched: {launched_at.strftime('%H:%M:%S')} ({total_runtime.total_seconds()/60:.1f} min ago)")
        print(f"  Total symbols: {launch_info['total_symbols']}")
        print(f"  Workers: {launch_info['num_workers']}")
        print()
        
        running_workers = 0
        total_files = 0
        
        for worker in launch_info['workers']:
            info = get_process_info(worker['pid'])
            print(f"  Worker {worker['worker_id']} (PID {worker['pid']}):")
            print(f"    Status: {info['status']}")
            print(f"    CPU: {info['cpu']}%")
            print(f"    Runtime: {info['runtime']}")
            print(f"    Symbols: {worker['symbols_count']} ({worker['symbols'][0]} to {worker['symbols'][-1]})")
            
            # Check data output
            worker_files = 0
            for symbol in worker['symbols'][:3]:  # Check first 3 symbols
                files = check_data_output(symbol)
                worker_files += files
                if files > 0:
                    print(f"      {symbol}: {files} files")
            
            total_files += worker_files
            if info['status'] == 'running':
                running_workers += 1
            print()
        
        print(f"  Summary: {running_workers}/{launch_info['num_workers']} workers running, {total_files} data files created")
        
    else:
        print("  No Docker parallel launch info found")

    print()

    # 3. Overall system status
    print("🖥️  SYSTEM OVERVIEW:")
    print("-" * 40)
    
    # Get all FirstRate processes
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        firstrate_processes = []
        for line in result.stdout.split('\n'):
            if 'populate_firstrate' in line and 'grep' not in line:
                firstrate_processes.append(line)
        
        print(f"  Total FirstRate processes: {len(firstrate_processes)}")
        
        # Calculate total CPU usage
        total_cpu = 0.0
        for line in firstrate_processes:
            parts = line.split()
            if len(parts) >= 3:
                try:
                    cpu = float(parts[2])
                    total_cpu += cpu
                except:
                    pass
        
        print(f"  Total CPU usage: {total_cpu:.1f}%")
        print()
        
    except Exception as e:
        print(f"  Error getting system info: {e}")

    # 4. Data output summary
    print("📁 DATA OUTPUT STATUS:")
    print("-" * 40)
    
    base_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
    if base_path.exists():
        symbol_dirs = [d for d in base_path.iterdir() if d.is_dir()]
        print(f"  Symbol directories: {len(symbol_dirs)}")
        
        total_parquet_files = 0
        for symbol_dir in symbol_dirs:
            parquet_files = list(symbol_dir.glob("*.parquet"))
            total_parquet_files += len(parquet_files)
        
        print(f"  Total parquet files: {total_parquet_files:,}")
        print(f"  Storage location: {base_path}")
    else:
        print(f"  Data directory not found: {base_path}")
    
    print()
    print("✅ Status check complete!")

if __name__ == "__main__":
    main()