#!/usr/bin/env python3
"""
Test Parallel FirstRate Backfill Launcher
Tests the simple background process approach with a small batch
"""

import subprocess
import time

def test_parallel_backfill():
    """Test parallel backfill with 8 symbols split into 2 workers"""

    print("🧪 Test Parallel FirstRate Backfill Launcher")
    print("=" * 50)

    # Small test batch - 8 symbols, 2 workers
    test_symbols = [
        'IBM', 'INTC', 'CSCO', 'ORCL',  # Worker 0
        'CRM', 'QCOM', 'TXN', 'AMAT'   # Worker 1
    ]

    # Split into 2 batches
    batch_1 = test_symbols[:4]
    batch_2 = test_symbols[4:]
    batches = [batch_1, batch_2]

    print(f"📊 Test Configuration:")
    print(f"   Total symbols: {len(test_symbols)}")
    print(f"   Parallel workers: {len(batches)}")
    print(f"   Worker 0: {batch_1}")
    print(f"   Worker 1: {batch_2}")

    # Launch workers
    launched_processes = []

    for i, batch in enumerate(batches):
        symbols_str = ",".join(batch)
        checkpoint_file = f"test_worker_{i}_checkpoint.json"
        log_file = f"/tmp/test_worker_{i}.log"

        # Use the exact command pattern that works
        cmd = [
            'nohup', 'python3', 'scripts/populate_firstrate_minute_bars.py',
            '--asset-type', 'stock',
            '--symbols', symbols_str,
            '--checkpoint-file', checkpoint_file,
            '--debug'
        ]

        print(f"\n🚀 Launching Test Worker {i}...")
        print(f"   Symbols: {batch}")
        print(f"   Command: {' '.join(cmd)}")

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

    print(f"\n🎉 Test launched {len(launched_processes)} parallel workers!")
    print("\n📋 Monitoring Commands:")

    for proc_info in launched_processes:
        print(f"\nTest Worker {proc_info['worker_id']} (PID {proc_info['pid']}):")
        print(f"   Status: ps -p {proc_info['pid']}")
        print(f"   Log: tail -f {proc_info['log_file']}")

    print(f"\n🔍 Quick Status Check:")
    print(f"   ps aux | grep populate_firstrate")

    # Wait 30 seconds and check status
    print("\n⏱️  Waiting 30 seconds to check initial status...")
    time.sleep(30)

    # Check if processes are still running
    for proc_info in launched_processes:
        pid = proc_info['pid']
        try:
            # Check if process is still running
            result = subprocess.run(['ps', '-p', str(pid)], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Test Worker {proc_info['worker_id']} (PID {pid}) is running")

                # Show first few lines of log
                try:
                    with open(proc_info['log_file'], 'r') as f:
                        lines = f.readlines()
                        if lines:
                            print(f"   Log preview: {lines[-1].strip()}")
                        else:
                            print(f"   Log: No output yet")
                except:
                    print(f"   Log: Cannot read log file")
            else:
                print(f"❌ Test Worker {proc_info['worker_id']} (PID {pid}) not running")

                # Show error from log
                try:
                    with open(proc_info['log_file'], 'r') as f:
                        content = f.read()
                        if content:
                            print(f"   Error: {content[-200:]}")  # Last 200 chars
                except:
                    print(f"   Error: Cannot read log file")

        except Exception as e:
            print(f"❌ Error checking Test Worker {proc_info['worker_id']}: {e}")

    return launched_processes

if __name__ == "__main__":
    test_parallel_backfill()