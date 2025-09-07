#!/usr/bin/env python3
"""
Restart Polygon minute bar backfill from checkpoint with fixes applied.
"""

import os
import subprocess
import sys

def main():
    """Restart the Polygon minute bar backfill."""
    print("🚀 Restarting Polygon minute bar backfill from checkpoint...")

    # Set environment variables
    env = os.environ.copy()
    env['POLYGON_API_KEY'] = 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD'
    env['PYTHONPATH'] = '/workspace/src'

    # Command to run
    cmd = [
        'python', 'scripts/populate_30year_polygon_minute_bars.py',
        '--mode', 'full',
        '--start-date', '2022-01-01',
        '--end-date', '2024-12-31',
        '--storage-path', '/mnt/d/ats-data',
        '--checkpoint-file', 'polygon_full_universe_production.json',
        '--resume'
    ]

    print(f"🔧 Running: {' '.join(cmd)}")

    # Run the backfill
    result = subprocess.run(cmd, env=env)

    if result.returncode == 0:
        print("✅ Polygon minute bar backfill restarted successfully!")
    else:
        print(f"❌ Backfill failed with exit code: {result.returncode}")
        sys.exit(result.returncode)

if __name__ == "__main__":
    main()