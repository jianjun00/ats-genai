#!/usr/bin/env python3
"""
Populate missing AAPL minute bar data for identified gaps
"""

import subprocess
import sys
import os

# Set working directory
os.chdir('/workspace')

# Execute the backfill script for AAPL specifically
cmd = [
    'python3', 
    'scripts/populate_firstrate_minute_bars.py',
    '--asset-type', 'stock',
    '--symbols', 'AAPL',
    '--checkpoint-file', 'firstrate_aapl_missing.json',
    '--debug'
]

# Set environment
env = os.environ.copy()
env['PYTHONPATH'] = '/workspace/src'

print(f"🚀 Starting FirstRate backfill for AAPL missing data")
print(f"📝 Using checkpoint: firstrate_aapl_missing.json")
print(f"🎯 Target gaps: 2023-08 to 2023-12, 2025-09")

try:
    result = subprocess.run(cmd, env=env, check=True)
    print("✅ AAPL missing data backfill completed successfully")
except subprocess.CalledProcessError as e:
    print(f"❌ AAPL backfill failed: {e}")
    sys.exit(1)