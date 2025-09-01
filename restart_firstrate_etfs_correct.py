#!/usr/bin/env python3
"""
Restart FirstRate minute bar backfill for ETF symbols using correct asset type
"""

import subprocess
import sys
import os

# Set working directory
os.chdir('/workspace')

# Execute the backfill script with ETF asset type
cmd = [
    'python3', 
    'scripts/populate_firstrate_minute_bars.py',
    '--asset-type', 'etf',  # Use ETF asset type instead of stock
    '--symbols', 'SPY,QQQ,IWM,TLT,GLD,USO',  # Remove DXY (likely in fx)
    '--checkpoint-file', 'firstrate_etf_production.json',  # Use separate checkpoint
    '--debug'
]

# Set environment
env = os.environ.copy()
env['PYTHONPATH'] = '/workspace/src'

print(f"🚀 Starting FirstRate ETF backfill for: SPY, QQQ, IWM, TLT, GLD, USO")
print(f"📝 Using checkpoint: firstrate_etf_production.json")
print(f"🎯 Asset type: ETF")

try:
    result = subprocess.run(cmd, env=env, check=True)
    print("✅ FirstRate ETF backfill completed successfully")
except subprocess.CalledProcessError as e:
    print(f"❌ FirstRate ETF backfill failed: {e}")
    sys.exit(1)