#!/usr/bin/env python3
"""
Restart FirstRate minute bar backfill for missing ETF symbols
"""

import subprocess
import sys
import os

# Set working directory
os.chdir('/workspace')

# Execute the backfill script
cmd = [
    'python3', 
    'scripts/populate_firstrate_minute_bars.py',
    '--symbols', 'SPY,QQQ,IWM,DXY,TLT,USO',
    '--checkpoint-file', 'firstrate_monthly_production.json', 
    '--resume',
    '--debug'
]

# Set environment
env = os.environ.copy()
env['PYTHONPATH'] = '/workspace/src'

print(f"🚀 Starting FirstRate backfill for: SPY, QQQ, IWM, DXY, TLT, USO")
print(f"📝 Using checkpoint: firstrate_monthly_production.json")
print(f"🔄 Resume mode enabled")

try:
    result = subprocess.run(cmd, env=env, check=True)
    print("✅ FirstRate backfill completed successfully")
except subprocess.CalledProcessError as e:
    print(f"❌ FirstRate backfill failed: {e}")
    sys.exit(1)