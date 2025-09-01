#!/usr/bin/env python3
"""
Restart FirstRate minute bar backfill for FX symbols (DXY)
"""

import subprocess
import sys
import os

# Set working directory
os.chdir('/workspace')

# Execute the backfill script with FX asset type
cmd = [
    'python3', 
    'scripts/populate_firstrate_minute_bars.py',
    '--asset-type', 'fx',  # Use FX asset type for DXY
    '--symbols', 'DXY',
    '--checkpoint-file', 'firstrate_fx_production.json',  # Use separate checkpoint
    '--debug'
]

# Set environment
env = os.environ.copy()
env['PYTHONPATH'] = '/workspace/src'

print(f"🚀 Starting FirstRate FX backfill for: DXY")
print(f"📝 Using checkpoint: firstrate_fx_production.json")
print(f"🎯 Asset type: FX")

try:
    result = subprocess.run(cmd, env=env, check=True)
    print("✅ FirstRate FX backfill completed successfully")
except subprocess.CalledProcessError as e:
    print(f"❌ FirstRate FX backfill failed: {e}")
    sys.exit(1)