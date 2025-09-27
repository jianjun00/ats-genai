#!/usr/bin/env python3
import os
from pathlib import Path

print("🔍 Checking Docker container paths:")
paths_to_check = [
    '/data',
    '/workspace',
    '/mnt/d',
    '/mnt/d/ats-data',
    '/mnt/d/ats-data/minute-bars',
    '/workspace/data',
    '/tmp'
]

for path in paths_to_check:
    exists = Path(path).exists()
    status = "✅" if exists else "❌"
    print(f"{status} {path}: {exists}")

    if exists and Path(path).is_dir():
        contents = list(Path(path).iterdir())[:5]  # First 5 items
        if contents:
            print(f"    Contents: {[str(c.name) for c in contents]}")
print("\n🔍 Environment variables:")
env_vars = ['ATS_DATA_PATH', 'ATS_BACKUP_PATH', 'ATS_LOGS_PATH']
for var in env_vars:
    value = os.getenv(var, 'Not set')
    print(f"  {var}: {value}")