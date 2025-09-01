#!/usr/bin/env python3
import os
from pathlib import Path

print("🔍 Searching for AAPL data...")

# Search in common data locations
search_paths = [
    "/data", 
    "/workspace/data",
    "/backup",
    "/logs"
]

for base_path in search_paths:
    if not Path(base_path).exists():
        print(f"❌ {base_path} does not exist")
        continue
        
    print(f"🔍 Searching in {base_path}...")
    
    # Look for AAPL directories
    for root, dirs, files in os.walk(base_path):
        if "AAPL" in dirs:
            aapl_path = Path(root) / "AAPL"
            print(f"✅ Found AAPL directory: {aapl_path}")
            
            # Check for parquet files
            try:
                parquet_files = list(aapl_path.rglob("*.parquet"))
                print(f"  📊 Contains {len(parquet_files)} parquet files")
                
                if parquet_files:
                    # Show first few files
                    for i, file in enumerate(parquet_files[:3]):
                        relative_path = file.relative_to(aapl_path)
                        file_size = file.stat().st_size
                        print(f"    📄 {relative_path} ({file_size:,} bytes)")
                    if len(parquet_files) > 3:
                        print(f"    ... and {len(parquet_files)-3} more files")
            except Exception as e:
                print(f"  ❌ Error checking parquet files: {e}")
                
        # Also check for "AAPL" in file/directory names
        for item in dirs + files:
            if "AAPL" in item.upper():
                item_path = Path(root) / item
                print(f"📄 Found AAPL-related item: {item_path}")

print("🔍 Search complete!")