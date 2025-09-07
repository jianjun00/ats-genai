#!/usr/bin/env python3
"""
Quick check of data paths in Docker container
"""

import os
from pathlib import Path

def main():
    print("🔍 Checking data paths in container...")

    # Check container data paths
    data_paths = [
        "/data",
        "/data/firstrate-data",
        "/data/firstrate-data/stock",
        "/mnt/d/ats-data",
        "/mnt/d/ats-data/firstrate-data",
        "/mnt/d/ats-data/firstrate-data/stock"
    ]

    for path in data_paths:
        path_obj = Path(path)
        if path_obj.exists():
            if path_obj.is_dir():
                files = list(path_obj.iterdir())[:5]  # First 5 items
                print(f"✅ {path}: Directory exists with {len(list(path_obj.iterdir()))} items")
                if files:
                    print(f"   Sample files: {[f.name for f in files]}")
            else:
                print(f"✅ {path}: File exists")
        else:
            print(f"❌ {path}: Does not exist")

    print("\n🔍 Checking for zip files...")
    zip_patterns = [
        "/data/firstrate-data/stock/stock_*.zip",
        "/mnt/d/ats-data/firstrate-data/stock/stock_*.zip"
    ]

    for pattern in zip_patterns:
        path_obj = Path(pattern).parent
        if path_obj.exists():
            zip_files = list(path_obj.glob("stock_*.zip"))
            if zip_files:
                print(f"✅ Found {len(zip_files)} zip files in {path_obj}")
                for zf in zip_files[:5]:
                    size_gb = zf.stat().st_size / 1024**3
                    print(f"   {zf.name}: {size_gb:.1f} GB")
            else:
                print(f"❌ No zip files found in {path_obj}")
        else:
            print(f"❌ Directory does not exist: {path_obj}")

if __name__ == "__main__":
    main()