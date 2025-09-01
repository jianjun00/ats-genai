#!/usr/bin/env python3
"""
Find FirstRate data directories and files
"""
import os
from pathlib import Path

def find_firstrate_data():
    """Find FirstRate data locations"""
    search_paths = [
        "/data",
        "/mnt/d/ats-data",
        "/mnt/d",
        "/home/jianjun"
    ]
    
    found_locations = []
    
    for base_path in search_paths:
        try:
            if os.path.exists(base_path):
                print(f"Searching in {base_path}...")
                for root, dirs, files in os.walk(base_path):
                    # Look for firstrate directories
                    if "firstrate" in root.lower():
                        found_locations.append(root)
                        print(f"Found directory: {root}")
                    
                    # Look for FirstRate-style zip files
                    for file in files:
                        if file.endswith('.zip') and any(keyword in file.lower() for keyword in ['1min', 'stock', 'etf', 'fx']):
                            zip_path = os.path.join(root, file)
                            found_locations.append(zip_path)
                            print(f"Found zip file: {zip_path}")
                            
                    # Limit search depth to avoid timeout
                    if len(root.split(os.sep)) > 6:
                        dirs.clear()  # Don't recurse deeper
                        
        except PermissionError:
            print(f"Permission denied accessing {base_path}")
        except Exception as e:
            print(f"Error searching {base_path}: {e}")
    
    return found_locations

if __name__ == "__main__":
    print("🔍 Searching for FirstRate data...")
    locations = find_firstrate_data()
    
    if locations:
        print(f"\n✅ Found {len(locations)} FirstRate-related locations:")
        for loc in locations[:20]:  # Show first 20
            print(f"  {loc}")
    else:
        print("\n❌ No FirstRate data found")
        
    # Check if we're in Docker and /data is mounted
    if os.path.exists("/data"):
        print(f"\n📁 /data directory exists (Docker mount)")
        try:
            data_contents = os.listdir("/data")
            print(f"Contents: {data_contents[:10]}")
        except:
            print("Cannot list /data contents")
    else:
        print("\n❌ /data directory not found (not in Docker)")