#!/usr/bin/env python3
"""
Debug FirstRate data availability
"""

import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from domains.market_data.services.agent.firstrate_adapter import FirstRateAdapter

def debug_firstrate():
    """Debug FirstRate data paths"""
    
    print("🔍 Debugging FirstRate data access...")
    
    # Check if data path exists
    data_path = Path("/mnt/d/ats-data/firstrate-data")
    print(f"📁 Data path exists: {data_path.exists()}")
    
    if data_path.exists():
        print(f"📂 Contents: {list(data_path.iterdir())}")
        
        stock_path = data_path / "stock"
        if stock_path.exists():
            zip_files = list(stock_path.glob("*.zip"))
            print(f"📦 Stock ZIP files: {len(zip_files)}")
            
            if zip_files:
                print(f"📄 Sample files: {[f.name for f in zip_files[:3]]}")
            else:
                print("❌ No ZIP files found in stock directory")
        else:
            print("❌ Stock directory does not exist")
    else:
        # Check Docker mount point
        docker_data_path = Path("/data/firstrate-data")  
        print(f"🐳 Docker data path exists: {docker_data_path.exists()}")
        
        if docker_data_path.exists():
            print(f"📂 Docker contents: {list(docker_data_path.iterdir())}")
        else:
            print("❌ No FirstRate data found in Docker container")
    
    # Initialize adapter and test
    try:
        adapter = FirstRateAdapter("/data/firstrate-data")  # Docker path
        zip_files = adapter.get_available_zip_files('stock')
        print(f"✅ Adapter found {len(zip_files)} ZIP files")
        
        if zip_files:
            first_zip = zip_files[0]
            symbols = adapter.extract_symbols_from_zip(first_zip)
            print(f"📊 Sample ZIP {first_zip.name} contains {len(symbols)} symbols")
            if symbols:
                print(f"🔸 Sample symbols: {symbols[:5]}")
        
    except Exception as e:
        print(f"❌ Adapter initialization failed: {e}")

if __name__ == "__main__":
    debug_firstrate()