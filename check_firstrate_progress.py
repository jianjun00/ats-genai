#!/usr/bin/env python3
"""
Check FirstRate backfill progress
"""

import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter

def check_progress():
    """Check what symbols are available vs processed"""
    
    print("📊 Checking FirstRate backfill progress...")
    
    # Initialize adapter
    adapter = FirstRateAdapter("/data/firstrate-data")
    
    # Get available symbols
    inventory = adapter.get_symbol_inventory('stock')
    available_symbols = set(inventory.keys())
    
    print(f"🔍 Available symbols: {len(available_symbols)}")
    
    # Check processed symbols on disk
    output_path = Path("/data/minute-bars/firstrate")
    processed_symbols = set()
    
    if output_path.exists():
        for symbol_dir in output_path.iterdir():
            if symbol_dir.is_dir() and not symbol_dir.name.startswith('.'):
                processed_symbols.add(symbol_dir.name)
    
    print(f"✅ Already processed: {len(processed_symbols)}")
    
    # Find remaining symbols
    remaining_symbols = available_symbols - processed_symbols
    print(f"⏳ Remaining to process: {len(remaining_symbols)}")
    
    if remaining_symbols:
        print(f"🎯 Sample remaining: {list(remaining_symbols)[:10]}")
    
    # Show some progress stats
    if processed_symbols:
        print(f"\n📈 Progress: {len(processed_symbols)}/{len(available_symbols)} ({100*len(processed_symbols)/len(available_symbols):.1f}%)")
        
        # Count total files
        total_files = 0
        for symbol_dir in output_path.iterdir():
            if symbol_dir.is_dir():
                files = list(symbol_dir.rglob("*.parquet"))
                total_files += len(files)
                
        print(f"📄 Total parquet files: {total_files:,}")
    
    return {
        'available': len(available_symbols),
        'processed': len(processed_symbols), 
        'remaining': len(remaining_symbols),
        'remaining_symbols': list(remaining_symbols)
    }

if __name__ == "__main__":
    result = check_progress()