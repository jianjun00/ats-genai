#!/usr/bin/env python3
"""
Simple FirstRate test to verify the data processing works.
"""

import sys
import asyncio
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter

async def test_firstrate():
    """Test FirstRate adapter functionality"""
    
    print("🚀 Testing FirstRate adapter...")
    
    # Initialize adapter
    adapter = FirstRateAdapter("/mnt/d/ats-data/firstrate-data")
    
    # Get symbol inventory
    print("📊 Building symbol inventory...")
    inventory = adapter.get_symbol_inventory('stock')
    
    print(f"✅ Found {len(inventory)} symbols")
    
    # Show first few symbols
    symbols = list(inventory.keys())[:3]
    for symbol in symbols:
        info = inventory[symbol]
        print(f"🔸 {symbol}: {info['zip_files']} files, range: {info['date_range']}")
        
        # Test processing a few ticks
        zip_file = Path(info['zip_files'][0])
        tick_count = 0
        
        try:
            for tick in adapter.process_minute_data_from_zip(zip_file, symbol):
                tick_count += 1
                if tick_count == 1:  # Show first tick
                    print(f"   📈 Sample tick: {tick.timestamp} OHLC: ${tick.open:.2f}/${tick.high:.2f}/${tick.low:.2f}/${tick.close:.2f} Vol: {tick.volume:,}")
                if tick_count >= 100:  # Limit to 100 ticks for testing
                    break
                    
            print(f"   ✅ Processed {tick_count} ticks for {symbol}")
            
        except Exception as e:
            print(f"   ❌ Error processing {symbol}: {e}")
    
    print(f"\n🎉 FirstRate test completed!")
    return inventory

if __name__ == "__main__":
    result = asyncio.run(test_firstrate())
    print(f"📊 Total symbols available: {len(result)}")