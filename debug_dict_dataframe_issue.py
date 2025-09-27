#!/usr/bin/env python3
"""
Debug the dict/DataFrame compatibility issue in get_minute_ohlc_batch.

Trace the exact data flow from FirstRate adapter to understand where the 'dict' object error occurs.
"""

import sys
import asyncio
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')

async def debug_firstrate_adapter():
    """Test FirstRate adapter directly to see what it returns."""
    print("🔍 Testing FirstRate adapter directly...")
    
    from core.market_data.unified_manager import FirstRateAdapter, TimeframeType
    
    # Initialize FirstRate adapter
    adapter = FirstRateAdapter("/mnt/d/ats-data/minute-bars/firstrate")
    
    # Test with AAPL for July 1, 2025
    symbols = ["AAPL"]
    start_date = datetime(2025, 7, 1, 14, 1, 0)
    end_date = datetime(2025, 7, 1, 14, 1, 59)
    
    print(f"   Calling get_ohlcv({symbols}, {start_date}, {end_date})")
    
    # Call get_ohlcv directly
    result = await adapter.get_ohlcv(symbols, start_date, end_date, TimeframeType.MINUTE_1)
    
    print(f"✅ FirstRate adapter result type: {type(result)}")
    print(f"   Keys: {list(result.keys())}")
    
    for symbol, data in result.items():
        print(f"   {symbol}: type={type(data)}, shape={getattr(data, 'shape', 'N/A')}")
        if hasattr(data, 'empty'):
            print(f"   {symbol}: empty={data.empty}")
        if hasattr(data, 'columns'):
            print(f"   {symbol}: columns={list(data.columns)}")
        if not data.empty if hasattr(data, 'empty') else False:
            print(f"   {symbol}: sample row:\n{data.iloc[0] if hasattr(data, 'iloc') else 'No iloc'}")
            
    return result
    
async def debug_unified_manager():
    """Test UnifiedMarketDataManager get_minute_ohlc_batch method."""
    print("\n🔍 Testing UnifiedMarketDataManager get_minute_ohlc_batch...")
    
    from core.market_data.unified_manager import (
        UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
    )
    
    # Create config for FirstRate
    config = MarketDataConfig(
        vendors=[VendorType.FIRSTRATE],
        storage_backend=StorageBackend.FILE,
        file_storage_path="/mnt/d/ats-data/minute-bars/firstrate"
    )
    
    # Initialize manager
    manager = UnifiedMarketDataManager(config)
    
    # Test with AAPL for July 1, 2025
    symbols = ["AAPL"]
    start_date = datetime(2025, 7, 1, 14, 1, 0)
    end_date = datetime(2025, 7, 1, 14, 1, 59)
    
    print(f"   Calling get_minute_ohlc_batch({symbols}, {start_date}, {end_date})")
    
    # Call get_minute_ohlc_batch
    result = await manager.get_minute_ohlc_batch(symbols, start_date, end_date)
    
    print(f"✅ get_minute_ohlc_batch result type: {type(result)}")
    print(f"   Keys: {list(result.keys())}")
    
    for symbol, data in result.items():
        print(f"   {symbol}: type={type(data)}")
        if data is not None:
            print(f"   {symbol}: value={data}")
            
    return result
    
async def debug_get_ohlcv_call():
    """Debug the specific get_ohlcv call that's failing."""
    print("\n🔍 Testing UnifiedMarketDataManager get_ohlcv directly...")
    
    from core.market_data.unified_manager import (
        UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend, TimeframeType
    )
    
    # Create config for FirstRate
    config = MarketDataConfig(
        vendors=[VendorType.FIRSTRATE],
        storage_backend=StorageBackend.FILE,
        file_storage_path="/mnt/d/ats-data/minute-bars/firstrate"
    )
    
    # Initialize manager
    manager = UnifiedMarketDataManager(config)
    
    # Test with AAPL for July 1, 2025
    symbols = ["AAPL"]
    start_date = datetime(2025, 7, 1, 14, 1, 0)
    end_date = datetime(2025, 7, 1, 14, 1, 59)
    
    print(f"   Calling get_ohlcv({symbols}, {start_date}, {end_date}, TimeframeType.MINUTE_1)")
    
    # Call get_ohlcv directly
    ohlcv_data = await manager.get_ohlcv(symbols, start_date, end_date, TimeframeType.MINUTE_1)
    
    print(f"✅ get_ohlcv result type: {type(ohlcv_data)}")
    print(f"   Keys: {list(ohlcv_data.keys())}")
    
    for symbol in symbols:
        if symbol in ohlcv_data:
            data = ohlcv_data[symbol]
            print(f"   {symbol}: type={type(data)}")
            print(f"   {symbol}: hasattr empty={hasattr(data, 'empty')}")
            
            if hasattr(data, 'empty'):
                print(f"   {symbol}: empty={data.empty}")
            else:
                print(f"   ❌ {symbol}: NO EMPTY ATTRIBUTE - this is the issue!")
            
            if hasattr(data, 'shape'):
                print(f"   {symbol}: shape={data.shape}")
            if hasattr(data, 'columns'):
                print(f"   {symbol}: columns={list(data.columns)}")
                
    return ohlcv_data
    
async def main():
    """Main debug function."""
    print("🚀 Debugging dict/DataFrame compatibility issue")
    print("=" * 60)
    
    # Test FirstRate adapter directly
    firstrate_result = await debug_firstrate_adapter()
    
    # Test get_ohlcv method
    ohlcv_result = await debug_get_ohlcv_call()
    
    # Test get_minute_ohlc_batch method
    batch_result = await debug_unified_manager()
    
    print("\n" + "=" * 60)
    print("🔍 SUMMARY:")
    print(f"   FirstRate adapter works: {'✅' if firstrate_result else '❌'}")
    print(f"   get_ohlcv works: {'✅' if ohlcv_result else '❌'}")
    print(f"   get_minute_ohlc_batch works: {'✅' if batch_result else '❌'}")

if __name__ == "__main__":
    asyncio.run(main())