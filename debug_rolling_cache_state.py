#!/usr/bin/env python3

"""Debug script to examine rolling cache state during training data generation."""

import asyncio
import gin
from datetime import datetime, timedelta
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from core.config.environment import Environment
import logging

async def debug_rolling_cache():
    """Check the rolling cache state to understand what timeframes have data."""
    
    # Setup environment
    env = Environment(env_type="intg", db_url="postgresql://postgres:intg_password@localhost:4432/intg_db")
    
    # Create UniverseStateManager
    universe_state_manager = UniverseStateManager(env)
    
    # Add some debug logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    print("🔍 Rolling Cache Debug Script")
    print("=" * 50)
    
    # Get cache debug info
    debug_info = universe_state_manager.get_rolling_cache_debug_info()
    
    print(f"🎯 Rolling cache timeframes: {list(debug_info.keys())}")
    
    for timeframe, info in debug_info.items():
        print(f"\n📊 Timeframe: {timeframe}")
        print(f"   Instruments: {info.get('instrument_count', 0)}")
        
        instruments_info = info.get('instruments', {})
        for inst_id, inst_info in instruments_info.items():
            print(f"   Instrument {inst_id}: {inst_info.get('interval_count', 0)} intervals")
            
            # Show sample data if available
            if inst_info.get('interval_count', 0) > 0:
                sample = inst_info.get('latest_interval')
                if sample:
                    print(f"     Latest: {sample.get('datetime')} O={sample.get('open')} H={sample.get('high')} L={sample.get('low')} C={sample.get('close')}")
    
    print("\n" + "=" * 50)
    print("✅ Rolling cache debug complete")

if __name__ == "__main__":
    asyncio.run(debug_rolling_cache())