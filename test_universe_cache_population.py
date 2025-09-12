#!/usr/bin/env python3
"""
Test script to verify universe state cache population works correctly.
"""

import sys
sys.path.append('src')

import asyncio
from datetime import datetime
import gin
from core.platform.config.environment import Environment, EnvironmentType
from domains.trading.services.state.universe_state_manager import UniverseStateManager


async def test_universe_cache_population():
    """Test universe state cache population during SOD."""
    print("🧪 Testing universe state cache population...")
    
    # Load gin config first
    gin.parse_config_file('config/training_data.gin')
    
    # Create environment
    env = Environment(EnvironmentType.INTEGRATION)
    
    # Create UniverseStateManager
    universe_manager = UniverseStateManager(env=env)
    
    print(f"📁 UniverseStateManager initialized")
    print(f"   states_dir: {universe_manager.states_dir}")
    print(f"   Initial cache size: {len(universe_manager._cache) if hasattr(universe_manager, '_cache') else 'No cache'}")
    print(f"   Initial instrument history: {len(universe_manager._instrument_history) if hasattr(universe_manager, '_instrument_history') else 'No history'}")
    
    # Call update_for_sod to populate cache
    current_time = datetime(2025, 7, 1, 14, 0)
    print(f"\n🔄 Calling update_for_sod at {current_time}...")
    
    universe_manager.update_for_sod(None, current_time)
    
    print(f"\n📊 After SOD cache population:")
    print(f"   Cache size: {len(universe_manager._cache)}")
    print(f"   Instrument history: {len(universe_manager._instrument_history)} instruments")
    
    if universe_manager._instrument_history:
        for instrument_id, history in universe_manager._instrument_history.items():
            print(f"   Instrument {instrument_id}: {len(history)} historical records")
            if history:
                print(f"      Sample record: {history[0]}")
                break
    
    # Test get_lag_prices
    if universe_manager._instrument_history:
        sample_instrument_id = list(universe_manager._instrument_history.keys())[0]
        print(f"\n🔍 Testing get_lag_prices for instrument {sample_instrument_id}...")
        
        try:
            lag_data = universe_manager.get_lag_prices(sample_instrument_id, current_time, 1)
            print(f"✅ get_lag_prices returned {len(lag_data)} records")
            print(f"   Columns: {list(lag_data.columns)}")
            if not lag_data.empty:
                print(f"   Sample data: {lag_data.iloc[0].to_dict()}")
        except Exception as e:
            print(f"❌ get_lag_prices failed: {e}")
    else:
        print("❌ No instrument history populated")


if __name__ == "__main__":
    asyncio.run(test_universe_cache_population())