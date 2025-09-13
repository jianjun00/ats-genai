#!/usr/bin/env python3
"""
Simple test to verify universe state cache population works correctly.
"""

import sys
sys.path.append('src')

from datetime import datetime
from pathlib import Path
from domains.trading.services.state.universe_state_manager import UniverseStateManager


def test_universe_cache_population():
    """Test universe state cache population during SOD."""
    print("🧪 Testing universe state cache population...")

    # Create UniverseStateManager with minimal configuration
    universe_manager = UniverseStateManager(env=None)

    print(f"📁 UniverseStateManager initialized")
    print(f"   states_dir: {universe_manager.states_dir}")
    print(f"   states_dir exists: {universe_manager.states_dir.exists()}")

    # List existing universe state files
    if universe_manager.states_dir.exists():
        state_files = list(universe_manager.states_dir.glob("universe_state_*.parquet"))
        print(f"   Found {len(state_files)} universe state files")
        for f in state_files[:3]:  # Show first 3
            print(f"     {f.name}")

    print(f"   Initial cache size: {len(universe_manager._cache)}")
    print(f"   Initial instrument history: {len(universe_manager._instrument_history)}")

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
    test_universe_cache_population()