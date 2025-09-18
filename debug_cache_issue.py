#!/usr/bin/env python3
"""
Quick test to debug cache synchronization issue.
"""

import os
os.environ.update({
    'DB_HOST': 'localhost',
    'DB_PORT': '4432', 
    'DB_USER': 'postgres',
    'DB_PASSWORD': 'intg_password',
    'DB_NAME': 'intg_db',
    'ENVIRONMENT_TYPE': 'intg',
    'PYTHONPATH': 'src'
})

import sys
sys.path.insert(0, 'src')

from datetime import datetime
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from core.shared.data_handling.utils.environment import Environment

def test_cache_issue():
    print("🔍 Testing cache synchronization issue...")
    
    # Create environment and universe state manager
    env = Environment()
    universe_manager = UniverseStateManager(env=env)
    
    print(f"📊 Rolling cache contents: {universe_manager._rolling_instrument_history}")
    
    # Test the get_universe_state_interval method directly
    test_time = datetime(2025, 7, 1, 14, 0, 0)
    timeframe = "5m"
    
    print(f"🔍 Testing get_universe_state_interval({timeframe}, {test_time})")
    
    result = universe_manager.get_universe_state_interval(timeframe, test_time)
    
    print(f"✅ Result: {result}")

if __name__ == "__main__":
    test_cache_issue()