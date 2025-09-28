#!/usr/bin/env python3
"""
Debug script to check what timeframes are being built by UniverseStateBuilder
"""
import sys
import os
sys.path.insert(0, 'src')

# Set minimal environment
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PORT'] = '4432'
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'intg_password'
os.environ['DB_NAME'] = 'intg_db'
os.environ['ENVIRONMENT_TYPE'] = 'intg'
os.environ['PYTHONPATH'] = 'src'

import gin
from datetime import datetime
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder

def main():
    print("🔍 DEBUG: Testing UniverseStateBuilder timeframe configuration")
    
    # Load gin config
    print("📋 Loading gin configuration...")
    gin.parse_config_file('config/training_data.gin')
    
    # Check what target_durations are configured
    target_durations = gin.query_parameter('domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.target_durations')
    print(f"✅ Gin target_durations: {target_durations}")
    base_duration = gin.query_parameter('domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.base_duration')
    print(f"✅ Gin base_duration: {base_duration}")
    print("🏗️ Creating UniverseStateIntervalBuilder...")
    builder = gin.get_configurable('domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder')()
    
    # Check what timeframes it will process
    print(f"🎯 Builder target_durations: {getattr(builder, 'target_durations', 'NOT SET')}")
    print(f"🎯 Builder base_duration: {getattr(builder, 'base_duration', 'NOT SET')}")
    
    # Test the boundary logic
    test_times = [
        datetime(2025, 7, 1, 13, 35, 0),  # 13:35 - should only build 5m
        datetime(2025, 7, 1, 13, 45, 0),  # 13:45 - should build 5m and 15m
        datetime(2025, 7, 1, 14, 0, 0),   # 14:00 - should build 5m, 15m, and 60m
    ]
    
    for test_time in test_times:
        print(f"\n⏰ Testing time: {test_time}")
        print(f"   Should process timeframes:")
        
        for duration_str in ['5m', '15m', '60m', '1d']:
            should_process = builder._should_process_timeframe(duration_str, test_time)
            print(f"     {duration_str}: {'✅ YES' if should_process else '❌ NO'}")
    
if __name__ == '__main__':
    main()