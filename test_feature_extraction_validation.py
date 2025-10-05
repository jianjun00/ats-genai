#!/usr/bin/env python3
"""
Simple validation test for feature extraction configuration.

This test validates that the gin configuration and core classes
work correctly without requiring a full database setup.
"""

import gin
import logging
from pathlib import Path

def test_gin_configuration():
    """Test that gin configuration loads without errors."""
    print("🔧 Testing gin configuration loading...")
    
    try:
        gin.clear_config()
        # Import classes before gin configuration
        from domains.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
        from domains.trading.services.indicators_core.indicator_builder import IndicatorBuilder
        gin.parse_config_file('config/feature_extraction.gin')
        print("✅ Gin configuration loaded successfully")
        return True
    except Exception as e:
        print(f"❌ Gin configuration failed: {e}")
        return False

def test_training_data_config():
    """Test TrainingDataConfig creation."""
    print("🔧 Testing TrainingDataConfig creation...")
    
    try:
        from domains.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
        config = TrainingDataConfig()
        print(f"✅ TrainingDataConfig created:")
        print(f"   - base_interval_minutes: {config.base_interval_minutes}")
        print(f"   - feature_types: {config.feature_types}")
        return True
    except Exception as e:
        print(f"❌ TrainingDataConfig creation failed: {e}")
        return False

def test_indicator_builder():
    """Test IndicatorBuilder creation."""
    print("🔧 Testing IndicatorBuilder creation...")
    
    try:
        from domains.trading.services.indicators_core.indicator_builder import IndicatorBuilder
        builder = IndicatorBuilder()
        signals = builder.get_signal_names()
        print(f"✅ IndicatorBuilder created with {len(signals)} signals:")
        print(f"   - First 5 signals: {signals[:5]}")
        return True
    except Exception as e:
        print(f"❌ IndicatorBuilder creation failed: {e}")
        return False

def test_universe_state_builder():
    """Test UniverseStateIntervalBuilder creation."""
    print("🔧 Testing UniverseStateIntervalBuilder creation...")
    
    try:
        from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
        from core.platform.config_env.environment import Environment, EnvironmentType
        
        env = Environment(env_type=EnvironmentType.DEV)
        builder = UniverseStateIntervalBuilder(
            env=env,
            base_duration='5m',
            target_durations='5m,15m,60m,1d'
        )
        print(f"✅ UniverseStateIntervalBuilder created:")
        print(f"   - Base duration: {builder.base_duration}")
        print(f"   - Target durations: {len(builder.target_durations)} timeframes")
        return True
    except Exception as e:
        print(f"❌ UniverseStateIntervalBuilder creation failed: {e}")
        return False

def test_runner_creation():
    """Test Runner creation with minimal parameters."""
    print("🔧 Testing Runner creation...")
    
    try:
        from domains.trading.services.core.app.runner import Runner
        from core.platform.config_env.environment import Environment, EnvironmentType
        
        env = Environment(env_type=EnvironmentType.DEV)
        runner = Runner(
            start_date='2024-08-01',
            end_date='2024-08-01',
            environment=env,
            universe_id=1,
            callbacks=[],
            base_duration='5m'
        )
        print(f"✅ Runner created successfully:")
        print(f"   - Start date: {runner.start_date}")
        print(f"   - End date: {runner.end_date}")
        print(f"   - Environment: {runner.env}")
        return True
    except Exception as e:
        print(f"❌ Runner creation failed: {e}")
        return False

def main():
    """Run all validation tests."""
    print("🚀 Feature Extraction Configuration Validation")
    print("=" * 50)
    
    # Set up basic logging to suppress warnings
    logging.basicConfig(level=logging.ERROR)
    
    tests = [
        test_gin_configuration,
        test_training_data_config,
        test_indicator_builder,
        test_universe_state_builder,
        test_runner_creation
    ]
    
    results = []
    for test_func in tests:
        print()
        result = test_func()
        results.append(result)
    
    print()
    print("=" * 50)
    print("🎯 Validation Results:")
    
    passed = sum(results)
    total = len(results)
    
    for i, (test_func, result) in enumerate(zip(tests, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {i+1}. {test_func.__name__}: {status}")
    
    print()
    if passed == total:
        print(f"🎉 ALL TESTS PASSED ({passed}/{total})")
        print("✅ Feature extraction configuration is working correctly!")
        return 0
    else:
        print(f"❌ SOME TESTS FAILED ({passed}/{total})")
        print("🔧 Please check the configuration and fix any issues.")
        return 1

if __name__ == '__main__':
    exit(main())