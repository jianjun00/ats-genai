#!/usr/bin/env python3
"""
Test to verify that base_duration and target_durations are configurable,
not hardcoded in runner_utils.py functions.

This test ensures that UniverseStateIntervalBuilder uses proper configuration
instead of hardcoded '1d' values.
"""
import os
import sys
from unittest.mock import Mock, patch, AsyncMock
import pytest

# Set test environment variables
os.environ['PYTEST_CURRENT_TEST'] = 'test_duration_configuration'
os.environ['SKIP_GLOBAL_ENV'] = '1'
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

# Add src to path for imports
sys.path.insert(0, 'src')

def test_runner_utils_accepts_configurable_durations():
    """
    Test that runner_utils.py functions should accept configurable durations
    instead of hardcoding '1d' values.
    """
    print("🔍 TESTING DURATION CONFIGURATION")
    print("=" * 45)
    
    from domains.trading.services.core.app.runner_utils import run_file_daily_price_ohlcv
    import inspect
    
    # Check function signature
    sig = inspect.signature(run_file_daily_price_ohlcv)
    params = list(sig.parameters.keys())
    
    print(f"Current function parameters: {params}")
    
    # Check if duration parameters are configurable
    has_base_duration = 'base_duration' in params
    has_target_durations = 'target_durations' in params
    
    print(f"Has base_duration parameter: {has_base_duration}")
    print(f"Has target_durations parameter: {has_target_durations}")
    
    if not has_base_duration and not has_target_durations:
        print(f"\n❌ PROBLEM IDENTIFIED:")
        print(f"   Function lacks configurable duration parameters")
        print(f"   This means UniverseStateIntervalBuilder uses hardcoded '1d' values")
        print(f"   Should accept base_duration and target_durations parameters")
        return False
    else:
        print(f"\n✅ FUNCTION PROPERLY CONFIGURABLE:")
        print(f"   Function accepts duration configuration parameters")
        return True


async def test_universe_state_builder_uses_provided_durations():
    """
    Test that UniverseStateIntervalBuilder uses provided durations
    instead of hardcoded values.
    """
    print(f"\n🔧 TESTING UNIVERSE STATE BUILDER CONFIGURATION")
    print("=" * 55)
    
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from core.platform.config_env.environment import Environment, EnvironmentType
    
    # Mock environment
    mock_env = Mock()
    mock_env.env_type = EnvironmentType.TEST
    
    # Test different duration configurations
    test_cases = [
        {'base': '5m', 'targets': '5m,15m,1h'},
        {'base': '1h', 'targets': '1h,1d'},
        {'base': '1d', 'targets': '1d'},
    ]
    
    for i, case in enumerate(test_cases):
        print(f"\nTest case {i+1}: base='{case['base']}', targets='{case['targets']}'")
        
        # Create builder with specific durations
        builder = UniverseStateIntervalBuilder(
            env=mock_env,
            base_duration=case['base'],
            target_durations=case['targets']
        )
        
        print(f"  Builder base_duration: {builder.base_duration}")
        print(f"  Builder target_durations: {builder.target_durations}")
        
        # Verify builder accepts and uses the provided durations
        assert builder.base_duration == case['base'], f"Expected base_duration='{case['base']}', got '{builder.base_duration}'"
        
        expected_targets = case['targets'].split(',')
        assert builder.target_durations == expected_targets, f"Expected target_durations={expected_targets}, got {builder.target_durations}"
        
        print(f"  ✅ Configuration accepted correctly")
    
    print(f"\n✅ UNIVERSE STATE BUILDER CONFIGURATION TEST PASSED")
    print(f"   Builder properly accepts and uses provided duration configuration")
    return True


def test_identify_hardcoded_duration_locations():
    """
    Test to identify and document locations where durations are hardcoded
    instead of using configuration.
    """
    print(f"\n🔍 IDENTIFYING HARDCODED DURATION LOCATIONS")
    print("=" * 50)
    
    import ast
    import os
    
    hardcoded_locations = []
    
    # Files to check for hardcoded durations
    files_to_check = [
        'src/domains/trading/services/core/app/runner_utils.py',
    ]
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Look for hardcoded '1d' in UniverseStateIntervalBuilder calls
            if "base_duration='1d'" in content or "target_durations='1d'" in content:
                hardcoded_locations.append(file_path)
                print(f"❌ Found hardcoded durations in: {file_path}")
                
                # Extract relevant lines
                lines = content.split('\n')
                for i, line in enumerate(lines, 1):
                    if 'base_duration=' in line and "'1d'" in line:
                        print(f"   Line {i}: {line.strip()}")
                    if 'target_durations=' in line and "'1d'" in line:
                        print(f"   Line {i}: {line.strip()}")
    
    if hardcoded_locations:
        print(f"\n🚨 HARDCODED DURATION ISSUE IDENTIFIED:")
        print(f"   Files with hardcoded '1d' values: {len(hardcoded_locations)}")
        for loc in hardcoded_locations:
            print(f"   - {loc}")
        print(f"\n💡 SOLUTION NEEDED:")
        print(f"   1. Add base_duration and target_durations parameters to function signature")
        print(f"   2. Pass these parameters to UniverseStateIntervalBuilder")
        print(f"   3. Use configurable defaults instead of hardcoded '1d'")
        return False
    else:
        print(f"\n✅ NO HARDCODED DURATIONS FOUND:")
        print(f"   All UniverseStateIntervalBuilder calls use proper configuration")
        return True


def test_expected_configuration_pattern():
    """
    Test demonstrating the expected configuration pattern that should be used.
    """
    print(f"\n📋 EXPECTED CONFIGURATION PATTERN")
    print("=" * 40)
    
    print(f"Expected function signature:")
    print(f"```python")
    print(f"async def run_file_daily_price_ohlcv(")
    print(f"    vendors_dirs: dict,")
    print(f"    instrument_ids: List[int],")
    print(f"    start_date: str,")
    print(f"    end_date: str,")
    print(f"    env,")
    print(f"    universe_id: int = 1,")
    print(f"    base_duration: str = '1d',          # ← CONFIGURABLE")
    print(f"    target_durations: str = '1d',       # ← CONFIGURABLE") 
    print(f"    output_dir: Optional[str] = None,")
    print(f"    indicator_config=None,")
    print(f"    print_ohlcv: bool = True,")
    print(f"    required_indicators: Optional[List[str]] = None,")
    print(f"):")
    print(f"```")
    
    print(f"\nExpected UniverseStateIntervalBuilder usage:")
    print(f"```python")
    print(f"builder = UniverseStateIntervalBuilder(")
    print(f"    env=env,")
    print(f"    base_duration=base_duration,        # ← USE PARAMETER")
    print(f"    target_durations=target_durations   # ← USE PARAMETER")
    print(f")")
    print(f"```")
    
    print(f"\n✅ CONFIGURATION PATTERN DOCUMENTED")
    return True


if __name__ == "__main__":
    print("🚨 DURATION CONFIGURATION TEST")
    print("=" * 60)
    
    # Test 1: Check if functions accept configurable durations
    configurable = test_runner_utils_accepts_configurable_durations()
    
    # Test 2: Verify UniverseStateIntervalBuilder accepts configuration
    import asyncio
    builder_configurable = asyncio.run(test_universe_state_builder_uses_provided_durations())
    
    # Test 3: Identify hardcoded locations
    no_hardcoded = test_identify_hardcoded_duration_locations()
    
    # Test 4: Document expected pattern
    pattern_documented = test_expected_configuration_pattern()
    
    print(f"\n" + "=" * 60)
    print(f"📋 TEST SUMMARY:")
    print(f"  Function accepts duration config: {configurable}")
    print(f"  Builder accepts duration config: {builder_configurable}")
    print(f"  No hardcoded durations found: {no_hardcoded}")
    print(f"  Expected pattern documented: {pattern_documented}")
    
    if configurable and builder_configurable and no_hardcoded:
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"   Duration configuration is properly implemented")
        print(f"   No hardcoded values found")
    else:
        print(f"\n❌ CONFIGURATION ISSUES FOUND!")
        print(f"   Hardcoded '1d' values need to be made configurable")
        print(f"   Function signatures need duration parameters")
    
    print(f"\n📝 NEXT STEPS IF FAILING:")
    print(f"  1. Add base_duration and target_durations parameters to runner_utils functions")
    print(f"  2. Pass these parameters to UniverseStateIntervalBuilder instead of hardcoded '1d'")
    print(f"  3. Use reasonable defaults (like '1d') but allow override")
    print(f"  4. Follow pattern from training_data_callback_runner.py")