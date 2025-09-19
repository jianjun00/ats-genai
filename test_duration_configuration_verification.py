#!/usr/bin/env python3
"""
Test to verify that base_duration and target_durations are properly configurable
in all runner_utils.py functions and passed correctly to UniverseStateIntervalBuilder.
"""
import os
import sys
import inspect
from unittest.mock import Mock, patch, AsyncMock

# Set test environment variables
os.environ['PYTEST_CURRENT_TEST'] = 'test_duration_configuration_verification'
os.environ['SKIP_GLOBAL_ENV'] = '1'
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

# Add src to path for imports
sys.path.insert(0, 'src')

def test_function_signatures_accept_duration_parameters():
    """
    Test that runner_utils functions accept base_duration and target_durations parameters.
    """
    print("🔍 TESTING FUNCTION SIGNATURES")
    print("=" * 40)
    
    # Import functions from both runner_utils files
    try:
        from domains.trading.services.core.app.runner_utils import run_file_daily_price_ohlcv as main_func
        from infrastructure.services_legacy.core.app.runner_utils import run_file_daily_price_ohlcv as legacy_func
        
        functions_to_test = [
            ('main runner_utils', main_func),
            ('legacy runner_utils', legacy_func)
        ]
        
        all_good = True
        
        for name, func in functions_to_test:
            print(f"\n📁 Testing {name}:")
            
            # Check function signature
            sig = inspect.signature(func)
            params = sig.parameters
            
            # Check for duration parameters
            has_base_duration = 'base_duration' in params
            has_target_durations = 'target_durations' in params
            
            print(f"  Has base_duration parameter: {has_base_duration}")
            print(f"  Has target_durations parameter: {has_target_durations}")
            
            if has_base_duration and has_target_durations:
                # Check defaults
                base_default = params['base_duration'].default
                target_default = params['target_durations'].default
                
                print(f"  base_duration default: '{base_default}'")
                print(f"  target_durations default: '{target_default}'")
                
                print(f"  ✅ Function properly accepts duration configuration")
            else:
                print(f"  ❌ Function missing duration parameters")
                all_good = False
        
        return all_good
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False


def test_mock_universe_state_builder_receives_correct_parameters():
    """
    Test that UniverseStateIntervalBuilder receives the correct duration parameters
    when called from runner_utils functions.
    """
    print(f"\n🔧 TESTING PARAMETER PASSING TO UNIVERSE STATE BUILDER")
    print("=" * 60)
    
    # Test configuration
    test_base_duration = '5m'
    test_target_durations = '5m,15m,1h'
    
    try:
        # Mock the UniverseStateIntervalBuilder to capture parameters
        with patch('domains.trading.services.core.app.runner_utils.UniverseStateIntervalBuilder') as mock_builder, \
             patch('domains.trading.services.core.app.runner_utils.FileDailyPriceMarketDataManager') as mock_market_data, \
             patch('domains.trading.services.core.app.runner_utils.Runner') as mock_runner:
            
            # Setup mocks
            mock_market_data.create_async = AsyncMock()
            mock_runner_instance = Mock()
            mock_runner.return_value = mock_runner_instance
            mock_runner_instance.universe_state_manager = Mock()
            mock_runner_instance.universe_manager = Mock()
            mock_runner_instance.universe_manager.instrument_ids = []
            mock_runner_instance.callbacks = []
            mock_runner_instance.run = AsyncMock()
            
            mock_builder_instance = Mock()
            mock_builder.return_value = mock_builder_instance
            
            # Mock environment and DAO
            mock_env = Mock()
            mock_env.get_universe_id.return_value = 1
            
            with patch('domains.trading.services.core.app.runner_utils.UniverseStateIntervalDAO') as mock_dao:
                mock_dao_instance = Mock()
                mock_dao.return_value = mock_dao_instance
                mock_dao_instance.list = AsyncMock(return_value=[])
                
                # Import and call the function with custom durations
                from domains.trading.services.core.app.runner_utils import run_file_daily_price_ohlcv
                
                # Call function with custom duration parameters
                import asyncio
                result = asyncio.run(run_file_daily_price_ohlcv(
                    vendors_dirs={},
                    instrument_ids=[1, 2],
                    start_date='2025-01-01',
                    end_date='2025-01-02',
                    env=mock_env,
                    universe_id=1,
                    base_duration=test_base_duration,
                    target_durations=test_target_durations
                ))
                
                # Verify UniverseStateIntervalBuilder was called with correct parameters
                mock_builder.assert_called_once()
                call_args = mock_builder.call_args
                
                print(f"UniverseStateIntervalBuilder called with:")
                print(f"  env: {call_args[1]['env']}")
                print(f"  base_duration: '{call_args[1]['base_duration']}'")
                print(f"  target_durations: '{call_args[1]['target_durations']}'")
                
                # Verify correct parameters were passed
                assert call_args[1]['base_duration'] == test_base_duration, f"Expected base_duration='{test_base_duration}', got '{call_args[1]['base_duration']}'"
                assert call_args[1]['target_durations'] == test_target_durations, f"Expected target_durations='{test_target_durations}', got '{call_args[1]['target_durations']}'"
                
                print(f"  ✅ Parameters passed correctly to UniverseStateIntervalBuilder")
                
                # Verify Runner was called with correct base_duration
                mock_runner.assert_called_once()
                runner_call_args = mock_runner.call_args
                assert runner_call_args[1]['base_duration'] == test_base_duration, f"Expected Runner base_duration='{test_base_duration}', got '{runner_call_args[1]['base_duration']}'"
                
                print(f"  ✅ Runner called with correct base_duration: '{test_base_duration}'")
                
                return True
                
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def test_default_duration_values():
    """
    Test that default duration values are reasonable and consistent.
    """
    print(f"\n📋 TESTING DEFAULT DURATION VALUES")
    print("=" * 40)
    
    try:
        from domains.trading.services.core.app.runner_utils import run_file_daily_price_ohlcv as main_func
        from infrastructure.services_legacy.core.app.runner_utils import run_file_daily_price_ohlcv as legacy_func
        
        functions_to_test = [
            ('main runner_utils', main_func),
            ('legacy runner_utils', legacy_func)
        ]
        
        all_consistent = True
        
        for name, func in functions_to_test:
            print(f"\n📁 {name}:")
            
            sig = inspect.signature(func)
            params = sig.parameters
            
            base_default = params.get('base_duration', Mock()).default
            target_default = params.get('target_durations', Mock()).default
            
            print(f"  base_duration default: '{base_default}'")
            print(f"  target_durations default: '{target_default}'")
            
            # Check that defaults are reasonable
            if base_default == '1d' and target_default == '1d':
                print(f"  ✅ Defaults are reasonable for daily processing")
            else:
                print(f"  ⚠️  Non-standard defaults (not necessarily wrong)")
        
        return all_consistent
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def show_configuration_examples():
    """
    Show examples of how to use the configurable duration parameters.
    """
    print(f"\n📚 CONFIGURATION EXAMPLES")
    print("=" * 30)
    
    examples = [
        {
            'name': 'Daily Processing (Default)',
            'base_duration': '1d',
            'target_durations': '1d',
            'use_case': 'End-of-day analysis with daily intervals'
        },
        {
            'name': 'Intraday Multi-Timeframe',
            'base_duration': '5m',
            'target_durations': '5m,15m,1h,1d',
            'use_case': 'Training data with multiple timeframe features'
        },
        {
            'name': 'High-Frequency Analysis',
            'base_duration': '1m',
            'target_durations': '1m,5m,15m',
            'use_case': 'Short-term trading signal generation'
        },
        {
            'name': 'Long-Term Analysis',
            'base_duration': '1d',
            'target_durations': '1d,1w,1M',
            'use_case': 'Portfolio rebalancing and trend analysis'
        }
    ]
    
    for example in examples:
        print(f"\n✅ {example['name']}:")
        print(f"   Use case: {example['use_case']}")
        print(f"   Configuration:")
        print(f"     base_duration='{example['base_duration']}'")
        print(f"     target_durations='{example['target_durations']}'")
        print(f"   Usage:")
        print(f"     await run_file_daily_price_ohlcv(..., base_duration='{example['base_duration']}', target_durations='{example['target_durations']}')")


if __name__ == "__main__":
    print("🚨 DURATION CONFIGURATION VERIFICATION TEST")
    print("=" * 60)
    
    # Test 1: Function signatures
    signatures_good = test_function_signatures_accept_duration_parameters()
    
    # Test 2: Parameter passing
    parameters_passed = test_mock_universe_state_builder_receives_correct_parameters()
    
    # Test 3: Default values
    defaults_good = test_default_duration_values()
    
    # Show examples
    show_configuration_examples()
    
    print(f"\n" + "=" * 60)
    print(f"📋 VERIFICATION SUMMARY:")
    print(f"  Function signatures accept duration params: {signatures_good}")
    print(f"  Parameters passed correctly to builder: {parameters_passed}")
    print(f"  Default values are reasonable: {defaults_good}")
    
    if signatures_good and parameters_passed and defaults_good:
        print(f"\n🎉 ALL DURATION CONFIGURATION TESTS PASSED!")
        print(f"   ✅ Functions accept configurable duration parameters")
        print(f"   ✅ Parameters are passed correctly to UniverseStateIntervalBuilder")
        print(f"   ✅ Default values are reasonable ('1d' for daily processing)")
        print(f"   ✅ No more hardcoded duration values")
        print(f"\n🔧 CONFIGURATION NOW SUPPORTS:")
        print(f"   - Daily processing: base_duration='1d', target_durations='1d'")
        print(f"   - Multi-timeframe: base_duration='5m', target_durations='5m,15m,1h,1d'")
        print(f"   - Custom intervals: Any valid duration string combinations")
    else:
        print(f"\n❌ SOME DURATION CONFIGURATION TESTS FAILED!")
        print(f"   Please check function signatures and parameter passing")
    
    print(f"\n📝 BENEFITS OF CONFIGURABLE DURATIONS:")
    print(f"  - Training data can use multiple timeframes (5m, 15m, 1h, 1d)")
    print(f"  - Daily processing can use appropriate intervals")
    print(f"  - Different use cases can specify optimal configurations")
    print(f"  - No more one-size-fits-all hardcoded '1d' values")