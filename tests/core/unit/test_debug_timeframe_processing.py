"""
Debug test to identify exactly where timeframe processing fails.

This test adds comprehensive debugging to trace the data flow from 
UniverseStateIntervalBuilder through UniverseStateManager to understand
why only 1 duration is processed instead of multiple timeframes.
"""

import pytest
import logging
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import gin
import asyncio

from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from core.platform.config_env.environment import Environment
from core.business.calendars.time_duration import TimeDuration


class TestDebugTimeframeProcessing:
    """Debug tests to identify timeframe processing issues."""

    def setup_method(self):
        """Set up test environment with comprehensive logging."""
        gin.clear_config()
        
        # Enable debug logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        
        # Create mock environment
        self.mock_env = Mock()
        self.mock_env.env_type = "test"
        self.mock_env.get_table_name.return_value = "test_table"
        self.mock_env.get_env_type.return_value = "test"

    def test_debug_universe_state_builder_target_durations(self):
        """Debug: Trace how target_durations are processed in UniverseStateIntervalBuilder."""
        
        print("\n=== DEBUGGING UNIVERSE STATE BUILDER TARGET DURATIONS ===")
        
        with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_universe_manager_class:
            mock_universe_manager = Mock()
            mock_universe_manager_class.return_value = mock_universe_manager
            
            # Configure with multiple timeframes
            target_durations_str = '5m,15m,30m,60m,1d'
            print(f"🔍 Input target_durations_str: '{target_durations_str}'")
            
            # Create builder and debug the parsing
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration='5m',
                target_durations=target_durations_str,
                universe_state_manager=mock_universe_manager
            )
            
            print(f"🔍 Parsed target_durations count: {len(builder.target_durations)}")
            for i, duration in enumerate(builder.target_durations):
                print(f"   [{i}] {duration.get_duration_string()} ({type(duration)})")
            
            # Check base_duration
            print(f"🔍 Base duration: {builder.base_duration}")
            print(f"   Type: {type(builder.base_duration)}")
            if hasattr(builder.base_duration, 'get_duration_string'):
                print(f"   String: {builder.base_duration.get_duration_string()}")
            
            # Check if target_durations includes base_duration
            base_str = builder.base_duration.get_duration_string() if hasattr(builder.base_duration, 'get_duration_string') else str(builder.base_duration)
            target_strings = [d.get_duration_string() for d in builder.target_durations]
            
            print(f"🔍 Base duration '{base_str}' in target_durations: {base_str in target_strings}")
            
            if base_str not in target_strings:
                print("⚠️  POTENTIAL ISSUE: Base duration not in target_durations list")
                print("   This might cause only base duration to be processed")

    def test_debug_universe_state_manager_duration_processing(self):
        """Debug: Trace how durations are processed in UniverseStateManager."""
        
        print("\n=== DEBUGGING UNIVERSE STATE MANAGER DURATION PROCESSING ===")
        
        # Create real UniverseStateManager to debug actual behavior
        universe_manager = UniverseStateManager()
        
        # Create test data for multiple durations
        test_time = datetime(2025, 7, 1, 14, 0)
        durations = [TimeDuration('5m'), TimeDuration('15m'), TimeDuration('30m'), TimeDuration('60m')]
        
        print(f"🔍 Testing with {len(durations)} durations:")
        for duration in durations:
            print(f"   - {duration.get_duration_string()}")
        
        # Create duration_to_state dict like UniverseStateIntervalBuilder would
        duration_to_state = {}
        
        for duration in durations:
            # Mock UniverseState object with all required attributes
            mock_universe_state = Mock()
            mock_universe_state.instrument_intervals = {
                1: Mock(  # instrument_id = 1
                    instrument_id=1,
                    start_date_time=test_time,
                    end_date_time=duration.get_end_time(test_time),
                    open=208.00 + (durations.index(duration) * 0.01),  # Slightly different values
                    high=208.10 + (durations.index(duration) * 0.01),
                    low=207.90 + (durations.index(duration) * 0.01),
                    close=208.05 + (durations.index(duration) * 0.01),
                    traded_volume=1000 * (durations.index(duration) + 1),
                    traded_dollar=200000 * (durations.index(duration) + 1),
                    status='ok'
                )
            }
            # Add empty instrument_indicator_intervals to avoid the error
            mock_universe_state.instrument_indicator_intervals = {}
            duration_to_state[duration] = mock_universe_state
            
            print(f"🔍 Created state for {duration.get_duration_string()}: OHLCV = "
                  f"({208.00 + (durations.index(duration) * 0.01)}, "
                  f"{208.10 + (durations.index(duration) * 0.01)}, "
                  f"{207.90 + (durations.index(duration) * 0.01)}, "
                  f"{208.05 + (durations.index(duration) * 0.01)}, "
                  f"{1000 * (durations.index(duration) + 1)})")
        
        print(f"\n🔍 duration_to_state contains {len(duration_to_state)} entries")
        print(f"🔍 duration_to_state keys: {[d.get_duration_string() for d in duration_to_state.keys()]}")
        
        # Call addUniverseStateInterval and observe behavior
        print(f"\n🔍 Calling addUniverseStateInterval...")
        
        # Patch the print statement in addUniverseState to capture debug output
        with patch('builtins.print') as mock_print:
            universe_manager.addUniverseStateInterval(duration_to_state, test_time)
            
            # Check what was printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            usm_debug_calls = [call for call in print_calls if 'USM.addUniverseState' in call]
            
            print(f"🔍 Debug output captured:")
            for call in usm_debug_calls:
                print(f"   {call}")
            
            # Extract durations count from debug output
            for call in usm_debug_calls:
                if 'durations=' in call:
                    import re
                    match = re.search(r'durations=(\d+)', call)
                    if match:
                        actual_durations = int(match.group(1))
                        print(f"🔍 Actual durations processed: {actual_durations}")
                        print(f"🔍 Expected durations: {len(duration_to_state)}")
                        
                        if actual_durations != len(duration_to_state):
                            print(f"❌ MISMATCH: Expected {len(duration_to_state)} durations, got {actual_durations}")
                        else:
                            print(f"✅ MATCH: Processed all {actual_durations} durations")

    def test_debug_async_vs_sync_processing(self):
        """Debug: Check if async/sync issues affect duration processing."""
        
        print("\n=== DEBUGGING ASYNC VS SYNC PROCESSING ===")
        
        universe_manager = UniverseStateManager()
        test_time = datetime(2025, 7, 1, 14, 0)
        
        # Create minimal test data
        duration = TimeDuration('5m')
        mock_universe_state = Mock()
        mock_universe_state.instrument_intervals = {
            1: Mock(
                instrument_id=1,
                start_date_time=test_time,
                end_date_time=duration.get_end_time(test_time),
                open=208.00, high=208.10, low=207.90, close=208.05,
                traded_volume=1000, traded_dollar=200000
            )
        }
        duration_to_state = {duration: mock_universe_state}
        
        print(f"🔍 Testing both addUniverseState (async) and addUniverseStateInterval (sync)")
        
        # Test async version directly
        print(f"\n🔍 Testing addUniverseState (async) directly...")
        async def test_async():
            with patch('builtins.print') as mock_print:
                await universe_manager.addUniverseState(duration_to_state, test_time)
                return [str(call) for call in mock_print.call_args_list]
        
        async_output = asyncio.run(test_async())
        usm_calls = [call for call in async_output if 'USM.addUniverseState' in call]
        print(f"   Async output: {usm_calls}")
        
        print(f"\n🔍 Testing addUniverseStateInterval (sync wrapper)...")
        with patch('builtins.print') as mock_print:
            universe_manager.addUniverseStateInterval(duration_to_state, test_time)
            sync_output = [str(call) for call in mock_print.call_args_list]
        
        usm_calls = [call for call in sync_output if 'USM.addUniverseState' in call]
        print(f"   Sync output: {usm_calls}")
        
    def test_debug_duration_filtering_logic(self):
        """Debug: Check if there's filtering logic that reduces durations."""
        
        print("\n=== DEBUGGING DURATION FILTERING LOGIC ===")
        
        universe_manager = UniverseStateManager()
        
        # Test with various duration combinations
        test_cases = [
            ['5m'],
            ['5m', '15m'],
            ['5m', '15m', '30m'],
            ['5m', '15m', '30m', '60m'],
            ['5m', '15m', '30m', '60m', '1d'],
            ['1d'],  # Only daily
            ['60m', '1d'],  # Only longer timeframes
        ]
        
        for i, duration_strings in enumerate(test_cases):
            print(f"\n🔍 Test case {i+1}: {duration_strings}")
            
            test_time = datetime(2025, 7, 1, 14, 0)
            duration_to_state = {}
            
            for dur_str in duration_strings:
                duration = TimeDuration(dur_str)
                mock_universe_state = Mock()
                mock_universe_state.instrument_intervals = {
                    1: Mock(
                        instrument_id=1,
                        start_date_time=test_time,
                        end_date_time=duration.get_end_time(test_time),
                        open=208.00, high=208.10, low=207.90, close=208.05,
                        traded_volume=1000, traded_dollar=200000
                    )
                }
                duration_to_state[duration] = mock_universe_state
            
            print(f"   Input: {len(duration_to_state)} durations")
            
            with patch('builtins.print') as mock_print:
                universe_manager.addUniverseStateInterval(duration_to_state, test_time)
                
                debug_calls = [str(call) for call in mock_print.call_args_list if 'USM.addUniverseState' in str(call)]
                
                for call in debug_calls:
                    if 'durations=' in call:
                        import re
                        match = re.search(r'durations=(\d+)', call)
                        if match:
                            processed = int(match.group(1))
                            print(f"   Output: {processed} durations processed")
                            
                            if processed != len(duration_to_state):
                                print(f"   ❌ FILTERING DETECTED: {len(duration_to_state)} → {processed}")
                            else:
                                print(f"   ✅ NO FILTERING: {len(duration_to_state)} → {processed}")
                            
    def test_debug_real_training_data_callback_flow(self):
        """Debug: Simulate the real training data callback flow."""
        
        print("\n=== DEBUGGING REAL TRAINING DATA CALLBACK FLOW ===")
        
        with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_universe_manager_class:
            mock_universe_manager = Mock()
            mock_universe_manager_class.return_value = mock_universe_manager
            
            # Create builder like in training data generation
            target_durations_str = '5m,15m,30m,60m,1d'
            base_duration_str = '5m'
            
            print(f"🔍 Creating UniverseStateIntervalBuilder:")
            print(f"   base_duration: '{base_duration_str}'")
            print(f"   target_durations: '{target_durations_str}'")
            
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration=base_duration_str,
                target_durations=target_durations_str,
                universe_state_manager=mock_universe_manager
            )
            
            # Debug the actual flow that happens in training data generation
            test_time = datetime(2025, 7, 1, 14, 0)
            
            print(f"\n🔍 Simulating callback flow:")
            print(f"   Current time: {test_time}")
            print(f"   Base duration: {builder.base_duration.get_duration_string()}")
            print(f"   Target durations: {[d.get_duration_string() for d in builder.target_durations]}")
            
            # Check if the issue is in how durations are prepared for UniverseStateManager
            print(f"\n🔍 Checking duration preparation logic...")
            
            # This would be called in on_interval_complete
            print(f"   builder.target_durations type: {type(builder.target_durations)}")
            print(f"   builder.target_durations length: {len(builder.target_durations)}")
            
            # Check if there's any filtering in the builder itself
            if hasattr(builder, 'target_durations'):
                for i, duration in enumerate(builder.target_durations):
                    print(f"   target_durations[{i}]: {duration} ({type(duration)})")
                    if hasattr(duration, 'get_duration_string'):
                        print(f"      String representation: '{duration.get_duration_string()}'")
            
            # The key question: How does UniverseStateIntervalBuilder pass durations to UniverseStateManager?
            print(f"\n🔍 Key question: How are durations passed to UniverseStateManager?")
            print(f"   This requires examining the on_interval_complete method in UniverseStateIntervalBuilder")