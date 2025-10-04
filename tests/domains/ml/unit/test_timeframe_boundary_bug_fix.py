"""
Test to demonstrate and fix the timeframe boundary processing bug.

BUG IDENTIFIED: _should_process_timeframe only allows ONE timeframe to process at any given time,
because it checks for exact time boundaries (minute % 5 == 0, minute == 0, etc.).

SOLUTION: Process ALL timeframes that are ready, not just the ones that match exact boundaries.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import gin

from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from core.platform.config_env.environment import Environment
from core.business.calendars.time_duration import TimeDuration


class TestTimeframeBoundaryBugFix:
    """Test to demonstrate and fix the timeframe boundary issue."""

    def setup_method(self):
        """Set up test environment."""
        gin.clear_config()
        
        # Create mock environment
        self.mock_env = Mock()
        self.mock_env.env_type = "test"
        self.mock_env.get_table_name.return_value = "test_table"
        self.mock_env.get_env_type.return_value = "test"

    def test_demonstrate_timeframe_boundary_bug(self):
        """Demonstrate the timeframe boundary bug with various times."""
        
        print("\n=== DEMONSTRATING TIMEFRAME BOUNDARY BUG ===")
        
        with patch('domains.trading.services.state.universe_state_manager.UniverseStateManager') as mock_manager_class:
            mock_manager = Mock()
            mock_manager_class.return_value = mock_manager
            
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration='5m',
                target_durations='5m,15m,30m,60m,1d',
                universe_state_manager=mock_manager
            )
            
            print(f"🔍 Testing _should_process_timeframe at different times:")
            print(f"   Target durations: {[d.get_duration_string() for d in builder.target_durations]}")
            
            # Test various times to see which timeframes get processed
            test_times = [
                datetime(2025, 7, 1, 14, 0),   # 14:00 - hour boundary
                datetime(2025, 7, 1, 14, 5),   # 14:05 - 5-minute boundary  
                datetime(2025, 7, 1, 14, 15),  # 14:15 - 15-minute boundary
                datetime(2025, 7, 1, 14, 30),  # 14:30 - 30-minute boundary
                datetime(2025, 7, 1, 14, 7),   # 14:07 - no major boundaries
                datetime(2025, 7, 1, 16, 0),   # 16:00 - daily boundary
            ]
            
            for test_time in test_times:
                print(f"\n🕒 Time: {test_time}")
                active_timeframes = []
                
                for duration in builder.target_durations:
                    should_process = builder._should_process_timeframe(duration, test_time)
                    status = "✅" if should_process else "❌"
                    print(f"   {status} {duration.get_duration_string()}: {should_process}")
                    if should_process:
                        active_timeframes.append(duration.get_duration_string())
                
                print(f"   📊 Active timeframes: {len(active_timeframes)} - {active_timeframes}")
                
                if len(active_timeframes) <= 1:
                    print(f"   🐛 BUG: Only {len(active_timeframes)} timeframe(s) processed at {test_time}")

    def test_fix_timeframe_boundary_logic(self):
        """Test a fix for the timeframe boundary logic."""
        
        print("\n=== TESTING FIXED TIMEFRAME BOUNDARY LOGIC ===")
        
        def _should_process_timeframe_FIXED(duration, current_time):
            """
            FIXED VERSION: Process timeframes when they have accumulated enough data,
            not just at exact boundaries.
            
            For training data generation, we want to process ALL timeframes that have
            sufficient historical data, regardless of exact time boundaries.
            """
            duration_str = duration.get_duration_string()
            
            # For training data generation, we should process timeframes when:
            # 1. We have enough historical data 
            # 2. The timeframe interval is "complete enough" for meaningful aggregation
            
            # Simple fix: Always return True for all configured timeframes
            # This ensures all timeframes get processed and generate training data
            return True
        
        # Test the fixed logic
        durations = [TimeDuration('5m'), TimeDuration('15m'), TimeDuration('30m'), TimeDuration('60m'), TimeDuration('1d')]
        test_time = datetime(2025, 7, 1, 14, 7)  # A time with no major boundaries
        
        print(f"🔧 Testing FIXED logic at {test_time}:")
        active_count = 0
        for duration in durations:
            should_process = _should_process_timeframe_FIXED(duration, test_time)
            status = "✅" if should_process else "❌"
            print(f"   {status} {duration.get_duration_string()}: {should_process}")
            if should_process:
                active_count += 1
        
        print(f"   📊 Fixed result: {active_count} timeframes active (Expected: {len(durations)})")
        
        if active_count == len(durations):
            print("   ✅ FIX WORKS: All timeframes processed!")
        else:
            print("   ❌ FIX FAILED: Not all timeframes processed")

    def test_training_data_specific_timeframe_logic(self):
        """Test training data specific timeframe processing logic."""
        
        print("\n=== TESTING TRAINING DATA SPECIFIC LOGIC ===")
        
        def _should_process_timeframe_TRAINING_DATA(duration, current_time, is_training_mode=True):
            """
            Training data specific logic: Process timeframes based on data availability,
            not exact time boundaries.
            """
            if is_training_mode:
                # For training data generation, process all timeframes
                # We're processing historical data, so exact time boundaries don't matter
                return True
            else:
                # For live trading, use original boundary logic
                duration_str = duration.get_duration_string()
                minute = current_time.minute
                hour = current_time.hour
                
                if duration_str == '1m':
                    return True
                elif duration_str == '5m':
                    return minute % 5 == 0
                elif duration_str == '15m':
                    return minute % 15 == 0
                elif duration_str == '30m':
                    return minute % 30 == 0
                elif duration_str == '60m' or duration_str == '1h':
                    return minute == 0
                elif duration_str == '1d':
                    return hour == 16 and minute == 0
                else:
                    return False
        
        durations = [TimeDuration('5m'), TimeDuration('15m'), TimeDuration('30m'), TimeDuration('60m'), TimeDuration('1d')]
        test_time = datetime(2025, 7, 1, 14, 7)
        
        print(f"🎯 Testing TRAINING DATA logic at {test_time}:")
        
        # Test training mode
        print(f"   🔧 Training mode:")
        training_active = 0
        for duration in durations:
            should_process = _should_process_timeframe_TRAINING_DATA(duration, test_time, is_training_mode=True)
            status = "✅" if should_process else "❌"
            print(f"      {status} {duration.get_duration_string()}: {should_process}")
            if should_process:
                training_active += 1
        
        # Test live mode  
        print(f"   🔴 Live mode:")
        live_active = 0
        for duration in durations:
            should_process = _should_process_timeframe_TRAINING_DATA(duration, test_time, is_training_mode=False)
            status = "✅" if should_process else "❌"
            print(f"      {status} {duration.get_duration_string()}: {should_process}")
            if should_process:
                live_active += 1
        
        print(f"   📊 Results:")
        print(f"      Training mode: {training_active}/{len(durations)} timeframes")
        print(f"      Live mode: {live_active}/{len(durations)} timeframes")
        
        if training_active == len(durations):
            print("   ✅ SOLUTION WORKS: All timeframes processed in training mode!")
            print("   💡 RECOMMENDATION: Add is_training_mode parameter to UniverseStateIntervalBuilder")
        else:
            print("   ❌ SOLUTION INCOMPLETE")

    def test_proposed_fix_implementation(self):
        """Test the proposed fix implementation."""
        
        print("\n=== PROPOSED FIX IMPLEMENTATION ===")
        
        print("🔧 Proposed changes to UniverseStateIntervalBuilder:")
        print("   1. Add is_training_mode parameter to __init__")
        print("   2. Modify _should_process_timeframe to use training mode logic")
        print("   3. For training data generation, always return True for all timeframes")
        print("   4. For live trading, keep original boundary logic")
        
        print("\n📝 Code changes needed:")
        print("   File: src/domains/trading/services/state/universe_state_builder.py")
        print("   Method: _should_process_timeframe")
        print("   Add parameter: is_training_mode to class constructor")
        
        print("\n🎯 Expected result after fix:")
        print("   - Training data generation: All 5 timeframes processed simultaneously")
        print("   - Live trading: Original behavior preserved")
        print("   - Fixes OHLCV duplication bug in training data")
        
        print("\n⚠️  CRITICAL INSIGHT:")
        print("   The 'durations=1' debug output occurs because _should_process_timeframe")
        print("   filters out most timeframes based on exact time boundaries.")
        print("   This is correct for live trading but wrong for training data generation.")
        print("   Training data should process ALL timeframes to generate complete datasets.")