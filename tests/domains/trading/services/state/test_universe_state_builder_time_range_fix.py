"""
Tests for UniverseStateBuilder time range logic fix.

Verifies that the universe state builder now uses [current_time - base_duration, current_time]
for feature extraction instead of [current_time, current_time + base_duration].
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import pandas as pd

from core.platform.config.environment import Environment
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from core.business.calendars.time_duration import TimeDuration


class TestUniverseStateBuilderTimeRangeFix:
    """Test that universe state builder uses correct time ranges for data fetching."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_env = Mock(spec=Environment)
        self.mock_env.indicator_rolling_window = 20
        
        # Create builder with 60-minute base duration
        self.builder = UniverseStateIntervalBuilder(
            env=self.mock_env,
            base_duration="60m",
            target_durations="60m"
        )
        
        # Setup mock runner
        self.mock_runner = Mock()
        self.mock_runner.universe_id = 1
        self.mock_runner.universe_manager = Mock()
        self.mock_runner.universe_manager.instrument_ids = [9034]  # TSLA
        
        # Setup mock market data manager
        self.mock_runner.market_data_manager = AsyncMock()
        self.mock_runner.market_data_manager.get_minute_ohlc_batch = AsyncMock()
        
        # Setup mock universe state manager
        self.mock_runner.universe_state_manager = AsyncMock()
        self.mock_runner.universe_state_manager.addUniverseState = AsyncMock()

    @pytest.mark.asyncio
    @patch('domains.trading.services.state.universe_state_builder.logging')
    async def test_time_range_fix_basic_logic(self, mock_logging):
        """Test that the time range fix uses correct past data range."""
        current_time = datetime(2025, 7, 1, 15, 0, 0)  # 3:00 PM
        
        # Mock market data response with real TSLA data
        mock_ohlc_data = {
            'TSLA': pd.DataFrame({
                'timestamp': [current_time],
                'open': [250.0],
                'high': [255.0], 
                'low': [245.0],
                'close': [252.0],
                'volume': [100000]
            })
        }
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.return_value = mock_ohlc_data
        
        # Mock market cap data
        with patch.object(self.builder.market_cap_dao, 'list_market_caps_for_date') as mock_market_cap:
            mock_market_cap.return_value = [{'instrument_id': 9034, 'market_cap': 800_000_000_000}]
            
            # Call handleInterval
            await self.builder.handleInterval(self.mock_runner, current_time)
        
        # Verify the market data manager was called with CORRECT time range
        # Should be [current_time - 60m, current_time] = [2:00 PM, 3:00 PM]
        expected_start = datetime(2025, 7, 1, 14, 0, 0)  # 2:00 PM (past data)
        expected_end = datetime(2025, 7, 1, 15, 0, 0)    # 3:00 PM (current time)
        
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.assert_called_once_with(
            ['TSLA'], expected_start, expected_end
        )

    @pytest.mark.asyncio
    @patch('domains.trading.services.state.universe_state_builder.logging')
    async def test_time_range_fix_different_durations(self, mock_logging):
        """Test time range fix works with different base durations."""
        test_cases = [
            ("5m", datetime(2025, 7, 1, 15, 0, 0), datetime(2025, 7, 1, 14, 55, 0)),   # 5 min back
            ("15m", datetime(2025, 7, 1, 15, 0, 0), datetime(2025, 7, 1, 14, 45, 0)),  # 15 min back
            ("60m", datetime(2025, 7, 1, 15, 0, 0), datetime(2025, 7, 1, 14, 0, 0)),   # 60 min back
        ]
        
        for duration_str, current_time, expected_start in test_cases:
            # Create builder with specific duration
            builder = UniverseStateIntervalBuilder(
                env=self.mock_env,
                base_duration=duration_str,
                target_durations=duration_str
            )
            
            # Reset mock
            self.mock_runner.market_data_manager.get_minute_ohlc_batch.reset_mock()
            
            # Mock empty market data response
            mock_empty_data = {'TSLA': pd.DataFrame()}
            self.mock_runner.market_data_manager.get_minute_ohlc_batch.return_value = mock_empty_data
            
            # Mock market cap data
            with patch.object(builder.market_cap_dao, 'list_market_caps_for_date') as mock_market_cap:
                mock_market_cap.return_value = [{'instrument_id': 9034, 'market_cap': 800_000_000_000}]
                
                # Call handleInterval
                await builder.handleInterval(self.mock_runner, current_time)
            
            # Verify correct time range was used
            self.mock_runner.market_data_manager.get_minute_ohlc_batch.assert_called_once_with(
                ['TSLA'], expected_start, current_time
            )

    @pytest.mark.asyncio
    @patch('domains.trading.services.state.universe_state_builder.logging')
    async def test_instrument_interval_uses_correct_time_range(self, mock_logging):
        """Test that created InstrumentInterval objects use the correct time range."""
        current_time = datetime(2025, 7, 1, 15, 30, 0)  # 3:30 PM
        expected_start = datetime(2025, 7, 1, 14, 30, 0)  # 2:30 PM (60m back)
        
        # Mock market data response with real data
        mock_ohlc_data = {
            'TSLA': pd.DataFrame({
                'timestamp': [current_time],
                'open': [250.0],
                'high': [255.0],
                'low': [245.0],
                'close': [252.0],
                'volume': [100000]
            })
        }
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.return_value = mock_ohlc_data
        
        # Mock market cap data
        with patch.object(self.builder.market_cap_dao, 'list_market_caps_for_date') as mock_market_cap:
            mock_market_cap.return_value = [{'instrument_id': 9034, 'market_cap': 800_000_000_000}]
            
            # Call handleInterval
            await self.builder.handleInterval(self.mock_runner, current_time)
        
        # Verify InstrumentInterval was created with correct time range
        # Check the instrument_history to see if interval has correct start/end times
        assert 9034 in self.builder.instrument_history
        interval = self.builder.instrument_history[9034][-1]  # Latest interval
        
        assert interval.start_date_time == expected_start  # ✅ Should be past time
        assert interval.end_date_time == current_time      # ✅ Should be current time
        assert interval.open == 250.0
        assert interval.close == 252.0

    @pytest.mark.asyncio
    async def test_time_range_fix_prevents_future_data_access(self):
        """Test that the fix prevents accessing future data."""
        current_time = datetime(2025, 7, 1, 14, 0, 0)  # 2:00 PM
        
        # The OLD LOGIC would have tried to fetch [2:00 PM, 3:00 PM] (includes future)
        # The NEW LOGIC should fetch [1:00 PM, 2:00 PM] (only past data)
        
        expected_start = datetime(2025, 7, 1, 13, 0, 0)  # 1:00 PM (past)
        expected_end = datetime(2025, 7, 1, 14, 0, 0)    # 2:00 PM (current)
        
        # Mock empty response
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.return_value = {'TSLA': pd.DataFrame()}
        
        # Mock market cap data
        with patch.object(self.builder.market_cap_dao, 'list_market_caps_for_date') as mock_market_cap:
            mock_market_cap.return_value = []
            
            # Call handleInterval
            await self.builder.handleInterval(self.mock_runner, current_time)
        
        # Verify that we're requesting PAST data, not future data
        call_args = self.mock_runner.market_data_manager.get_minute_ohlc_batch.call_args
        actual_symbols, actual_start, actual_end = call_args[0]
        
        # Verify start time is in the past
        assert actual_start == expected_start
        assert actual_start < current_time, "Start time should be before current time"
        
        # Verify end time is current time (not future)
        assert actual_end == expected_end
        assert actual_end == current_time, "End time should be current time"
        
        # Verify we're not accessing future data
        assert actual_end <= current_time, "Should not access future data"

    @pytest.mark.asyncio
    @patch('domains.trading.services.state.universe_state_builder.logging')
    async def test_time_range_debug_logging(self, mock_logging):
        """Test that debug logging shows the fixed time range."""
        current_time = datetime(2025, 7, 1, 16, 0, 0)  # 4:00 PM
        
        # Mock empty data
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.return_value = {'TSLA': pd.DataFrame()}
        
        with patch.object(self.builder.market_cap_dao, 'list_market_caps_for_date') as mock_market_cap:
            mock_market_cap.return_value = []
            
            # Capture print output
            with patch('builtins.print') as mock_print:
                await self.builder.handleInterval(self.mock_runner, current_time)
        
        # Verify debug logging shows the fixed time range
        print_calls = [call.args[0] for call in mock_print.call_args_list]
        
        # Look for the debug message about fixed time range
        time_range_msg = next((msg for msg in print_calls if "FIXED TIME RANGE" in msg), None)
        assert time_range_msg is not None, "Should log the fixed time range"
        
        # Verify it shows the correct past time range
        expected_start = datetime(2025, 7, 1, 15, 0, 0)  # 3:00 PM (past)
        expected_end = datetime(2025, 7, 1, 16, 0, 0)    # 4:00 PM (current)
        
        assert str(expected_start) in time_range_msg
        assert str(expected_end) in time_range_msg
        assert "past data for features" in time_range_msg

    @pytest.mark.asyncio
    async def test_multiple_instruments_use_same_time_range(self):
        """Test that all instruments use the same corrected time range."""
        current_time = datetime(2025, 7, 1, 15, 0, 0)
        expected_start = datetime(2025, 7, 1, 14, 0, 0)
        
        # Setup multiple instruments
        self.mock_runner.universe_manager.instrument_ids = [9034, 1234, 5678]  # Multiple IDs
        
        # Mock market data for multiple instruments
        mock_ohlc_data = {
            'TSLA': pd.DataFrame({'open': [250.0], 'close': [252.0]}),
            # Only TSLA has data, others will be empty
        }
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.return_value = mock_ohlc_data
        
        with patch.object(self.builder.market_cap_dao, 'list_market_caps_for_date') as mock_market_cap:
            mock_market_cap.return_value = []
            
            await self.builder.handleInterval(self.mock_runner, current_time)
        
        # Verify that get_minute_ohlc_batch was called once with the correct time range
        # (all instruments are processed in one batch call)
        self.mock_runner.market_data_manager.get_minute_ohlc_batch.assert_called_once_with(
            ['TSLA'],  # Only TSLA has symbol mapping in hardcoded mapping
            expected_start,
            current_time
        )


class TestTimeRangeFixProblemReproduction:
    """Test to reproduce and verify the original time range problem is fixed."""

    def test_original_problem_reproduction(self):
        """Test the original problem: old logic used future time ranges."""
        current_time = datetime(2025, 7, 1, 14, 0, 0)  # 2:00 PM
        base_duration = TimeDuration("60m")
        
        # OLD LOGIC (wrong): [current_time, current_time + base_duration]
        old_start = current_time
        old_end = base_duration.get_end_time(current_time)
        
        # This was the problem: looking at future data [2:00 PM, 3:00 PM]
        assert old_start == datetime(2025, 7, 1, 14, 0, 0)
        assert old_end == datetime(2025, 7, 1, 15, 0, 0)  # Future data!
        assert old_end > current_time, "Old logic looked at future data"

    def test_fixed_behavior_verification(self):
        """Test the fixed behavior: new logic uses past time ranges."""
        current_time = datetime(2025, 7, 1, 14, 0, 0)  # 2:00 PM
        base_duration = TimeDuration("60m")
        
        # NEW LOGIC (correct): [current_time - base_duration, current_time]
        new_start = base_duration.get_start_time(current_time)
        new_end = current_time
        
        # This is the fix: looking at past data [1:00 PM, 2:00 PM]
        assert new_start == datetime(2025, 7, 1, 13, 0, 0)  # Past data ✅
        assert new_end == datetime(2025, 7, 1, 14, 0, 0)    # Current time ✅
        assert new_start < new_end <= current_time, "New logic looks at past data only"

    def test_zero_values_problem_fix(self):
        """Test that the fix should resolve the zero values problem."""
        # This was the original issue:
        # - Training data generator processed at 1:00 AM UTC
        # - Old logic: fetch [1:00 AM UTC, 2:00 AM UTC] (future/non-market hours)
        # - Result: No data found -> zero values
        
        problem_time_utc = datetime(2025, 7, 1, 1, 0, 0)  # 1:00 AM UTC (problematic time)
        base_duration = TimeDuration("60m")
        
        # OLD LOGIC would fetch [1:00 AM UTC, 2:00 AM UTC] - no market data
        old_start = problem_time_utc  
        old_end = base_duration.get_end_time(problem_time_utc)
        assert old_start == datetime(2025, 7, 1, 1, 0, 0)  
        assert old_end == datetime(2025, 7, 1, 2, 0, 0)    # Still no market data
        
        # NEW LOGIC fetches [12:00 AM UTC, 1:00 AM UTC] - still no market data
        # BUT with trading hours filter, this interval won't be processed at all!
        new_start = base_duration.get_start_time(problem_time_utc)
        new_end = problem_time_utc
        assert new_start == datetime(2025, 7, 1, 0, 0, 0)   # 12:00 AM UTC
        assert new_end == datetime(2025, 7, 1, 1, 0, 0)     # 1:00 AM UTC
        
        # The real fix is the combination of:
        # 1. Trading hours filter (prevents processing at 1:00 AM UTC)  
        # 2. Correct time range logic (looks at past data when processing does occur)
        assert new_start < new_end <= problem_time_utc

    def test_market_hours_data_availability(self):
        """Test that during market hours, past data should be available."""
        # Market hours example: 3:00 PM EDT = 19:00 UTC (during EDT)
        market_time_utc = datetime(2025, 7, 1, 19, 0, 0)  # 3:00 PM EDT 
        base_duration = TimeDuration("60m")
        
        # NEW LOGIC: fetch [18:00 UTC, 19:00 UTC] = [2:00 PM EDT, 3:00 PM EDT]
        new_start = base_duration.get_start_time(market_time_utc)
        new_end = market_time_utc
        
        assert new_start == datetime(2025, 7, 1, 18, 0, 0)  # 18:00 UTC = 2:00 PM EDT
        assert new_end == datetime(2025, 7, 1, 19, 0, 0)    # 19:00 UTC = 3:00 PM EDT
        
        # Both times are during market hours (9:35 AM - 4:00 PM EDT = 13:35-20:00 UTC)
        # So data should be available in this range
        assert 13 <= new_start.hour <= 20, "Start time should be during market hours"
        assert 13 <= new_end.hour <= 20, "End time should be during market hours"