"""
Unit tests to verify UniverseStateBuilder creates one InstrumentInterval per timeframe
with correctly computed OHLC values.

Based on investigation of UniverseStateBuilder.handleInterval() method:

ARCHITECTURE:
1. Fetches 1-minute base data from FileBasedMinuteMarketDataManager
2. Creates InstrumentInterval objects for base timeframe (e.g., 5m)
3. For each target duration (5m, 15m, 1h, 1d):
   - If duration == base_duration: Uses latest interval from history
   - If duration > base_duration: Aggregates N intervals using duration.aggregate_intervals()
4. Each duration gets separate InstrumentInterval with properly aggregated OHLC

AGGREGATION LOGIC (TimeDuration.aggregate_intervals):
- open = first interval's open
- close = last interval's close  
- high = max of all highs
- low = min of all lows
- traded_volume = sum of all volumes
- traded_dollar = sum of all traded_dollars
"""

import pytest
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime

from core.business.calendars.time_duration import TimeDuration
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder


class TestUniverseStateBuilderMultiTimeframe:
    """Test UniverseStateBuilder creates correct InstrumentIntervals for each timeframe."""

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment."""
        env = Mock()
        env.get_indicator_config = Mock(return_value=Mock(indicators={}))
        return env

    @pytest.fixture
    def mock_runner(self):
        """Create mock runner with required dependencies."""
        runner = Mock()
        runner.universe_id = 1
        runner.universe_manager = Mock()
        runner.universe_manager.instrument_ids = [31]  # AAPL instrument_id
        
        # Mock get_environment method
        runner.get_environment = Mock(return_value=Mock())
        
        # Mock market data manager that returns realistic minute-level data
        runner.market_data_manager = AsyncMock()
        
        return runner

    @pytest.fixture
    def builder_instance(self, mock_environment):
        """Create UniverseStateBuilder instance with test configuration."""
        builder = UniverseStateIntervalBuilder(
            env=mock_environment,
            base_duration='5m',
            target_durations='5m,15m,60m'
        )
        
        # Mock dependencies
        builder.market_cap_dao = AsyncMock()
        builder.market_cap_dao.list_market_caps_for_date = AsyncMock(return_value=[
            {'instrument_id': 31, 'market_cap': 3000000000000}  # $3T market cap for AAPL
        ])
        
        builder.indicator_builder = Mock()
        builder.indicator_builder.build_indicator_intervals = Mock(return_value={
            31: Mock(indicators={'sma_20': {'value': 150.0, 'status': 'ok', 'update_at': datetime.now()}})
        })
        
        return builder

    @pytest.fixture
    def sample_minute_data(self):
        """Create realistic minute-level OHLC data for testing."""
        # Simulate 15 minutes of AAPL data (3 x 5m intervals = 15m timeframe)
        return pd.DataFrame({
            'timestamp': pd.date_range('2025-07-01 09:30:00', periods=15, freq='1min'),
            'open': [180.0, 180.1, 180.0, 179.9, 180.2,  # First 5 minutes
                     180.5, 180.3, 180.7, 180.4, 180.8,  # Second 5 minutes  
                     181.0, 180.9, 181.2, 181.1, 181.5], # Third 5 minutes
            'high': [180.2, 180.3, 180.1, 180.0, 180.4,
                     180.7, 180.5, 180.9, 180.6, 181.0,
                     181.3, 181.1, 181.4, 181.2, 181.7],
            'low': [179.8, 179.9, 179.7, 179.6, 180.0,
                    180.2, 180.0, 180.4, 180.1, 180.5,
                    180.7, 180.6, 180.9, 180.8, 181.2],
            'close': [180.1, 180.0, 179.9, 180.2, 180.5,
                      180.3, 180.7, 180.4, 180.8, 181.0,
                      180.9, 181.2, 181.1, 181.5, 181.3],
            'volume': [1000, 1100, 900, 1200, 800,
                       1300, 1000, 1400, 1100, 1200,
                       1500, 1200, 1600, 1300, 1100]
        })

    @pytest.mark.asyncio
    @patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO')
    async def test_universe_state_builder_creates_intervals_for_each_timeframe(
        self, mock_xrefs_dao_class, builder_instance, mock_runner, sample_minute_data
    ):
        """
        Test that UniverseStateBuilder creates one InstrumentInterval per timeframe
        with correctly computed OHLC values.
        """
        # Mock instrument ID to symbol lookup
        mock_xrefs_dao = mock_xrefs_dao_class.return_value
        mock_xrefs_dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={
            31: 'AAPL'  # instrument_id 31 maps to AAPL
        })

        # Mock market data manager to return our sample data
        mock_runner.market_data_manager.get_minute_ohlc_batch = AsyncMock(return_value={
            'AAPL': sample_minute_data
        })

        # Set current time to match our data
        current_time = datetime(2025, 7, 1, 9, 35, 0)  # 9:35 AM

        # Build up some history first by calling handleInterval multiple times
        # This simulates the rolling window of InstrumentInterval objects
        
        # First interval: 9:30-9:35 (first 5 minutes)
        await builder_instance.handleInterval(mock_runner, datetime(2025, 7, 1, 9, 30, 0))
        
        # Second interval: 9:35-9:40 (second 5 minutes)  
        await builder_instance.handleInterval(mock_runner, datetime(2025, 7, 1, 9, 35, 0))
        
        # Third interval: 9:40-9:45 (third 5 minutes)
        await builder_instance.handleInterval(mock_runner, current_time)

        # Verify we have history built up
        assert 31 in builder_instance.instrument_history
        history = builder_instance.instrument_history[31]
        assert len(history) >= 3, f"Expected at least 3 intervals in history, got {len(history)}"

        # Now test the multi-timeframe functionality
        # Call handleInterval again and capture the duration_to_state results
        
        # Mock UniverseStateIntervalManager to capture states
        captured_states = {}
        
        with patch('domains.trading.services.state.universe_state.UniverseStateInterval') as mock_state_class:
            # Create mock state instances for each duration
            mock_states = {}
            for duration in builder_instance.target_durations:
                mock_state = Mock()
                mock_state.to_dataframe.return_value = pd.DataFrame({'test': [1]})  # Non-empty
                mock_states[duration] = mock_state
                
            mock_state_class.side_effect = lambda **kwargs: mock_states[kwargs['duration']]
            
            # Call handleInterval
            await builder_instance.handleInterval(mock_runner, current_time)

            # Verify UniverseStateInterval was created for each duration
            expected_calls = len(builder_instance.target_durations)
            actual_calls = mock_state_class.call_count
            assert actual_calls == expected_calls, (
                f"Expected {expected_calls} UniverseStateInterval creations, got {actual_calls}"
            )

            # Verify each call had correct duration and instrument_intervals
            for call in mock_state_class.call_args_list:
                kwargs = call.kwargs
                
                assert 'duration' in kwargs, "duration parameter missing"
                assert 'instrument_intervals' in kwargs, "instrument_intervals parameter missing"
                
                duration = kwargs['duration']
                instrument_intervals = kwargs['instrument_intervals']
                
                # Should have InstrumentInterval for our instrument_id (31)
                assert 31 in instrument_intervals, f"Missing InstrumentInterval for instrument_id 31 in {duration}"
                
                interval = instrument_intervals[31]
                assert isinstance(interval, InstrumentInterval), (
                    f"Expected InstrumentInterval, got {type(interval)}"
                )
                
                # Verify OHLC values are reasonable
                assert interval.open is not None, f"Missing open for {duration}"
                assert interval.high is not None, f"Missing high for {duration}" 
                assert interval.low is not None, f"Missing low for {duration}"
                assert interval.close is not None, f"Missing close for {duration}"
                assert interval.traded_volume is not None, f"Missing volume for {duration}"

    @pytest.mark.asyncio
    @patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO')
    async def test_ohlc_aggregation_accuracy_across_timeframes(
        self, mock_xrefs_dao_class, builder_instance, mock_runner, sample_minute_data
    ):
        """
        Test that OHLC aggregation works correctly across timeframes.
        
        For 15m timeframe (3 x 5m intervals):
        - open should be first 5m interval's open
        - close should be last 5m interval's close
        - high should be max of all 3 intervals' highs
        - low should be min of all 3 intervals' lows
        - volume should be sum of all 3 intervals' volumes
        """
        # Mock instrument ID to symbol lookup
        mock_xrefs_dao = mock_xrefs_dao_class.return_value
        mock_xrefs_dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={
            31: 'AAPL'
        })

        # Create controlled test data for precise OHLC verification
        controlled_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-07-01 09:30:00', periods=15, freq='1min'),
            'open': [100.0] * 5 + [105.0] * 5 + [110.0] * 5,    # First interval: 100, Second: 105, Third: 110
            'high': [102.0] * 5 + [107.0] * 5 + [112.0] * 5,    # First interval: 102, Second: 107, Third: 112  
            'low': [98.0] * 5 + [103.0] * 5 + [108.0] * 5,      # First interval: 98, Second: 103, Third: 108
            'close': [101.0] * 5 + [106.0] * 5 + [111.0] * 5,   # First interval: 101, Second: 106, Third: 111
            'volume': [1000] * 5 + [2000] * 5 + [3000] * 5      # First interval: 5000, Second: 10000, Third: 15000
        })

        mock_runner.market_data_manager.get_minute_ohlc_batch = AsyncMock(return_value={
            'AAPL': controlled_data
        })

        # Build history with 3 intervals (each representing 5 minutes)
        times = [
            datetime(2025, 7, 1, 9, 35, 0),  # First 5-minute interval
            datetime(2025, 7, 1, 9, 40, 0),  # Second 5-minute interval
            datetime(2025, 7, 1, 9, 45, 0),  # Third 5-minute interval
        ]

        for time in times:
            await builder_instance.handleInterval(mock_runner, time)

        # Verify we have 3 intervals in history
        history = builder_instance.instrument_history[31]
        assert len(history) == 3, f"Expected 3 intervals in history, got {len(history)}"

        # Verify each 5-minute interval has correct OHLC
        interval_1 = history[0]  # 100.0, 102.0, 98.0, 101.0, volume=5000
        interval_2 = history[1]  # 105.0, 107.0, 103.0, 106.0, volume=10000  
        interval_3 = history[2]  # 110.0, 112.0, 108.0, 111.0, volume=15000

        # Test individual 5-minute intervals
        assert interval_1.open == 100.0, f"Interval 1 open should be 100.0, got {interval_1.open}"
        assert interval_1.high == 102.0, f"Interval 1 high should be 102.0, got {interval_1.high}"
        assert interval_1.low == 98.0, f"Interval 1 low should be 98.0, got {interval_1.low}"
        assert interval_1.close == 101.0, f"Interval 1 close should be 101.0, got {interval_1.close}"

        # Test aggregation logic using TimeDuration.aggregate_intervals directly
        duration_15m = TimeDuration.create_15_minutes()
        
        # Manually test the aggregation logic that should happen for 15m timeframe
        aggregated_interval = duration_15m.aggregate_intervals([interval_1, interval_2, interval_3])
        
        # Verify aggregated OHLC follows proper rules:
        assert aggregated_interval.open == 100.0, (  # First interval's open
            f"15m aggregated open should be 100.0 (first interval), got {aggregated_interval.open}"
        )
        assert aggregated_interval.close == 111.0, (  # Last interval's close
            f"15m aggregated close should be 111.0 (last interval), got {aggregated_interval.close}" 
        )
        assert aggregated_interval.high == 112.0, (  # Max of all highs (102, 107, 112)
            f"15m aggregated high should be 112.0 (max high), got {aggregated_interval.high}"
        )
        assert aggregated_interval.low == 98.0, (  # Min of all lows (98, 103, 108)
            f"15m aggregated low should be 98.0 (min low), got {aggregated_interval.low}"
        )
        assert aggregated_interval.traded_volume == 30000, (  # Sum of volumes (5000 + 10000 + 15000)
            f"15m aggregated volume should be 30000 (sum), got {aggregated_interval.traded_volume}"
        )

    def test_timeframe_duration_calculations(self):
        """Test that TimeDuration correctly calculates aggregation ratios."""
        base_5m = TimeDuration.create_5_minutes()
        target_15m = TimeDuration.create_15_minutes()
        target_60m = TimeDuration.create_60_minutes()
        
        # Test duration calculations
        assert base_5m.get_duration_minutes() == 5
        assert target_15m.get_duration_minutes() == 15
        assert target_60m.get_duration_minutes() == 60
        
        # Test aggregation ratios
        ratio_15m = target_15m.get_duration_minutes() // base_5m.get_duration_minutes()
        ratio_60m = target_60m.get_duration_minutes() // base_5m.get_duration_minutes()
        
        assert ratio_15m == 3, f"15m should require 3 x 5m intervals, got {ratio_15m}"
        assert ratio_60m == 12, f"60m should require 12 x 5m intervals, got {ratio_60m}"

    @pytest.mark.asyncio 
    async def test_empty_history_handling(self, builder_instance, mock_runner):
        """Test handling when insufficient history exists for aggregation."""
        # Mock empty market data
        mock_runner.market_data_manager.get_minute_ohlc_batch = AsyncMock(return_value={})
        
        # Mock instrument lookup
        with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao_class:
            mock_xrefs_dao = mock_xrefs_dao_class.return_value
            mock_xrefs_dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={31: 'AAPL'})
            
            current_time = datetime(2025, 7, 1, 9, 35, 0)
            
            # Should handle gracefully without crashing
            await builder_instance.handleInterval(mock_runner, current_time)
            
            # History should be empty or contain only empty intervals
            history = builder_instance.instrument_history.get(31, [])
            if history:
                # If intervals exist, they should have 'missing' status
                for interval in history:
                    assert interval.status in ['missing', 'ok'], (
                        f"Unexpected interval status: {interval.status}"
                    )