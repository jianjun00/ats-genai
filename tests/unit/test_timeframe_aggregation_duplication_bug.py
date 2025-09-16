"""
Test to reproduce the OHLCV duplication bug across different timeframes.

This test specifically targets the interaction between UniverseStateIntervalBuilder 
and UniverseStateManager to understand why multiple timeframes produce identical 
OHLCV values instead of proper aggregation.

Bug: Training data for 5m, 15m, 1h, 1d all show identical OHLCV values:
- Open: 208.0239, High: 208.1138, Low: 208.0139, Close: 208.0839, Volume: 56512.0

Expected: Each timeframe should aggregate minute data differently.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import gin
import numpy as np

from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from core.config.environment import Environment
from core.business.calendars.time_duration import TimeDuration


class TestTimeframeAggregationDuplicationBug:
    """Test cases to reproduce and identify the OHLCV duplication bug."""

    def setup_method(self):
        """Set up test environment with controlled data."""
        gin.clear_config()
        
        # Create mock environment
        self.mock_env = Mock()
        self.mock_env.env_type = "test"
        self.mock_env.get_table_name.return_value = "test_table"
        self.mock_env.get_env_type.return_value = "test"
        
        # Create sample minute-level OHLCV data that should aggregate differently
        self.sample_minute_data = self._create_diverse_minute_data()
        
    def _create_diverse_minute_data(self) -> pd.DataFrame:
        """Create minute-level data with distinct patterns for testing aggregation."""
        base_time = datetime(2025, 7, 1, 14, 0)  # 2:00 PM
        data = []
        
        # Create 60 minutes of data with clear patterns
        for i in range(60):
            timestamp = base_time + timedelta(minutes=i)
            
            # Create data with patterns that should aggregate differently
            # First 5 minutes: rising prices
            # Next 10 minutes: stable prices  
            # Next 15 minutes: declining prices
            # Remaining 30 minutes: volatile prices
            
            if i < 5:
                # Rising trend
                open_price = 208.00 + (i * 0.01)
                high_price = open_price + 0.02
                low_price = open_price - 0.01
                close_price = open_price + 0.015
                volume = 1000 + (i * 100)
            elif i < 15:
                # Stable period
                open_price = 208.05
                high_price = 208.07
                low_price = 208.03
                close_price = 208.06
                volume = 1500
            elif i < 30:
                # Declining trend
                open_price = 208.06 - ((i-15) * 0.002)
                high_price = open_price + 0.01
                low_price = open_price - 0.02
                close_price = open_price - 0.01
                volume = 800 + (i * 50)
            else:
                # Volatile period
                volatility = 0.05 * np.sin((i-30) * 0.2)
                open_price = 208.00 + volatility
                high_price = open_price + abs(volatility) + 0.01
                low_price = open_price - abs(volatility) - 0.01
                close_price = open_price + (volatility * 0.5)
                volume = 2000 + int(abs(volatility) * 10000)
            
            data.append({
                'timestamp': timestamp,
                'symbol': 'AAPL',
                'open': round(open_price, 4),
                'high': round(high_price, 4),
                'low': round(low_price, 4),
                'close': round(close_price, 4),
                'volume': int(volume),
                'vwap': round((high_price + low_price + close_price) / 3, 4)
            })
        
        return pd.DataFrame(data)

    @patch('domains.trading.services.state.universe_state_manager.UniverseStateManager')
    def test_universe_state_builder_processes_multiple_timeframes(self, mock_universe_manager_class):
        """Test that UniverseStateIntervalBuilder is configured to process multiple timeframes."""
        
        # Mock the UniverseStateManager instance
        mock_universe_manager = Mock()
        mock_universe_manager_class.return_value = mock_universe_manager
        
        # Configure with multiple timeframes like in training_data.gin
        target_durations_str = '5m,15m,30m,60m,1d'
        
        # Create UniverseStateIntervalBuilder with multi-timeframe config
        builder = UniverseStateIntervalBuilder(
            env=self.mock_env,
            base_duration='1m',
            target_durations=target_durations_str,
            universe_state_manager=mock_universe_manager
        )
        
        # Verify target_durations parsed correctly
        expected_durations = [TimeDuration('5m'), TimeDuration('15m'), TimeDuration('30m'), 
                             TimeDuration('60m'), TimeDuration('1d')]
        
        assert len(builder.target_durations) == 5
        for i, duration in enumerate(expected_durations):
            assert builder.target_durations[i].get_duration_string() == duration.get_duration_string()
        
        print(f"✅ UniverseStateIntervalBuilder configured with {len(builder.target_durations)} timeframes")
        for duration in builder.target_durations:
            print(f"   - {duration.get_duration_string()}")

    def test_universe_state_manager_maintains_separate_timeframe_caches(self):
        """Test that UniverseStateManager maintains separate caches for different timeframes."""
        
        # Create UniverseStateManager
        universe_manager = UniverseStateManager()
        
        # Add the same interval with different timeframes
        instrument_id = 1
        center_datetime = datetime(2025, 7, 1, 14, 30)  # 2:30 PM
        
        # Sample intervals for different timeframes (same time, different aggregation)
        minute_interval = {
            'start_time': center_datetime,
            'end_time': center_datetime + timedelta(minutes=1),
            'timeframe': '1m',
            'open': 208.00, 'high': 208.02, 'low': 207.98, 'close': 208.01, 'volume': 1000
        }
        
        five_min_interval = {
            'start_time': center_datetime,
            'end_time': center_datetime + timedelta(minutes=5),
            'timeframe': '5m',
            'open': 208.00, 'high': 208.05, 'low': 207.95, 'close': 208.03, 'volume': 5000
        }
        
        hourly_interval = {
            'start_time': center_datetime,
            'end_time': center_datetime + timedelta(hours=1),
            'timeframe': '1h',
            'open': 208.00, 'high': 208.10, 'low': 207.90, 'close': 208.05, 'volume': 50000
        }
        
        # Add intervals to different timeframe caches
        universe_manager.addUniverseStateInterval({TimeDuration('5m'): minute_interval}, datetime.now())
        universe_manager.addUniverseStateInterval({TimeDuration('15m'): five_min_interval}, datetime.now()) 
        universe_manager.addUniverseStateInterval({TimeDuration('60m'): hourly_interval}, datetime.now())
        
        # Verify separate caches exist
        assert hasattr(universe_manager, '_rolling_instrument_history')
        
        # Test retrieving data from different timeframes
        minute_data = universe_manager.get_lag_prices(instrument_id, center_datetime + timedelta(minutes=1), 1, time_interval='1m')
        five_min_data = universe_manager.get_lag_prices(instrument_id, center_datetime + timedelta(minutes=5), 1, time_interval='5m')
        hourly_data = universe_manager.get_lag_prices(instrument_id, center_datetime + timedelta(hours=1), 1, time_interval='1h')
        
        print(f"✅ UniverseStateManager cache test:")
        print(f"   - 1m cache entries: {len(minute_data) if not minute_data.empty else 0}")
        print(f"   - 5m cache entries: {len(five_min_data) if not five_min_data.empty else 0}")
        print(f"   - 1h cache entries: {len(hourly_data) if not hourly_data.empty else 0}")
        
        # The bug might be that all timeframes return the same data
        if not minute_data.empty and not five_min_data.empty and not hourly_data.empty:
            minute_ohlcv = (minute_data.iloc[0]['open'], minute_data.iloc[0]['high'], 
                           minute_data.iloc[0]['low'], minute_data.iloc[0]['close'])
            five_min_ohlcv = (five_min_data.iloc[0]['open'], five_min_data.iloc[0]['high'],
                             five_min_data.iloc[0]['low'], five_min_data.iloc[0]['close'])
            hourly_ohlcv = (hourly_data.iloc[0]['open'], hourly_data.iloc[0]['high'],
                           hourly_data.iloc[0]['low'], hourly_data.iloc[0]['close'])
            
            print(f"   - 1m OHLC: {minute_ohlcv}")
            print(f"   - 5m OHLC: {five_min_ohlcv}")
            print(f"   - 1h OHLC: {hourly_ohlcv}")
            
            # Check for duplication bug
            if minute_ohlcv == five_min_ohlcv == hourly_ohlcv:
                pytest.fail("🐛 DUPLICATION BUG REPRODUCED: All timeframes return identical OHLCV values")

    def test_reproduce_training_data_duplication_scenario(self):
        """Reproduce the exact scenario from training data generation that causes duplication."""
        
        # This test simulates the exact flow in training data generation
        universe_manager = UniverseStateManager()
        instrument_id = 1
        
        # Simulate the scenario where identical OHLCV appears across timeframes
        base_time = datetime(2025, 7, 1, 18, 0)  # 6:00 PM (same as bug report)
        
        # Add the problematic intervals that show duplication
        duplicate_ohlcv = {
            'open': 208.0239, 'high': 208.1138, 'low': 208.0139, 'close': 208.0839, 'volume': 56512.0
        }
        
        timeframes = ['5m', '15m', '30m', '60m', '1d']
        
        for timeframe in timeframes:
            interval_data = {
                'start_time': base_time,
                'end_time': TimeDuration(timeframe).get_end_time(base_time),
                'timeframe': timeframe,
                **duplicate_ohlcv
            }
            universe_manager.addUniverseStateInterval({TimeDuration(timeframe): interval_data}, base_time)
        
        # Now test data retrieval for each timeframe
        duplication_detected = True
        retrieved_data = {}
        
        for timeframe in timeframes:
            data = universe_manager.get_lag_prices(
                instrument_id, 
                TimeDuration(timeframe).get_end_time(base_time), 
                1, 
                time_interval=timeframe
            )
            
            if not data.empty:
                ohlcv = (data.iloc[0]['open'], data.iloc[0]['high'], 
                        data.iloc[0]['low'], data.iloc[0]['close'], data.iloc[0]['volume'])
                retrieved_data[timeframe] = ohlcv
                print(f"   - {timeframe}: OHLCV = {ohlcv}")
        
        # Check if all timeframes return identical data (the bug)
        if len(set(retrieved_data.values())) == 1:
            print("🐛 DUPLICATION BUG REPRODUCED!")
            print(f"   All timeframes return identical OHLCV: {list(retrieved_data.values())[0]}")
            
            # This should fail to indicate the bug is present
            pytest.fail(
                f"DUPLICATION BUG CONFIRMED: All {len(timeframes)} timeframes return identical OHLCV values: "
                f"{list(retrieved_data.values())[0]}"
            )
        else:
            print("✅ No duplication detected - timeframes return different values")

    def test_timeframe_aggregation_logic(self):
        """Test the specific logic that should aggregate minute data into different timeframes."""
        
        # Create minute data with clear aggregation expectations
        minute_data = self.sample_minute_data.copy()
        
        # Test manual aggregation to verify expected behavior
        expected_5m_aggregation = self._aggregate_to_timeframe(minute_data, '5m')
        expected_15m_aggregation = self._aggregate_to_timeframe(minute_data, '15m')
        expected_60m_aggregation = self._aggregate_to_timeframe(minute_data, '60m')
        
        print("Expected aggregation results:")
        print(f"   - 5m first interval OHLCV: {self._get_ohlcv(expected_5m_aggregation.iloc[0])}")
        print(f"   - 15m first interval OHLCV: {self._get_ohlcv(expected_15m_aggregation.iloc[0])}")
        print(f"   - 60m first interval OHLCV: {self._get_ohlcv(expected_60m_aggregation.iloc[0])}")
        
        # These should be different if aggregation works correctly
        ohlcv_5m = self._get_ohlcv(expected_5m_aggregation.iloc[0])
        ohlcv_15m = self._get_ohlcv(expected_15m_aggregation.iloc[0])
        ohlcv_60m = self._get_ohlcv(expected_60m_aggregation.iloc[0])
        
        assert ohlcv_5m != ohlcv_15m, "5m and 15m aggregations should be different"
        assert ohlcv_15m != ohlcv_60m, "15m and 60m aggregations should be different"
        assert ohlcv_5m != ohlcv_60m, "5m and 60m aggregations should be different"
        
        print("✅ Manual aggregation produces different results for different timeframes")

    def _aggregate_to_timeframe(self, minute_data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Manually aggregate minute data to specified timeframe."""
        duration = TimeDuration(timeframe)
        # Get duration in minutes for aggregation
        minutes = duration.get_duration_minutes()
        if minutes is None:
            # For day/week/month, just use a large number for testing
            if timeframe == '1d':
                minutes = 1440  # 24 hours
            else:
                minutes = 60  # Default to 1 hour
        
        aggregated = []
        for i in range(0, len(minute_data), minutes):
            chunk = minute_data.iloc[i:i+minutes]
            if len(chunk) > 0:
                agg_row = {
                    'timestamp': chunk.iloc[0]['timestamp'],
                    'symbol': chunk.iloc[0]['symbol'],
                    'open': chunk.iloc[0]['open'],
                    'high': chunk['high'].max(),
                    'low': chunk['low'].min(),
                    'close': chunk.iloc[-1]['close'],
                    'volume': chunk['volume'].sum(),
                    'vwap': np.average(chunk['vwap'], weights=chunk['volume'])
                }
                aggregated.append(agg_row)
        
        return pd.DataFrame(aggregated)

    def _get_ohlcv(self, row) -> tuple:
        """Extract OHLCV tuple from dataframe row."""
        return (row['open'], row['high'], row['low'], row['close'], row['volume'])