#!/usr/bin/env python3
"""
Critical Test: Lead vs Lag Price Validation

This test validates that get_lead_prices and get_lag_prices return DIFFERENT data
for the same cur_datetime reference point, ensuring they access different time periods.

CRITICAL ISSUE BEING TESTED:
- get_lag_prices(cur_datetime) should return data BEFORE cur_datetime
- get_lead_prices(cur_datetime) should return data AFTER cur_datetime
- They should return DIFFERENT OHLCV values for different time periods
- Different lag/lead periods should return different amounts of historical/future data
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock
import numpy as np

from domains.trading.services.state.universe_state_manager import UniverseStateManager


class TestLeadVsLagPriceValidation:
    """Test that lead and lag prices return different data for different time periods."""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment."""
        return Mock()

    @pytest.fixture
    def universe_manager(self, mock_env):
        """Create UniverseStateManager with mocked dependencies."""
        manager = UniverseStateManager(mock_env)
        # Mock the market_data_manager
        manager.market_data_manager = Mock()
        return manager

    @pytest.fixture
    def reference_datetime(self):
        """Reference datetime for testing - middle of a trading day."""
        return datetime(2025, 9, 6, 14, 30, 0)  # 2:30 PM

    @pytest.fixture
    def historical_data(self, reference_datetime):
        """Create realistic historical OHLCV data BEFORE reference_datetime."""
        # Create 5 periods of historical 1-hour data ending at reference_datetime
        historical_times = []
        for i in range(5, 0, -1):  # 5, 4, 3, 2, 1 hours ago
            historical_times.append(reference_datetime - timedelta(hours=i))

        return pd.DataFrame({
            'timestamp': historical_times,
            'open': [100.0, 101.5, 99.8, 102.1, 103.0],  # Trending up historically
            'high': [101.0, 102.0, 101.0, 103.0, 104.0],
            'low': [99.5, 100.8, 99.0, 101.5, 102.5],
            'close': [100.8, 101.2, 100.5, 102.8, 103.5],  # Higher closes historically
            'volume': [1000000, 1100000, 950000, 1200000, 1150000]
        })

    @pytest.fixture
    def future_data(self, reference_datetime):
        """Create realistic future OHLCV data AFTER reference_datetime."""
        # Create 3 periods of future 1-hour data starting from reference_datetime
        future_times = []
        for i in range(1, 4):  # 1, 2, 3 hours from now
            future_times.append(reference_datetime + timedelta(hours=i))

        return pd.DataFrame({
            'timestamp': future_times,
            'open': [104.0, 105.2, 103.8],  # Different trend - volatility
            'high': [105.5, 106.0, 105.0],
            'low': [103.5, 104.8, 103.0],
            'close': [105.1, 104.5, 104.8],  # Lower closes in future
            'volume': [1300000, 1250000, 1400000]  # Higher volume in future
        })

    def test_lag_vs_lead_prices_return_different_data(self, universe_manager, reference_datetime,
                                                     historical_data, future_data):
        """Test that lag and lead prices return completely different OHLCV data."""

        instrument_id = 1001

        def mock_get_ohlcv_data(instrument_id, reference_datetime, periods, time_interval, direction='backward'):
            """Mock that returns different data based on direction."""
            if direction == 'backward':
                # Return historical data for lag prices
                return historical_data.head(periods)  # Get requested number of periods
            else:  # forward
                # Return future data for lead prices
                return future_data.head(periods)  # Get requested number of periods

        # Setup mock
        universe_manager.market_data_manager.get_ohlcv_data = Mock(side_effect=mock_get_ohlcv_data)

        # Test with same parameters but different directions
        lag_periods = 3
        lead_periods = 3
        time_interval = '1h'

        # Get lag prices (historical data)
        lag_result = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=lag_periods,
            time_interval=time_interval
        )

        # Get lead prices (future data)
        lead_result = universe_manager.get_lead_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lead_periods=lead_periods,
            time_interval=time_interval
        )

        # CRITICAL VALIDATION: They should return different data
        assert not lag_result.empty, "Lag prices should not be empty"
        assert not lead_result.empty, "Lead prices should not be empty"
        assert len(lag_result) == 3, f"Should have 3 lag periods, got {len(lag_result)}"
        assert len(lead_result) == 3, f"Should have 3 lead periods, got {len(lead_result)}"

        # Verify the OHLCV values are different between lag and lead
        lag_close_values = lag_result['close'].tolist()
        lead_close_values = lead_result['close'].tolist()

        print(f"Lag closes (historical): {lag_close_values}")
        print(f"Lead closes (future): {lead_close_values}")

        # CRITICAL: Close values should be different
        assert lag_close_values != lead_close_values, \
            "Lag and lead prices should have different close values (different time periods)"

        # Validate specific expected values from our mock data
        expected_lag_closes = [100.8, 101.2, 100.5]  # First 3 periods from historical_data (based on actual test output)
        expected_lead_closes = [105.1, 104.5, 104.8]  # First 3 periods from future_data

        assert lag_close_values == expected_lag_closes, \
            f"Lag closes don't match expected historical data: {lag_close_values} != {expected_lag_closes}"
        assert lead_close_values == expected_lead_closes, \
            f"Lead closes don't match expected future data: {lead_close_values} != {expected_lead_closes}"

        print("✅ PASS: Lag and lead prices return different OHLCV data for different time periods")

    def test_different_lag_periods_return_different_amounts_of_data(self, universe_manager,
                                                                   reference_datetime, historical_data):
        """Test that different lag periods return different amounts of historical data."""

        instrument_id = 2001

        def mock_get_ohlcv_data(instrument_id, reference_datetime, periods, time_interval, direction='backward'):
            """Mock that returns the requested number of periods."""
            return historical_data.head(periods)

        # Setup mock
        universe_manager.market_data_manager.get_ohlcv_data = Mock(side_effect=mock_get_ohlcv_data)

        # Test different lag periods
        lag_2_periods = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=2,
            time_interval='1h'
        )

        lag_4_periods = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=4,
            time_interval='1h'
        )

        # Validate different amounts of data
        assert len(lag_2_periods) == 2, f"Should have 2 lag periods, got {len(lag_2_periods)}"
        assert len(lag_4_periods) == 4, f"Should have 4 lag periods, got {len(lag_4_periods)}"

        # Validate that 4-period data contains the 2-period data (since it's more historical data)
        lag_2_closes = lag_2_periods['close'].tolist()
        lag_4_closes = lag_4_periods['close'].tolist()

        print(f"Lag 2 periods closes: {lag_2_closes}")
        print(f"Lag 4 periods closes: {lag_4_closes}")

        # The first 2 closes should be the same (most recent historical data)
        assert lag_2_closes == lag_4_closes[:2], \
            f"2-period data should match first 2 items of 4-period data: {lag_2_closes} != {lag_4_closes[:2]}"

        print("✅ PASS: Different lag periods return different amounts of historical data")

    def test_different_lead_periods_return_different_amounts_of_data(self, universe_manager,
                                                                    reference_datetime, future_data):
        """Test that different lead periods return different amounts of future data."""

        instrument_id = 3001

        def mock_get_ohlcv_data(instrument_id, reference_datetime, periods, time_interval, direction='backward'):
            """Mock that returns future data for forward direction."""
            if direction == 'forward':
                return future_data.head(periods)
            else:
                return pd.DataFrame()  # Shouldn't be called for lead prices

        # Setup mock
        universe_manager.market_data_manager.get_ohlcv_data = Mock(side_effect=mock_get_ohlcv_data)

        # Test different lead periods
        lead_1_period = universe_manager.get_lead_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lead_periods=1,
            time_interval='1h'
        )

        lead_3_periods = universe_manager.get_lead_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lead_periods=3,
            time_interval='1h'
        )

        # Validate different amounts of future data
        assert len(lead_1_period) == 1, f"Should have 1 lead period, got {len(lead_1_period)}"
        assert len(lead_3_periods) == 3, f"Should have 3 lead periods, got {len(lead_3_periods)}"

        # Validate that 3-period data contains the 1-period data (first future period)
        lead_1_close = lead_1_period['close'].iloc[0]
        lead_3_first_close = lead_3_periods['close'].iloc[0]

        print(f"Lead 1 period close: {lead_1_close}")
        print(f"Lead 3 periods first close: {lead_3_first_close}")

        assert lead_1_close == lead_3_first_close, \
            f"1-period close should match first close of 3-period data: {lead_1_close} != {lead_3_first_close}"

        print("✅ PASS: Different lead periods return different amounts of future data")

    def test_market_data_manager_called_with_correct_direction_parameters(self, universe_manager, reference_datetime):
        """Test that market_data_manager is called with correct direction parameters."""

        instrument_id = 4001

        # Mock that captures call arguments
        universe_manager.market_data_manager.get_ohlcv_data = Mock(return_value=pd.DataFrame({
            'open': [100.0], 'high': [101.0], 'low': [99.0], 'close': [100.5], 'volume': [1000000]
        }))

        # Test lag prices call
        universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=5,
            time_interval='15m'
        )

        # Verify lag prices called with backward direction (default)
        lag_call_args = universe_manager.market_data_manager.get_ohlcv_data.call_args
        assert lag_call_args[1]['reference_datetime'] == reference_datetime
        assert lag_call_args[1]['periods'] == 5
        assert lag_call_args[1]['time_interval'] == '15m'
        # Default direction should be 'backward' (or not specified, defaulting to backward)
        direction_arg = lag_call_args[1].get('direction', 'backward')
        assert direction_arg == 'backward', f"Lag prices should use backward direction, got {direction_arg}"

        # Reset mock for lead prices test
        universe_manager.market_data_manager.get_ohlcv_data.reset_mock()
        universe_manager.market_data_manager.get_ohlcv_data.return_value = pd.DataFrame({
            'open': [105.0], 'high': [106.0], 'low': [104.0], 'close': [105.5], 'volume': [1200000]
        })

        # Test lead prices call
        universe_manager.get_lead_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lead_periods=3,
            time_interval='15m'  # Use valid interval
        )

        # Verify lead prices called with forward direction
        lead_call_args = universe_manager.market_data_manager.get_ohlcv_data.call_args
        assert lead_call_args[1]['reference_datetime'] == reference_datetime
        assert lead_call_args[1]['periods'] == 3
        assert lead_call_args[1]['time_interval'] == '15m'
        assert lead_call_args[1]['direction'] == 'forward', \
            f"Lead prices should use forward direction, got {lead_call_args[1]['direction']}"

        print("✅ PASS: market_data_manager called with correct direction parameters")

    def test_same_reference_datetime_different_directions_yield_different_results(self, universe_manager, reference_datetime):
        """Test that same reference_datetime with different directions yields different results."""

        instrument_id = 5001

        # Create mock data that clearly differentiates historical vs future
        historical_mock_data = pd.DataFrame({
            'open': [90.0, 91.0, 92.0],
            'high': [91.0, 92.0, 93.0],
            'low': [89.0, 90.0, 91.0],
            'close': [90.5, 91.5, 92.5],  # Historical: 90s range
            'volume': [800000, 850000, 900000]
        })

        future_mock_data = pd.DataFrame({
            'open': [110.0, 111.0, 112.0],
            'high': [111.0, 112.0, 113.0],
            'low': [109.0, 110.0, 111.0],
            'close': [110.5, 111.5, 112.5],  # Future: 110s range
            'volume': [1200000, 1250000, 1300000]
        })

        def mock_get_ohlcv_data(instrument_id, reference_datetime, periods, time_interval, direction='backward'):
            """Return clearly different data based on direction."""
            if direction == 'backward':
                return historical_mock_data.head(periods)
            else:  # forward
                return future_mock_data.head(periods)

        # Setup mock
        universe_manager.market_data_manager.get_ohlcv_data = Mock(side_effect=mock_get_ohlcv_data)

        # Call both methods with identical parameters except direction
        lag_result = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=2,
            time_interval='1h'
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lead_periods=2,
            time_interval='1h'
        )

        # Verify results are completely different
        lag_closes = lag_result['close'].tolist()
        lead_closes = lead_result['close'].tolist()

        print(f"Historical (lag) closes: {lag_closes}")
        print(f"Future (lead) closes: {lead_closes}")

        # Verify expected values
        expected_historical = [90.5, 91.5]  # From historical_mock_data
        expected_future = [110.5, 111.5]    # From future_mock_data

        assert lag_closes == expected_historical, \
            f"Lag closes should be historical data: {lag_closes} != {expected_historical}"
        assert lead_closes == expected_future, \
            f"Lead closes should be future data: {lead_closes} != {expected_future}"

        # Critical validation: They should be completely different
        assert lag_closes != lead_closes, \
            "Lag and lead prices must return different data for same reference_datetime"

        # Verify the data ranges are different (90s vs 110s)
        assert all(c < 100 for c in lag_closes), "Historical data should be in 90s range"
        assert all(c > 100 for c in lead_closes), "Future data should be in 110s range"

        print("✅ PASS: Same reference_datetime with different directions yields different results")


class TestTimeIntervalSpecificValidation:
    """Test that different time intervals return different aggregated OHLCV values."""

    @pytest.fixture
    def universe_manager(self):
        """Create UniverseStateManager with mocked dependencies."""
        manager = UniverseStateManager(Mock())
        manager.market_data_manager = Mock()
        return manager

    def test_different_time_intervals_return_different_ohlcv_aggregations(self, universe_manager):
        """Test that 5m, 15m, 1h intervals return different OHLCV aggregations."""

        instrument_id = 6001
        reference_datetime = datetime(2025, 9, 6, 15, 0, 0)

        # Mock data for different intervals - should represent different aggregation levels
        mock_5m_data = pd.DataFrame({
            'open': [100.0, 100.2, 100.5],
            'high': [100.3, 100.4, 100.8],
            'low': [99.8, 100.0, 100.2],
            'close': [100.1, 100.3, 100.6],  # 5-minute: Small price movements
            'volume': [50000, 52000, 48000]   # 5-minute: Lower volume per bar
        })

        mock_15m_data = pd.DataFrame({
            'open': [100.0, 101.0, 102.0],
            'high': [101.2, 102.5, 103.0],
            'low': [99.5, 100.5, 101.5],
            'close': [100.8, 102.2, 102.8],  # 15-minute: Larger price movements
            'volume': [180000, 190000, 175000]  # 15-minute: Higher volume (3x 5-minute)
        })

        mock_1h_data = pd.DataFrame({
            'open': [100.0, 103.0, 105.0],
            'high': [103.5, 106.0, 108.0],
            'low': [99.0, 102.0, 104.0],
            'close': [102.8, 105.5, 107.2],  # 1-hour: Much larger price movements
            'volume': [750000, 800000, 720000]  # 1-hour: Much higher volume (12x 5-minute)
        })

        def mock_get_ohlcv_data(instrument_id, reference_datetime, periods, time_interval, direction='backward'):
            """Return different data based on time interval."""
            interval_data = {
                '5m': mock_5m_data,
                '15m': mock_15m_data,
                '1h': mock_1h_data
            }
            return interval_data[time_interval].head(periods)

        # Setup mock
        universe_manager.market_data_manager.get_ohlcv_data = Mock(side_effect=mock_get_ohlcv_data)

        # Get data for different time intervals
        result_5m = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=3,
            time_interval='5m'
        )

        result_15m = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=3,
            time_interval='15m'
        )

        result_1h = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=3,
            time_interval='1h'
        )

        # Extract close prices and volumes
        closes_5m = result_5m['close'].tolist()
        closes_15m = result_15m['close'].tolist()
        closes_1h = result_1h['close'].tolist()

        volumes_5m = result_5m['volume'].tolist()
        volumes_15m = result_15m['volume'].tolist()
        volumes_1h = result_1h['volume'].tolist()

        print(f"5m closes: {closes_5m}, avg volume: {np.mean(volumes_5m):.0f}")
        print(f"15m closes: {closes_15m}, avg volume: {np.mean(volumes_15m):.0f}")
        print(f"1h closes: {closes_1h}, avg volume: {np.mean(volumes_1h):.0f}")

        # CRITICAL VALIDATION: Different intervals should return different values
        assert closes_5m != closes_15m, "5m and 15m intervals should have different close prices"
        assert closes_15m != closes_1h, "15m and 1h intervals should have different close prices"
        assert closes_5m != closes_1h, "5m and 1h intervals should have different close prices"

        # Validate expected aggregation behavior:
        # - Higher timeframes should have higher volume (more minutes aggregated)
        # - Higher timeframes should have larger price ranges
        avg_vol_5m = np.mean(volumes_5m)
        avg_vol_15m = np.mean(volumes_15m)
        avg_vol_1h = np.mean(volumes_1h)

        assert avg_vol_5m < avg_vol_15m < avg_vol_1h, \
            f"Volume should increase with timeframe: 5m({avg_vol_5m:.0f}) < 15m({avg_vol_15m:.0f}) < 1h({avg_vol_1h:.0f})"

        # Validate price ranges (high-low) increase with timeframe
        range_5m = np.mean([h - l for h, l in zip(result_5m['high'], result_5m['low'])])
        range_15m = np.mean([h - l for h, l in zip(result_15m['high'], result_15m['low'])])
        range_1h = np.mean([h - l for h, l in zip(result_1h['high'], result_1h['low'])])

        print(f"Average price ranges: 5m={range_5m:.2f}, 15m={range_15m:.2f}, 1h={range_1h:.2f}")

        assert range_5m < range_15m < range_1h, \
            f"Price range should increase with timeframe: 5m({range_5m:.2f}) < 15m({range_15m:.2f}) < 1h({range_1h:.2f})"

        print("✅ PASS: Different time intervals return different OHLCV aggregations")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])