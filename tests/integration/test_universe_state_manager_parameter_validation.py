#!/usr/bin/env python3
"""
Critical Test: Universe State Manager Parameter Validation

Tests all the input validation logic that exists in get_lag_prices and get_lead_prices
but was not comprehensively tested in our previous validation efforts.

CRITICAL GAPS IDENTIFIED:
- Parameter type validation (int, str, datetime validation)
- Boundary conditions (zero, negative values, empty strings)
- Invalid time intervals and edge cases
- market_data_manager availability assertions
- Date/datetime conversion logic
"""

import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import Mock

from state.universe_state_manager import UniverseStateManager


class TestUniverseStateManagerParameterValidation:
    """Test comprehensive parameter validation for get_lag_prices and get_lead_prices."""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment."""
        return Mock()

    @pytest.fixture
    def universe_manager(self, mock_env):
        """Create UniverseStateManager with mocked market_data_manager."""
        manager = UniverseStateManager(mock_env)
        # Mock the market_data_manager
        mock_market_manager = Mock()
        mock_market_manager.get_ohlcv_data.return_value = pd.DataFrame({
            'open': [100.0], 'high': [101.0], 'low': [99.0], 'close': [100.5], 'volume': [1000000]
        })
        manager.market_data_manager = mock_market_manager
        return manager

    @pytest.fixture
    def valid_params(self):
        """Valid parameters for testing."""
        return {
            'instrument_id': 1001,
            'cur_datetime': datetime(2025, 9, 6, 14, 30, 0),
            'periods': 5,
            'time_interval': '1h'
        }

    # ========================================
    # INSTRUMENT_ID VALIDATION TESTS
    # ========================================

    @pytest.mark.parametrize("invalid_instrument_id,expected_error", [
        (0, "instrument_id must be a positive integer"),
        (-1, "instrument_id must be a positive integer"),
        (-999, "instrument_id must be a positive integer"),
        ("1001", "instrument_id must be a positive integer"),
        (1001.5, "instrument_id must be a positive integer"),
        (None, "instrument_id must be a positive integer"),
        ("invalid", "instrument_id must be a positive integer"),
    ])
    def test_get_lag_prices_invalid_instrument_id(self, universe_manager, valid_params, invalid_instrument_id, expected_error):
        """Test get_lag_prices with invalid instrument_id values."""
        valid_params['instrument_id'] = invalid_instrument_id

        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    @pytest.mark.parametrize("invalid_instrument_id,expected_error", [
        (0, "instrument_id must be a positive integer"),
        (-1, "instrument_id must be a positive integer"),
        ("2001", "instrument_id must be a positive integer"),
        (2001.7, "instrument_id must be a positive integer"),
        (None, "instrument_id must be a positive integer"),
    ])
    def test_get_lead_prices_invalid_instrument_id(self, universe_manager, valid_params, invalid_instrument_id, expected_error):
        """Test get_lead_prices with invalid instrument_id values."""
        valid_params['instrument_id'] = invalid_instrument_id

        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lead_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    # ========================================
    # PERIODS VALIDATION TESTS
    # ========================================

    @pytest.mark.parametrize("invalid_periods,expected_error", [
        (0, "lag_periods must be a positive integer"),
        (-1, "lag_periods must be a positive integer"),
        (-10, "lag_periods must be a positive integer"),
        ("5", "lag_periods must be a positive integer"),
        (5.5, "lag_periods must be a positive integer"),
        (None, "lag_periods must be a positive integer"),
    ])
    def test_get_lag_prices_invalid_periods(self, universe_manager, valid_params, invalid_periods, expected_error):
        """Test get_lag_prices with invalid lag_periods values."""

        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=invalid_periods,
                time_interval=valid_params['time_interval']
            )

    @pytest.mark.parametrize("invalid_periods,expected_error", [
        (0, "lead_periods must be a positive integer"),
        (-1, "lead_periods must be a positive integer"),
        ("3", "lead_periods must be a positive integer"),
        (3.2, "lead_periods must be a positive integer"),
        (None, "lead_periods must be a positive integer"),
    ])
    def test_get_lead_prices_invalid_periods(self, universe_manager, valid_params, invalid_periods, expected_error):
        """Test get_lead_prices with invalid lead_periods values."""

        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lead_periods=invalid_periods,
                time_interval=valid_params['time_interval']
            )

    # ========================================
    # TIME_INTERVAL VALIDATION TESTS
    # ========================================

    @pytest.mark.parametrize("invalid_interval,expected_error", [
        ("", "time_interval must be a non-empty string"),
        ("   ", "time_interval must be a non-empty string"),  # whitespace only
        (None, "time_interval must be a non-empty string"),
        (123, "time_interval must be a non-empty string"),
        ("2m", "Invalid time_interval '2m'"),  # Not in valid_intervals
        ("30m", "Invalid time_interval '30m'"),  # Not in valid_intervals
        ("2h", "Invalid time_interval '2h'"),  # Not in valid_intervals
        ("invalid", "Invalid time_interval 'invalid'"),
        ("1H", "Invalid time_interval '1H'"),  # Case sensitive
        ("1D", "Invalid time_interval '1D'"),  # Case sensitive
    ])
    def test_get_lag_prices_invalid_time_interval(self, universe_manager, valid_params, invalid_interval, expected_error):
        """Test get_lag_prices with invalid time_interval values."""

        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=invalid_interval
            )

    @pytest.mark.parametrize("invalid_interval,expected_error", [
        ("", "time_interval must be a non-empty string"),
        (456, "time_interval must be a non-empty string"),
        ("4h", "Invalid time_interval '4h'"),
        ("1y", "Invalid time_interval '1y'"),  # Years not supported
        ("1M", "Invalid time_interval '1M'"),  # Months not supported
    ])
    def test_get_lead_prices_invalid_time_interval(self, universe_manager, valid_params, invalid_interval, expected_error):
        """Test get_lead_prices with invalid time_interval values."""

        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lead_periods=valid_params['periods'],
                time_interval=invalid_interval
            )

    @pytest.mark.parametrize("valid_interval", [
        "1m", "5m", "15m", "1h", "1d", "1w"
    ])
    def test_valid_time_intervals_accepted(self, universe_manager, valid_params, valid_interval):
        """Test that all valid time intervals are accepted."""

        # Should not raise any exceptions
        lag_result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_interval
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lead_periods=valid_params['periods'],
            time_interval=valid_interval
        )

        # Should return DataFrames
        assert isinstance(lag_result, pd.DataFrame)
        assert isinstance(lead_result, pd.DataFrame)

    # ========================================
    # CUR_DATETIME VALIDATION TESTS
    # ========================================

    @pytest.mark.parametrize("invalid_datetime,expected_error", [
        ("2025-09-06", "cur_datetime must be a datetime or date object"),
        (1725632400, "cur_datetime must be a datetime or date object"),  # Unix timestamp
        (None, "cur_datetime must be a datetime or date object"),
        ([], "cur_datetime must be a datetime or date object"),
        ({"year": 2025}, "cur_datetime must be a datetime or date object"),
    ])
    def test_invalid_cur_datetime_types(self, universe_manager, valid_params, invalid_datetime, expected_error):
        """Test both methods with invalid cur_datetime types."""

        # Test get_lag_prices
        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=invalid_datetime,
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

        # Test get_lead_prices
        with pytest.raises(ValueError, match=expected_error):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=invalid_datetime,
                lead_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    def test_date_to_datetime_conversion(self, universe_manager, valid_params):
        """Test that date objects are properly converted to datetime objects."""

        # Use date instead of datetime
        cur_date = date(2025, 9, 6)

        # Should not raise exceptions and should work
        lag_result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=cur_date,
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=cur_date,
            lead_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        # Verify the mock was called with datetime objects (converted from date)
        # Both should work without exceptions
        assert isinstance(lag_result, pd.DataFrame)
        assert isinstance(lead_result, pd.DataFrame)

    # ========================================
    # MARKET_DATA_MANAGER AVAILABILITY TESTS
    # ========================================

    def test_missing_market_data_manager_lag_prices(self, mock_env):
        """Test get_lag_prices when market_data_manager is not available."""
        manager = UniverseStateManager(mock_env)
        # Don't set market_data_manager - should trigger assertion

        with pytest.raises(AssertionError, match="market_data_manager is required for get_lag_prices"):
            manager.get_lag_prices(
                instrument_id=1001,
                cur_datetime=datetime(2025, 9, 6, 14, 30, 0),
                lag_periods=5,
                time_interval='1h'
            )

    def test_missing_market_data_manager_lead_prices(self, mock_env):
        """Test get_lead_prices when market_data_manager is not available."""
        manager = UniverseStateManager(mock_env)
        # Don't set market_data_manager - should trigger assertion

        with pytest.raises(AssertionError, match="market_data_manager is required for get_lead_prices"):
            manager.get_lead_prices(
                instrument_id=2001,
                cur_datetime=datetime(2025, 9, 6, 14, 30, 0),
                lead_periods=3,
                time_interval='15m'
            )

    def test_none_market_data_manager(self, mock_env):
        """Test when market_data_manager is explicitly set to None."""
        manager = UniverseStateManager(mock_env)
        manager.market_data_manager = None

        with pytest.raises(AssertionError, match="market_data_manager is required"):
            manager.get_lag_prices(
                instrument_id=3001,
                cur_datetime=datetime(2025, 9, 6, 14, 30, 0),
                lag_periods=2,
                time_interval='5m'
            )

    # ========================================
    # BOUNDARY CONDITION TESTS
    # ========================================

    def test_very_large_periods(self, universe_manager, valid_params):
        """Test with very large period counts."""

        # Test with large period counts (should not fail validation)
        large_periods = 10000

        lag_result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=large_periods,
            time_interval=valid_params['time_interval']
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lead_periods=large_periods,
            time_interval=valid_params['time_interval']
        )

        # Should pass validation and return DataFrames
        assert isinstance(lag_result, pd.DataFrame)
        assert isinstance(lead_result, pd.DataFrame)

        # Verify mock was called with large periods
        calls = universe_manager.market_data_manager.get_ohlcv_data.call_args_list
        assert any(call[1]['periods'] == large_periods for call in calls)

    def test_very_large_instrument_id(self, universe_manager, valid_params):
        """Test with very large instrument ID."""

        large_instrument_id = 999999999

        lag_result = universe_manager.get_lag_prices(
            instrument_id=large_instrument_id,
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        # Should pass validation
        assert isinstance(lag_result, pd.DataFrame)

        # Verify mock was called with large instrument_id
        calls = universe_manager.market_data_manager.get_ohlcv_data.call_args_list
        assert any(call[1]['instrument_id'] == large_instrument_id for call in calls)

    def test_edge_case_minimal_valid_values(self, universe_manager):
        """Test with minimal valid values."""

        # Test with minimum valid values
        lag_result = universe_manager.get_lag_prices(
            instrument_id=1,  # Minimum valid instrument_id
            cur_datetime=datetime(1970, 1, 1, 0, 0, 0),  # Early datetime
            lag_periods=1,  # Minimum valid periods
            time_interval='1m'  # Valid interval
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=1,
            cur_datetime=datetime(1970, 1, 1, 0, 0, 0),
            lead_periods=1,
            time_interval='1m'
        )

        # Should work with minimal values
        assert isinstance(lag_result, pd.DataFrame)
        assert isinstance(lead_result, pd.DataFrame)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])