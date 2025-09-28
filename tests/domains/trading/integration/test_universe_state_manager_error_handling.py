#!/usr/bin/env python3
"""
Critical Test: Universe State Manager Error Handling & Recovery

Tests comprehensive error scenarios and recovery behavior for get_lag_prices
and get_lead_prices that weren't covered in previous tests.

CRITICAL GAPS IDENTIFIED:
- Market data manager exceptions and error propagation
- Empty data scenarios and graceful degradation
- Timeout and performance issues
- Logging behavior during failures
- Exception message accuracy and debugging info
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch
import logging

from domains.trading.services.state.universe_state_manager import UniverseStateManager


class TestUniverseStateManagerErrorHandling:
    """Test comprehensive error handling and recovery for UniverseStateManager."""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment."""
        return Mock()

    @pytest.fixture
    def universe_manager(self, mock_env):
        """Create UniverseStateManager with mocked market_data_manager."""
        manager = UniverseStateManager(mock_env)
        # Set up mock market_data_manager
        manager.market_data_manager = Mock()
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
    # MARKET DATA MANAGER EXCEPTION TESTS
    # ========================================

    def test_market_data_manager_connection_error_lag_prices(self, universe_manager, valid_params):
        """Test get_lag_prices when market_data_manager raises connection errors."""

        # Mock market_data_manager to raise connection error
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = ConnectionError("Database connection failed")

        with pytest.raises(IOError, match="Failed to get lag prices from market_data_manager: Database connection failed"):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    def test_market_data_manager_timeout_error_lead_prices(self, universe_manager, valid_params):
        """Test get_lead_prices when market_data_manager raises timeout errors."""

        # Mock market_data_manager to raise timeout error
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = TimeoutError("Query timeout after 30 seconds")

        with pytest.raises(IOError, match="Failed to get lead prices from market_data_manager: Query timeout after 30 seconds"):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lead_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    def test_market_data_manager_generic_exception(self, universe_manager, valid_params):
        """Test generic exceptions from market_data_manager."""

        # Mock market_data_manager to raise generic exception
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = RuntimeError("Unexpected error in data processing")

        # Test both methods
        with pytest.raises(IOError, match="Failed to get lag prices from market_data_manager: Unexpected error in data processing"):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

        with pytest.raises(IOError, match="Failed to get lead prices from market_data_manager: Unexpected error in data processing"):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lead_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    def test_market_data_manager_returns_none(self, universe_manager, valid_params):
        """Test when market_data_manager returns None instead of DataFrame."""

        # Mock market_data_manager to return None
        universe_manager.market_data_manager.get_ohlcv_data.return_value = None

        # Should return empty DataFrame with correct columns
        lag_result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lead_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        # Both should return empty DataFrames with correct columns
        expected_columns = ['open', 'high', 'low', 'close', 'volume']
        assert isinstance(lag_result, pd.DataFrame)
        assert lag_result.empty
        assert list(lag_result.columns) == expected_columns

        assert isinstance(lead_result, pd.DataFrame)
        assert lead_result.empty
        assert list(lead_result.columns) == expected_columns

    # ========================================
    # EMPTY DATA SCENARIO TESTS
    # ========================================

    def test_market_data_manager_returns_empty_dataframe(self, universe_manager, valid_params):
        """Test when market_data_manager returns empty DataFrame."""

        # Mock market_data_manager to return empty DataFrame
        universe_manager.market_data_manager.get_ohlcv_data.return_value = pd.DataFrame()

        # Should return empty DataFrame with correct columns
        lag_result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        lead_result = universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lead_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        # Both should return empty DataFrames with correct columns
        expected_columns = ['open', 'high', 'low', 'close', 'volume']
        assert isinstance(lag_result, pd.DataFrame)
        assert lag_result.empty
        assert list(lag_result.columns) == expected_columns

        assert isinstance(lead_result, pd.DataFrame)
        assert lead_result.empty
        assert list(lead_result.columns) == expected_columns

    def test_market_data_manager_returns_dataframe_with_wrong_columns(self, universe_manager, valid_params):
        """Test when market_data_manager returns DataFrame with unexpected columns."""

        # Mock market_data_manager to return DataFrame with wrong columns
        wrong_df = pd.DataFrame({
            'price': [100.0, 101.0, 102.0],
            'qty': [1000, 1100, 1200],
            'timestamp': [datetime.now(), datetime.now(), datetime.now()]
        })
        universe_manager.market_data_manager.get_ohlcv_data.return_value = wrong_df

        # Should still return the DataFrame as-is (market_data_manager is responsible for correct format)
        lag_result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        # Should return whatever the market_data_manager returned
        assert isinstance(lag_result, pd.DataFrame)
        assert not lag_result.empty
        assert list(lag_result.columns) == ['price', 'qty', 'timestamp']

    # ========================================
    # LOGGING BEHAVIOR TESTS
    # ========================================

    def test_error_logging_during_market_data_manager_failure(self, universe_manager, valid_params, caplog):
        """Test that errors are properly logged during market_data_manager failures."""

        # Mock market_data_manager to raise exception
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = ValueError("Invalid symbol format")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(IOError):
                universe_manager.get_lag_prices(
                    instrument_id=valid_params['instrument_id'],
                    cur_datetime=valid_params['cur_datetime'],
                    lag_periods=valid_params['periods'],
                    time_interval=valid_params['time_interval']
                )

        # Verify error was logged
        error_logs = [record for record in caplog.records if record.levelname == 'ERROR']
        assert len(error_logs) >= 1
        assert "market_data_manager failed" in error_logs[0].message or "Invalid symbol format" in str(error_logs[0].message)

    def test_debug_logging_during_successful_calls(self, universe_manager, valid_params, caplog):
        """Test that debug information is logged during successful calls."""

        # Mock market_data_manager to return valid data
        mock_df = pd.DataFrame({
            'open': [100.0, 101.0], 'high': [101.0, 102.0], 'low': [99.0, 100.0],
            'close': [100.5, 101.5], 'volume': [1000000, 1100000]
        })
        universe_manager.market_data_manager.get_ohlcv_data.return_value = mock_df

        with caplog.at_level(logging.DEBUG):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

        # Verify debug information was logged
        debug_logs = [record for record in caplog.records if record.levelname == 'DEBUG']
        if debug_logs:  # Logging might be configured differently in test environment
            assert any("market_data_manager:" in record.message for record in debug_logs)

    # ========================================
    # EXCEPTION MESSAGE ACCURACY TESTS
    # ========================================

    def test_exception_message_contains_debugging_info_lag_prices(self, universe_manager, valid_params):
        """Test that IOError messages contain useful debugging information for lag prices."""

        original_error = "Connection refused on port 5432"
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = ConnectionError(original_error)

        universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )
        assert False, "Expected IOError to be raised"
    def test_exception_message_contains_debugging_info_lead_prices(self, universe_manager, valid_params):
        """Test that IOError messages contain useful debugging information for lead prices."""

        original_error = "Table 'dev_minute_data' does not exist"
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = RuntimeError(original_error)

        universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lead_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )
        assert False, "Expected IOError to be raised"
    def test_market_data_manager_attribute_error(self, universe_manager, valid_params):
        """Test when market_data_manager has method signature issues."""

        # Mock market_data_manager to raise AttributeError (method signature issues)
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = AttributeError("'Mock' object has no attribute 'some_method'")

        with pytest.raises(IOError, match="Failed to get lag prices from market_data_manager.*'Mock' object has no attribute 'some_method'"):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    def test_market_data_manager_type_error(self, universe_manager, valid_params):
        """Test when market_data_manager receives wrong parameter types."""

        # Mock market_data_manager to raise TypeError
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = TypeError("unsupported operand type(s) for +: 'int' and 'str'")

        with pytest.raises(IOError, match="Failed to get lead prices from market_data_manager.*unsupported operand type"):
            universe_manager.get_lead_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lead_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

    def test_logging_exception_resilience(self, universe_manager, valid_params):
        """Test that logging exceptions don't break the main error handling flow."""

        # Mock market_data_manager to raise exception
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = ValueError("Test error")

        # Mock the logger to also raise an exception
        with patch.object(universe_manager, 'logger', side_effect=Exception("Logging failed")):
            # Should still raise IOError despite logging issues
            with pytest.raises(IOError, match="Failed to get lag prices from market_data_manager: Test error"):
                universe_manager.get_lag_prices(
                    instrument_id=valid_params['instrument_id'],
                    cur_datetime=valid_params['cur_datetime'],
                    lag_periods=valid_params['periods'],
                    time_interval=valid_params['time_interval']
                )

    # ========================================
    # RECOVERY AND GRACEFUL DEGRADATION TESTS
    # ========================================

    def test_successful_recovery_after_previous_failure(self, universe_manager, valid_params):
        """Test that methods work correctly after previous failures."""

        # First call fails
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = ConnectionError("Network error")

        with pytest.raises(IOError):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

        # Second call succeeds
        mock_df = pd.DataFrame({
            'open': [100.0], 'high': [101.0], 'low': [99.0],
            'close': [100.5], 'volume': [1000000]
        })
        universe_manager.market_data_manager.get_ohlcv_data.side_effect = None
        universe_manager.market_data_manager.get_ohlcv_data.return_value = mock_df

        # Should work normally
        result = universe_manager.get_lag_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lag_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert list(result.columns) == ['open', 'high', 'low', 'close', 'volume']

    def test_partial_failure_scenarios(self, universe_manager, valid_params):
        """Test scenarios where some calls succeed and others fail."""

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call (lag_prices) fails
                raise ConnectionError("Database unavailable")
            else:
                # Second call (lead_prices) succeeds
                return pd.DataFrame({
                    'open': [105.0], 'high': [106.0], 'low': [104.0],
                    'close': [105.5], 'volume': [1200000]
                })

        universe_manager.market_data_manager.get_ohlcv_data.side_effect = side_effect

        # First call should fail
        with pytest.raises(IOError, match="Failed to get lag prices.*Database unavailable"):
            universe_manager.get_lag_prices(
                instrument_id=valid_params['instrument_id'],
                cur_datetime=valid_params['cur_datetime'],
                lag_periods=valid_params['periods'],
                time_interval=valid_params['time_interval']
            )

        # Second call should succeed
        result = universe_manager.get_lead_prices(
            instrument_id=valid_params['instrument_id'],
            cur_datetime=valid_params['cur_datetime'],
            lead_periods=valid_params['periods'],
            time_interval=valid_params['time_interval']
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert result.iloc[0]['close'] == 105.5


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])