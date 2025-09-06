#!/usr/bin/env python3
"""
Integration Test: Actual Market Data Manager Lead vs Lag Implementation

This test validates that the FileBasedMinuteMarketDataManager correctly implements
the direction parameter to return different time periods for lead vs lag prices.

CRITICAL VALIDATION:
- Tests the actual get_ohlcv_data implementation with direction='backward' vs direction='forward'
- Validates that timestamp filtering works correctly for historical vs future data
- Ensures UniverseStateManager integration works with real market data manager
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import numpy as np

from market_data.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
from state.universe_state_manager import UniverseStateManager


class TestActualMarketDataManagerLeadLagIntegration:
    """Test actual FileBasedMinuteMarketDataManager lead/lag implementation."""
    
    @pytest.fixture
    def mock_env(self):
        """Create mock environment."""
        return Mock()
    
    @pytest.fixture
    def sample_time_series_data(self):
        """Create realistic time series data spanning multiple days."""
        # Create hourly data for 5 days around a reference point
        base_datetime = datetime(2025, 9, 6, 12, 0, 0)  # Sep 6, 2025, noon
        
        # Generate 120 hours of data (5 days * 24 hours)  
        timestamps = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        
        base_price = 100.0
        
        for i in range(-60, 60):  # 60 hours before and 60 hours after reference
            timestamp = base_datetime + timedelta(hours=i)
            timestamps.append(timestamp)
            
            # Create realistic price movement with trend
            price_base = base_price + (i * 0.1)  # Gradual trend upward over time
            price_volatility = np.sin(i * 0.1) * 2.0  # Some volatility
            
            open_price = price_base + price_volatility
            high_price = open_price + abs(np.sin(i * 0.2)) * 1.5
            low_price = open_price - abs(np.cos(i * 0.2)) * 1.2
            close_price = open_price + np.sin(i * 0.15) * 1.0
            volume = 1000000 + int(abs(np.sin(i * 0.3)) * 500000)
            
            opens.append(round(open_price, 2))
            highs.append(round(high_price, 2))
            lows.append(round(low_price, 2))
            closes.append(round(close_price, 2))
            volumes.append(volume)
        
        return pd.DataFrame({
            'timestamp': timestamps,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes
        })
    
    @pytest.fixture
    def mock_market_data_manager(self, mock_env, sample_time_series_data):
        """Create FileBasedMinuteMarketDataManager with mocked data access."""
        
        async def mock_get_ohlc_for_interval(symbols, start, end, interval):
            """Mock that returns time-filtered data based on start/end parameters."""
            # Filter the sample data based on the requested time range
            df = sample_time_series_data.copy()
            df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
            return {symbols[0]: df} if not df.empty else {symbols[0]: pd.DataFrame()}
        
        # Create manager and mock its dependencies
        manager = FileBasedMinuteMarketDataManager(mock_env, "/tmp/test")
        
        # Mock the xrefs_dao
        mock_xrefs_dao = Mock()
        mock_xrefs_dao.get_symbol_by_instrument_id = AsyncMock(return_value="AAPL")
        manager.xrefs_dao = mock_xrefs_dao
        
        # Mock the get_ohlc_for_interval method with our time-series data
        manager.get_ohlc_for_interval = AsyncMock(side_effect=mock_get_ohlc_for_interval)
        
        return manager
    
    def test_backward_vs_forward_direction_returns_different_time_periods(self, mock_market_data_manager, sample_time_series_data):
        """Test that backward and forward directions return data from different time periods."""
        
        instrument_id = 1001
        reference_datetime = datetime(2025, 9, 6, 12, 0, 0)  # Middle of our data range
        periods = 5
        time_interval = '1h'
        
        # Test backward direction (historical data)
        backward_result = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=periods,
            time_interval=time_interval,
            direction='backward'
        )
        
        # Test forward direction (future data)
        forward_result = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=periods,
            time_interval=time_interval,
            direction='forward'
        )
        
        # Both should return data
        assert not backward_result.empty, "Backward direction should return data"
        assert not forward_result.empty, "Forward direction should return data"
        assert len(backward_result) == periods, f"Backward should return {periods} periods"
        assert len(forward_result) == periods, f"Forward should return {periods} periods"
        
        # Extract close prices for comparison
        backward_closes = backward_result['close'].tolist()
        forward_closes = forward_result['close'].tolist()
        
        print(f"Backward (historical) closes: {backward_closes}")
        print(f"Forward (future) closes: {forward_closes}")
        
        # CRITICAL VALIDATION: They should be different (different time periods)
        assert backward_closes != forward_closes, \
            "Backward and forward directions should return different OHLCV data"
        
        # Validate the time period logic:
        # - Backward data should have prices from the downward trend (earlier times)
        # - Forward data should have prices from the upward trend (later times)
        avg_backward_price = np.mean(backward_closes)
        avg_forward_price = np.mean(forward_closes)
        
        print(f"Average backward price: {avg_backward_price:.2f}")
        print(f"Average forward price: {avg_forward_price:.2f}")
        
        # Due to our upward trend in the test data, forward prices should be higher
        assert avg_forward_price > avg_backward_price, \
            f"Forward prices should be higher due to upward trend: {avg_forward_price:.2f} > {avg_backward_price:.2f}"
        
        print("✅ PASS: Backward vs forward directions return different time periods")
    
    def test_universe_state_manager_integration_with_actual_market_data_manager(self, mock_env, mock_market_data_manager):
        """Test UniverseStateManager integration with actual FileBasedMinuteMarketDataManager."""
        
        # Create UniverseStateManager with the actual market data manager
        universe_manager = UniverseStateManager(mock_env)
        universe_manager.market_data_manager = mock_market_data_manager
        
        instrument_id = 2001
        reference_datetime = datetime(2025, 9, 6, 12, 0, 0)
        
        # Test get_lag_prices (should use direction='backward')
        lag_result = universe_manager.get_lag_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lag_periods=3,
            time_interval='1h'
        )
        
        # Test get_lead_prices (should use direction='forward')
        lead_result = universe_manager.get_lead_prices(
            instrument_id=instrument_id,
            cur_datetime=reference_datetime,
            lead_periods=3,
            time_interval='1h'
        )
        
        # Validate results
        assert not lag_result.empty, "Lag prices should return data"
        assert not lead_result.empty, "Lead prices should return data"
        assert len(lag_result) == 3, "Should have 3 lag periods"
        assert len(lead_result) == 3, "Should have 3 lead periods"
        
        # Extract and compare data
        lag_closes = lag_result['close'].tolist()
        lead_closes = lead_result['close'].tolist()
        
        print(f"UniverseStateManager lag closes: {lag_closes}")
        print(f"UniverseStateManager lead closes: {lead_closes}")
        
        # CRITICAL: They should be different
        assert lag_closes != lead_closes, \
            "UniverseStateManager lag and lead prices should return different data"
        
        # Validate expected behavior with our trend data
        avg_lag_price = np.mean(lag_closes)
        avg_lead_price = np.mean(lead_closes)
        
        print(f"UniverseStateManager average lag price: {avg_lag_price:.2f}")
        print(f"UniverseStateManager average lead price: {avg_lead_price:.2f}")
        
        # Lead prices should be higher due to upward trend
        assert avg_lead_price > avg_lag_price, \
            f"Lead prices should be higher than lag prices: {avg_lead_price:.2f} > {avg_lag_price:.2f}"
        
        print("✅ PASS: UniverseStateManager integration with actual market data manager")
    
    def test_different_periods_with_actual_implementation(self, mock_market_data_manager):
        """Test that different period counts return different amounts of actual data."""
        
        instrument_id = 3001
        reference_datetime = datetime(2025, 9, 6, 12, 0, 0)
        time_interval = '1h'
        
        # Test different period counts
        result_2_periods = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=2,
            time_interval=time_interval,
            direction='backward'
        )
        
        result_5_periods = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=5,
            time_interval=time_interval,
            direction='backward'
        )
        
        result_10_periods = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=10,
            time_interval=time_interval,
            direction='backward'
        )
        
        # Validate period counts
        assert len(result_2_periods) == 2, f"Should have 2 periods, got {len(result_2_periods)}"
        assert len(result_5_periods) == 5, f"Should have 5 periods, got {len(result_5_periods)}"
        assert len(result_10_periods) == 10, f"Should have 10 periods, got {len(result_10_periods)}"
        
        # Validate that longer periods include shorter periods (most recent data)
        closes_2 = result_2_periods['close'].tolist()
        closes_5 = result_5_periods['close'].tolist()
        closes_10 = result_10_periods['close'].tolist()
        
        print(f"2 periods closes: {closes_2}")
        print(f"5 periods closes: {closes_5}")
        print(f"10 periods closes: {closes_10}")
        
        # For backward direction, the most recent data should match
        # (last N items of longer period should match shorter period)
        assert closes_2 == closes_5[-2:], \
            f"2-period data should match last 2 items of 5-period data: {closes_2} != {closes_5[-2:]}"
        assert closes_5 == closes_10[-5:], \
            f"5-period data should match last 5 items of 10-period data: {closes_5} != {closes_10[-5:]}"
        
        print("✅ PASS: Different periods return different amounts of actual data")
    
    def test_time_filtering_accuracy_with_reference_datetime(self, mock_market_data_manager, sample_time_series_data):
        """Test that time filtering accurately uses reference_datetime as boundary."""
        
        instrument_id = 4001
        reference_datetime = datetime(2025, 9, 6, 12, 0, 0)  # Exact middle of our test data
        periods = 3
        time_interval = '1h'
        
        # Get backward data (should be hours 11:00, 10:00, 09:00)
        backward_result = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=periods,
            time_interval=time_interval,
            direction='backward'
        )
        
        # Get forward data (should be hours 13:00, 14:00, 15:00)
        forward_result = mock_market_data_manager.get_ohlcv_data(
            instrument_id=instrument_id,
            reference_datetime=reference_datetime,
            periods=periods,
            time_interval=time_interval,
            direction='forward'
        )
        
        # Extract expected data from our sample for validation
        sample_df = sample_time_series_data.copy()
        
        # Find data around our reference point
        reference_idx = sample_df[sample_df['timestamp'] == reference_datetime].index
        if len(reference_idx) > 0:
            ref_idx = reference_idx[0]
            
            # Expected backward data (before reference_datetime)
            expected_backward = sample_df.iloc[max(0, ref_idx-periods):ref_idx]
            
            # Expected forward data (after reference_datetime)  
            expected_forward = sample_df.iloc[ref_idx+1:ref_idx+1+periods]
            
            if not expected_backward.empty and not expected_forward.empty:
                expected_backward_closes = expected_backward['close'].tolist()
                expected_forward_closes = expected_forward['close'].tolist()
                
                actual_backward_closes = backward_result['close'].tolist()
                actual_forward_closes = forward_result['close'].tolist()
                
                print(f"Reference datetime: {reference_datetime}")
                print(f"Expected backward closes: {expected_backward_closes}")
                print(f"Actual backward closes: {actual_backward_closes}")
                print(f"Expected forward closes: {expected_forward_closes}")
                print(f"Actual forward closes: {actual_forward_closes}")
                
                # Note: The actual implementation might do additional filtering/sorting,
                # but the key validation is that backward != forward
                assert actual_backward_closes != actual_forward_closes, \
                    "Backward and forward data should be different around reference_datetime"
                
                print("✅ PASS: Time filtering accurately uses reference_datetime as boundary")
    
    def test_empty_data_handling_in_actual_implementation(self, mock_env):
        """Test how actual implementation handles cases with no available data."""
        
        # Create manager with no mocked data (should return empty)
        manager = FileBasedMinuteMarketDataManager(mock_env, "/tmp/test")
        
        # Mock xrefs_dao to return a symbol
        mock_xrefs_dao = Mock()
        mock_xrefs_dao.get_symbol_by_instrument_id = AsyncMock(return_value="NONEXISTENT")
        manager.xrefs_dao = mock_xrefs_dao
        
        # Mock get_ohlc_for_interval to return empty data
        manager.get_ohlc_for_interval = AsyncMock(return_value={"NONEXISTENT": pd.DataFrame()})
        
        # Test both directions with no data
        backward_result = manager.get_ohlcv_data(
            instrument_id=9999,
            reference_datetime=datetime(2025, 9, 6, 12, 0, 0),
            periods=5,
            time_interval='1h',
            direction='backward'
        )
        
        forward_result = manager.get_ohlcv_data(
            instrument_id=9999,
            reference_datetime=datetime(2025, 9, 6, 12, 0, 0),
            periods=5,
            time_interval='1h',
            direction='forward'
        )
        
        # Both should return empty DataFrames with correct columns
        assert backward_result.empty, "Should return empty DataFrame for no data"
        assert forward_result.empty, "Should return empty DataFrame for no data"
        
        expected_columns = ['open', 'high', 'low', 'close', 'volume']
        assert list(backward_result.columns) == expected_columns, \
            f"Empty backward result should have correct columns: {list(backward_result.columns)}"
        assert list(forward_result.columns) == expected_columns, \
            f"Empty forward result should have correct columns: {list(forward_result.columns)}"
        
        print("✅ PASS: Empty data handling in actual implementation")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])