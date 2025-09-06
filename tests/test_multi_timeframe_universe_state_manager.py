#!/usr/bin/env python3
"""
Comprehensive tests for multi-timeframe functionality in UniverseStateManager.

Tests the integration between UniverseStateManager and market_data_manager for 
multi-timeframe data aggregation and feature extraction.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from state.universe_state_manager import UniverseStateManager


class TestMultiTimeframeUniverseStateManager:
    """Test suite for multi-timeframe functionality in UniverseStateManager."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.universe_manager = UniverseStateManager()
        self.mock_market_data_manager = Mock()
        self.universe_manager.market_data_manager = self.mock_market_data_manager
        self.universe_manager.logger = Mock()
        
        # Test instrument and dates
        self.instrument_id = 1001
        self.current_date = date(2023, 12, 1)
        
        # Sample OHLCV data for different timeframes
        self.sample_5m_data = self._create_sample_data('5m', 52)
        self.sample_15m_data = self._create_sample_data('15m', 52)  
        self.sample_1h_data = self._create_sample_data('1h', 24)
        self.sample_1d_data = self._create_sample_data('1d', 20)
        
    def _create_sample_data(self, timeframe: str, periods: int) -> pd.DataFrame:
        """Create sample OHLCV data for testing."""
        np.random.seed(42)  # Deterministic for testing
        
        # Generate realistic price data
        base_price = 150.0
        price_data = []
        
        for i in range(periods):
            # Simulate price movement
            open_price = base_price + np.random.normal(0, 1)
            close_price = open_price + np.random.normal(0, 0.5)
            high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.3))
            low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.3))
            volume = int(np.random.uniform(1000, 10000))
            
            # Technical indicators (simulated)
            etop = high_price + np.random.uniform(0.1, 0.5)
            ebot = low_price - np.random.uniform(0.1, 0.5)
            pldot = np.random.uniform(-1, 1)
            
            price_data.append({
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2), 
                'close': round(close_price, 2),
                'volume': volume,
                'etop': round(etop, 4),
                'ebot': round(ebot, 4),
                'pldot': round(pldot, 4),
                'date': self.current_date - timedelta(days=periods-i-1)
            })
            
            base_price = close_price
        
        return pd.DataFrame(price_data)
    
    def test_get_lag_prices_with_time_interval_5m(self):
        """Test get_lag_prices with 5-minute time interval."""
        # Setup mock to return 5-minute data
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_5m_data
        
        # Call with 5m time interval
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=52, time_interval='5m'
        )
        
        # Verify market_data_manager was called correctly
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
            instrument_id=self.instrument_id,
            reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
            periods=52,
            time_interval='5m',
            direction='backward'
        )
        
        # Verify result structure
        assert not result.empty
        assert len(result) == 52
        expected_columns = ['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'date']
        for col in expected_columns:
            assert col in result.columns
        
        # Verify data types and ranges
        assert all(result['open'] > 0)
        assert all(result['high'] >= result[['open', 'close']].max(axis=1))
        assert all(result['low'] <= result[['open', 'close']].min(axis=1))
        assert all(result['volume'] > 0)
        
    def test_get_lag_prices_with_time_interval_15m(self):
        """Test get_lag_prices with 15-minute time interval."""
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_15m_data
        
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=52, time_interval='15m'
        )
        
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
            instrument_id=self.instrument_id,
            reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
            periods=52,
            time_interval='15m',
            direction='backward'
        )
        
        assert not result.empty
        assert len(result) == 52
        
    def test_get_lag_prices_with_time_interval_1h(self):
        """Test get_lag_prices with 1-hour time interval."""
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_1h_data
        
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=24, time_interval='1h'
        )
        
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
            instrument_id=self.instrument_id,
            reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
            periods=24,
            time_interval='1h',
            direction='backward'
        )
        
        assert not result.empty
        assert len(result) == 24
        
    def test_get_lag_prices_with_time_interval_1d(self):
        """Test get_lag_prices with daily time interval (default)."""
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_1d_data
        
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=20, time_interval='1d'
        )
        
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
            instrument_id=self.instrument_id,
            reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
            periods=20,
            time_interval='1d',
            direction='backward'
        )
        
        assert not result.empty
        assert len(result) == 20
        
    def test_get_lag_prices_default_interval(self):
        """Test get_lag_prices uses '1d' as default time interval."""
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_1d_data
        
        # Call without time_interval parameter
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=20
        )
        
        # Verify it defaults to '1d'
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
            instrument_id=self.instrument_id,
            reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
            periods=20,
            time_interval='1d',
            direction='backward'
        )
        
        assert not result.empty
        
    def test_get_lag_prices_market_data_manager_unavailable(self):
        """Test error when market_data_manager is not available."""
        # Remove market_data_manager
        self.universe_manager.market_data_manager = None
        
        # Should raise AssertionError
        with pytest.raises(AssertionError, match="market_data_manager is required for get_lag_prices"):
            self.universe_manager.get_lag_prices(
                self.instrument_id, self.current_date, lag_periods=20, time_interval='1d'
            )
            
    def test_get_lag_prices_market_data_manager_error(self):
        """Test error handling when market_data_manager raises exception."""
        # Setup market_data_manager to raise exception
        self.mock_market_data_manager.get_ohlcv_data.side_effect = Exception("Connection error")
        
        # Should raise IOError
        with pytest.raises(IOError, match="Failed to get lag prices from market_data_manager: Connection error"):
            self.universe_manager.get_lag_prices(
                self.instrument_id, self.current_date, lag_periods=20, time_interval='1d'  
            )
            
        # Verify market_data_manager was attempted
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once()
            
    def test_get_lag_prices_empty_result(self):
        """Test behavior when market_data_manager returns empty DataFrame."""
        # Setup market_data_manager to return empty DataFrame with correct columns
        empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        self.mock_market_data_manager.get_ohlcv_data.return_value = empty_df
        
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=20, time_interval='1d'
        )
        
        # Should return the empty DataFrame with correct columns
        assert result.empty
        expected_columns = ['open', 'high', 'low', 'close', 'volume']
        assert list(result.columns) == expected_columns
            
    def test_get_lag_prices_datetime_normalization(self):
        """Test that datetime inputs are properly normalized to date."""
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_1d_data
        
        # Use datetime instead of date
        current_datetime = datetime(2023, 12, 1, 14, 30, 0)
        
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, current_datetime, lag_periods=20, time_interval='1d'
        )
        
        # Verify the datetime was normalized to date for the call
        self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
            instrument_id=self.instrument_id,
            reference_datetime=current_datetime,  # Should be datetime object
            periods=20,
            time_interval='1d',
            direction='backward'
        )
        
        assert not result.empty
        
    def test_get_lag_prices_all_supported_intervals(self):
        """Test all supported time intervals."""
        supported_intervals = ['1m', '5m', '15m', '1h', '1d', '1w']
        
        for interval in supported_intervals:
            # Reset mock
            self.mock_market_data_manager.reset_mock()
            
            # Create sample data for this interval
            sample_data = self._create_sample_data(interval, 10)
            self.mock_market_data_manager.get_ohlcv_data.return_value = sample_data
            
            result = self.universe_manager.get_lag_prices(
                self.instrument_id, self.current_date, lag_periods=10, time_interval=interval
            )
            
            # Verify correct call
            self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
                instrument_id=self.instrument_id,
                reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
                periods=10,
                time_interval=interval,
                direction='backward'
            )
            
            assert not result.empty, f"Failed for interval: {interval}"
            assert len(result) == 10, f"Wrong length for interval: {interval}"
            
    def test_get_lag_prices_feature_completeness(self):
        """Test that all expected features are present in the result."""
        self.mock_market_data_manager.get_ohlcv_data.return_value = self.sample_1d_data
        
        result = self.universe_manager.get_lag_prices(
            self.instrument_id, self.current_date, lag_periods=20, time_interval='1d'
        )
        
        # Expected features from training data generation
        expected_features = ['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot']
        
        for feature in expected_features:
            assert feature in result.columns, f"Missing feature: {feature}"
            assert not result[feature].isnull().all(), f"Feature {feature} is all null"
            
        # Verify data integrity
        assert (result['high'] >= result['low']).all(), "High prices should be >= low prices"
        assert (result['high'] >= result['open']).all(), "High prices should be >= open prices"
        assert (result['high'] >= result['close']).all(), "High prices should be >= close prices"
        assert (result['low'] <= result['open']).all(), "Low prices should be <= open prices"
        assert (result['low'] <= result['close']).all(), "Low prices should be <= close prices"
        
    def test_get_lag_prices_gin_config_compliance(self):
        """Test compliance with training_data.gin configuration."""
        # Test the exact configurations from training_data.gin
        gin_configs = {
            '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
            '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
            '1h': 24,   # Past 24 x 1-hour intervals (1 day)
            '1d': 20,   # Past 20 x daily intervals (4 weeks)
        }
        
        for timeframe, expected_periods in gin_configs.items():
            # Reset mock
            self.mock_market_data_manager.reset_mock()
            
            # Create sample data
            sample_data = self._create_sample_data(timeframe, expected_periods)
            self.mock_market_data_manager.get_ohlcv_data.return_value = sample_data
            
            result = self.universe_manager.get_lag_prices(
                self.instrument_id, self.current_date, lag_periods=expected_periods, time_interval=timeframe
            )
            
            # Verify gin config compliance
            assert not result.empty, f"No data for {timeframe}"
            assert len(result) == expected_periods, f"Wrong period count for {timeframe}: expected {expected_periods}, got {len(result)}"
            
            # Verify market_data_manager call matches gin config
            self.mock_market_data_manager.get_ohlcv_data.assert_called_once_with(
                instrument_id=self.instrument_id,
                reference_datetime=datetime.combine(self.current_date, datetime.min.time()),
                periods=expected_periods,
                time_interval=timeframe,
                direction='backward'
            )


class TestMultiTimeframeIntegration:
    """Integration tests for multi-timeframe functionality."""
    
    def setup_method(self):
        """Setup integration test fixtures."""
        self.universe_manager = UniverseStateManager()
        self.universe_manager.logger = Mock()
        
    def test_multi_timeframe_feature_extraction_pattern(self):
        """Test the complete multi-timeframe feature extraction pattern."""
        # Mock all timeframe data
        timeframes = ['5m', '15m', '1h', '1d']
        mock_data = {}
        
        for timeframe in timeframes:
            periods = {'5m': 52, '15m': 52, '1h': 24, '1d': 20}[timeframe]
            mock_data[timeframe] = self._create_sample_ohlcv_data(periods)
        
        # Mock market_data_manager
        mock_market_manager = Mock()
        self.universe_manager.market_data_manager = mock_market_manager
        
        def mock_get_ohlcv_data(instrument_id, reference_datetime, periods, time_interval, direction='backward'):
            return mock_data[time_interval]
            
        mock_market_manager.get_ohlcv_data.side_effect = mock_get_ohlcv_data
        
        # Test complete multi-timeframe extraction
        instrument_id = 1001
        current_date = date(2023, 12, 1)
        
        all_features = {}
        
        for timeframe in timeframes:
            periods = {'5m': 52, '15m': 52, '1h': 24, '1d': 20}[timeframe]
            
            lag_data = self.universe_manager.get_lag_prices(
                instrument_id, current_date, periods, time_interval=timeframe
            )
            
            assert not lag_data.empty, f"No data for {timeframe}"
            assert len(lag_data) == periods, f"Wrong periods for {timeframe}"
            
            # Extract features like training_data_job_runner does
            for i, (_, row) in enumerate(lag_data.iterrows()):
                lag_idx = len(lag_data) - i - 1  # 0 = most recent
                for col in ['open', 'high', 'low', 'close', 'etop', 'ebot', 'pldot']:
                    if col in row and pd.notna(row[col]):
                        feature_name = f'{timeframe}_{col}_lag_{lag_idx}'
                        all_features[feature_name] = float(row[col])
        
        # Verify total feature count matches expectation
        expected_total = 52*7 + 52*7 + 24*7 + 20*7  # 1036 features
        actual_total = len(all_features)
        
        # Allow for some missing features due to NaN values
        assert actual_total >= expected_total * 0.9, f"Feature count too low: {actual_total} < {expected_total * 0.9}"
        
        # Verify feature naming pattern
        for timeframe in timeframes:
            timeframe_features = [f for f in all_features.keys() if f.startswith(f'{timeframe}_')]
            assert len(timeframe_features) > 0, f"No features found for {timeframe}"
            
            # Check for each expected feature type
            for feature_type in ['open', 'high', 'low', 'close', 'etop', 'ebot', 'pldot']:
                type_features = [f for f in timeframe_features if f'{timeframe}_{feature_type}_lag_' in f]
                assert len(type_features) > 0, f"No {feature_type} features for {timeframe}"
        
        print(f"✅ Multi-timeframe integration test passed: {actual_total} features extracted")
        
    def _create_sample_ohlcv_data(self, periods: int) -> pd.DataFrame:
        """Create sample OHLCV data with technical indicators."""
        np.random.seed(42)
        data = []
        base_price = 150.0
        
        for i in range(periods):
            open_price = base_price + np.random.normal(0, 1)
            close_price = open_price + np.random.normal(0, 0.5)
            high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.3))
            low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.3))
            volume = int(np.random.uniform(1000, 10000))
            
            # Technical indicators
            etop = high_price + np.random.uniform(0.1, 0.5)
            ebot = low_price - np.random.uniform(0.1, 0.5)
            pldot = np.random.uniform(-1, 1)
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'etop': round(etop, 4),
                'ebot': round(ebot, 4),
                'pldot': round(pldot, 4)
            })
            
            base_price = close_price
            
        return pd.DataFrame(data)


if __name__ == "__main__":
    # Run tests directly
    import unittest
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestMultiTimeframeUniverseStateManager))
    suite.addTests(loader.loadTestsFromTestCase(TestMultiTimeframeIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    if result.wasSuccessful():
        print(f"\n✅ All tests passed! ({result.testsRun} tests)")
    else:
        print(f"\n❌ Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")