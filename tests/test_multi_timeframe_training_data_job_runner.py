#!/usr/bin/env python3
"""
Comprehensive tests for multi-timeframe functionality in TrainingDataJobRunner.

Tests the multi-timeframe feature extraction and hourly training data generation
with proper integration to UniverseStateManager and market_data_manager.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from pathlib import Path
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.app.training_data_job_runner import TrainingDataJobRunner, TrainingDataJobConfig


class TestMultiTimeframeTrainingDataJobRunner:
    """Test suite for multi-timeframe functionality in TrainingDataJobRunner."""

    def setup_method(self):
        """Setup test fixtures."""
        # Create test configuration
        self.config = TrainingDataJobConfig(
            job_name="test_multi_timeframe",
            symbols=['AAPL', 'TSLA'],
            start_date=date(2023, 11, 1),
            end_date=date(2023, 12, 1),
            base_interval_minutes=1,
            training_interval_minutes=60,
            output_structure="hourly_rows",
            use_universe_state_indicators=True,
            normalize_features=False,
            feature_configs=[{"name": "multi_timeframe", "enabled": True}],
            label_configs=[{"name": "none", "enabled": False}]
        )

        # Create runner instance with mocked environment
        self.mock_env = Mock()
        self.mock_env.get_database_url.return_value = "mock://database"
        self.mock_env.get_table_name.return_value = "mock_table"

        self.runner = TrainingDataJobRunner(self.config, env=self.mock_env)

        # Test data
        self.test_symbol = 'AAPL'
        self.test_timestamp = pd.Timestamp('2023-12-01 14:30:00')

    def _create_mock_universe_manager_with_multi_timeframe_data(self):
        """Create a mock universe manager with multi-timeframe data."""
        mock_universe_manager = Mock()

        # Mock data for different timeframes
        timeframe_data = {
            '5m': self._create_sample_lag_data(52, base_price=150.0),
            '15m': self._create_sample_lag_data(52, base_price=150.1),
            '1h': self._create_sample_lag_data(24, base_price=150.2),
            '1d': self._create_sample_lag_data(20, base_price=150.3)
        }

        def mock_get_lag_prices(instrument_id, cur_date, lag_days, time_interval='1d'):
            return timeframe_data.get(time_interval, pd.DataFrame())

        mock_universe_manager.get_lag_prices.side_effect = mock_get_lag_prices

        return mock_universe_manager

    def _create_sample_lag_data(self, periods: int, base_price: float = 150.0) -> pd.DataFrame:
        """Create sample lag data for testing."""
        np.random.seed(42)  # Deterministic for testing
        data = []

        for i in range(periods):
            price_variation = np.random.normal(0, 0.5)
            open_price = base_price + price_variation
            close_price = open_price + np.random.normal(0, 0.2)
            high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.1))
            low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.1))

            # Technical indicators
            etop = high_price + np.random.uniform(0.05, 0.2)
            ebot = low_price - np.random.uniform(0.05, 0.2)
            pldot = np.random.uniform(-0.5, 0.5)

            data.append({
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': int(np.random.uniform(1000, 5000)),
                'etop': round(etop, 4),
                'ebot': round(ebot, 4),
                'pldot': round(pldot, 4)
            })

        return pd.DataFrame(data)

    def test_get_multi_timeframe_features_from_universe_state_basic(self):
        """Test basic multi-timeframe feature extraction."""
        mock_universe_manager = self._create_mock_universe_manager_with_multi_timeframe_data()

        # Call the method
        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Verify features were extracted
        assert isinstance(features, dict)
        assert len(features) > 0

        # Verify timeframe structure
        timeframes = ['5m', '15m', '1h', '1d']
        feature_types = ['open', 'high', 'low', 'close', 'etop', 'ebot', 'pldot']

        for timeframe in timeframes:
            timeframe_features = [f for f in features.keys() if f.startswith(f'{timeframe}_')]
            assert len(timeframe_features) > 0, f"No features found for {timeframe}"

            # Check for each feature type
            for feature_type in feature_types:
                type_features = [f for f in timeframe_features if f'{timeframe}_{feature_type}_lag_' in f]
                if type_features:  # Allow for missing features due to NaN values
                    # Verify lag index pattern
                    for feature in type_features[:3]:  # Check first few
                        assert '_lag_' in feature
                        lag_part = feature.split('_lag_')[1]
                        assert lag_part.isdigit(), f"Invalid lag index in {feature}"

        print(f"✅ Basic test passed: {len(features)} features extracted")

    def test_get_multi_timeframe_features_gin_config_compliance(self):
        """Test compliance with training_data.gin configuration."""
        mock_universe_manager = self._create_mock_universe_manager_with_multi_timeframe_data()

        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Expected from training_data.gin
        expected_config = {
            '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
            '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
            '1h': 24,   # Past 24 x 1-hour intervals (1 day)
            '1d': 20,   # Past 20 x daily intervals (4 weeks)
        }

        # Verify universe_manager was called with correct parameters
        for timeframe, expected_periods in expected_config.items():
            mock_universe_manager.get_lag_prices.assert_any_call(
                mock_universe_manager.get_lag_prices.call_args_list[0][0][0],  # instrument_id
                self.test_timestamp.date(),
                expected_periods,
                time_interval=timeframe
            )

        # Verify feature naming pattern compliance
        for timeframe, expected_periods in expected_config.items():
            timeframe_features = [f for f in features.keys() if f.startswith(f'{timeframe}_')]

            # Should have features with lag indices from 0 to (expected_periods-1)
            max_lag_found = -1
            for feature in timeframe_features:
                if '_lag_' in feature:
                    lag_idx = int(feature.split('_lag_')[1])
                    max_lag_found = max(max_lag_found, lag_idx)

            # Allow some tolerance for missing data
            assert max_lag_found >= expected_periods * 0.5, \
                f"Not enough lag features for {timeframe}: max_lag={max_lag_found}, expected>={expected_periods}"

        print(f"✅ Gin config compliance test passed")

    def test_get_multi_timeframe_features_expected_feature_count(self):
        """Test that we get approximately the expected number of features."""
        mock_universe_manager = self._create_mock_universe_manager_with_multi_timeframe_data()

        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Expected feature breakdown (from gin config):
        # 5m: 52 intervals × 7 features = 364
        # 15m: 52 intervals × 7 features = 364
        # 1h: 24 intervals × 7 features = 168
        # 1d: 20 intervals × 7 features = 140
        # Total expected: 1036 features

        expected_total = 52*7 + 52*7 + 24*7 + 20*7  # 1036
        actual_total = len(features)

        # Allow for some missing features (NaN values, etc.)
        min_expected = expected_total * 0.7  # 70% tolerance

        assert actual_total >= min_expected, \
            f"Feature count too low: {actual_total} < {min_expected} (expected ~{expected_total})"

        print(f"✅ Feature count test passed: {actual_total} features (expected ~{expected_total})")

    def test_get_multi_timeframe_features_data_quality(self):
        """Test data quality of extracted features."""
        mock_universe_manager = self._create_mock_universe_manager_with_multi_timeframe_data()

        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Data quality checks
        for feature_name, value in features.items():
            # All features should be numeric
            assert isinstance(value, (int, float)), f"Non-numeric feature: {feature_name} = {value}"

            # No NaN or infinite values
            assert not pd.isna(value), f"NaN feature: {feature_name}"
            assert np.isfinite(value), f"Infinite feature: {feature_name}"

            # Price features should be positive
            if any(price_type in feature_name for price_type in ['open', 'high', 'low', 'close']):
                assert value > 0, f"Non-positive price: {feature_name} = {value}"

            # Volume should be positive
            if 'volume' in feature_name:
                assert value > 0, f"Non-positive volume: {feature_name} = {value}"

        # Test OHLC relationships within each timeframe and lag
        timeframes = ['5m', '15m', '1h', '1d']
        for timeframe in timeframes:
            timeframe_features = {k: v for k, v in features.items() if k.startswith(f'{timeframe}_')}

            # Group by lag index
            lag_groups = {}
            for feature_name, value in timeframe_features.items():
                if '_lag_' in feature_name:
                    parts = feature_name.split('_lag_')
                    base_name = parts[0]
                    lag_idx = int(parts[1])

                    if lag_idx not in lag_groups:
                        lag_groups[lag_idx] = {}
                    lag_groups[lag_idx][base_name] = value

            # Test OHLC relationships for each lag
            for lag_idx, ohlc_data in lag_groups.items():
                ohlc_keys = {
                    'open': f'{timeframe}_open',
                    'high': f'{timeframe}_high',
                    'low': f'{timeframe}_low',
                    'close': f'{timeframe}_close'
                }

                ohlc_values = {}
                for ohlc_type, key in ohlc_keys.items():
                    if key in ohlc_data:
                        ohlc_values[ohlc_type] = ohlc_data[key]

                # Test OHLC relationships if we have all values
                if len(ohlc_values) >= 3:  # Need at least open, high, low or similar
                    if 'high' in ohlc_values and 'low' in ohlc_values:
                        assert ohlc_values['high'] >= ohlc_values['low'], \
                            f"{timeframe} lag_{lag_idx}: high < low"

                    if 'high' in ohlc_values and 'open' in ohlc_values:
                        assert ohlc_values['high'] >= ohlc_values['open'], \
                            f"{timeframe} lag_{lag_idx}: high < open"

                    if 'high' in ohlc_values and 'close' in ohlc_values:
                        assert ohlc_values['high'] >= ohlc_values['close'], \
                            f"{timeframe} lag_{lag_idx}: high < close"

        print(f"✅ Data quality test passed")

    def test_get_multi_timeframe_features_error_handling(self):
        """Test error handling in multi-timeframe feature extraction."""
        # Test with universe manager that raises exceptions
        mock_universe_manager = Mock()
        mock_universe_manager.get_lag_prices.side_effect = Exception("Database connection error")

        # Should handle errors gracefully
        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Should return empty dict on error
        assert isinstance(features, dict)
        assert len(features) == 0

        print(f"✅ Error handling test passed")

    def test_get_multi_timeframe_features_missing_data_handling(self):
        """Test handling of missing data in different timeframes."""
        mock_universe_manager = Mock()

        # Return data for some timeframes, empty for others
        def mock_get_lag_prices(instrument_id, cur_date, lag_days, time_interval='1d'):
            if time_interval in ['5m', '1d']:
                return self._create_sample_lag_data(lag_days)
            else:
                return pd.DataFrame()  # Empty data

        mock_universe_manager.get_lag_prices.side_effect = mock_get_lag_prices

        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Should have features for available timeframes only
        assert len(features) > 0  # Some features should be extracted

        # Should have 5m and 1d features
        has_5m = any(f.startswith('5m_') for f in features.keys())
        has_1d = any(f.startswith('1d_') for f in features.keys())
        assert has_5m, "Should have 5m features"
        assert has_1d, "Should have 1d features"

        # Should not have 15m and 1h features
        has_15m = any(f.startswith('15m_') for f in features.keys())
        has_1h = any(f.startswith('1h_') for f in features.keys())
        assert not has_15m, "Should not have 15m features"
        assert not has_1h, "Should not have 1h features"

        print(f"✅ Missing data handling test passed")

    def test_get_multi_timeframe_features_feature_naming_consistency(self):
        """Test consistency of feature naming across timeframes."""
        mock_universe_manager = self._create_mock_universe_manager_with_multi_timeframe_data()

        features = self.runner._get_multi_timeframe_features_from_universe_state(
            mock_universe_manager, self.test_symbol, self.test_timestamp
        )

        # Test naming pattern: {timeframe}_{feature_type}_lag_{N}
        feature_pattern = r'^(5m|15m|1h|1d)_(open|high|low|close|volume|etop|ebot|pldot)_lag_\d+$'
        import re

        invalid_names = []
        for feature_name in features.keys():
            if not re.match(feature_pattern, feature_name):
                invalid_names.append(feature_name)

        assert len(invalid_names) == 0, f"Invalid feature names: {invalid_names}"

        # Test that lag indices are consistent (0 = most recent, higher = older)
        timeframes = ['5m', '15m', '1h', '1d']
        for timeframe in timeframes:
            timeframe_features = [f for f in features.keys() if f.startswith(f'{timeframe}_')]

            if timeframe_features:  # Only test if we have features for this timeframe
                # Extract lag indices
                lag_indices = set()
                for feature in timeframe_features:
                    if '_lag_' in feature:
                        lag_idx = int(feature.split('_lag_')[1])
                        lag_indices.add(lag_idx)

                if lag_indices:
                    # Should start from 0 (most recent)
                    min_lag = min(lag_indices)
                    assert min_lag == 0, f"{timeframe}: lag indices should start from 0, got min={min_lag}"

                    # Should be consecutive or nearly consecutive
                    max_lag = max(lag_indices)
                    expected_count = max_lag + 1
                    actual_count = len(lag_indices)

                    # Allow for some gaps due to missing data
                    completeness = actual_count / expected_count
                    assert completeness >= 0.8, \
                        f"{timeframe}: lag indices not sufficiently complete: {completeness:.2f}"

        print(f"✅ Feature naming consistency test passed")


class TestMultiTimeframeTrainingDataGeneration:
    """Integration tests for complete multi-timeframe training data generation."""

    def setup_method(self):
        """Setup integration test fixtures."""
        self.config = TrainingDataJobConfig(
            job_name="test_integration",
            symbols=['AAPL'],
            start_date=date(2023, 11, 1),
            end_date=date(2023, 12, 1),
            base_interval_minutes=1,
            training_interval_minutes=60,
            output_structure="hourly_rows",
            use_universe_state_indicators=True,
            normalize_features=False,
            feature_configs=[{"name": "multi_timeframe", "enabled": True}],
            label_configs=[{"name": "none", "enabled": False}]
        )

        self.mock_env = Mock()
        self.mock_env.get_database_url.return_value = "mock://database"
        self.mock_env.get_table_name.return_value = "mock_table"

    def test_hourly_training_data_generation_with_multi_timeframe_features(self):
        """Test that hourly training data generation includes multi-timeframe features."""
        runner = TrainingDataJobRunner(self.config, env=self.mock_env)

        # Mock minute data
        mock_minute_data = self._create_mock_minute_data()

        # Mock universe manager with multi-timeframe capabilities
        mock_universe_manager = self._create_mock_universe_manager()

        # Test the aggregation method
        hourly_data = runner._aggregate_minutes_to_hourly(
            mock_minute_data, 'AAPL', mock_universe_manager
        )

        # Verify structure
        assert isinstance(hourly_data, list)
        assert len(hourly_data) > 0

        # Check first hourly row
        first_row = hourly_data[0]
        assert isinstance(first_row, dict)

        # Should have basic hourly OHLCV
        basic_features = ['datetime', 'symbol', 'hour_open', 'hour_high', 'hour_low', 'hour_close', 'hour_volume']
        for feature in basic_features:
            assert feature in first_row, f"Missing basic feature: {feature}"

        # Should have multi-timeframe features
        multi_timeframe_features = [k for k in first_row.keys()
                                  if any(tf in k for tf in ['5m_', '15m_', '1h_', '1d_'])]

        assert len(multi_timeframe_features) > 0, "No multi-timeframe features found"

        # Verify feature count is substantial
        total_features = len(first_row.keys())
        assert total_features >= 50, f"Too few features: {total_features} (expected >50 with multi-timeframe)"

        print(f"✅ Integration test passed: {total_features} total features per hourly row")

    def _create_mock_minute_data(self) -> pd.DataFrame:
        """Create mock minute-level data."""
        # Create 2 hours of minute data (120 minutes)
        timestamps = pd.date_range('2023-12-01 09:30', periods=120, freq='1T')

        data = []
        base_price = 150.0

        for i, timestamp in enumerate(timestamps):
            price_change = np.random.normal(0, 0.1)
            open_price = base_price + price_change
            close_price = open_price + np.random.normal(0, 0.05)
            high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.02))
            low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.02))

            data.append({
                'datetime': timestamp,
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': int(np.random.uniform(100, 1000))
            })

            base_price = close_price

        return pd.DataFrame(data)

    def _create_mock_universe_manager(self):
        """Create mock universe manager with multi-timeframe data."""
        mock_manager = Mock()

        # Return sample data for each timeframe
        def mock_get_lag_prices(instrument_id, cur_date, lag_days, time_interval='1d'):
            np.random.seed(42)  # Deterministic
            periods = lag_days

            data = []
            base_price = 150.0

            for i in range(periods):
                open_price = base_price + np.random.normal(0, 0.5)
                close_price = open_price + np.random.normal(0, 0.2)
                high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.1))
                low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.1))

                data.append({
                    'open': round(open_price, 2),
                    'high': round(high_price, 2),
                    'low': round(low_price, 2),
                    'close': round(close_price, 2),
                    'volume': int(np.random.uniform(1000, 5000)),
                    'etop': round(high_price + 0.1, 4),
                    'ebot': round(low_price - 0.1, 4),
                    'pldot': round(np.random.uniform(-0.5, 0.5), 4)
                })

                base_price = close_price

            return pd.DataFrame(data)

        mock_manager.get_lag_prices.side_effect = mock_get_lag_prices
        return mock_manager


if __name__ == "__main__":
    # Run tests directly
    import unittest

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestMultiTimeframeTrainingDataJobRunner))
    suite.addTests(loader.loadTestsFromTestCase(TestMultiTimeframeTrainingDataGeneration))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    if result.wasSuccessful():
        print(f"\n✅ All tests passed! ({result.testsRun} tests)")
    else:
        print(f"\n❌ Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")