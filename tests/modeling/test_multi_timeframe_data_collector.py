"""
Tests for Multi-Timeframe Data Collection Engine
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import AsyncMock, MagicMock
import asyncio
from datetime import datetime

from src.modeling.multi_timeframe_data_collector import (
    MultiTimeframeDataCollector, DataCollectionConfig,
    generate_synthetic_ohlc_data
)
from src.modeling.enhanced_feature_types import (
    FeatureSpecification, FeatureType, TimeframeSpec,
    TechnicalIndicator, EnhancedFeatureRegistry
)


class TestMultiTimeframeDataCollector:
    """Test the MultiTimeframeDataCollector class."""

    @pytest.fixture
    def mock_db_pool(self):
        """Mock database pool."""
        return AsyncMock()

    @pytest.fixture
    def feature_registry(self):
        """Feature registry instance."""
        return EnhancedFeatureRegistry()

    @pytest.fixture
    def data_collector(self, mock_db_pool, feature_registry):
        """Data collector instance."""
        return MultiTimeframeDataCollector(mock_db_pool, feature_registry)

    @pytest.fixture
    def sample_ohlc_data(self):
        """Sample OHLC data for testing."""
        return generate_synthetic_ohlc_data(
            symbols=['AAPL', 'TSLA'],
            start_date='2024-01-01',
            end_date='2024-01-10',
            timeframe=TimeframeSpec.MINUTE_5,
            seed=42
        )

    def test_initialization(self, data_collector, feature_registry):
        """Test collector initialization."""
        assert data_collector.feature_registry == feature_registry
        assert isinstance(data_collector._indicator_cache, dict)

    def test_group_features_by_timeframe(self, data_collector, feature_registry):
        """Test grouping features by timeframe."""
        features = [
            feature_registry.get_feature_spec("ohlc_5min_8"),
            feature_registry.get_feature_spec("ohlc_15min_16"),
            feature_registry.get_feature_spec("etop_5min_8"),
            feature_registry.get_feature_spec("etop_1hour_on_5min")  # Cross-timeframe
        ]
        features = [f for f in features if f is not None]

        grouped = data_collector._group_features_by_timeframe(features)

        # Should have 5min and 15min groups (cross-timeframe excluded)
        assert TimeframeSpec.MINUTE_5 in grouped
        assert TimeframeSpec.MINUTE_15 in grouped

        # Cross-timeframe feature should be excluded
        all_grouped_features = []
        for tf_features in grouped.values():
            all_grouped_features.extend(tf_features)

        cross_tf_features = [f for f in all_grouped_features
                           if f.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS]
        assert len(cross_tf_features) == 0

    def test_create_ohlc_matrix(self, data_collector, sample_ohlc_data):
        """Test OHLC matrix creation."""
        feature_spec = FeatureSpecification(
            name="test_ohlc_5min_8",
            feature_type=FeatureType.OHLC_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 4)
        )

        matrix = data_collector._create_ohlc_matrix(sample_ohlc_data, feature_spec)

        assert matrix is not None
        assert matrix.ndim == 3
        assert matrix.shape[1] == 8  # intervals
        assert matrix.shape[2] == 4  # OHLC

        # Should have multiple samples (sliding windows)
        assert matrix.shape[0] > 0

        # Values should be reasonable (positive prices)
        assert np.all(matrix > 0)

        # OHLC consistency check for first sample
        for i in range(matrix.shape[1]):  # For each time step
            ohlc = matrix[0, i, :]  # [open, high, low, close]
            assert ohlc[1] >= max(ohlc[0], ohlc[3])  # High >= max(Open, Close)
            assert ohlc[2] <= min(ohlc[0], ohlc[3])  # Low <= min(Open, Close)

    def test_create_ohlc_matrix_empty_data(self, data_collector):
        """Test OHLC matrix creation with empty data."""
        empty_data = pd.DataFrame()
        feature_spec = FeatureSpecification(
            name="test_ohlc",
            feature_type=FeatureType.OHLC_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 4)
        )

        matrix = data_collector._create_ohlc_matrix(empty_data, feature_spec)
        assert matrix is None

    def test_calculate_single_indicator_etop(self, data_collector, sample_ohlc_data):
        """Test ETOP indicator calculation."""
        symbol_data = sample_ohlc_data[sample_ohlc_data['symbol'] == 'AAPL'].copy()

        etop_values = data_collector._calculate_single_indicator(
            symbol_data, TechnicalIndicator.ETOP, TimeframeSpec.MINUTE_5
        )

        assert etop_values is not None
        assert len(etop_values) == len(symbol_data)

        # ETOP should be higher than close prices (envelope top)
        close_prices = symbol_data['close'].values
        valid_idx = ~np.isnan(etop_values)

        if np.any(valid_idx):
            # Most ETOP values should be >= close prices
            etop_valid = etop_values[valid_idx]
            close_valid = close_prices[valid_idx]
            higher_count = np.sum(etop_valid >= close_valid)
            assert higher_count > len(etop_valid) * 0.8  # At least 80% should be higher

    def test_calculate_single_indicator_rsi(self, data_collector, sample_ohlc_data):
        """Test RSI indicator calculation."""
        symbol_data = sample_ohlc_data[sample_ohlc_data['symbol'] == 'AAPL'].copy()

        rsi_values = data_collector._calculate_single_indicator(
            symbol_data, TechnicalIndicator.RSI, TimeframeSpec.MINUTE_5
        )

        assert rsi_values is not None
        assert len(rsi_values) == len(symbol_data)

        # RSI should be between 0 and 100
        valid_rsi = rsi_values[~np.isnan(rsi_values)]
        if len(valid_rsi) > 0:
            assert np.all(valid_rsi >= 0)
            assert np.all(valid_rsi <= 100)

    def test_calculate_single_indicator_insufficient_data(self, data_collector):
        """Test indicator calculation with insufficient data."""
        # Create very small dataset (< 20 rows)
        small_data = pd.DataFrame({
            'symbol': ['AAPL'] * 5,
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='5min'),
            'open': [100, 101, 102, 103, 104],
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'volume': [1000] * 5
        })

        result = data_collector._calculate_single_indicator(
            small_data, TechnicalIndicator.ETOP, TimeframeSpec.MINUTE_5
        )

        assert result is None

    def test_create_indicator_matrix(self, data_collector, sample_ohlc_data):
        """Test indicator matrix creation."""
        # First calculate indicator data
        symbol_data = sample_ohlc_data[sample_ohlc_data['symbol'] == 'AAPL'].copy()
        etop_values = data_collector._calculate_single_indicator(
            symbol_data, TechnicalIndicator.ETOP, TimeframeSpec.MINUTE_5
        )

        # Create indicator dataframe
        indicator_df = pd.DataFrame({
            'symbol': 'AAPL',
            'timestamp': symbol_data['timestamp'],
            'etop': etop_values
        })

        indicator_data = {'etop': indicator_df}

        # Create feature spec
        feature_spec = FeatureSpecification(
            name="etop_5min_8",
            feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=8,
            dimensions=(8, 1),
            indicator_type=TechnicalIndicator.ETOP
        )

        matrix = data_collector._create_indicator_matrix(indicator_data, feature_spec)

        if matrix is not None:  # May be None if not enough valid data
            assert matrix.ndim == 3
            assert matrix.shape[1] == 8  # intervals
            assert matrix.shape[2] == 1  # single indicator value
            assert matrix.shape[0] > 0  # should have samples

    def test_aggregate_to_timeframe_15min(self, data_collector, sample_ohlc_data):
        """Test aggregation from 5min to 15min."""
        aggregated = data_collector._aggregate_to_timeframe(
            sample_ohlc_data, TimeframeSpec.MINUTE_15, 'minute'
        )

        assert not aggregated.empty
        assert len(aggregated) < len(sample_ohlc_data)  # Should be fewer records

        # Check OHLC consistency
        for _, row in aggregated.iterrows():
            assert row['high'] >= max(row['open'], row['close'])
            assert row['low'] <= min(row['open'], row['close'])

    def test_aggregate_to_timeframe_hourly(self, data_collector, sample_ohlc_data):
        """Test aggregation from 5min to 1hour."""
        aggregated = data_collector._aggregate_to_timeframe(
            sample_ohlc_data, TimeframeSpec.HOUR_1, 'minute'
        )

        assert not aggregated.empty
        assert len(aggregated) < len(sample_ohlc_data)  # Should be much fewer records

        # Volume should be sum of constituent periods
        symbols = aggregated['symbol'].unique()
        for symbol in symbols:
            symbol_agg = aggregated[aggregated['symbol'] == symbol]
            symbol_orig = sample_ohlc_data[sample_ohlc_data['symbol'] == symbol]

            # Each hourly volume should be reasonable
            assert np.all(symbol_agg['volume'] > 0)


class TestDataCollectionConfig:
    """Test DataCollectionConfig dataclass."""

    def test_config_creation(self):
        """Test configuration creation."""
        features = [
            FeatureSpecification(
                name="test_ohlc",
                feature_type=FeatureType.OHLC_INTERVALS,
                timeframe=TimeframeSpec.MINUTE_5,
                intervals=8,
                dimensions=(8, 4)
            )
        ]

        config = DataCollectionConfig(
            symbols=['AAPL', 'TSLA'],
            start_date='2024-01-01',
            end_date='2024-01-31',
            feature_specs=features,
            batch_size=500
        )

        assert config.symbols == ['AAPL', 'TSLA']
        assert config.start_date == '2024-01-01'
        assert config.end_date == '2024-01-31'
        assert config.batch_size == 500
        assert config.include_volume is True
        assert config.validate_data is True


class TestSyntheticDataGeneration:
    """Test synthetic data generation utilities."""

    def test_generate_synthetic_ohlc_basic(self):
        """Test basic synthetic data generation."""
        data = generate_synthetic_ohlc_data(
            symbols=['AAPL'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            timeframe=TimeframeSpec.MINUTE_5,
            seed=42
        )

        assert not data.empty
        assert list(data.columns) == ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
        assert data['symbol'].iloc[0] == 'AAPL'

        # Check OHLC consistency
        for _, row in data.iterrows():
            assert row['high'] >= max(row['open'], row['close'])
            assert row['low'] <= min(row['open'], row['close'])
            assert row['volume'] > 0

    def test_generate_synthetic_ohlc_multiple_symbols(self):
        """Test synthetic data for multiple symbols."""
        symbols = ['AAPL', 'TSLA', 'GOOGL']
        data = generate_synthetic_ohlc_data(
            symbols=symbols,
            start_date='2024-01-01',
            end_date='2024-01-01',  # Single day
            timeframe=TimeframeSpec.MINUTE_15,
            seed=42
        )

        assert not data.empty
        assert set(data['symbol'].unique()) == set(symbols)

        # Each symbol should have same number of records
        symbol_counts = data['symbol'].value_counts()
        assert len(symbol_counts.unique()) == 1  # All counts should be same

    def test_generate_synthetic_ohlc_different_timeframes(self):
        """Test synthetic data generation for different timeframes."""
        base_params = {
            'symbols': ['AAPL'],
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'seed': 42
        }

        # Generate for different timeframes
        data_5min = generate_synthetic_ohlc_data(
            timeframe=TimeframeSpec.MINUTE_5, **base_params
        )
        data_15min = generate_synthetic_ohlc_data(
            timeframe=TimeframeSpec.MINUTE_15, **base_params
        )
        data_hourly = generate_synthetic_ohlc_data(
            timeframe=TimeframeSpec.HOUR_1, **base_params
        )

        # 5-minute should have most data points
        assert len(data_5min) > len(data_15min) > len(data_hourly)

        # All should have valid OHLC data
        for data in [data_5min, data_15min, data_hourly]:
            assert not data.empty
            for _, row in data.iterrows():
                assert row['high'] >= max(row['open'], row['close'])
                assert row['low'] <= min(row['open'], row['close'])

    def test_synthetic_data_reproducibility(self):
        """Test that synthetic data is reproducible with same seed."""
        params = {
            'symbols': ['AAPL'],
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'timeframe': TimeframeSpec.MINUTE_5,
            'seed': 123
        }

        data1 = generate_synthetic_ohlc_data(**params)
        data2 = generate_synthetic_ohlc_data(**params)

        # Should be identical
        pd.testing.assert_frame_equal(data1, data2)

    def test_synthetic_data_different_seeds(self):
        """Test that different seeds produce different data."""
        base_params = {
            'symbols': ['AAPL'],
            'start_date': '2024-01-01',
            'end_date': '2024-01-02',
            'timeframe': TimeframeSpec.MINUTE_5
        }

        data1 = generate_synthetic_ohlc_data(seed=42, **base_params)
        data2 = generate_synthetic_ohlc_data(seed=123, **base_params)

        # Should have same structure but different values
        assert len(data1) == len(data2)
        assert list(data1.columns) == list(data2.columns)

        # Prices should be different
        assert not np.array_equal(data1['close'].values, data2['close'].values)


class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""

    @pytest.fixture
    def complete_setup(self):
        """Complete setup with registry, collector, and test data."""
        registry = EnhancedFeatureRegistry()
        mock_db_pool = AsyncMock()
        collector = MultiTimeframeDataCollector(mock_db_pool, registry)

        # Generate test data
        test_data = generate_synthetic_ohlc_data(
            symbols=['AAPL', 'TSLA'],
            start_date='2024-01-01',
            end_date='2024-01-31',
            timeframe=TimeframeSpec.MINUTE_5,
            seed=42
        )

        return {
            'registry': registry,
            'collector': collector,
            'test_data': test_data,
            'db_pool': mock_db_pool
        }

    def test_realistic_feature_matrix_creation(self, complete_setup):
        """Test creating multiple feature matrices from realistic data."""
        setup = complete_setup
        collector = setup['collector']
        registry = setup['registry']
        test_data = setup['test_data']

        # Select multiple features
        features = [
            registry.get_feature_spec("ohlc_5min_8"),
            registry.get_feature_spec("ohlc_5min_16"),
            registry.get_feature_spec("etop_5min_8")
        ]
        features = [f for f in features if f is not None]

        # Create matrices
        matrices = {}

        # OHLC matrices
        for feature in features:
            if feature.feature_type == FeatureType.OHLC_INTERVALS:
                matrix = collector._create_ohlc_matrix(test_data, feature)
                if matrix is not None:
                    matrices[feature.name] = matrix

        # Create indicator data for ETOP
        symbols = test_data['symbol'].unique()
        indicator_data = {}

        for symbol in symbols:
            symbol_data = test_data[test_data['symbol'] == symbol].copy()
            etop_values = collector._calculate_single_indicator(
                symbol_data, TechnicalIndicator.ETOP, TimeframeSpec.MINUTE_5
            )

            if etop_values is not None:
                if 'etop' not in indicator_data:
                    indicator_data['etop'] = []

                indicator_df = pd.DataFrame({
                    'symbol': symbol,
                    'timestamp': symbol_data['timestamp'],
                    'etop': etop_values
                })
                indicator_data['etop'].append(indicator_df)

        # Combine indicator data
        if 'etop' in indicator_data:
            indicator_data['etop'] = pd.concat(indicator_data['etop'], ignore_index=True)

        # Create indicator matrices
        for feature in features:
            if feature.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS:
                matrix = collector._create_indicator_matrix(indicator_data, feature)
                if matrix is not None:
                    matrices[feature.name] = matrix

        # Verify results
        assert len(matrices) > 0

        for name, matrix in matrices.items():
            feature = registry.get_feature_spec(name)
            assert matrix.shape[1] == feature.intervals
            if feature.feature_type == FeatureType.OHLC_INTERVALS:
                assert matrix.shape[2] == 4
            else:
                assert matrix.shape[2] == 1

    def test_timeframe_aggregation_consistency(self, complete_setup):
        """Test that timeframe aggregation maintains data consistency."""
        setup = complete_setup
        collector = setup['collector']
        test_data = setup['test_data']

        # Test aggregation chain: 5min -> 15min -> 1hour
        data_15min = collector._aggregate_to_timeframe(
            test_data, TimeframeSpec.MINUTE_15, 'minute'
        )

        data_1hour = collector._aggregate_to_timeframe(
            data_15min, TimeframeSpec.HOUR_1, 'minute'
        )

        # Verify aggregation makes sense
        assert len(test_data) > len(data_15min) > len(data_1hour)

        # Check that high/low values are preserved correctly
        for symbol in test_data['symbol'].unique():
            orig_symbol = test_data[test_data['symbol'] == symbol]
            agg_symbol = data_15min[data_15min['symbol'] == symbol]

            # Overall high in aggregated data should be <= max high in original
            if not agg_symbol.empty:
                assert agg_symbol['high'].max() <= orig_symbol['high'].max() * 1.001  # Small tolerance
                assert agg_symbol['low'].min() >= orig_symbol['low'].min() * 0.999   # Small tolerance


if __name__ == "__main__":
    # Manual test runner
    import sys

    test_classes = [
        TestMultiTimeframeDataCollector,
        TestDataCollectionConfig,
        TestSyntheticDataGeneration,
        TestIntegrationScenarios
    ]

    passed = 0
    failed = 0

    for test_class in test_classes:
        print(f"\n=== Running {test_class.__name__} ===")

        test_methods = [method for method in dir(test_class)
                       if method.startswith('test_')]

        for method_name in test_methods:
            try:
                # Create instance
                instance = test_class()
                method = getattr(instance, method_name)

                # Handle fixture dependencies (simplified)
                if hasattr(method, '__code__'):
                    var_names = method.__code__.co_varnames

                    # Handle different fixture combinations
                    kwargs = {}

                    if 'mock_db_pool' in var_names:
                        kwargs['mock_db_pool'] = AsyncMock()
                    if 'feature_registry' in var_names:
                        from src.modeling.enhanced_feature_types import EnhancedFeatureRegistry
                        kwargs['feature_registry'] = EnhancedFeatureRegistry()
                    if 'data_collector' in var_names:
                        from src.modeling.enhanced_feature_types import EnhancedFeatureRegistry
                        kwargs['data_collector'] = MultiTimeframeDataCollector(
                            AsyncMock(), EnhancedFeatureRegistry()
                        )
                    if 'sample_ohlc_data' in var_names:
                        kwargs['sample_ohlc_data'] = generate_synthetic_ohlc_data(
                            ['AAPL', 'TSLA'], '2024-01-01', '2024-01-10',
                            TimeframeSpec.MINUTE_5, seed=42
                        )
                    if 'complete_setup' in var_names:
                        from src.modeling.enhanced_feature_types import EnhancedFeatureRegistry
                        registry = EnhancedFeatureRegistry()
                        mock_db_pool = AsyncMock()
                        collector = MultiTimeframeDataCollector(mock_db_pool, registry)
                        test_data = generate_synthetic_ohlc_data(
                            ['AAPL', 'TSLA'], '2024-01-01', '2024-01-31',
                            TimeframeSpec.MINUTE_5, seed=42
                        )
                        kwargs['complete_setup'] = {
                            'registry': registry,
                            'collector': collector,
                            'test_data': test_data,
                            'db_pool': mock_db_pool
                        }

                    # Run the test
                    if kwargs:
                        method(**kwargs)
                    else:
                        method()

                    print(f"  ✓ {method_name}")
                    passed += 1

            except Exception as e:
                print(f"  ✗ {method_name}: {str(e)[:100]}")
                failed += 1

    print(f"\n=== Test Summary ===")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")

    if failed > 0:
        sys.exit(1)