#!/usr/bin/env python3
"""
Comprehensive Data Quality and Validation Tests.

Tests realistic data quality issues that occur with financial market data.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.ml.services.configurable_train_data_generator import (
    ConfigurableTrainingDataGenerator,
    ConfigurableTrainingDataConfig
)
from domains.trading.services.feature_registry import FeatureRegistry, FeatureConfig
from domains.trading.services.label_registry import LabelRegistry, LabelConfig

class TestDataQualityIssues:
    """Test various data quality issues common in financial data."""

    def test_price_data_inconsistencies(self):
        """Test handling of OHLC price inconsistencies."""
        dates = pd.date_range('2023-01-01', periods=30, freq='D')

        # Create data with OHLC inconsistencies
        data_rows = []
        for i, date in enumerate(dates):
            if i == 10:
                # Invalid OHLC: high < low
                row = {
                    'symbol': 'AAPL',
                    'open': 100,
                    'high': 95,  # High less than low (invalid)
                    'low': 99,
                    'close': 98,
                    'volume': 1000000
                }
            elif i == 15:
                # Invalid OHLC: close outside high-low range
                row = {
                    'symbol': 'AAPL',
                    'open': 100,
                    'high': 102,
                    'low': 98,
                    'close': 105,  # Close above high (invalid)
                    'volume': 1000000
                }
            elif i == 20:
                # Invalid OHLC: open outside high-low range
                row = {
                    'symbol': 'AAPL',
                    'open': 95,  # Open below low (invalid)
                    'high': 102,
                    'low': 98,
                    'close': 100,
                    'volume': 1000000
                }
            else:
                # Valid OHLC data
                base_price = 100 + i * 0.5
                row = {
                    'symbol': 'AAPL',
                    'open': base_price,
                    'high': base_price + 2,
                    'low': base_price - 1,
                    'close': base_price + 0.5,
                    'volume': 1000000 + i * 10000
                }

            data_rows.append({**row, 'date': date})

        data = pd.DataFrame(data_rows)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date')

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.6  # Allow some invalid data
        )

        generator = ConfigurableTrainingDataGenerator(config)

        # Should handle OHLC inconsistencies without crashing
        result = generator.generate_training_data(data, symbols=['AAPL'])
        assert result['features'].shape[0] > 0

    def test_extreme_price_movements(self):
        """Test handling of extreme price movements (circuit breakers, splits)."""
        dates = pd.date_range('2023-01-01', periods=50, freq='D')

        # Create data with extreme movements
        base_prices = np.linspace(100, 110, 50)
        data_rows = []

        for i, (date, base_price) in enumerate(zip(dates, base_prices)):
            if i == 15:
                # Stock split (price halved overnight)
                split_price = base_price / 2
                row = {
                    'open': split_price,
                    'high': split_price * 1.01,
                    'low': split_price * 0.99,
                    'close': split_price,
                    'volume': 5000000  # Higher volume on split day
                }
            elif i == 25:
                # Flash crash (50% drop intraday)
                row = {
                    'open': base_price,
                    'high': base_price * 1.02,
                    'low': base_price * 0.5,  # Flash crash
                    'close': base_price * 0.95,  # Partial recovery
                    'volume': 10000000  # Very high volume
                }
            elif i == 35:
                # Takeover announcement (30% gap up)
                row = {
                    'open': base_price * 1.3,  # Gap up
                    'high': base_price * 1.35,
                    'low': base_price * 1.28,
                    'close': base_price * 1.32,
                    'volume': 8000000
                }
            else:
                # Normal trading
                row = {
                    'open': base_price,
                    'high': base_price * 1.02,
                    'low': base_price * 0.98,
                    'close': base_price * np.random.uniform(0.99, 1.01),
                    'volume': np.random.uniform(1000000, 2000000)
                }

            data_rows.append({
                'symbol': 'AAPL',
                'date': date,
                **row
            })

        data = pd.DataFrame(data_rows)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date')

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volatility", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 10})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1),
            LabelConfig("direction", "classification", {'class_type': 'direction', 'column': 'close'}, 1)
        ])

        # Test with outlier removal enabled
        config_with_outlier_removal = ConfigurableTrainingDataConfig(
            sequence_length=8,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            remove_outliers=True,
            outlier_threshold=2.0  # Moderate threshold
        )

        # Test without outlier removal
        config_without_outlier_removal = ConfigurableTrainingDataConfig(
            sequence_length=8,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            remove_outliers=False
        )

        generator_with = ConfigurableTrainingDataGenerator(config_with_outlier_removal)
        generator_without = ConfigurableTrainingDataGenerator(config_without_outlier_removal)

        result_with = generator_with.generate_training_data(data, symbols=['AAPL'])
        result_without = generator_without.generate_training_data(data, symbols=['AAPL'])

        # Both should succeed but with different characteristics
        assert result_with['features'].shape[0] > 0
        assert result_without['features'].shape[0] > 0

        # With outlier removal should have less extreme values
        features_with = result_with['features'].numpy() if hasattr(result_with['features'], 'numpy') else result_with['features']
        features_without = result_without['features'].numpy() if hasattr(result_without['features'], 'numpy') else result_without['features']

        # Check that outlier removal reduces extreme values
        max_with = np.nanmax(np.abs(features_with))
        max_without = np.nanmax(np.abs(features_without))
        assert max_with <= max_without

    def test_weekend_and_holiday_gaps(self):
        """Test handling of weekend and holiday gaps in data."""
        # Create weekday-only data with some holiday gaps
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 3, 31)

        # Get business days
        business_days = pd.bdate_range(start_date, end_date)

        # Remove some "holidays"
        holidays = [
            datetime(2023, 1, 16),  # MLK Day
            datetime(2023, 2, 20),  # Presidents Day
            datetime(2023, 2, 21),  # Market closure
        ]

        trading_days = [day for day in business_days if day.date() not in [h.date() for h in holidays]]

        # Create price data with gaps
        data_rows = []
        last_close = 100

        for i, date in enumerate(trading_days):
            # Check if this is after a gap (weekend/holiday)
            if i > 0:
                prev_date = trading_days[i-1]
                days_gap = (date - prev_date).days

                if days_gap > 1:
                    # Apply gap effect (random walk during closure)
                    gap_return = np.random.normal(0, 0.01 * days_gap)  # Bigger gaps = more uncertainty
                    gap_open = last_close * (1 + gap_return)
                else:
                    gap_open = last_close * np.random.uniform(0.995, 1.005)  # Small overnight gap
            else:
                gap_open = last_close

            # Intraday movement
            intraday_return = np.random.normal(0, 0.015)
            close_price = gap_open * (1 + intraday_return)

            row = {
                'symbol': 'AAPL',
                'date': date,
                'open': gap_open,
                'high': max(gap_open, close_price) * np.random.uniform(1.0, 1.02),
                'low': min(gap_open, close_price) * np.random.uniform(0.98, 1.0),
                'close': close_price,
                'volume': np.random.uniform(1000000, 3000000)
            }

            data_rows.append(row)
            last_close = close_price

        data = pd.DataFrame(data_rows)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date')

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("log_returns", "transform", {'transform_type': 'log_return', 'column': 'close'}),
            FeatureConfig("volatility", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 15})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1),
            LabelConfig("future_return_3d", "return", {'return_type': 'simple', 'column': 'close'}, 3)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=10,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle irregular calendar successfully
        assert result['features'].shape[0] > 0

        # Returns should reflect gap effects but be finite
        features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']
        assert np.isfinite(features_array).any()
        assert not np.isinf(features_array).any()

    def test_corporate_actions_impact(self):
        """Test handling of corporate actions (dividends, splits, spinoffs)."""
        dates = pd.date_range('2023-01-01', periods=60, freq='D')

        data_rows = []
        base_price = 100
        cumulative_adjustments = 1.0

        for i, date in enumerate(dates):
            adjusted_base = base_price * cumulative_adjustments

            if i == 20:
                # Dividend payment (ex-dividend date)
                dividend_amount = 2.0  # $2 dividend
                dividend_adjustment = dividend_amount / adjusted_base

                # Price drops by dividend amount at open
                open_price = adjusted_base * (1 - dividend_adjustment)
                close_price = open_price * np.random.uniform(0.99, 1.01)

            elif i == 40:
                # 2-for-1 stock split
                split_ratio = 2.0
                cumulative_adjustments *= split_ratio

                # Price halved, volume doubled
                open_price = adjusted_base / split_ratio
                close_price = open_price * np.random.uniform(0.99, 1.01)
                volume_multiplier = split_ratio

            else:
                # Normal trading
                open_price = adjusted_base * np.random.uniform(0.995, 1.005)
                close_price = open_price * np.random.uniform(0.98, 1.02)
                volume_multiplier = 1.0

            row = {
                'symbol': 'AAPL',
                'date': date,
                'open': round(open_price, 2),
                'high': round(max(open_price, close_price) * 1.01, 2),
                'low': round(min(open_price, close_price) * 0.99, 2),
                'close': round(close_price, 2),
                'volume': int(np.random.uniform(1000000, 2000000) * volume_multiplier)
            }

            data_rows.append(row)
            base_price = close_price / cumulative_adjustments  # Maintain adjusted price progression

        data = pd.DataFrame(data_rows)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date')

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volume_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 10})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=8,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry,
            remove_outliers=True,  # Should handle corporate action artifacts
            outlier_threshold=3.0
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle corporate actions without issues
        assert result['features'].shape[0] > 0

        # Volume ratio should capture split effects
        features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']
        volume_ratio_feature = features_array[:, :, 1]  # Assuming volume ratio is second feature

        # Should have some periods with unusual volume ratios
        assert np.nanmax(volume_ratio_feature) > 1.5  # Split should create volume spike

    def test_timezone_and_timestamp_issues(self):
        """Test handling of timezone and timestamp inconsistencies."""
        # Create data with mixed timezone information
        dates_utc = pd.date_range('2023-01-01', periods=30, freq='D', tz='UTC')
        dates_est = pd.date_range('2023-02-01', periods=30, freq='D', tz='US/Eastern')
        dates_naive = pd.date_range('2023-03-01', periods=30, freq='D')  # No timezone

        all_data = []

        # Create data for each timezone scenario
        for symbol, dates, base_price in [('AAPL', dates_utc, 100), ('MSFT', dates_est, 200), ('GOOGL', dates_naive, 150)]:
            symbol_data = []

            for i, date in enumerate(dates):
                price = base_price + i * 0.5 + np.random.normal(0, 1)
                row = {
                    'symbol': symbol,
                    'date': date,
                    'open': price,
                    'high': price * 1.02,
                    'low': price * 0.98,
                    'close': price,
                    'volume': np.random.uniform(1000000, 2000000)
                }
                symbol_data.append(row)

            all_data.extend(symbol_data)

        data = pd.DataFrame(all_data)

        # Convert to naive datetime for consistency (simulating data cleaning)
        data['date'] = pd.to_datetime(data['date']).dt.tz_localize(None)
        data = data.set_index('date').sort_index()

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL', 'MSFT', 'GOOGL'])

        # Should handle mixed timezone data after normalization
        assert result['features'].shape[0] > 0

    def test_data_vendor_inconsistencies(self):
        """Test handling of inconsistencies between data vendors."""
        dates = pd.date_range('2023-01-01', periods=40, freq='D')

        # Simulate data from two vendors with slight inconsistencies
        vendor_a_data = []
        vendor_b_data = []

        for i, date in enumerate(dates):
            base_price = 100 + i * 0.3

            # Vendor A (higher precision, slightly different values)
            a_row = {
                'symbol': 'AAPL',
                'date': date,
                'open': round(base_price * np.random.uniform(0.998, 1.002), 4),  # Higher precision
                'high': round(base_price * 1.015, 4),
                'low': round(base_price * 0.985, 4),
                'close': round(base_price, 4),
                'volume': int(np.random.uniform(1000000, 2000000))  # Exact volume
            }

            # Vendor B (lower precision, slightly different values due to rounding/timing)
            b_row = {
                'symbol': 'AAPL',
                'date': date,
                'open': round(base_price * np.random.uniform(0.999, 1.001), 2),  # Lower precision
                'high': round(base_price * 1.014, 2),  # Slightly different
                'low': round(base_price * 0.986, 2),
                'close': round(base_price * 1.001, 2),  # Slight difference
                'volume': int(np.random.uniform(950000, 1950000))  # Different volume reporting
            }

            if i < 20:
                vendor_a_data.append(a_row)
            else:
                vendor_b_data.append(b_row)  # Switch vendors midway

        # Combine data (simulating vendor switch)
        data = pd.DataFrame(vendor_a_data + vendor_b_data)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date').sort_index()

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volume_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 10})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=8,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle vendor inconsistencies smoothly
        assert result['features'].shape[0] > 0

        # Features should be stable across vendor switch
        features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']

        # No extreme jumps in features due to vendor switch
        returns_feature = features_array[:, :, 0]  # Assuming returns is first feature
        assert np.nanmax(np.abs(returns_feature)) < 0.5  # No 50%+ jumps

class TestDataCompleteness:
    """Test handling of incomplete data scenarios."""

    def test_partial_symbol_coverage(self):
        """Test with symbols having different data coverage periods."""
        # Create symbols with different listing dates
        all_dates = pd.date_range('2023-01-01', periods=100, freq='D')

        symbols_data = []

        # Mature stock: full coverage
        mature_dates = all_dates
        mature_data = self._create_symbol_data('MATURE', mature_dates, 100)
        symbols_data.append(mature_data)

        # Recent IPO: only last 30 days
        ipo_dates = all_dates[-30:]
        ipo_data = self._create_symbol_data('IPO', ipo_dates, 50)
        symbols_data.append(ipo_data)

        # Delisted stock: only first 60 days
        delisted_dates = all_dates[:60]
        delisted_data = self._create_symbol_data('DELISTED', delisted_dates, 150)
        symbols_data.append(delisted_data)

        # Sparse trading: missing random days
        sparse_dates = all_dates[::2]  # Every other day
        sparse_data = self._create_symbol_data('SPARSE', sparse_dates, 75)
        symbols_data.append(sparse_data)

        data = pd.concat(symbols_data)

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volatility", "transform", {'transform_type': 'volatility', 'column': 'close', 'window': 10})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=15,
            prediction_horizon=5,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.7
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['MATURE', 'IPO', 'DELISTED', 'SPARSE'])

        # Should handle partial coverage gracefully
        assert result['features'].shape[0] > 0

        # MATURE should contribute most sequences, IPO and DELISTED fewer
        # (Cannot test directly, but should complete without error)

    def test_missing_ohlcv_components(self):
        """Test with missing individual OHLCV components."""
        dates = pd.date_range('2023-01-01', periods=50, freq='D')

        data_rows = []
        for i, date in enumerate(dates):
            base_price = 100 + i * 0.5

            row = {
                'symbol': 'AAPL',
                'date': date,
                'open': base_price,
                'high': base_price * 1.02,
                'low': base_price * 0.98,
                'close': base_price,
                'volume': np.random.uniform(1000000, 2000000)
            }

            # Introduce missing components
            if i == 10:
                row['open'] = np.nan  # Missing open
            elif i == 15:
                row['high'] = np.nan  # Missing high
            elif i == 20:
                row['low'] = np.nan   # Missing low
            elif i == 25:
                row['volume'] = np.nan  # Missing volume
            elif i == 30:
                # Missing multiple components
                row['high'] = np.nan
                row['volume'] = np.nan

            data_rows.append(row)

        data = pd.DataFrame(data_rows)
        data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date')

        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("volume_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 10})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=8,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.6  # Allow some missing data
        )

        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Should handle missing OHLCV components
        assert result['features'].shape[0] > 0

    def _create_symbol_data(self, symbol, dates, base_price):
        """Helper to create data for a symbol."""
        np.random.seed(hash(symbol) % 2**32)

        data_rows = []
        last_price = base_price

        for date in dates:
            # Random walk
            change = np.random.normal(0.001, 0.02)
            price = last_price * (1 + change)

            row = {
                'symbol': symbol,
                'date': date,
                'open': last_price,
                'high': price * np.random.uniform(1.0, 1.02),
                'low': price * np.random.uniform(0.98, 1.0),
                'close': price,
                'volume': np.random.uniform(500000, 2000000)
            }

            data_rows.append(row)
            last_price = price

        return pd.DataFrame(data_rows)

class TestErrorHandlingAndRecovery:
    """Test error handling and recovery mechanisms."""

    def test_feature_calculation_errors(self):
        """Test handling of feature calculation errors."""
        dates = pd.date_range('2023-01-01', periods=20, freq='D')

        # Create data that might cause calculation errors
        problematic_data = pd.DataFrame({
            'symbol': ['AAPL'] * 20,
            'open': [0] * 5 + [100] * 10 + [np.inf] * 5,  # Zeros and infinity
            'high': [0] * 5 + [102] * 10 + [np.inf] * 5,
            'low': [0] * 5 + [98] * 10 + [-np.inf] * 5,   # Negative infinity
            'close': [0] * 5 + [100] * 10 + [np.inf] * 5,
            'volume': [0] * 5 + [1000000] * 10 + [np.inf] * 5
        }, index=dates)

        # Feature that might fail with problematic data
        feature_registry = FeatureRegistry([
            FeatureConfig("returns", "transform", {'transform_type': 'pct_change', 'column': 'close'}),
            FeatureConfig("log_returns", "transform", {'transform_type': 'log_return', 'column': 'close'}),
            FeatureConfig("volume_ratio", "transform", {'transform_type': 'volume_ratio', 'window': 5})
        ])

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=5,
            prediction_horizon=3,
            feature_registry=feature_registry,
            label_registry=label_registry,
            min_valid_ratio=0.3  # Very lenient due to problematic data
        )

        generator = ConfigurableTrainingDataGenerator(config)

        # Should handle calculation errors gracefully
        try:
            result = generator.generate_training_data(problematic_data, symbols=['AAPL'])

            # If it succeeds, check for reasonable output
            if result['features'].shape[0] > 0:
                features_array = result['features'].numpy() if hasattr(result['features'], 'numpy') else result['features']

                # Should not contain infinite values in final output
                assert not np.isinf(features_array).any()

        except ValueError as e:
            # It's acceptable to fail with clearly invalid data
            assert "No training sequences generated" in str(e) or "insufficient data" in str(e).lower()

    def test_memory_pressure_handling(self):
        """Test behavior under memory pressure."""
        # Create moderately large dataset
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
        dates = pd.date_range('2022-01-01', periods=500, freq='D')  # ~2 years

        all_data = []
        for symbol in symbols:
            np.random.seed(hash(symbol) % 2**32)

            returns = np.random.normal(0.0005, 0.02, len(dates))
            prices = [100]
            for ret in returns:
                prices.append(prices[-1] * (1 + ret))

            symbol_data = pd.DataFrame({
                'symbol': [symbol] * len(dates),
                'open': prices[1:],
                'high': [p * 1.02 for p in prices[1:]],
                'low': [p * 0.98 for p in prices[1:]],
                'close': prices[1:],
                'volume': np.random.uniform(1000000, 5000000, len(dates))
            }, index=dates)

            all_data.append(symbol_data)

        data = pd.concat(all_data)

        # Create many features to increase memory usage
        feature_configs = []
        for window in [5, 10, 20, 50]:
            feature_configs.extend([
                FeatureConfig(f"returns_{window}d", "transform",
                             {'transform_type': 'pct_change', 'column': 'close', 'periods': window}),
                FeatureConfig(f"volatility_{window}d", "transform",
                             {'transform_type': 'volatility', 'column': 'close', 'window': window})
            ])

        feature_registry = FeatureRegistry(feature_configs)

        label_registry = LabelRegistry([
            LabelConfig("future_return", "return", {'return_type': 'simple', 'column': 'close'}, 1)
        ])

        config = ConfigurableTrainingDataConfig(
            sequence_length=50,  # Long sequences
            prediction_horizon=10,
            feature_registry=feature_registry,
            label_registry=label_registry,
            window_stride=10  # Reduce memory by skipping windows
        )

        generator = ConfigurableTrainingDataGenerator(config)

        import psutil
        import os

        # Monitor memory during processing
        process = psutil.Process(os.getpid())

        try:
            result = generator.generate_training_data(data, symbols=symbols)

            # Should complete successfully
            assert result['features'].shape[0] > 0

            # Memory should not be excessive (adjust threshold as needed)
            memory_mb = process.memory_info().rss / 1024 / 1024
            assert memory_mb < 4000  # Less than 4GB

        except MemoryError:
            # Acceptable failure mode under memory pressure
            pytest.skip("Insufficient memory for test")

def run_comprehensive_tests():
    """Run all comprehensive data quality tests."""
    import pytest

    return pytest.main([__file__, '-v', '--tb=short', '-s'])

if __name__ == "__main__":
    run_comprehensive_tests()