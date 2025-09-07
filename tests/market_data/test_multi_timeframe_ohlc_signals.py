#!/usr/bin/env python3
"""
Comprehensive Test Suite for Multi-Timeframe OHLC and Signals

Tests the FileBasedMinuteMarketDataManager's enhanced APIs to ensure:
1. Accurate OHLC aggregation across all timeframes (5m, 15m, 1h, 1d, 1w)
2. Correct technical signal computation for all indicators
3. Proper integration with IntervalBasedTrainingDataCallback
4. Data consistency and mathematical correctness
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import tempfile
import shutil
import logging

# Import test subjects
from core.config.environment import Environment, EnvironmentType
from market_data.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

# Setup logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestMultiTimeframeOHLCSignals:
    """Test suite for multi-timeframe OHLC aggregation and signal computation."""

    @pytest.fixture
    def test_env(self):
        """Create test environment."""
        return Environment(None, EnvironmentType.TEST)

    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary directory for test data."""
        temp_dir = tempfile.mkdtemp(prefix="test_minute_data_")
        yield Path(temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def sample_minute_data(self):
        """Generate sample 1-minute OHLCV data for testing."""
        # Generate 7 days of minute data (7 * 24 * 60 = 10,080 minutes)
        start_time = datetime(2025, 1, 1, 9, 30)  # Market open
        end_time = start_time + timedelta(days=7)

        # Create minute-by-minute timestamps (market hours only for realistic data)
        timestamps = []
        current = start_time
        while current < end_time:
            # Skip weekends and non-market hours (simplified)
            if current.weekday() < 5:  # Monday to Friday
                if 9 <= current.hour < 16:  # Market hours 9 AM to 4 PM
                    timestamps.append(current)
            current += timedelta(minutes=1)

        n_bars = len(timestamps)

        # Generate realistic OHLCV data
        base_price = 150.0

        # Generate price movements with some trend and volatility
        price_changes = np.random.normal(0, 0.001, n_bars)  # Small random changes
        price_levels = base_price + np.cumsum(price_changes)

        # Generate OHLCV for each minute
        data = []
        for i, ts in enumerate(timestamps):
            price = price_levels[i]

            # Generate intrabar price action
            high_offset = abs(np.random.normal(0, 0.002))
            low_offset = abs(np.random.normal(0, 0.002))

            # Ensure OHLC relationships are maintained
            if i == 0:
                open_price = price
            else:
                open_price = data[i-1]['close']  # Open = previous close

            high = price + high_offset
            low = price - low_offset
            close = price
            volume = int(np.random.normal(1000, 300))  # Random volume

            # Ensure high >= low and OHLC within range
            high = max(high, open_price, close)
            low = min(low, open_price, close)
            volume = max(volume, 1)

            data.append({
                'timestamp': ts,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })

        return pd.DataFrame(data)

    @pytest.fixture
    async def market_data_manager(self, test_env, temp_data_dir, sample_minute_data):
        """Create FileBasedMinuteMarketDataManager with test data."""
        # Create test symbol directory structure
        symbol_dir = temp_data_dir / "test_vendor" / "AAPL"
        symbol_dir.mkdir(parents=True)

        # Save sample data as parquet files (one file per day)
        sample_minute_data['date'] = sample_minute_data['timestamp'].dt.date
        for date, day_data in sample_minute_data.groupby('date'):
            file_path = symbol_dir / f"AAPL_{date.strftime('%Y%m%d')}.parquet"
            day_data.drop('date', axis=1).to_parquet(file_path, index=False)

        # Create mock FileBasedMinuteManager
        class MockMinuteManager:
            def __init__(self, base_path):
                self.base_path = base_path

            async def query_minute_data(self, symbol, start_date, end_date):
                # Return the sample data for any query
                mask = (sample_minute_data['timestamp'] >= start_date) & \
                       (sample_minute_data['timestamp'] <= end_date)
                return sample_minute_data[mask].reset_index(drop=True)

        # Create manager with mock data
        manager = FileBasedMinuteMarketDataManager(test_env, str(temp_data_dir))
        manager.minute_manager = MockMinuteManager(temp_data_dir)

        return manager

    @pytest.mark.asyncio
    async def test_interval_parsing(self, market_data_manager):
        """Test interval notation parsing accuracy."""

        test_cases = [
            ('1m', 1),
            ('5m', 5),
            ('15m', 15),
            ('30m', 30),
            ('1h', 60),
            ('2h', 120),
            ('4h', 240),
            ('1d', 1440),
            ('1w', 10080),
            ('1M', 43800)
        ]

        for interval, expected_minutes in test_cases:
            result = market_data_manager._parse_interval_to_minutes(interval)
            assert result == expected_minutes, f"Interval {interval} should parse to {expected_minutes} minutes"

        # Test invalid intervals
        with pytest.raises(ValueError):
            market_data_manager._parse_interval_to_minutes('invalid')

        with pytest.raises(ValueError):
            market_data_manager._parse_interval_to_minutes('1x')

    @pytest.mark.asyncio
    async def test_ohlc_aggregation_mathematical_correctness(self, market_data_manager):
        """Test OHLC aggregation mathematical correctness for all timeframes."""

        symbols = ['AAPL']
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 8)

        # Test all supported timeframes
        timeframes = ['1m', '5m', '15m', '1h', '1d', '1w']

        for interval in timeframes:
            logger.info(f"Testing OHLC aggregation for interval: {interval}")

            result = await market_data_manager.get_ohlc_for_interval(
                symbols=symbols,
                start=start,
                end=end,
                interval=interval
            )

            assert 'AAPL' in result, f"AAPL data missing for interval {interval}"
            df = result['AAPL']

            # Verify basic structure
            assert not df.empty, f"No data returned for interval {interval}"
            assert all(col in df.columns for col in ['timestamp', 'open', 'high', 'low', 'close', 'volume']), \
                f"Missing required columns for interval {interval}"

            # Verify OHLC mathematical relationships
            assert (df['high'] >= df['low']).all(), f"High < Low violation in {interval} data"
            assert (df['high'] >= df['open']).all(), f"High < Open violation in {interval} data"
            assert (df['high'] >= df['close']).all(), f"High < Close violation in {interval} data"
            assert (df['low'] <= df['open']).all(), f"Low > Open violation in {interval} data"
            assert (df['low'] <= df['close']).all(), f"Low > Close violation in {interval} data"
            assert (df['volume'] >= 0).all(), f"Negative volume in {interval} data"

            # Verify timestamps are in ascending order
            assert df['timestamp'].is_monotonic_increasing, f"Timestamps not ascending for {interval}"

            logger.info(f"✅ {interval}: {len(df)} bars, OHLC relationships verified")

    @pytest.mark.asyncio
    async def test_manual_aggregation_verification(self, market_data_manager, sample_minute_data):
        """Manually verify aggregation logic against known minute data."""

        symbols = ['AAPL']
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 2)

        # Get 5-minute aggregated data
        result_5m = await market_data_manager.get_ohlc_for_interval(
            symbols=symbols,
            start=start,
            end=end,
            interval='5m'
        )

        df_5m = result_5m['AAPL']

        if not df_5m.empty:
            # Manually verify first 5-minute bar
            first_bar = df_5m.iloc[0]
            first_5min_start = first_bar['timestamp']
            first_5min_end = first_5min_start + timedelta(minutes=5)

            # Get corresponding minute data
            minute_mask = (sample_minute_data['timestamp'] >= first_5min_start) & \
                          (sample_minute_data['timestamp'] < first_5min_end)
            minute_subset = sample_minute_data[minute_mask]

            if not minute_subset.empty:
                # Verify aggregation rules
                expected_open = minute_subset['open'].iloc[0]  # First open
                expected_high = minute_subset['high'].max()    # Max high
                expected_low = minute_subset['low'].min()      # Min low
                expected_close = minute_subset['close'].iloc[-1]  # Last close
                expected_volume = minute_subset['volume'].sum()   # Sum volume

                tolerance = 1e-10  # Floating point tolerance

                assert abs(first_bar['open'] - expected_open) < tolerance, \
                    f"5m open aggregation incorrect: {first_bar['open']} vs {expected_open}"
                assert abs(first_bar['high'] - expected_high) < tolerance, \
                    f"5m high aggregation incorrect: {first_bar['high']} vs {expected_high}"
                assert abs(first_bar['low'] - expected_low) < tolerance, \
                    f"5m low aggregation incorrect: {first_bar['low']} vs {expected_low}"
                assert abs(first_bar['close'] - expected_close) < tolerance, \
                    f"5m close aggregation incorrect: {first_bar['close']} vs {expected_close}"
                assert first_bar['volume'] == expected_volume, \
                    f"5m volume aggregation incorrect: {first_bar['volume']} vs {expected_volume}"

                logger.info("✅ Manual 5-minute aggregation verification passed")

    @pytest.mark.asyncio
    async def test_technical_signal_computation(self, market_data_manager):
        """Test technical signal computation accuracy for all indicators."""

        symbols = ['AAPL']
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 8)

        # Test signals computation for 1-hour timeframe (good balance of data points)
        signals = ['sma_20', 'ema_12', 'rsi_14', 'etop', 'ebot', 'pldot', 'vwap', 'bb_upper', 'bb_lower']

        result = await market_data_manager.get_ohlc_with_signals(
            symbols=symbols,
            start=start,
            end=end,
            interval='1h',
            signals=signals
        )

        assert 'AAPL' in result, "AAPL data missing from signals result"
        df = result['AAPL']

        # Verify all signals are present
        for signal in signals:
            assert signal in df.columns, f"Signal {signal} missing from result"

            # Verify signal has some non-null values
            non_null_count = df[signal].notna().sum()
            assert non_null_count > 0, f"Signal {signal} has no non-null values"

            logger.info(f"✅ Signal {signal}: {non_null_count}/{len(df)} non-null values")

        # Verify specific signal properties
        if len(df) >= 20:  # Need enough data for meaningful tests

            # SMA should be average of last 20 periods
            sma_20 = df['sma_20'].iloc[-1]
            manual_sma = df['close'].tail(20).mean()
            assert abs(sma_20 - manual_sma) < 1e-10, f"SMA calculation incorrect: {sma_20} vs {manual_sma}"

            # RSI should be between 0 and 100
            rsi_values = df['rsi_14'].dropna()
            assert (rsi_values >= 0).all() and (rsi_values <= 100).all(), "RSI values outside 0-100 range"

            # ETOP should be above EBOT
            etop_ebot_mask = df['etop'].notna() & df['ebot'].notna()
            if etop_ebot_mask.any():
                etop_above_ebot = (df.loc[etop_ebot_mask, 'etop'] >= df.loc[etop_ebot_mask, 'ebot']).all()
                assert etop_above_ebot, "ETOP should always be >= EBOT"

            # PLDOT should be reasonable (between low and high)
            pldot_mask = df['pldot'].notna()
            if pldot_mask.any():
                pldot_in_range = ((df.loc[pldot_mask, 'pldot'] >= df.loc[pldot_mask, 'low']) &
                                  (df.loc[pldot_mask, 'pldot'] <= df.loc[pldot_mask, 'high'])).all()
                assert pldot_in_range, "PLDOT should be between high and low"

            logger.info("✅ Technical signal mathematical properties verified")

    @pytest.mark.asyncio
    async def test_multi_timeframe_data_consistency(self, market_data_manager):
        """Test multi-timeframe data retrieval and consistency."""

        symbols = ['AAPL']
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 8)
        intervals = ['5m', '15m', '1h', '1d', '1w']
        signals = ['sma_20', 'ema_12', 'rsi_14', 'etop', 'ebot', 'pldot']

        result = await market_data_manager.get_multi_timeframe_data(
            symbols=symbols,
            start=start,
            end=end,
            intervals=intervals,
            signals=signals
        )

        assert 'AAPL' in result, "AAPL missing from multi-timeframe result"
        symbol_data = result['AAPL']

        # Verify all timeframes are present
        for interval in intervals:
            assert interval in symbol_data, f"Interval {interval} missing from result"

            df = symbol_data[interval]
            logger.info(f"Interval {interval}: {len(df)} bars")

            # Verify basic structure
            assert all(col in df.columns for col in ['timestamp', 'open', 'high', 'low', 'close', 'volume']), \
                f"Missing OHLCV columns in {interval} data"

            # Verify signals are present
            for signal in signals:
                if signal in df.columns:
                    non_null_count = df[signal].notna().sum()
                    logger.info(f"  Signal {signal}: {non_null_count} non-null values")

        # Verify timeframe relationships (higher timeframes should have fewer bars)
        bar_counts = {interval: len(symbol_data[interval]) for interval in intervals}

        # 5m should have more bars than 15m, which should have more than 1h, etc.
        if all(bar_counts[interval] > 0 for interval in intervals):
            assert bar_counts['5m'] >= bar_counts['15m'], "5m should have >= bars than 15m"
            assert bar_counts['15m'] >= bar_counts['1h'], "15m should have >= bars than 1h"
            assert bar_counts['1h'] >= bar_counts['1d'], "1h should have >= bars than 1d"

            logger.info(f"✅ Timeframe bar count progression verified: {bar_counts}")

    @pytest.mark.asyncio
    async def test_training_callback_integration(self, market_data_manager, test_env):
        """Test integration with IntervalBasedTrainingDataCallback."""

        # Create callback configuration
        from dataclasses import dataclass, field
        from typing import Dict

        @dataclass
        class TestTrainingConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440, '1w': 10080
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 12, '15m': 16, '1h': 24, '1d': 20, '1w': 12
            })
            minute_data_base_path: str = "/test/path"

        config = TestTrainingConfig()

        # Create callback
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir="/tmp/test_output"
        )

        # Inject the market data manager
        callback.minute_data_manager = market_data_manager
        callback.start_date = datetime(2025, 1, 1).date()
        callback.end_date = datetime(2025, 1, 8).date()

        # Test multi-timeframe example generation
        current_time = datetime(2025, 1, 5, 12, 0)  # Noon on Jan 5th

        example = await callback._generate_multi_timeframe_example('AAPL', current_time)

        if example is not None:  # May be None if insufficient data
            # Verify example structure
            assert 'symbol' in example, "Example missing symbol"
            assert 'timestamp' in example, "Example missing timestamp"
            assert 'features' in example, "Example missing features"
            assert 'timeframes' in example, "Example missing timeframes"
            assert 'signals' in example, "Example missing signals"

            features = example['features']

            # Verify multi-timeframe features are present
            expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
            for tf in expected_timeframes:
                # Check for OHLCV features
                ohlcv_features = [f'{tf}_open', f'{tf}_high', f'{tf}_low', f'{tf}_close', f'{tf}_volume']

                found_ohlcv = sum(1 for feat in ohlcv_features if feat in features)
                logger.info(f"Timeframe {tf}: {found_ohlcv}/5 OHLCV features found")

                # Check for signal features
                signal_features = [f'{tf}_{sig}' for sig in ['sma_20', 'ema_12', 'rsi_14', 'etop', 'ebot', 'pldot']]
                found_signals = sum(1 for feat in signal_features if feat in features)
                logger.info(f"Timeframe {tf}: {found_signals}/6 signal features found")

            # Verify feature data types and shapes
            for feature_name, feature_data in features.items():
                if isinstance(feature_data, list):
                    assert len(feature_data) > 0, f"Feature {feature_name} has empty data"
                    assert all(isinstance(x, (int, float)) for x in feature_data), \
                        f"Feature {feature_name} contains non-numeric data"

                    # Verify sequence length matches configuration
                    timeframe = feature_name.split('_')[0]
                    if timeframe in config.sequence_lengths:
                        expected_length = config.sequence_lengths[timeframe]
                        assert len(feature_data) <= expected_length, \
                            f"Feature {feature_name} exceeds expected length {expected_length}"

            logger.info(f"✅ Training callback integration verified")
            logger.info(f"   Total features: {example['feature_count']}")
            logger.info(f"   Feature categories: {len(features)}")
            logger.info(f"   Timeframes: {example['timeframes']}")
        else:
            logger.warning("Training example was None - insufficient test data")

    @pytest.mark.asyncio
    async def test_edge_cases_and_error_handling(self, market_data_manager):
        """Test edge cases and error handling scenarios."""

        symbols = ['AAPL']
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 2)

        # Test with invalid interval
        with pytest.raises(ValueError):
            await market_data_manager.get_ohlc_for_interval(
                symbols=symbols,
                start=start,
                end=end,
                interval='invalid_interval'
            )

        # Test with empty symbol list
        result = await market_data_manager.get_ohlc_for_interval(
            symbols=[],
            start=start,
            end=end,
            interval='5m'
        )
        assert result == {}, "Empty symbol list should return empty dict"

        # Test with non-existent symbol
        result = await market_data_manager.get_ohlc_for_interval(
            symbols=['NONEXISTENT'],
            start=start,
            end=end,
            interval='5m'
        )

        # Should return empty DataFrame for non-existent symbol
        assert 'NONEXISTENT' in result, "Non-existent symbol should be in result"
        assert result['NONEXISTENT'].empty, "Non-existent symbol should have empty DataFrame"

        # Test with future date range (no data)
        future_start = datetime(2030, 1, 1)
        future_end = datetime(2030, 1, 2)

        result = await market_data_manager.get_ohlc_for_interval(
            symbols=symbols,
            start=future_start,
            end=future_end,
            interval='5m'
        )

        # Should handle gracefully
        assert 'AAPL' in result, "Symbol should be in result even with no data"

        logger.info("✅ Edge cases and error handling verified")

    @pytest.mark.asyncio
    async def test_performance_benchmarks(self, market_data_manager):
        """Test performance benchmarks for batch processing."""

        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN']  # 5 symbols
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 8)

        import time

        # Test single timeframe performance
        start_time = time.time()
        result = await market_data_manager.get_ohlc_for_interval(
            symbols=symbols,
            start=start,
            end=end,
            interval='5m'
        )
        single_tf_time = time.time() - start_time

        logger.info(f"Single timeframe (5m) for {len(symbols)} symbols: {single_tf_time:.3f}s")

        # Test multi-timeframe performance
        start_time = time.time()
        result = await market_data_manager.get_multi_timeframe_data(
            symbols=symbols,
            start=start,
            end=end,
            intervals=['5m', '15m', '1h', '1d'],
            signals=['sma_20', 'ema_12', 'rsi_14']
        )
        multi_tf_time = time.time() - start_time

        logger.info(f"Multi-timeframe (4 intervals) for {len(symbols)} symbols: {multi_tf_time:.3f}s")

        # Test signals computation performance
        start_time = time.time()
        result = await market_data_manager.get_ohlc_with_signals(
            symbols=symbols,
            start=start,
            end=end,
            interval='1h',
            signals=['sma_20', 'ema_12', 'rsi_14', 'etop', 'ebot', 'pldot', 'vwap']
        )
        signals_time = time.time() - start_time

        logger.info(f"Signals computation (7 signals) for {len(symbols)} symbols: {signals_time:.3f}s")

        # Performance assertions (reasonable thresholds for test environment)
        assert single_tf_time < 10.0, f"Single timeframe took too long: {single_tf_time}s"
        assert multi_tf_time < 20.0, f"Multi-timeframe took too long: {multi_tf_time}s"
        assert signals_time < 15.0, f"Signals computation took too long: {signals_time}s"

        logger.info("✅ Performance benchmarks passed")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])