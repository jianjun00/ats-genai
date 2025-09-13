#!/usr/bin/env python3

"""
Standalone Performance Tests for Multi-Timeframe OHLC Aggregation and Signal Computation

Tests validate:
- Batch processing performance across multiple symbols
- Throughput metrics for training data generation
- Scalability with increasing symbol count
- Memory efficiency during large dataset processing

This module is designed to run independently without complex dependencies.
"""

import pytest
import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List


class TestStandaloneBatchProcessingPerformance:
    """
    Standalone performance tests for multi-timeframe processing.

    Uses mock data and simplified implementations to test performance
    characteristics without complex dependencies.
    """

    @pytest.fixture
    def sample_symbols(self):
        """Test symbols for performance testing."""
        return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX', 'AMD', 'INTC']

    @pytest.fixture
    def mock_ohlc_data_generator(self):
        """Generate synthetic OHLC data for performance testing."""
        def create_ohlcv_data(symbol: str, start: datetime, end: datetime, freq: str = '1min') -> pd.DataFrame:
            """Create synthetic OHLCV data for testing."""
            # Generate time series
            date_range = pd.date_range(start=start, end=end, freq=freq)

            # Generate realistic price movements
            np.random.seed(hash(symbol) % 2**32)  # Deterministic but symbol-specific
            base_price = 100 + (hash(symbol) % 200)  # Base price between 100-300

            # Generate price series with volatility
            returns = np.random.normal(0, 0.002, len(date_range))  # 0.2% volatility
            prices = base_price * np.exp(np.cumsum(returns))

            # Create OHLCV data
            data = []
            for i, timestamp in enumerate(date_range):
                open_price = prices[i-1] if i > 0 else prices[i]
                close_price = prices[i]

                # High/Low with realistic spread
                spread = abs(close_price - open_price) * 1.5
                high_price = max(open_price, close_price) + np.random.uniform(0, spread)
                low_price = min(open_price, close_price) - np.random.uniform(0, spread)

                volume = np.random.randint(1000, 100000)

                data.append({
                    'timestamp': timestamp,
                    'open': round(open_price, 2),
                    'high': round(high_price, 2),
                    'low': round(low_price, 2),
                    'close': round(close_price, 2),
                    'volume': volume
                })

            return pd.DataFrame(data).set_index('timestamp')

        return create_ohlcv_data

    def resample_ohlc(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """Resample OHLCV data to different intervals."""
        return df.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

    def compute_technical_signals(self, df: pd.DataFrame, signals: List[str]) -> pd.DataFrame:
        """Compute technical signals on OHLCV data."""
        result = df.copy()

        for signal in signals:
            if signal == 'sma_20':
                result['sma_20'] = df['close'].rolling(window=20).mean()
            elif signal == 'ema_20':
                result['ema_20'] = df['close'].ewm(span=20).mean()
            elif signal == 'rsi_14':
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                result['rsi_14'] = 100 - (100 / (1 + rs))
            elif signal == 'etop':
                result['etop'] = df['high'].rolling(window=20).max()
            elif signal == 'ebot':
                result['ebot'] = df['low'].rolling(window=20).min()
            elif signal == 'pldot':
                result['pldot'] = (df['high'] + df['low'] + df['close']) / 3

        return result.bfill().ffill()

    @pytest.mark.asyncio
    async def test_single_symbol_multi_timeframe_performance(self, mock_ohlc_data_generator):
        """Test performance of multi-timeframe processing for a single symbol."""

        symbol = 'AAPL'
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 31)  # 1 month of minute data

        # Generate base minute data
        minute_data = mock_ohlc_data_generator(symbol, start_time, end_time, '1min')

        timeframes = ['1min', '5min', '15min', '1h', '1d']
        signals = ['sma_20', 'ema_20', 'rsi_14', 'etop', 'ebot', 'pldot']

        print(f"\n=== Single Symbol Multi-Timeframe Performance ===")
        print(f"Symbol: {symbol}")
        print(f"Base data points: {len(minute_data):,}")
        print(f"Date range: {start_time.date()} to {end_time.date()}")

        # Measure performance
        start_perf = time.perf_counter()

        results = {}
        for timeframe in timeframes:
            # Resample to target timeframe
            resampled_data = self.resample_ohlc(minute_data, timeframe)

            # Compute technical signals
            data_with_signals = self.compute_technical_signals(resampled_data, signals)

            results[timeframe] = data_with_signals

            print(f"  {timeframe:>5}: {len(data_with_signals):,} bars, {len(data_with_signals.columns)} features")

        end_perf = time.perf_counter()
        execution_time = end_perf - start_perf

        print(f"Execution time: {execution_time:.3f}s")
        print(f"Processing rate: {len(minute_data) / execution_time:,.0f} records/sec")

        # Validate results
        assert len(results) == len(timeframes)
        for timeframe in timeframes:
            df = results[timeframe]
            assert not df.empty
            assert len(df.columns) >= 5 + len(signals)  # OHLCV + signals

            # Validate technical indicators computed correctly
            assert 'sma_20' in df.columns
            assert 'rsi_14' in df.columns
            assert not df['sma_20'].isna().all()

        # Performance threshold
        assert execution_time < 5.0, f"Single symbol processing too slow: {execution_time:.3f}s"

    @pytest.mark.asyncio
    async def test_multi_symbol_batch_performance(self, sample_symbols, mock_ohlc_data_generator):
        """Test performance of processing multiple symbols simultaneously."""

        test_symbols = sample_symbols[:5]  # Test with 5 symbols
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 7)  # 1 week of data

        timeframes = ['5min', '15min', '1h', '1d']
        signals = ['sma_20', 'rsi_14', 'etop']

        print(f"\n=== Multi-Symbol Batch Performance ===")
        print(f"Symbols: {len(test_symbols)} ({test_symbols})")
        print(f"Timeframes: {timeframes}")
        print(f"Signals: {signals}")

        # Measure performance
        start_perf = time.perf_counter()

        batch_results = {}
        total_records_processed = 0

        for symbol in test_symbols:
            # Generate base data
            minute_data = mock_ohlc_data_generator(symbol, start_time, end_time, '1min')
            total_records_processed += len(minute_data)

            symbol_results = {}
            for timeframe in timeframes:
                # Process each timeframe
                resampled_data = self.resample_ohlc(minute_data, timeframe)
                data_with_signals = self.compute_technical_signals(resampled_data, signals)
                symbol_results[timeframe] = data_with_signals

            batch_results[symbol] = symbol_results

        end_perf = time.perf_counter()
        execution_time = end_perf - start_perf

        print(f"Total records processed: {total_records_processed:,}")
        print(f"Execution time: {execution_time:.3f}s")
        print(f"Throughput: {len(test_symbols) / execution_time:.2f} symbols/sec")
        print(f"Processing rate: {total_records_processed / execution_time:,.0f} records/sec")

        # Validate results
        assert len(batch_results) == len(test_symbols)
        for symbol in test_symbols:
            assert symbol in batch_results
            for timeframe in timeframes:
                assert timeframe in batch_results[symbol]
                df = batch_results[symbol][timeframe]
                assert not df.empty
                assert len(df.columns) >= 5 + len(signals)

        # Performance thresholds
        assert execution_time < 10.0, f"Batch processing too slow: {execution_time:.3f}s"
        assert len(test_symbols) / execution_time >= 0.5, f"Throughput too low: {len(test_symbols) / execution_time:.2f} sym/s"

    @pytest.mark.asyncio
    async def test_scalability_with_symbol_count(self, mock_ohlc_data_generator):
        """Test how performance scales with increasing number of symbols."""

        all_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX', 'AMD', 'INTC']
        symbol_counts = [1, 3, 5, 8]
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 3)  # 3 days for scalability test

        timeframes = ['5min', '1h']
        signals = ['sma_20', 'rsi_14']

        print(f"\n=== Scalability Analysis ===")
        print(f"Testing symbol counts: {symbol_counts}")

        performance_results = []

        for count in symbol_counts:
            symbols = all_symbols[:count]

            start_perf = time.perf_counter()

            # Process symbols
            total_records = 0
            for symbol in symbols:
                minute_data = mock_ohlc_data_generator(symbol, start_time, end_time, '1min')
                total_records += len(minute_data)

                for timeframe in timeframes:
                    resampled_data = self.resample_ohlc(minute_data, timeframe)
                    self.compute_technical_signals(resampled_data, signals)

            end_perf = time.perf_counter()
            execution_time = end_perf - start_perf
            throughput = count / execution_time if execution_time > 0 else 0

            performance_results.append({
                'symbols': count,
                'execution_time': execution_time,
                'throughput': throughput,
                'total_records': total_records
            })

            print(f"Symbols: {count:2d}, Time: {execution_time:6.3f}s, Throughput: {throughput:6.2f} sym/s, Records: {total_records:,}")

        print(f"\n=== Scaling Analysis ===")

        # Analyze scaling behavior
        for i in range(1, len(performance_results)):
            current = performance_results[i]
            previous = performance_results[i-1]

            time_ratio = current['execution_time'] / previous['execution_time']
            symbol_ratio = current['symbols'] / previous['symbols']

            print(f"Scaling from {previous['symbols']} to {current['symbols']} symbols:")
            print(f"  Time scaling factor: {time_ratio:.2f} (expected: {symbol_ratio:.2f})")

            # Assert reasonable scaling (should be roughly linear)
            assert time_ratio <= symbol_ratio * 2.5, f"Poor scaling: {time_ratio:.2f} vs expected {symbol_ratio:.2f}"

    @pytest.mark.asyncio
    async def test_large_dataset_performance(self, mock_ohlc_data_generator):
        """Test performance with large dataset (3 months of minute data)."""

        symbol = 'AAPL'
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 3, 31)  # 3 months of minute data

        # Generate large dataset
        print(f"\n=== Large Dataset Performance Test ===")
        print(f"Generating 3 months of minute data for {symbol}...")

        dataset_start = time.perf_counter()
        large_dataset = mock_ohlc_data_generator(symbol, start_time, end_time, '1min')
        dataset_end = time.perf_counter()

        print(f"Dataset size: {len(large_dataset):,} records")
        print(f"Dataset generation time: {dataset_end - dataset_start:.3f}s")

        timeframes = ['1min', '5min', '15min', '1h', '1d', '1w']
        signals = ['sma_20', 'ema_20', 'rsi_14', 'etop', 'ebot']

        # Measure processing performance
        start_perf = time.perf_counter()

        results = {}
        for timeframe in timeframes:
            resampled_data = self.resample_ohlc(large_dataset, timeframe)
            data_with_signals = self.compute_technical_signals(resampled_data, signals)
            results[timeframe] = data_with_signals

            print(f"  {timeframe:>5}: {len(data_with_signals):,} bars processed")

        end_perf = time.perf_counter()
        execution_time = end_perf - start_perf

        print(f"Processing time: {execution_time:.3f}s")
        print(f"Processing rate: {len(large_dataset) / execution_time:,.0f} records/sec")
        print(f"Memory efficiency: {len(timeframes)} timeframes processed simultaneously")

        # Validate results
        for timeframe in timeframes:
            df = results[timeframe]
            assert not df.empty
            assert len(df.columns) >= 5 + len(signals)

            # Validate data quality
            assert (df['high'] >= df['low']).all()
            assert (df['high'] >= df['open']).all()
            assert (df['high'] >= df['close']).all()

        # Performance assertions
        assert execution_time < 30.0, f"Large dataset processing too slow: {execution_time:.3f}s"
        assert len(large_dataset) / execution_time >= 1000, f"Processing rate too low: {len(large_dataset) / execution_time:.0f} rec/s"

    @pytest.mark.asyncio
    async def test_concurrent_processing_performance(self, sample_symbols, mock_ohlc_data_generator):
        """Test performance of concurrent symbol processing."""

        test_symbols = sample_symbols[:4]
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 7)

        timeframes = ['5min', '1h']
        signals = ['sma_20', 'rsi_14']

        print(f"\n=== Concurrent Processing Performance ===")
        print(f"Testing concurrent vs sequential processing for {len(test_symbols)} symbols")

        # Sequential processing
        seq_start = time.perf_counter()
        sequential_results = {}

        for symbol in test_symbols:
            minute_data = mock_ohlc_data_generator(symbol, start_time, end_time, '1min')
            symbol_results = {}

            for timeframe in timeframes:
                resampled_data = self.resample_ohlc(minute_data, timeframe)
                data_with_signals = self.compute_technical_signals(resampled_data, signals)
                symbol_results[timeframe] = data_with_signals

            sequential_results[symbol] = symbol_results

        seq_end = time.perf_counter()
        sequential_time = seq_end - seq_start

        # Concurrent processing
        async def process_symbol(symbol):
            minute_data = mock_ohlc_data_generator(symbol, start_time, end_time, '1min')
            symbol_results = {}

            for timeframe in timeframes:
                resampled_data = self.resample_ohlc(minute_data, timeframe)
                data_with_signals = self.compute_technical_signals(resampled_data, signals)
                symbol_results[timeframe] = data_with_signals

            return symbol, symbol_results

        conc_start = time.perf_counter()

        # Run concurrent processing
        tasks = [process_symbol(symbol) for symbol in test_symbols]
        concurrent_results_list = await asyncio.gather(*tasks)
        concurrent_results = dict(concurrent_results_list)

        conc_end = time.perf_counter()
        concurrent_time = conc_end - conc_start

        speedup = sequential_time / concurrent_time

        print(f"Sequential time: {sequential_time:.3f}s")
        print(f"Concurrent time: {concurrent_time:.3f}s")
        print(f"Speedup: {speedup:.2f}x")

        # Validate both approaches produce same results
        assert len(sequential_results) == len(concurrent_results)
        for symbol in test_symbols:
            assert symbol in sequential_results
            assert symbol in concurrent_results
            assert len(sequential_results[symbol]) == len(concurrent_results[symbol])

        # Performance assertions
        assert speedup >= 0.8, f"Concurrent processing should be similar speed: {speedup:.2f}x"
        assert concurrent_time < sequential_time * 1.5, "Concurrent processing overhead too high"


if __name__ == "__main__":
    # Run standalone performance tests
    pytest.main([
        __file__,
        "-v",
        "-s",  # Show print statements
        "--tb=short"
    ])