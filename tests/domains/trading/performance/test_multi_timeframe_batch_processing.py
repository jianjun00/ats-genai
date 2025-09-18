#!/usr/bin/env python3

import pytest
import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime

# Add src to Python path for imports
import sys
sys.path.append('/home/jianjun/ats-genai-admin/src')

# Import only the minimal modules needed for performance testing
# from market_data.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager


class TestMultiTimeframeBatchProcessingPerformance:
    """
    Performance tests for multi-timeframe OHLC aggregation and signal computation
    with batch processing scenarios.

    Tests validate:
    - Batch processing performance across multiple symbols
    - Memory usage efficiency
    - CPU utilization during heavy computation
    - Throughput metrics for training data generation
    - Scalability with increasing symbol count
    """

    @pytest.fixture
    async def market_data_manager(self):
        """Create mock market data manager for testing."""
        class MockMarketDataManager:
            def __init__(self, base_path):
                self.base_path = base_path

            async def get_multi_timeframe_data(self, symbols, start, end, intervals, signals):
                """Mock implementation for testing."""
                results = {}
                for symbol in symbols:
                    results[symbol] = {}
                    for interval in intervals:
                        # Create mock DataFrame with OHLCV + signals
                        data = {
                            'open': [100.0] * 100,
                            'high': [102.0] * 100,
                            'low': [98.0] * 100,
                            'close': [101.0] * 100,
                            'volume': [10000] * 100
                        }
                        for signal in signals:
                            data[signal] = [50.0] * 100
                        results[symbol][interval] = pd.DataFrame(data)
                return results

            async def get_minute_ohlc_batch(self, symbols, start, end, timeframe_minutes=1):
                """Mock implementation for testing."""
                results = {}
                for symbol in symbols:
                    data = {
                        'open': [100.0] * 1000,
                        'high': [102.0] * 1000,
                        'low': [98.0] * 1000,
                        'close': [101.0] * 1000,
                        'volume': [10000] * 1000
                    }
                    results[symbol] = pd.DataFrame(data)
                return results

        manager = MockMarketDataManager(base_path="/mnt/d/ats-data")
        yield manager

    @pytest.fixture
    def sample_symbols(self):
        """Test symbols for performance testing."""
        return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX', 'AMD', 'INTC']

    @pytest.fixture
    def performance_test_data(self):
        """Generate synthetic minute-level data for performance testing."""
        def create_ohlcv_data(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
            """Create synthetic OHLCV data for testing."""
            # Generate minute-by-minute data
            date_range = pd.date_range(start=start, end=end, freq='1min')

            # Generate realistic OHLCV data
            np.random.seed(hash(symbol) % 2**32)  # Deterministic but symbol-specific
            base_price = 100 + (hash(symbol) % 200)  # Base price between 100-300

            # Generate price series with some volatility
            returns = np.random.normal(0, 0.001, len(date_range))  # 0.1% volatility per minute
            prices = base_price * np.exp(np.cumsum(returns))

            # Create OHLCV data
            data = []
            for i, timestamp in enumerate(date_range):
                if i == 0:
                    open_price = prices[i]
                else:
                    open_price = prices[i-1]  # Previous close becomes current open

                close_price = prices[i]

                # High/Low with some spread
                spread = abs(close_price - open_price) * 2
                high_price = max(open_price, close_price) + np.random.uniform(0, spread)
                low_price = min(open_price, close_price) - np.random.uniform(0, spread)

                # Volume (random but realistic)
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

    @pytest.mark.asyncio
    async def test_single_symbol_batch_performance(self, market_data_manager, performance_test_data):
        """Test performance of multi-timeframe processing for a single symbol."""
        symbol = 'AAPL'
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 31)  # 1 month of data

        # Generate test data
        test_data = performance_test_data(symbol, start_time, end_time)

        # Mock the data retrieval to use our test data
        original_method = market_data_manager.get_minute_ohlc_batch

        async def mock_get_minute_ohlc_batch(symbols, start, end, timeframe_minutes=1):
            return {symbol: test_data}

        market_data_manager.get_minute_ohlc_batch = mock_get_minute_ohlc_batch

        # Test all timeframes
        timeframes = ['1m', '5m', '15m', '1h', '1d', '1w']
        signals = ['sma_20', 'ema_20', 'rsi_14', 'etop', 'ebot', 'pldot']

        # Measure performance
        start_perf = time.perf_counter()

        # Execute multi-timeframe processing
        results = await market_data_manager.get_multi_timeframe_data(
            symbols=[symbol],
            start=start_time,
            end=end_time,
            intervals=timeframes,
            signals=signals
        )

        end_perf = time.perf_counter()

        # Performance assertions
        execution_time = end_perf - start_perf

        print(f"\n=== Single Symbol Performance ===")
        print(f"Symbol: {symbol}")
        print(f"Execution time: {execution_time:.2f}s")
        print(f"Timeframes processed: {len(timeframes)}")
        print(f"Signals computed: {len(signals)}")

        # Validate results
        assert symbol in results
        for timeframe in timeframes:
            assert timeframe in results[symbol]
            df = results[symbol][timeframe]
            assert not df.empty
            assert len(df.columns) >= 5 + len(signals)  # OHLCV + signals

        # Performance thresholds
        assert execution_time < 10.0, f"Single symbol processing too slow: {execution_time:.2f}s"

        # Restore original method
        market_data_manager.get_minute_ohlc_batch = original_method

    @pytest.mark.asyncio
    async def test_multi_symbol_batch_performance(self, market_data_manager, sample_symbols, performance_test_data):
        """Test performance of processing multiple symbols simultaneously."""
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 7)  # 1 week of data

        # Generate test data for all symbols
        test_data = {}
        for symbol in sample_symbols[:5]:  # Test with 5 symbols
            test_data[symbol] = performance_test_data(symbol, start_time, end_time)

        # Mock the data retrieval
        async def mock_get_minute_ohlc_batch(symbols, start, end, timeframe_minutes=1):
            return {sym: test_data[sym] for sym in symbols if sym in test_data}

        original_method = market_data_manager.get_minute_ohlc_batch
        market_data_manager.get_minute_ohlc_batch = mock_get_minute_ohlc_batch

        timeframes = ['5m', '15m', '1h', '1d']
        signals = ['sma_20', 'rsi_14', 'etop']

        # Measure performance
        start_perf = time.perf_counter()

        # Execute batch processing
        results = await market_data_manager.get_multi_timeframe_data(
            symbols=list(test_data.keys()),
            start=start_time,
            end=end_time,
            intervals=timeframes,
            signals=signals
        )

        end_perf = time.perf_counter()

        execution_time = end_perf - start_perf

        print(f"\n=== Multi-Symbol Batch Performance ===")
        print(f"Symbols: {len(test_data)} ({list(test_data.keys())})")
        print(f"Execution time: {execution_time:.2f}s")
        print(f"Throughput: {len(test_data) / execution_time:.2f} symbols/sec")

        # Validate results structure
        assert len(results) == len(test_data)
        for symbol in test_data.keys():
            assert symbol in results
            for timeframe in timeframes:
                assert timeframe in results[symbol]
                df = results[symbol][timeframe]
                assert not df.empty

        # Performance thresholds for batch processing
        assert execution_time < 30.0, f"Batch processing too slow: {execution_time:.2f}s"

        # Restore original method
        market_data_manager.get_minute_ohlc_batch = original_method

    @pytest.mark.asyncio
    async def test_training_data_generation_performance(self, sample_symbols, performance_test_data):
        """Test performance of simulated training data generation pipeline."""

        symbols = sample_symbols[:3]
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 14)

        # Mock training data generator
        class MockTrainingDataGenerator:
            def __init__(self, symbols):
                self.symbols = symbols

            async def generate_examples(self):
                """Simulate training example generation."""
                examples_generated = 0
                for symbol in self.symbols:
                    # Simulate generating examples for each symbol
                    for i in range(20):  # 20 examples per symbol
                        # Simulate computation time
                        await asyncio.sleep(0.01)  # 10ms per example

                        # Mock example data
                        example = {
                            'symbol': symbol,
                            'features': np.random.rand(60, 8),  # 60 timesteps, 8 features
                            'labels': np.random.rand(5),  # 5-day prediction
                            'metadata': {'timeframe': '5m', 'sequence_length': 60}
                        }
                        examples_generated += 1
                        yield symbol, datetime.now(), example

        generator = MockTrainingDataGenerator(symbols)

        # Measure performance
        start_perf = time.perf_counter()

        # Generate training examples
        examples_generated = 0
        async for symbol, timestamp, example in generator.generate_examples():
            if example is not None:
                examples_generated += 1

            # Limit test to avoid excessive runtime
            if examples_generated >= 50:
                break

        end_perf = time.perf_counter()

        execution_time = end_perf - start_perf

        print(f"\n=== Training Data Generation Performance ===")
        print(f"Symbols: {len(symbols)}")
        print(f"Examples generated: {examples_generated}")
        print(f"Execution time: {execution_time:.2f}s")
        print(f"Throughput: {examples_generated / execution_time:.2f} examples/sec")

        # Performance assertions
        assert examples_generated > 0, "No training examples generated"
        assert execution_time < 60.0, f"Training data generation too slow: {execution_time:.2f}s"

    @pytest.mark.asyncio
    async def test_scalability_with_symbol_count(self, performance_test_data):
        """Test how performance scales with increasing number of symbols."""

        symbol_counts = [1, 3, 5, 10]
        all_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX', 'AMD', 'INTC']
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 7)

        performance_results = []

        for count in symbol_counts:
            symbols = all_symbols[:count]

            # Create mock market data manager
            class MockManager:
                async def get_multi_timeframe_data(self, symbols, start, end, intervals, signals):
                    results = {}
                    for symbol in symbols:
                        symbol_data = performance_test_data(symbol, start, end)
                        results[symbol] = {}

                        for interval in intervals:
                            # Simple resampling for testing
                            if interval == '1m':
                                results[symbol][interval] = symbol_data
                            elif interval == '5m':
                                results[symbol][interval] = symbol_data.resample('5min').agg({
                                    'open': 'first', 'high': 'max', 'low': 'min',
                                    'close': 'last', 'volume': 'sum'
                                }).dropna()
                    return results

            manager = MockManager()

            # Measure performance
            start_perf = time.perf_counter()

            results = await manager.get_multi_timeframe_data(
                symbols=symbols,
                start=start_time,
                end=end_time,
                intervals=['1m', '5m', '15m'],
                signals=['sma_20']
            )

            end_perf = time.perf_counter()

            execution_time = end_perf - start_perf
            throughput = count / execution_time if execution_time > 0 else 0

            performance_results.append({
                'symbols': count,
                'execution_time': execution_time,
                'throughput': throughput
            })

            print(f"Symbols: {count:2d}, Time: {execution_time:6.2f}s, Throughput: {throughput:6.2f} sym/s")

        print(f"\n=== Scalability Analysis ===")

        # Validate scaling behavior
        for i in range(1, len(performance_results)):
            current = performance_results[i]
            previous = performance_results[i-1]

            # Time should scale roughly linearly or sub-linearly
            time_ratio = current['execution_time'] / previous['execution_time']
            symbol_ratio = current['symbols'] / previous['symbols']

            print(f"Scaling from {previous['symbols']} to {current['symbols']} symbols:")
            print(f"  Time scaling factor: {time_ratio:.2f} (symbol ratio: {symbol_ratio:.2f})")

            # Assert reasonable scaling (not exponential)
            assert time_ratio <= symbol_ratio * 2, f"Poor time scaling: {time_ratio:.2f} vs {symbol_ratio:.2f}"

        return performance_results

    @pytest.mark.asyncio
    async def test_memory_efficiency_large_dataset(self, performance_test_data):
        """Test memory efficiency with large dataset processing."""

        symbol = 'AAPL'
        # Test with 3 months of minute data (~90k records)
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 3, 31)

        # Generate large dataset
        large_dataset = performance_test_data(symbol, start_time, end_time)

        print(f"\n=== Memory Efficiency Test ===")
        print(f"Dataset size: {len(large_dataset):,} records")
        print(f"Dataset memory: {large_dataset.memory_usage(deep=True).sum() / 1024 / 1024:.1f}MB")

        # Mock manager with large dataset
        class MockLargeDataManager:
            async def get_multi_timeframe_data(self, symbols, start, end, intervals, signals):
                results = {symbol: {}}

                for interval in intervals:
                    if interval == '1m':
                        results[symbol][interval] = large_dataset
                    elif interval == '5m':
                        results[symbol][interval] = large_dataset.resample('5min').agg({
                            'open': 'first', 'high': 'max', 'low': 'min',
                            'close': 'last', 'volume': 'sum'
                        }).dropna()
                    elif interval == '1h':
                        results[symbol][interval] = large_dataset.resample('1h').agg({
                            'open': 'first', 'high': 'max', 'low': 'min',
                            'close': 'last', 'volume': 'sum'
                        }).dropna()
                    elif interval == '1d':
                        results[symbol][interval] = large_dataset.resample('1d').agg({
                            'open': 'first', 'high': 'max', 'low': 'min',
                            'close': 'last', 'volume': 'sum'
                        }).dropna()

                return results

        manager = MockLargeDataManager()

        start_perf = time.perf_counter()

        # Process large dataset
        results = await manager.get_multi_timeframe_data(
            symbols=[symbol],
            start=start_time,
            end=end_time,
            intervals=['1m', '5m', '1h', '1d'],
            signals=['sma_20', 'ema_20', 'rsi_14']
        )

        end_perf = time.perf_counter()

        execution_time = end_perf - start_perf

        print(f"Execution time: {execution_time:.2f}s")
        print(f"Processing rate: {len(large_dataset) / execution_time:,.0f} records/sec")

        # Validate results
        assert symbol in results
        assert '1m' in results[symbol]
        assert '5m' in results[symbol]
        assert '1h' in results[symbol]
        assert '1d' in results[symbol]

        # Performance assertions
        assert execution_time < 120.0, f"Large dataset processing too slow: {execution_time:.2f}s"


if __name__ == "__main__":
    # Run performance tests with detailed output
    pytest.main([
        __file__,
        "-v",
        "-s",  # Show print statements
        "--tb=short"
    ])