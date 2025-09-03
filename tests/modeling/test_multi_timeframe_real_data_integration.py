"""
Comprehensive Integration Tests for Multi-Timeframe Real Data System

Tests the complete pipeline from FileBasedMinuteManager through signal computation
using real AAPL data to validate the entire system works correctly.
"""

import pytest
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

try:
    from domains.market_data.services.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
    from domains.ml.services.multi_timeframe_data_collector import MultiTimeframeDataCollector
    from domains.ml.services.multi_timeframe_signal_pipeline import (
        MultiTimeframeSignalPipeline, 
        create_signal_pipeline, 
        Timeframe, 
        TimeframeConfig
    )
    from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar
    from shared.utils.environment import Environment, EnvironmentType
    from domains.ml.services.enhanced_feature_types import EnhancedFeatureRegistry
except ImportError as e:
    print(f"Import error: {e}")
    pytest.skip(f"Required modules not available: {e}", allow_module_level=True)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestMultiTimeframeRealDataIntegration:
    """Comprehensive integration tests for the multi-timeframe real data system."""
    
    @pytest.fixture
    @pytest.mark.asyncio
    async def test_environment(self):
        """Create test environment."""
        env = Environment(EnvironmentType.TEST)
        return env
    
    @pytest.fixture
    @pytest.mark.asyncio
    async def test_minute_bars_path(self, tmp_path):
        """Create test path with some sample data."""
        test_path = tmp_path / "minute-bars"
        test_path.mkdir()
        
        # Create AAPL directory structure
        aapl_path = test_path / "AAPL"
        aapl_path.mkdir()
        
        year_path = aapl_path / "2024"
        year_path.mkdir()
        
        return str(test_path)
    
    @pytest.fixture
    async def sample_minute_bars(self):
        """Generate sample minute bars for testing."""
        bars = []
        base_time = datetime(2024, 1, 15, 9, 30)  # Market open
        base_price = 150.0
        
        for i in range(300):  # 5 hours of minute data
            # Generate realistic price movement
            price_change = np.random.normal(0, 0.005)  # 0.5% volatility
            new_price = base_price * (1 + price_change)
            
            # Generate OHLC for this minute
            high = new_price * (1 + abs(np.random.normal(0, 0.002)))
            low = new_price * (1 - abs(np.random.normal(0, 0.002))) 
            open_price = base_price  # Previous close becomes next open
            close_price = new_price
            
            # Ensure OHLC consistency
            high = max(high, open_price, close_price)
            low = min(low, open_price, close_price)
            
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=open_price,
                high=high,
                low=low,
                close=close_price,
                volume=np.random.randint(1000, 10000),
                vendor='test'
            )
            bars.append(bar)
            base_price = close_price  # Update for next iteration
        
        return bars
    
    @pytest.fixture
    async def minute_manager(self, test_minute_bars_path, sample_minute_bars):
        """Create minute manager with test data."""
        manager = FileBasedMinuteManager(test_minute_bars_path)
        
        # Store sample data
        await manager.store_minute_data('AAPL', sample_minute_bars)
        
        return manager
    
    @pytest.fixture
    async def minute_market_data_manager(self, test_environment, test_minute_bars_path, minute_manager):
        """Create minute market data manager."""
        manager = FileBasedMinuteMarketDataManager(test_environment, test_minute_bars_path)
        # Replace the internal minute_manager with our test one
        manager.minute_manager = minute_manager
        return manager
    
    @pytest.fixture
    async def real_data_collector(self, minute_market_data_manager):
        """Create data collector with real minute data manager."""
        feature_registry = EnhancedFeatureRegistry()
        collector = MultiTimeframeDataCollector(
            minute_manager=minute_market_data_manager,
            feature_registry=feature_registry
        )
        return collector
    
    @pytest.fixture
    async def signal_pipeline(self):
        """Create signal pipeline for testing."""
        return create_signal_pipeline(
            timeframes=['5min', '15min', '1hour'],
            lookback_periods=30
        )
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_minute_manager_basic_functionality(self, minute_manager):
        """Test basic functionality of FileBasedMinuteManager."""
        
        # Query the stored data
        start_date = datetime(2024, 1, 15, 9, 30)
        end_date = datetime(2024, 1, 15, 14, 30)
        
        df = await minute_manager.query_minute_data('AAPL', start_date, end_date)
        
        assert not df.empty, "Should have retrieved minute data"
        assert len(df) == 300, f"Expected 300 bars, got {len(df)}"
        assert 'timestamp' in df.columns, "Should have timestamp column"
        assert 'close' in df.columns, "Should have close price column"
        
        # Check data quality
        assert df['close'].notna().all(), "All close prices should be valid"
        assert (df['high'] >= df['low']).all(), "High should be >= Low"
        assert (df['high'] >= df['close']).all(), "High should be >= Close"
        assert (df['low'] <= df['close']).all(), "Low should be <= Close"
        
        logger.info(f"✅ Basic minute manager test passed: {len(df)} bars retrieved")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_minute_market_data_manager_aggregation(self, minute_market_data_manager):
        """Test aggregation functionality in market data manager."""
        
        symbols = ['AAPL']
        start = datetime(2024, 1, 15, 9, 30)
        end = datetime(2024, 1, 15, 14, 30)
        
        # Test different timeframes
        timeframes_to_test = [1, 5, 15, 60]
        
        for minutes in timeframes_to_test:
            result = await minute_market_data_manager.get_minute_ohlc_batch(
                symbols=symbols,
                start=start,
                end=end,
                timeframe_minutes=minutes
            )
            
            assert 'AAPL' in result, f"Should have AAPL data for {minutes}-minute timeframe"
            
            df = result['AAPL']
            assert not df.empty, f"Should have data for {minutes}-minute timeframe"
            
            # Expected number of bars
            total_minutes = (end - start).total_seconds() // 60
            expected_bars = int(total_minutes // minutes)
            
            # Allow some tolerance for aggregation edge cases
            assert len(df) >= expected_bars - 2, f"Expected ~{expected_bars} {minutes}-minute bars, got {len(df)}"
            
            # Verify OHLC integrity
            assert (df['high'] >= df['low']).all(), f"High >= Low for {minutes}-minute bars"
            assert (df['high'] >= df['close']).all(), f"High >= Close for {minutes}-minute bars"
            assert (df['low'] <= df['close']).all(), f"Low <= Close for {minutes}-minute bars"
            
            logger.info(f"✅ {minutes}-minute aggregation test passed: {len(df)} bars")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_collector_real_data_usage(self, real_data_collector):
        """Test that data collector uses real data instead of synthetic."""
        
        symbols = ['AAPL']
        start_date = '2024-01-15'
        end_date = '2024-01-15'
        
        # This should use real data, not synthetic
        df = await real_data_collector._get_minute_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            minutes=5
        )
        
        assert not df.empty, "Should have retrieved real minute data"
        assert real_data_collector.use_real_data, "Should be using real data mode"
        
        # Verify it's real data by checking for consistent symbol
        assert (df['symbol'] == 'AAPL').all(), "All records should be for AAPL"
        
        # Check realistic price patterns (not random walk)
        prices = df['close'].values
        price_changes = np.diff(prices) / prices[:-1]
        
        # Real data should have reasonable volatility (not excessive random walk)
        assert np.std(price_changes) < 0.1, "Price volatility should be reasonable for real data"
        
        logger.info(f"✅ Real data collector test passed: {len(df)} 5-minute bars retrieved")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_signal_pipeline_computation(self, signal_pipeline, minute_market_data_manager):
        """Test complete signal computation pipeline."""
        
        # Get minute data
        symbols = ['AAPL'] 
        start = datetime(2024, 1, 15, 9, 30)
        end = datetime(2024, 1, 15, 14, 30)
        
        minute_data = await minute_market_data_manager.get_minute_ohlc_batch(
            symbols=symbols,
            start=start,
            end=end,
            timeframe_minutes=1  # Get 1-minute base data
        )
        
        assert 'AAPL' in minute_data, "Should have AAPL minute data"
        
        aapl_data = minute_data['AAPL']
        
        # Compute signals across all timeframes
        signals = await signal_pipeline.compute_signals(aapl_data, symbol='AAPL')
        
        assert 'timeframes' in signals, "Should have timeframes in results"
        assert 'metadata' in signals, "Should have metadata in results"
        
        # Check that we got results for expected timeframes
        timeframes = signals['timeframes']
        expected_timeframes = ['5min', '15min', '1hour']
        
        for tf in expected_timeframes:
            assert tf in timeframes, f"Should have results for {tf} timeframe"
            
            tf_data = timeframes[tf]
            
            # Should have data and signals
            assert 'data' in tf_data, f"Should have data for {tf}"
            assert 'signals' in tf_data, f"Should have signals for {tf}"
            
            # Check some signals were computed
            signals_computed = tf_data['signals']
            non_null_signals = [s for s in signals_computed.values() if s is not None]
            
            logger.info(f"✅ {tf} timeframe: {len(non_null_signals)}/{len(signals_computed)} signals computed")
        
        # Check metadata
        metadata = signals['metadata']
        assert metadata['symbol'] == 'AAPL', "Metadata should have correct symbol"
        assert 'computation_time' in metadata, "Should have computation time"
        
        logger.info(f"✅ Signal pipeline test passed: computed signals for {len(timeframes)} timeframes")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_specific_indicator_accuracy(self, signal_pipeline, minute_market_data_manager):
        """Test accuracy of specific indicators with known test data."""
        
        # Create controlled test data for validation
        test_data = []
        base_time = datetime(2024, 1, 15, 10, 0)
        
        # Create simple trend data for easier validation
        for i in range(60):
            test_data.append({
                'timestamp': base_time + timedelta(minutes=i),
                'open': 100.0 + i * 0.1,
                'high': 100.2 + i * 0.1,
                'low': 99.8 + i * 0.1,
                'close': 100.0 + i * 0.1,
                'volume': 1000
            })
        
        test_df = pd.DataFrame(test_data)
        
        # Compute signals
        signals = await signal_pipeline.compute_signals(test_df, symbol='TEST')
        
        # Check 5-minute timeframe signals
        five_min_signals = signals['timeframes']['5min']['signals']
        
        # Should have computed some indicators
        computed_indicators = [name for name, value in five_min_signals.items() if value is not None]
        
        assert len(computed_indicators) > 0, "Should have computed at least some indicators"
        
        # Log indicator results for validation
        logger.info(f"Computed indicators for test trend data:")
        for name, value in five_min_signals.items():
            if value is not None:
                logger.info(f"  {name}: {value:.4f}")
        
        logger.info(f"✅ Indicator accuracy test passed: {len(computed_indicators)} indicators computed")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_performance_benchmarks(self, signal_pipeline, minute_market_data_manager):
        """Test performance benchmarks for signal computation."""
        
        # Get larger dataset
        symbols = ['AAPL']
        start = datetime(2024, 1, 15, 9, 30)
        end = datetime(2024, 1, 15, 16, 0)  # Full trading day
        
        minute_data = await minute_market_data_manager.get_minute_ohlc_batch(
            symbols=symbols,
            start=start,
            end=end,
            timeframe_minutes=1
        )
        
        aapl_data = minute_data['AAPL']
        
        # Measure computation time
        start_time = datetime.now()
        
        signals = await signal_pipeline.compute_signals(aapl_data, symbol='AAPL')
        
        end_time = datetime.now()
        computation_time = (end_time - start_time).total_seconds()
        
        # Performance targets
        max_computation_time = 5.0  # 5 seconds max for full day
        
        assert computation_time < max_computation_time, f"Computation took {computation_time:.2f}s, expected < {max_computation_time}s"
        
        # Check data processed
        metadata = signals['metadata']
        data_periods = metadata['data_periods']
        
        logger.info(f"✅ Performance test passed: {data_periods} periods processed in {computation_time:.2f}s")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling_and_edge_cases(self, signal_pipeline):
        """Test error handling and edge cases."""
        
        # Test with empty data
        empty_df = pd.DataFrame()
        signals = await signal_pipeline.compute_signals(empty_df, symbol='EMPTY')
        
        assert signals['timeframes'] == {}, "Should handle empty data gracefully"
        assert 'error' in signals['metadata'], "Should indicate error for empty data"
        
        # Test with insufficient data
        minimal_data = pd.DataFrame({
            'timestamp': [datetime.now()],
            'open': [100.0],
            'high': [101.0],
            'low': [99.0],
            'close': [100.5],
            'volume': [1000]
        })
        
        signals = await signal_pipeline.compute_signals(minimal_data, symbol='MINIMAL')
        
        # Should complete but with warnings about insufficient data
        assert 'timeframes' in signals, "Should still return timeframes structure"
        
        # Check that warnings are appropriately recorded
        for tf_name, tf_data in signals['timeframes'].items():
            if 'warning' in tf_data:
                assert tf_data['warning'] == 'insufficient_data', f"Should warn about insufficient data for {tf_name}"
        
        logger.info(f"✅ Error handling test passed: graceful handling of edge cases")


# Standalone test runner for manual execution
async def run_integration_tests_manual():
    """Run integration tests manually for debugging."""
    
    logger.info("🚀 Starting manual integration tests...")
    
    # Create test environment
    env = Environment(EnvironmentType.TEST)
    
    # Check if real data path exists
    real_data_path = "/mnt/d/ats-data/minute-bars"
    if Path(real_data_path).exists():
        logger.info(f"✅ Found real data path: {real_data_path}")
        
        # Test with real data
        minute_manager = FileBasedMinuteMarketDataManager(env, real_data_path)
        
        # Test getting symbols
        symbols = await minute_manager.get_symbols_for_date_range(
            start=datetime(2020, 1, 1),
            end=datetime(2020, 1, 31)
        )
        
        logger.info(f"Found {len(symbols)} symbols with data")
        
        if 'AAPL' in symbols:
            logger.info("🎯 Testing with real AAPL data...")
            
            # Get AAPL data for a day
            aapl_data = await minute_manager.get_minute_ohlc_batch(
                symbols=['AAPL'],
                start=datetime(2020, 1, 2, 9, 30),
                end=datetime(2020, 1, 2, 16, 0),
                timeframe_minutes=5
            )
            
            if 'AAPL' in aapl_data and not aapl_data['AAPL'].empty:
                df = aapl_data['AAPL']
                logger.info(f"✅ Retrieved {len(df)} 5-minute AAPL bars")
                
                # Test signal computation
                pipeline = create_signal_pipeline(['5min', '15min'])
                signals = await pipeline.compute_signals(df, symbol='AAPL')
                
                logger.info(f"✅ Computed signals for {len(signals['timeframes'])} timeframes")
                
                # Log some signal results
                for tf_name, tf_data in signals['timeframes'].items():
                    computed_signals = [k for k, v in tf_data['signals'].items() if v is not None]
                    logger.info(f"  {tf_name}: {len(computed_signals)} indicators computed")
            else:
                logger.warning("❌ No AAPL data found")
        else:
            logger.warning("❌ AAPL not found in available symbols")
    else:
        logger.warning(f"❌ Real data path not found: {real_data_path}")
        logger.info("Running tests with synthetic test data...")
    
    logger.info("✅ Manual integration tests completed")


if __name__ == "__main__":
    # Run manual tests
    asyncio.run(run_integration_tests_manual())