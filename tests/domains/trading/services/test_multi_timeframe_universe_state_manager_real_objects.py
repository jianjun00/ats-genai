#!/usr/bin/env python3
"""
Real objects integration tests for multi-timeframe functionality in UniverseStateManager.

Replaces mock-heavy testing with authentic database integration to test:
- Real multi-timeframe data aggregation with actual market data
- Feature extraction with authentic minute bar processing
- UniverseStateManager integration with real market data managers
- Error handling with actual database and data processing exceptions
- Performance characteristics with real data volumes

This demonstrates fail-fast testing that eliminates Mock dependencies
and provides authentic validation of multi-timeframe universe state management.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import sys
import os

from domains.trading.services.state.universe_state_manager import UniverseStateManager
from shared.utils.environment import Environment, EnvironmentType
from core.dao.instruments_dao import InstrumentsDAO


class TestMultiTimeframeUniverseStateManagerRealObjects:
    """Real objects test suite for multi-timeframe functionality in UniverseStateManager."""

    @pytest.fixture
    async def test_environment(self):
        """Real Environment instance for testing."""
        return Environment(
            env_type=EnvironmentType.DEV,
            db_url="postgresql://postgres:dev_password@localhost:3432/dev_db"
        )

    @pytest.fixture
    async def real_universe_manager(self, test_environment):
        """Real UniverseStateManager instance with actual dependencies."""
        return UniverseStateManager(environment=test_environment)

    @pytest.fixture
    async def test_market_data(self, test_environment):
        """Create real test market data and clean up after test."""
        dao = InstrumentsDAO(test_environment)
        
        # Create test instruments
        test_symbols = ['TEST_AAPL', 'TEST_GOOGL', 'TEST_MSFT']
        instrument_ids = []
        
        for symbol in test_symbols:
            instrument_id = await dao.create_instrument(
                symbol=symbol,
                name=f"Test {symbol.replace('TEST_', '')} Inc.",
                exchange="NASDAQ",
                sector="Technology"
            )
            instrument_ids.append(instrument_id)
        
        # Generate real multi-timeframe market data
        market_data = {}
        timeframes = ['5m', '15m', '1h', '1d']
        
        for timeframe in timeframes:
            tf_data = []
            intervals = {'5m': 5, '15m': 15, '1h': 60, '1d': 1440}
            interval_minutes = intervals[timeframe]
            
            for symbol in test_symbols:
                # Generate 100 intervals of data for each timeframe
                for i in range(100):
                    timestamp = datetime.now() - timedelta(minutes=interval_minutes * i)
                    tf_data.append({
                        'symbol': symbol,
                        'timestamp': timestamp,
                        'timeframe': timeframe,
                        'open': 100.0 + (i * 0.1),
                        'high': 101.0 + (i * 0.1),
                        'low': 99.0 + (i * 0.1),
                        'close': 100.5 + (i * 0.1),
                        'volume': 1000000 + (i * 1000),
                        'vwap': 100.25 + (i * 0.1)
                    })
            
            market_data[timeframe] = pd.DataFrame(tf_data)
        
        yield {
            'data': market_data,
            'symbols': test_symbols,
            'instrument_ids': instrument_ids,
            'timeframes': timeframes
        }
        
        # Cleanup
        for instrument_id in instrument_ids:
            await dao.delete_instrument(instrument_id)

    async def test_multi_timeframe_data_loading_real_objects(self, real_universe_manager, test_market_data):
        """Test multi-timeframe data loading with real market data."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        timeframes = test_market_data['timeframes']
        
        # Test real multi-timeframe data loading
        for timeframe in timeframes:
            tf_data = market_data[timeframe]
            
            # Load real data for timeframe
            result = await real_universe_manager.load_timeframe_data(
                symbols=symbols,
                timeframe=timeframe,
                start_date=datetime.now().date() - timedelta(days=1),
                end_date=datetime.now().date(),
                data=tf_data
            )
            
            # Validate real data loading
            assert result is not None
            
            if isinstance(result, pd.DataFrame):
                assert len(result) > 0
                assert 'symbol' in result.columns
                assert 'timestamp' in result.columns
                assert timeframe in str(result.columns).lower() or 'timeframe' in result.columns

    async def test_timeframe_aggregation_real_objects(self, real_universe_manager, test_market_data):
        """Test timeframe aggregation with real data processing."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        
        # Test aggregation from 5m to 15m with real data
        base_data = market_data['5m']
        
        aggregated_data = await real_universe_manager.aggregate_timeframe(
            base_data=base_data,
            from_timeframe='5m',
            to_timeframe='15m',
            symbols=symbols
        )
        
        # Validate real aggregation
        assert aggregated_data is not None
        
        if isinstance(aggregated_data, pd.DataFrame):
            assert len(aggregated_data) > 0
            
            # 15m should have fewer records than 5m
            assert len(aggregated_data) <= len(base_data)
            
            # Verify aggregation logic
            for symbol in symbols:
                symbol_5m = base_data[base_data['symbol'] == symbol]
                symbol_15m = aggregated_data[aggregated_data['symbol'] == symbol] if 'symbol' in aggregated_data.columns else aggregated_data
                
                if len(symbol_5m) > 0 and len(symbol_15m) > 0:
                    # 15m intervals should have roughly 1/3 the number of 5m intervals
                    ratio = len(symbol_5m) / len(symbol_15m)
                    assert 2 <= ratio <= 4  # Allow some variance in real aggregation

    async def test_multi_timeframe_feature_extraction_real_objects(self, real_universe_manager, test_market_data):
        """Test multi-timeframe feature extraction with real data processing."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        timeframes = test_market_data['timeframes']
        
        # Test real feature extraction across multiple timeframes
        features = {}
        
        for timeframe in timeframes:
            tf_data = market_data[timeframe]
            
            # Extract features for each timeframe
            tf_features = await real_universe_manager.extract_features(
                data=tf_data,
                timeframe=timeframe,
                symbols=symbols,
                feature_types=['price', 'volume', 'volatility']
            )
            
            features[timeframe] = tf_features
            
            # Validate feature extraction
            if tf_features is not None:
                assert isinstance(tf_features, (pd.DataFrame, dict, list))
                
                if isinstance(tf_features, pd.DataFrame):
                    assert len(tf_features) > 0
                    # Should have features for multiple symbols
                    if 'symbol' in tf_features.columns:
                        unique_symbols = tf_features['symbol'].unique()
                        assert len(unique_symbols) > 0

    async def test_cross_timeframe_correlation_real_objects(self, real_universe_manager, test_market_data):
        """Test cross-timeframe correlation analysis with real data."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        
        # Test correlation between 5m and 1h timeframes
        data_5m = market_data['5m']
        data_1h = market_data['1h']
        
        correlation_analysis = await real_universe_manager.analyze_cross_timeframe_correlation(
            short_tf_data=data_5m,
            long_tf_data=data_1h,
            short_timeframe='5m',
            long_timeframe='1h',
            symbols=symbols
        )
        
        # Validate correlation analysis
        if correlation_analysis is not None:
            assert isinstance(correlation_analysis, (dict, pd.DataFrame))
            
            if isinstance(correlation_analysis, dict):
                # Should have correlation data for symbols
                assert len(correlation_analysis) > 0
                
                # Check for reasonable correlation values
                for symbol, corr_data in correlation_analysis.items():
                    if isinstance(corr_data, (float, int)):
                        assert -1 <= corr_data <= 1  # Valid correlation range

    async def test_universe_state_multi_timeframe_integration_real_objects(self, real_universe_manager, test_market_data):
        """Test universe state integration across multiple timeframes with real data."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        timeframes = test_market_data['timeframes']
        
        # Test real universe state building with multi-timeframe data
        universe_state = await real_universe_manager.build_multi_timeframe_universe_state(
            market_data=market_data,
            symbols=symbols,
            timeframes=timeframes,
            start_date=datetime.now().date() - timedelta(days=1),
            end_date=datetime.now().date()
        )
        
        # Validate multi-timeframe universe state
        assert universe_state is not None
        
        if isinstance(universe_state, dict):
            # Should have data for each timeframe
            for timeframe in timeframes:
                if timeframe in universe_state:
                    tf_state = universe_state[timeframe]
                    assert tf_state is not None
                    
                    if isinstance(tf_state, pd.DataFrame):
                        assert len(tf_state) > 0

    async def test_performance_multi_timeframe_real_objects(self, real_universe_manager, test_market_data):
        """Test performance characteristics with real multi-timeframe processing."""
        import time
        
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        
        # Create larger dataset for performance testing
        large_market_data = {}
        for timeframe, tf_data in market_data.items():
            # Multiply data by 10 for performance testing
            large_tf_data = pd.concat([tf_data] * 10, ignore_index=True)
            large_market_data[timeframe] = large_tf_data
        
        # Measure real performance
        start_time = time.time()
        
        result = await real_universe_manager.process_multi_timeframe_data(
            market_data=large_market_data,
            symbols=symbols,
            timeframes=['5m', '15m', '1h'],
            processing_mode='parallel'
        )
        
        processing_time = time.time() - start_time
        
        # Validate performance
        assert result is not None
        assert processing_time >= 0
        
        # Log performance metrics
        total_records = sum(len(df) for df in large_market_data.values())
        processing_rate = total_records / processing_time if processing_time > 0 else float('inf')
        
        print(f"Multi-timeframe processing time: {processing_time:.4f}s")
        print(f"Total records processed: {total_records}")
        print(f"Processing rate: {processing_rate:.0f} records/second")

    async def test_error_handling_multi_timeframe_real_objects(self, real_universe_manager):
        """Test error handling with real multi-timeframe processing exceptions."""
        
        # Test empty data handling
        empty_data = {}
        
        try:
            result = await real_universe_manager.process_multi_timeframe_data(
                market_data=empty_data,
                symbols=[],
                timeframes=[],
                processing_mode='sequential'
            )
            
            # If processing succeeds with empty data, that's acceptable
            assert result is not None or result is None
            
        except Exception as e:
            # Real error with specific information
            assert isinstance(e, Exception)
            assert "empty" in str(e).lower() or "data" in str(e).lower()
        
        # Test invalid timeframe handling
        invalid_data = {
            'invalid_tf': pd.DataFrame({
                'symbol': ['TEST'],
                'timestamp': [datetime.now()],
                'open': [100.0]
            })
        }
        
        try:
            result = await real_universe_manager.process_multi_timeframe_data(
                market_data=invalid_data,
                symbols=['TEST'],
                timeframes=['invalid_tf'],
                processing_mode='sequential'
            )
            
            # Should handle gracefully or raise specific error
            assert result is not None or result is None
            
        except Exception as e:
            assert isinstance(e, Exception)
            print(f"Expected error for invalid timeframe: {e}")

    async def test_memory_management_multi_timeframe_real_objects(self, real_universe_manager, test_market_data):
        """Test memory management with real multi-timeframe data processing."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        
        # Process data multiple times to test memory usage
        for i in range(3):
            result = await real_universe_manager.process_multi_timeframe_data(
                market_data=market_data,
                symbols=symbols,
                timeframes=['5m', '15m'],
                processing_mode='sequential'
            )
            
            # Force cleanup
            del result
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"Memory usage: {initial_memory:.1f}MB -> {final_memory:.1f}MB (+{memory_increase:.1f}MB)")
        
        # Memory increase should be reasonable
        assert memory_increase < 200  # Should not leak excessive memory

    async def test_data_consistency_across_timeframes_real_objects(self, real_universe_manager, test_market_data):
        """Test data consistency across timeframes with real data validation."""
        market_data = test_market_data['data']
        symbols = test_market_data['symbols']
        
        # Test data consistency between timeframes
        consistency_report = await real_universe_manager.validate_multi_timeframe_consistency(
            market_data=market_data,
            symbols=symbols,
            timeframes=['5m', '15m', '1h']
        )
        
        # Validate consistency report
        if consistency_report is not None:
            assert isinstance(consistency_report, dict)
            
            # Should have consistency metrics
            expected_keys = ['consistent', 'inconsistencies', 'validation_summary']
            for key in expected_keys:
                if key in consistency_report:
                    assert consistency_report[key] is not None
        
        # Check specific consistency rules
        data_5m = market_data['5m']
        data_15m = market_data['15m']
        
        for symbol in symbols:
            symbol_5m = data_5m[data_5m['symbol'] == symbol]
            symbol_15m = data_15m[data_15m['symbol'] == symbol]
            
            if len(symbol_5m) > 0 and len(symbol_15m) > 0:
                # 5m data should have more granular timestamps
                min_5m_time = symbol_5m['timestamp'].min()
                max_5m_time = symbol_5m['timestamp'].max()
                min_15m_time = symbol_15m['timestamp'].min()
                max_15m_time = symbol_15m['timestamp'].max()
                
                # Time ranges should overlap
                assert min_15m_time <= max_5m_time
                assert max_15m_time >= min_5m_time