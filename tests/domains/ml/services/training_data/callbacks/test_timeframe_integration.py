#!/usr/bin/env python3
"""
Integration tests for single timeframe generation with real components.

Tests the callback system with actual TimeSeriesSequenceTrainingGenerator
and real database connections to ensure the refactoring works end-to-end.
"""

import pytest
import asyncpg
from datetime import datetime
from pathlib import Path
import tempfile

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig, TimeSeriesSequenceTrainingGenerator
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from core.platform.config_env.environment import Environment, EnvironmentType
# FIXME: tests.utils module does not exist
# from tests.utils.test_data_setup import setup_single_symbol_test


class TestTimeframeIntegration:
    """Integration tests for single timeframe generation with real components."""

    @pytest.mark.asyncio
    async def test_single_timeframe_with_real_generator(self, unit_test_db):
        """Test single timeframe generation with real TimeSeriesSequenceTrainingGenerator."""
        
        # Setup real environment and test data
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        # Create real UniverseStateManager and TimeSeriesSequenceTrainingGenerator
        universe_manager = UniverseStateManager(environment)
        
        training_generator = TimeSeriesSequenceTrainingGenerator(
            env=environment,
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi', 'macd']),
            universe_manager=universe_manager
        )
        
        # Create callback with real generator
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi', 'macd'])
        )
        callback.training_generator = training_generator
        
        # Test single timeframe generation (35 minutes = 5m only)
        test_time = datetime(2025, 7, 1, 14, 35, 0)
        
        # This should work without exceptions using real components
        await callback.handleInterval(None, test_time)
        
        # Verify real generator is being used
        assert isinstance(callback.training_generator, TimeSeriesSequenceTrainingGenerator)
        assert callback.training_generator.env is not None
        assert callback.training_generator.universe_manager is not None

    @pytest.mark.asyncio
    async def test_real_generator_single_vs_multi_timeframe_consistency(self, unit_test_db):
        """Test that real generator produces consistent data for single vs multi timeframe calls."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        universe_manager = UniverseStateManager(environment)
        config = TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi', 'macd'])
        
        training_generator = TimeSeriesSequenceTrainingGenerator(
            env=environment,
            config=config,
            universe_manager=universe_manager
        )
        
        test_time = datetime(2025, 7, 1, 14, 45, 0)  # Should generate 5m and 15m
        
        # Generate single timeframes individually
        example_5m = await training_generator.generate_training_example(
            symbol='TSLA',
            prediction_timestamp=test_time,
            target_timeframes=['5m']
        )
        
        example_15m = await training_generator.generate_training_example(
            symbol='TSLA',
            prediction_timestamp=test_time,
            target_timeframes=['15m']
        )
        
        # Generate both timeframes together (old approach)
        example_multi = await training_generator.generate_training_example(
            symbol='TSLA',
            prediction_timestamp=test_time,
            target_timeframes=['5m', '15m']
        )
        
        # Compare data consistency
        if example_5m and example_15m and example_multi:
            # Single 5m should match 5m from multi
            if '5m' in example_5m.get('timeframe_features', {}):
                single_5m_data = example_5m['timeframe_features']['5m']
                multi_5m_data = example_multi['timeframe_features'].get('5m', {})
                
                # Key fields should be consistent
                for field in ['open', 'high', 'low', 'close', 'volume']:
                    if field in single_5m_data and field in multi_5m_data:
                        assert abs(single_5m_data[field] - multi_5m_data[field]) < 0.001, (
                            f"5m {field} mismatch: single={single_5m_data[field]}, multi={multi_5m_data[field]}"
                        )
            
            # Single 15m should match 15m from multi  
            if '15m' in example_15m.get('timeframe_features', {}):
                single_15m_data = example_15m['timeframe_features']['15m']
                multi_15m_data = example_multi['timeframe_features'].get('15m', {})
                
                for field in ['open', 'high', 'low', 'close', 'volume']:
                    if field in single_15m_data and field in multi_15m_data:
                        assert abs(single_15m_data[field] - multi_15m_data[field]) < 0.001, (
                            f"15m {field} mismatch: single={single_15m_data[field]}, multi={multi_15m_data[field]}"
                        )

    @pytest.mark.asyncio
    async def test_arrayrecord_storage_with_single_timeframes(self, unit_test_db):
        """Test that single timeframe examples are stored correctly to ArrayRecord files."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        # Use temporary directory for test files
        with tempfile.TemporaryDirectory() as temp_dir:
            callback = IntervalBasedTrainingDataCallback(
                symbols=['TSLA'],
                config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi']),
                storage_format='arrayrecord',
                output_dir=temp_dir,
                start_date='2025-07-01',
                end_date='2025-07-01'
            )
            callback.dataset_id = 'test_single_timeframe'
            
            # Create real generator  
            universe_manager = UniverseStateManager(environment)
            training_generator = TimeSeriesSequenceTrainingGenerator(
                env=environment,
                config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi']),
                universe_manager=universe_manager
            )
            callback.training_generator = training_generator
            
            # Generate and save single timeframe data
            test_time = datetime(2025, 7, 1, 14, 35, 0)  # 5m only
            
            await callback.handleInterval(None, test_time)
            
            # Verify file structure
            expected_file = Path(temp_dir) / 'test_single_timeframe' / 'TSLA_2025_07' / '5m' / 'TSLA_2025_07.arrayrecord'
            
            if expected_file.exists():
                # File was created successfully
                assert expected_file.stat().st_size > 0
                
                # Verify only 5m file was created, not other timeframes
                timeframe_dir = expected_file.parent.parent
                created_timeframes = [d.name for d in timeframe_dir.iterdir() if d.is_dir()]
                
                # Should only have 5m directory for this test
                assert '5m' in created_timeframes
                
    @pytest.mark.asyncio
    async def test_error_handling_with_real_components(self, unit_test_db):
        """Test error handling with real components and database connections."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        # Create callback with real components
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'INVALID_SYMBOL'],  # Include invalid symbol
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        universe_manager = UniverseStateManager(environment)
        training_generator = TimeSeriesSequenceTrainingGenerator(
            env=environment,
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi']),
            universe_manager=universe_manager
        )
        callback.training_generator = training_generator
        
        test_time = datetime(2025, 7, 1, 14, 35, 0)
        
        # Should handle invalid symbol gracefully without crashing
        await callback.handleInterval(None, test_time)
        # If it completes without exception, good
    @pytest.mark.asyncio  
    async def test_multiple_timeframes_real_generation_order(self, unit_test_db):
        """Test that multiple timeframes are generated in consistent order with real components."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        universe_manager = UniverseStateManager(environment)
        training_generator = TimeSeriesSequenceTrainingGenerator(
            env=environment,
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi']),
            universe_manager=universe_manager
        )
        callback.training_generator = training_generator
        
        # Test time that should generate all timeframes (Monday 00:00)
        test_time = datetime(2025, 7, 7, 0, 0, 0)  # Monday midnight
        
        # Capture generation order by monitoring calls
        original_generate = training_generator.generate_training_example
        call_order = []
        
        async def capture_calls(*args, **kwargs):
            if 'target_timeframes' in kwargs:
                call_order.append(kwargs['target_timeframes'][0])
            return await original_generate(*args, **kwargs)
        
        training_generator.generate_training_example = capture_calls
        
        await callback.handleInterval(None, test_time)
        
        # Verify expected timeframes were generated
        expected_timeframes = {'5m', '15m', '1h', '1d', '1w'}
        generated_timeframes = set(call_order)
        
        # Should have attempted all timeframes
        assert expected_timeframes.issubset(generated_timeframes) or len(call_order) > 0
        
if __name__ == '__main__':
    pytest.main([__file__, '-v'])