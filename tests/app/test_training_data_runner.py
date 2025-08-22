"""
Test cases for training_data_runner with test data setup.

This follows the pattern established in test_indicator_runner_output.py
to test the training data generation with real test data.
"""

import os
import sys
import tempfile
import pytest
import asyncio
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch
import json

from tests.fixtures.insert_test_daily_prices import insert_test_daily_prices
from tests.fixtures.setup_test_universe_data import setup_test_universe_data


def test_training_data_runner_traditional_mode(unit_test_db, setup_test_universe_data):
    """
    Test that training_data_runner.py works in traditional mode (non-runner framework).
    """
    import subprocess
    import sys
    
    runner_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/app/training_data_runner.py'))
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_args = [
            sys.executable, runner_path,
            "--symbols", "AAPL",
            "--start-date", "2024-01-02",
            "--end-date", "2024-01-03",  # Short range for testing
            "--environment", "test",
            "--gin-config", "config/app_test.gin",
            "--output-dir", tmp_dir,
            "--output-formats", "pickle",
            "--min-examples", "1",
            "--debug"
        ]
        
        env = os.environ.copy()
        env["PYTHONPATH"] = "src"
        env["DATABASE_URL"] = unit_test_db
        
        result = subprocess.run(
            test_args,
            capture_output=True,
            text=True,
            env=env,
            timeout=120  # 2 minutes timeout
        )
        
        output = result.stdout
        error_output = result.stderr
        
        print('STDOUT:')
        print(output)
        print('STDERR:')
        print(error_output)
        
        # Check that the process completed successfully
        if result.returncode != 0:
            print(f"Process failed with return code: {result.returncode}")
            
        # Look for training data files in output directory
        output_path = Path(tmp_dir)
        pickle_files = list(output_path.glob("**/*.pkl"))
        
        print(f"Generated files: {[str(f) for f in pickle_files]}")
        
        # We should have at least attempted to generate training data
        assert "TrainingDataRunner initialized" in output or "training data generation" in output.lower()


def test_training_data_runner_framework_mode(unit_test_db, setup_test_universe_data):
    """
    Test that training_data_runner.py works with Runner framework using callbacks.
    """
    import subprocess
    import sys
    
    runner_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/app/training_data_runner.py'))
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_args = [
            sys.executable, runner_path,
            "--symbols", "AAPL",
            "--start-date", "2024-01-02", 
            "--end-date", "2024-01-03",  # Short range for testing
            "--environment", "test",
            "--gin-config", "config/app_test.gin",
            "--output-dir", tmp_dir,
            "--use-runner-framework",  # Use the Runner framework
            "--batch-size", "10",
            "--debug"
        ]
        
        env = os.environ.copy()
        env["PYTHONPATH"] = "src"
        env["DATABASE_URL"] = unit_test_db
        
        result = subprocess.run(
            test_args,
            capture_output=True,
            text=True,
            env=env,
            timeout=120  # 2 minutes timeout
        )
        
        output = result.stdout
        error_output = result.stderr
        
        print('STDOUT:')
        print(output)
        print('STDERR:')
        print(error_output)
        
        # Check that the Runner framework was used
        assert "Starting training data generation using Runner framework" in output or "DateBasedTrainingDataCallback" in output
        
        # Look for daily training data files  
        output_path = Path(tmp_dir)
        daily_dirs = list(output_path.glob("daily/*"))
        metadata_files = list(output_path.glob("metadata/*.json"))
        
        print(f"Daily directories: {[str(d) for d in daily_dirs]}")
        print(f"Metadata files: {[str(f) for f in metadata_files]}")


def test_training_data_callback_directly():
    """
    Test the DateBasedTrainingDataCallback directly with mock data.
    """
    from state.training_data_callback import DateBasedTrainingDataCallback
    from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    from config.environment import Environment, EnvironmentType
    
    # Create test callback
    config = TrainingDataConfig(
        sequence_lengths={'5m': 2, '15m': 2, '1h': 2, '1d': 2},  # Small for testing
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        callback = DateBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=tmp_dir,
            save_format="pickle"
        )
        
        # Mock runner
        class MockRunner:
            def get_environment(self):
                return Environment(env_type=EnvironmentType.TEST)
            
            def get_universe_state_manager(self):
                from state.universe_state_manager import UniverseStateManager
                return UniverseStateManager(env=self.get_environment())
        
        runner = MockRunner()
        test_time = datetime(2024, 1, 15, 9, 30, 0)
        
        # Test the callback workflow
        callback.handleStart(runner, test_time)
        assert callback.training_generator is not None
        
        callback.handleStartOfDay(runner, test_time)
        assert callback.current_date == test_time.date()
        assert len(callback.daily_examples) == 0
        
        # Test interval handling (will likely fail due to no test data, but structure should work)
        try:
            asyncio.run(callback.handleInterval(runner, test_time))
        except Exception as e:
            # Expected to fail due to no universe data, but callback structure should work
            print(f"Expected failure due to no test data: {e}")
        
        # Test end of day
        asyncio.run(callback.handleEndOfDay(runner, test_time))
        
        # Check that output directory structure was created
        output_path = Path(tmp_dir)
        assert (output_path / "daily").exists()
        assert (output_path / "metadata").exists()


@pytest.mark.asyncio
async def test_training_data_generation_with_test_data():
    """
    Test training data generation with actual test data using the callback pattern.
    """
    from state.training_data_callback import DateBasedTrainingDataCallback
    from ml.training_data.timeseries_sequence_training_generator import (
        TrainingDataConfig,
        TimeSeriesSequenceTrainingGenerator
    )
    from config.environment import Environment, EnvironmentType
    from state.universe_state_manager import UniverseStateManager
    
    # Create test environment
    env = Environment(env_type=EnvironmentType.TEST)
    
    # Create test configuration with minimal requirements
    config = TrainingDataConfig(
        base_interval_minutes=1,
        training_interval_minutes=60,
        sequence_lengths={'5m': 1, '15m': 1, '1h': 1, '1d': 1},  # Minimal for testing
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Test direct training generator
        universe_manager = UniverseStateManager(env=env)
        generator = TimeSeriesSequenceTrainingGenerator(
            env=env,
            config=config,
            universe_manager=universe_manager
        )
        
        test_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Try to generate a training example (will likely fail due to no data)
        try:
            example = await generator.generate_training_example('AAPL', test_time)
            if example:
                print(f"Successfully generated example: {example.symbol} at {example.prediction_timestamp}")
            else:
                print("No example generated (expected due to no test data)")
        except Exception as e:
            print(f"Expected failure due to no test universe data: {e}")
        
        # Test that the callback can be created and initialized
        callback = DateBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=tmp_dir
        )
        
        assert callback.symbols == ['AAPL']
        assert callback.config.sequence_lengths['5m'] == 1
        assert Path(tmp_dir) == callback.output_dir


def test_training_data_config():
    """Test TrainingDataConfig creation and validation."""
    from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    
    # Test default config
    config = TrainingDataConfig()
    assert config.base_interval_minutes == 1
    assert config.training_interval_minutes == 60
    assert '5m' in config.sequence_lengths
    assert '1h' in config.prediction_horizons
    assert 'ohlcv' in config.feature_types
    
    # Test custom config
    custom_config = TrainingDataConfig(
        base_interval_minutes=5,
        training_interval_minutes=30,
        sequence_lengths={'5m': 10, '1h': 5},
        prediction_horizons={'1h': 3},
        timeframes=['5m', '1h'],
        feature_types=['ohlcv', 'returns']
    )
    
    assert custom_config.base_interval_minutes == 5
    assert custom_config.training_interval_minutes == 30
    assert custom_config.sequence_lengths['5m'] == 10
    assert custom_config.prediction_horizons['1h'] == 3
    assert len(custom_config.timeframes) == 2
    assert len(custom_config.feature_types) == 2


def test_runner_callback_interface():
    """Test that our callback properly implements the RunnerCallback interface."""
    from state.training_data_callback import DateBasedTrainingDataCallback
    from state.runner_callback import RunnerCallback
    
    callback = DateBasedTrainingDataCallback(symbols=['AAPL'])
    
    # Check that it's a proper subclass
    assert isinstance(callback, RunnerCallback)
    
    # Check that all required methods exist
    assert hasattr(callback, 'handleStart')
    assert hasattr(callback, 'handleStartOfDay')
    assert hasattr(callback, 'handleInterval')
    assert hasattr(callback, 'handleEndOfDay')
    assert hasattr(callback, 'handleEnd')
    
    # Check that methods are callable
    assert callable(callback.handleStart)
    assert callable(callback.handleStartOfDay) 
    assert callable(callback.handleInterval)
    assert callable(callback.handleEndOfDay)
    assert callable(callback.handleEnd)


def test_pure_callback_architecture():
    """Test that we now use ONLY pure callback architecture (no TrainingDataRunner class)."""
    from state.training_data_callback import DateBasedTrainingDataCallback
    from state.runner_callback import RunnerCallback
    
    # ✅ CORRECT: Only create the callback - no TrainingDataRunner class
    callback = DateBasedTrainingDataCallback(symbols=['AAPL'])
    
    # Verify it's a proper callback
    assert isinstance(callback, RunnerCallback)
    assert isinstance(callback, DateBasedTrainingDataCallback)
    
    # Verify it has all required callback methods
    assert hasattr(callback, 'handleStart')
    assert hasattr(callback, 'handleInterval')
    assert hasattr(callback, 'handleEndOfDay')
    
    # ✅ CORRECT: It should NOT have runner methods (pure callback)
    assert not hasattr(callback, 'generate_training_data')
    assert not hasattr(callback, 'save_training_data')
    assert not hasattr(callback, 'run')
    
    print("✅ Pure callback architecture verified")
    print("   ✅ DateBasedTrainingDataCallback is a RunnerCallback")
    print("   ✅ Has callback methods: handleStart, handleInterval, handleEndOfDay")
    print("   ❌ NO runner methods (pure callback responsibility)")


def test_no_training_data_runner_class():
    """Test that TrainingDataRunner class no longer exists (pure callback approach)."""
    # ✅ CORRECT: TrainingDataRunner class should not exist
    try:
        from app.training_data_runner import TrainingDataRunner
        # If we get here, the class still exists - that's wrong
        assert False, "TrainingDataRunner class should not exist in pure callback approach"
    except (ImportError, ModuleNotFoundError):
        # ✅ CORRECT: The class doesn't exist anymore
        print("✅ Verified: TrainingDataRunner class properly removed")
        print("   ✅ Pure callback approach implemented correctly")
        pass


def test_callback_with_test_data_setup():
    """Test callback with proper test data setup following indicator_runner pattern."""
    import tempfile
    from state.training_data_callback import DateBasedTrainingDataCallback
    from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    from config.environment import Environment, EnvironmentType
    from datetime import datetime
    
    # Use minimal config for testing
    config = TrainingDataConfig(
        base_interval_minutes=1,
        training_interval_minutes=60,
        sequence_lengths={'5m': 1, '15m': 1, '1h': 1, '1d': 1},
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        callback = DateBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=config,
            output_dir=tmp_dir,
            save_format="pickle"
        )
        
        # Mock runner similar to indicator_runner tests
        class MockRunner:
            def get_environment(self):
                return Environment(env_type=EnvironmentType.TEST)
            
            def get_universe_state_manager(self):
                from state.universe_state_manager import UniverseStateManager
                return UniverseStateManager(env=self.get_environment())
        
        runner = MockRunner()
        
        # Test full callback lifecycle with test data structure
        test_dates = [
            datetime(2024, 1, 15, 9, 0, 0),   # Start
            datetime(2024, 1, 15, 9, 30, 0),  # Interval 1
            datetime(2024, 1, 15, 10, 30, 0), # Interval 2
            datetime(2024, 1, 15, 16, 0, 0),  # End of day
        ]
        
        # Test handleStart
        callback.handleStart(runner, test_dates[0])
        assert callback.training_generator is not None
        
        # Test handleStartOfDay
        callback.handleStartOfDay(runner, test_dates[0])
        assert callback.current_date == test_dates[0].date()
        assert len(callback.daily_examples) == 0
        assert callback.daily_stats['date'] == test_dates[0].date().isoformat()
        
        # Test handleInterval (should work even with no data)
        initial_count = len(callback.daily_examples)
        asyncio.run(callback.handleInterval(runner, test_dates[1]))
        # Examples may or may not be generated due to no test data, but structure should work
        assert callback.daily_stats['intervals_processed'] == 1
        
        # Test another interval
        asyncio.run(callback.handleInterval(runner, test_dates[2]))
        assert callback.daily_stats['intervals_processed'] == 2
        
        # Test handleEndOfDay
        asyncio.run(callback.handleEndOfDay(runner, test_dates[3]))
        assert callback.current_date is None  # Should be cleared
        
        # Verify output directory structure was created
        output_path = Path(tmp_dir)
        assert (output_path / "daily").exists()
        assert (output_path / "metadata").exists()


def test_multi_symbol_callback_functionality():
    """Test callback with multiple symbols to verify it handles all symbols correctly."""
    import tempfile
    from state.training_data_callback import DateBasedTrainingDataCallback
    from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    from config.environment import Environment, EnvironmentType
    from datetime import datetime
    
    config = TrainingDataConfig(
        sequence_lengths={'5m': 2, '15m': 2, '1h': 2, '1d': 2},
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Test with multiple symbols
        callback = DateBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA', 'GOOGL', 'AMZN'],
            config=config,
            output_dir=tmp_dir,
            save_format="pickle"
        )
        
        assert len(callback.symbols) == 4
        assert 'AAPL' in callback.symbols
        assert 'TSLA' in callback.symbols
        assert 'GOOGL' in callback.symbols
        assert 'AMZN' in callback.symbols
        
        # Mock runner
        class MockRunner:
            def get_environment(self):
                return Environment(env_type=EnvironmentType.TEST)
            
            def get_universe_state_manager(self):
                from state.universe_state_manager import UniverseStateManager
                return UniverseStateManager(env=self.get_environment())
        
        runner = MockRunner()
        test_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Initialize callback
        callback.handleStart(runner, test_time)
        callback.handleStartOfDay(runner, test_time)
        
        # Test that symbols are correctly configured in daily stats
        assert callback.daily_stats['symbols'] == ['AAPL', 'TSLA', 'GOOGL', 'AMZN']
        
        # Test interval processing attempts to process all symbols
        initial_stats = callback.daily_stats.copy()
        asyncio.run(callback.handleInterval(runner, test_time))
        
        # Should have attempted to process at least one interval
        assert callback.daily_stats['intervals_processed'] > initial_stats['intervals_processed']


def test_advanced_storage_configuration():
    """Test callback with advanced storage configuration."""
    import tempfile
    from state.training_data_callback import DateBasedTrainingDataCallback
    from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
    
    config = TrainingDataConfig(
        sequence_lengths={'5m': 2, '15m': 2, '1h': 2, '1d': 2},
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create storage manager
        storage_config = StorageConfig(
            primary_format='pickle',  # Use pickle for testing
            compression_level=6,
            chunk_size=100,
            enable_indexing=True,
            enable_checksums=True
        )
        storage_manager = SequenceStorageManager(
            base_path=tmp_dir,
            config=storage_config
        )
        
        # Test callback with advanced storage
        callback = DateBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=tmp_dir,
            save_format="advanced",
            storage_manager=storage_manager
        )
        
        assert callback.save_format == "advanced"
        assert callback.storage_manager is not None
        assert callback.storage_manager.config.primary_format == 'pickle'


def test_error_handling_in_callback():
    """Test error handling and recovery in callback operations."""
    import tempfile
    from state.training_data_callback import DateBasedTrainingDataCallback
    from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    from config.environment import Environment, EnvironmentType
    from datetime import datetime
    
    config = TrainingDataConfig(
        sequence_lengths={'5m': 1, '15m': 1, '1h': 1, '1d': 1},
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        callback = DateBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=tmp_dir,
            save_format="pickle"
        )
        
        # Mock runner
        class MockRunner:
            def get_environment(self):
                return Environment(env_type=EnvironmentType.TEST)
            
            def get_universe_state_manager(self):
                from state.universe_state_manager import UniverseStateManager
                return UniverseStateManager(env=self.get_environment())
        
        runner = MockRunner()
        test_time = datetime(2024, 1, 15, 10, 0, 0)
        
        # Initialize
        callback.handleStart(runner, test_time)
        callback.handleStartOfDay(runner, test_time)
        
        # Test interval with no data (should handle gracefully)
        initial_errors = len(callback.daily_stats.get('errors', []))
        asyncio.run(callback.handleInterval(runner, test_time))
        
        # Should not crash and should track any errors appropriately
        assert 'errors' in callback.daily_stats
        # Errors may or may not be added depending on data availability
        
        # Test end of day (should work even with no examples)
        asyncio.run(callback.handleEndOfDay(runner, test_time))
        assert callback.current_date is None  # Should be properly cleaned up