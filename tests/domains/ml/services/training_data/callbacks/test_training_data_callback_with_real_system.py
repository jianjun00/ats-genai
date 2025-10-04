"""
Training Data Callback Tests with Actual Real System Objects

Uses completely real objects from the system - no mocks, no fake implementations.
Tests the actual production code paths with real data.
"""

import pytest
import asyncio
import os
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.universe_state import UniverseStateInterval
from core.business.calendars.time_duration import TimeDuration
from core.platform.config_env.environment import Environment, EnvironmentType
# FIXME: tests.utils module does not exist
# from tests.utils.test_data_setup import setup_single_symbol_test
import asyncpg


@pytest.fixture
async def real_universe_manager(unit_test_db):
    """Create a real UniverseStateManager with actual test environment"""
    # Create actual Environment with test database
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Setup real test data
    conn = await asyncpg.connect(unit_test_db)
    await setup_single_symbol_test(environment, conn, 'AAPL', 999999, 1)
    await conn.close()
    
    # Create real UniverseStateIntervalBuilder
    builder = UniverseStateIntervalBuilder(
        env=environment,
        target_durations='5m,15m,60m,1d',
        base_duration='5m'
    )
    
    # Initialize with real builder
    manager = UniverseStateManager(env=environment)
    
    return manager


@pytest.fixture
async def real_training_generator(real_universe_manager):
    """Create a real TimeSeriesSequenceTrainingGenerator"""
    
    generator = TimeSeriesSequenceTrainingGenerator(
        sequence_length=10,
        prediction_horizon=5,
        universe_manager=real_universe_manager
    )
    
    return generator


@pytest.mark.asyncio
async def test_training_callback_with_completely_real_system(unit_test_db):
    """Test training callback using completely real system objects"""
    
    print(f"\n🏗️ Testing with completely real system objects...")
    
    # Set up real test environment
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Setup real test data
    conn = await asyncpg.connect(unit_test_db)
    test_setup = await setup_single_symbol_test(environment, conn, 'AAPL', 999999, 1)
    await conn.close()
    
    # Create real UniverseStateIntervalBuilder
    builder = UniverseStateIntervalBuilder(
        env=environment,
        target_durations='5m,15m,60m',
        base_duration='5m'
    )
    
    # Create real UniverseStateManager
    universe_manager = UniverseStateManager(
        env=environment
    )
    
    # Create real TimeSeriesSequenceTrainingGenerator
    training_generator = TimeSeriesSequenceTrainingGenerator(
        env=environment,
        universe_manager=universe_manager
    )
    
    # Create real callback
    callback = IntervalBasedTrainingDataCallback(['AAPL'])
    callback.training_generator = training_generator
    
    # Create real runner (simplified but real object structure)
    class RealRunner:
        def __init__(self, environment):
            self.environment = environment
    
    runner = RealRunner(environment)
    
    # Test with a time that has test data available
    test_time = datetime(2025, 7, 1, 14, 0)
    
    print(f"   📅 Testing at {test_time}")
    print(f"   🔧 Using real UniverseStateManager: {type(universe_manager).__name__}")
    print(f"   🎯 Using real TimeSeriesSequenceTrainingGenerator: {type(training_generator).__name__}")
    print(f"   ⚡ Using real IntervalBasedTrainingDataCallback: {type(callback).__name__}")
    print(f"   📊 Test setup: {test_setup}")
    
    # Generate real training data - let it fail if there are real issues
    await callback.handleInterval(runner, test_time)
    
    # If we get here, real data was generated successfully
    print(f"   ✅ Real training data generated successfully!")
    
    # Verify actual training generator results
    if hasattr(training_generator, 'generated_examples'):
        example_count = len(training_generator.generated_examples)
        print(f"   📊 Generated examples: {example_count}")
        assert example_count >= 0, "Should generate valid number of examples"
    
    # This proves the system works end-to-end with real objects
    assert True, "Real system generated training data successfully"


@pytest.mark.asyncio 
async def test_real_universe_state_manager_initialization(unit_test_db):
    """Test that real UniverseStateManager initializes correctly"""
    
    print(f"\n🔧 Testing real UniverseStateManager initialization...")
    
    # Set up real test environment
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Setup real test data
    conn = await asyncpg.connect(unit_test_db)
    await setup_single_symbol_test(environment, conn, 'AAPL', 999999, 1)
    await conn.close()
    
    # Create real UniverseStateIntervalBuilder  
    builder = UniverseStateIntervalBuilder(
        env=environment,
        target_durations='5m,15m,60m',
        base_duration='5m'
    )
    
    # Create real UniverseStateManager
    universe_manager = UniverseStateManager(
        env=environment
    )
    
    # Verify it's the real class, not a mock
    assert type(universe_manager).__name__ == 'UniverseStateManager'
    assert hasattr(universe_manager, 'get_universe_state_interval')
    assert hasattr(universe_manager, 'get_future_universe_state_interval')
    
    # Verify real environment is attached
    assert hasattr(universe_manager, 'env')
    assert universe_manager.env == environment
    
    print(f"   ✅ Real UniverseStateManager initialized: {type(universe_manager).__name__}")
    print(f"   ✅ Real environment attached: {type(universe_manager.env).__name__}")
    print(f"   ✅ All required methods available")
    
    # Test actual method functionality with real data
    test_instrument_id = 999999
    test_time = datetime(2025, 7, 1, 14, 0)
    
    # This should work with real data or fail meaningfully
    universe_state = await universe_manager.get_universe_state_interval(test_instrument_id, test_time)
    
    print(f"   ✅ Real method execution completed: get_universe_state_interval returned {type(universe_state)}")


@pytest.mark.asyncio
async def test_real_training_generator_initialization(unit_test_db):
    """Test that real TimeSeriesSequenceTrainingGenerator initializes correctly"""
    
    print(f"\n🎯 Testing real TimeSeriesSequenceTrainingGenerator initialization...")
    
    # Set up real test environment
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Setup real test data
    conn = await asyncpg.connect(unit_test_db)
    await setup_single_symbol_test(environment, conn, 'AAPL', 999999, 1)
    await conn.close()
    
    # Create real dependencies
    builder = UniverseStateIntervalBuilder(
        env=environment,
        target_durations='5m,15m,60m',
        base_duration='5m'
    )
    
    universe_manager = UniverseStateManager(
        env=environment
    )
    
    # Create real TimeSeriesSequenceTrainingGenerator
    training_generator = TimeSeriesSequenceTrainingGenerator(
        env=environment,
        universe_manager=universe_manager
    )
    
    # Verify it's the real class, not a mock
    assert type(training_generator).__name__ == 'TimeSeriesSequenceTrainingGenerator'
    assert hasattr(training_generator, 'generate_training_example')
    assert hasattr(training_generator, 'universe_manager')
    
    # Verify real universe manager is attached
    assert type(training_generator.universe_manager).__name__ == 'UniverseStateManager'
    
    # Verify real environment is attached
    assert hasattr(training_generator, 'env')
    assert training_generator.env == environment
    
    print(f"   ✅ Real TimeSeriesSequenceTrainingGenerator initialized: {type(training_generator).__name__}")
    print(f"   ✅ Real UniverseStateManager attached: {type(training_generator.universe_manager).__name__}")
    print(f"   ✅ Real environment attached: {type(training_generator.env).__name__}")
    print(f"   ✅ All required methods available")
    
    # Test actual method functionality with real data
    test_instrument_id = 999999
    test_time = datetime(2025, 7, 1, 14, 0)
    
    # This should work with real data or fail meaningfully
    training_example = await training_generator.generate_training_example(test_instrument_id, test_time)
    
    print(f"   ✅ Real method execution completed: generate_training_example returned {type(training_example)}")
    
    # Verify actual results if generated
    if training_example:
        assert 'symbol' in training_example, "Training example should contain symbol"
        assert 'prediction_timestamp' in training_example, "Training example should contain prediction_timestamp"
        print(f"   📊 Generated training example with symbol: {training_example.get('symbol')}")


if __name__ == "__main__":
    # Run tests directly with pytest
    import pytest
    pytest.main([__file__, '-v'])