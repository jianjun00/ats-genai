#!/usr/bin/env python3
"""
Test to reproduce the universe state constraint violation error.

CRITICAL: This test reproduces the exact issue described in the error:
OSError: Failed to save universe state: duplicate key value violates unique constraint 
"intg_universe_state_interval_universe_id_duration_start_date_ru"
DETAIL: Key (universe_id, duration, start_date_time, run_id)=(1, 5m, 2025-07-0X, XXX)

The root cause is that UniverseStateManager instances without proper run_context
default to static run_id values like 'default_run' or 'no_run_context', causing 
duplicate key violations when multiple instances try to save data.
"""
import os
import asyncio
from datetime import datetime

# Set test environment variables to avoid Gin config issues
os.environ['PYTEST_CURRENT_TEST'] = 'test_universe_state_duplicate_run_id'
os.environ['SKIP_GLOBAL_ENV'] = '1'
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

from core.platform.config.environment import Environment, EnvironmentType


async def test_reproduce_duplicate_run_id_constraint_violation():
    """
    This test MUST FAIL initially to reproduce the constraint violation.
    
    Reproduces the exact scenario:
    1. Create two UniverseStateManager instances without run_context
    2. Both will default to the same run_id ('default_run' or 'no_run_context')
    3. Try to save universe states with same (universe_id, duration, start_date_time, run_id)
    4. Second save should fail with constraint violation
    """
    # Use integration environment to test against the actual constraint
    env = Environment(env_type=EnvironmentType.INTEGRATION)
    
    # TEST INPUT DATA that causes the failure (from error message)
    universe_id = 1
    duration = "5m"
    start_date_time = datetime(2025, 7, 1, 9, 30, 0)  # 2025-07-01 09:30:00
    end_date_time = datetime(2025, 7, 1, 9, 35, 0)    # 2025-07-01 09:35:00
    
    # Create sample universe state data
    universe_state = UniverseStateInterval(
        duration=TimeDuration(duration),
        start_date_time=start_date_time,
        end_date_time=end_date_time,
        factor_intervals=[],
        universe_id=universe_id
    )
    
    # Add a sample instrument interval
    instrument_interval = InstrumentInterval(
        instrument_id=1,
        start_date_time=start_date_time,
        end_date_time=end_date_time,
        open=100.0,
        high=105.0,
        low=99.0,
        close=103.0,
        traded_volume=1000,
        traded_dollar=103000.0
    )
    universe_state.instrument_intervals[1] = instrument_interval
    
    # REPRODUCE THE ISSUE: Create two managers without run_context
    # This simulates the issue in runner_utils.py line 28
    manager1 = UniverseStateManager(env=env, run_context=None)  # Will default to 'no_run_context'
    manager2 = UniverseStateManager(env=env, run_context=None)  # Will default to 'no_run_context'
    
    print(f"Manager1 run_id: {getattr(manager1.run_context, 'run_id', 'no_run_context') if manager1.run_context else 'no_run_context'}")
    print(f"Manager2 run_id: {getattr(manager2.run_context, 'run_id', 'no_run_context') if manager2.run_context else 'no_run_context'}")
    
    # Prepare metadata for saving
    metadata = {
        'universe_id': universe_id,
        'duration': duration,
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'universe_state': universe_state
    }
    
    timestamp = start_date_time.isoformat()
    
    # First save should succeed
    print("🔹 Attempting first save...")
    result1 = await manager1.save_universe_state(timestamp, metadata)
    print(f"✅ First save succeeded: {result1}")
    
    # Second save with same parameters should fail with constraint violation
    print("🔹 Attempting second save with same run_id...")
    result2 = await manager2.save_universe_state(timestamp, metadata)
    print(f"❌ Second save unexpectedly succeeded: {result2}")
    
    # If we get here, the test failed to reproduce the issue
    pytest.fail("Expected constraint violation did not occur - both saves succeeded unexpectedly")
    
async def test_show_run_id_collision_mechanism():
    """
    This test demonstrates WHY the constraint violation happens.
    Shows that managers without run_context use the same default run_id.
    """
    env = Environment(env_type=EnvironmentType.INTEGRATION)
    
    # Create multiple managers without run_context (simulates runner_utils.py issue)
    managers = [
        UniverseStateManager(env=env, run_context=None) for _ in range(3)
    ]
    
    # Extract the run_id that each manager would use
    run_ids = []
    for i, manager in enumerate(managers):
        # This mimics the logic in universe_state_manager.py line 397
        run_id = getattr(manager.run_context, 'run_id', 'default_run') if manager.run_context else 'no_run_context'
        run_ids.append(run_id)
        print(f"Manager {i+1} run_id: {run_id}")
    
    # Verify they all have the same run_id (this is the problem)
    unique_run_ids = set(run_ids)
    print(f"Unique run_ids: {unique_run_ids}")
    print(f"All managers have same run_id: {len(unique_run_ids) == 1}")
    
    # This demonstrates the root cause
    assert len(unique_run_ids) == 1, "All managers should have the same run_id (demonstrating the problem)"
    assert run_ids[0] == 'no_run_context', f"Expected 'no_run_context', got {run_ids[0]}"


if __name__ == "__main__":
    print("🚨 REPRODUCING UNIVERSE STATE CONSTRAINT VIOLATION")
    print("=" * 60)
    
    # Run the reproduction test
    asyncio.run(test_reproduce_duplicate_run_id_constraint_violation())
    
    print("\n" + "=" * 60)
    print("🔍 DEMONSTRATING RUN_ID COLLISION MECHANISM")
    
    # Run the mechanism demonstration
    asyncio.run(test_show_run_id_collision_mechanism())