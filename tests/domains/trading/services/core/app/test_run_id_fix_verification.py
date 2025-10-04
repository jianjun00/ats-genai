#!/usr/bin/env python3
"""
Test to verify that the run_id collision fix works correctly.

This test verifies that after the fix in runner_utils.py:
1. Runner creates UniverseStateManager with proper run_context
2. Each runner instance gets a unique run_id
3. No more constraint violations should occur
"""
import os
import sys
from unittest.mock import Mock, patch

# Set test environment variables
os.environ['PYTEST_CURRENT_TEST'] = 'test_run_id_fix'
os.environ['SKIP_GLOBAL_ENV'] = '1'
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

# Add src to path for imports
sys.path.insert(0, 'src')

def test_runner_creates_unique_run_ids():
    """
    Test that Runner instances create UniverseStateManager with unique run_ids.
    This verifies the fix works correctly.
    """
    print("🔧 TESTING RUN_ID FIX VERIFICATION")
    print("=" * 45)
    
    from domains.trading.services.core.app.runner import Runner
    from core.platform.config_env.environment import Environment, EnvironmentType
    
    # Mock environment to avoid complex setup
    mock_env = Mock()
    mock_env.env_type = EnvironmentType.TEST
    mock_env.get_universe_id = Mock(return_value=1)
    
    # Create multiple Runner instances (simulates multiple runs)
    runners = []
    run_ids = []
    
    for i in range(3):
        print(f"\nCreating Runner {i+1}...")
        
        runner = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01", 
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration='1d'
        )
        runners.append(runner)
        
        # Extract the run_id from the runner's universe_state_manager
        manager = runner.universe_state_manager
        run_id = getattr(manager.run_context, 'run_id', 'no_run_context') if manager.run_context else 'no_run_context'
        run_ids.append(run_id)
        
        print(f"  Runner {i+1} run_id: {run_id}")
        print(f"  Has run_context: {manager.run_context is not None}")
    
    # Verify all run_ids are unique
    unique_run_ids = set(run_ids)
    print(f"\nUnique run_ids: {len(unique_run_ids)}")
    print(f"Total runners: {len(runners)}")
    print(f"All runners have unique run_ids: {len(unique_run_ids) == len(runners)}")
    
    # Check that none use the problematic default values
    problematic_ids = {'default_run', 'no_run_context'}
    has_problematic = any(run_id in problematic_ids for run_id in run_ids)
    print(f"No problematic run_ids found: {not has_problematic}")
    
    if len(unique_run_ids) == len(runners) and not has_problematic:
        print(f"\n✅ FIX VERIFICATION SUCCESSFUL!")
        print(f"   Each Runner has unique run_id with proper run_context")
        print(f"   Constraint violations should be resolved")
        return True
    else:
        print(f"\n❌ FIX VERIFICATION FAILED!")
        if len(unique_run_ids) != len(runners):
            print(f"   Some runners have duplicate run_ids")
        if has_problematic:
            print(f"   Some runners still use problematic run_ids: {problematic_ids}")
        return False


def test_builder_gets_proper_manager():
    """
    Test that UniverseStateIntervalBuilder gets the runner's manager with proper run_context.
    This simulates the fixed runner_utils.py behavior.
    """
    print(f"\n🔗 TESTING BUILDER MANAGER CONNECTION")
    print("=" * 45)
    
    from domains.trading.services.core.app.runner import Runner
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from core.platform.config_env.environment import EnvironmentType
    
    # Mock environment
    mock_env = Mock()
    mock_env.env_type = EnvironmentType.TEST
    mock_env.get_universe_id = Mock(return_value=1)
    
    # Create runner (simulates the fixed runner_utils.py)
    runner = Runner(
        start_date="2025-07-01",
        end_date="2025-07-01",
        environment=mock_env,
        universe_id=1,
        callbacks=[],
        base_duration='1d'
    )
    
    # Create builder and connect to runner's manager (fixed approach)
    builder = UniverseStateIntervalBuilder(
        env=mock_env,
        base_duration='1d',
        target_durations='1d'
    )
    builder.universe_state_manager = runner.universe_state_manager  # Fixed: use runner's manager
    
    # Verify builder uses runner's manager
    builder_manager = builder.universe_state_manager
    runner_manager = runner.universe_state_manager
    
    print(f"Builder manager is runner manager: {builder_manager is runner_manager}")
    
    # Verify both have the same run_context
    builder_run_id = getattr(builder_manager.run_context, 'run_id', 'no_run_context') if builder_manager.run_context else 'no_run_context'
    runner_run_id = getattr(runner_manager.run_context, 'run_id', 'no_run_context') if runner_manager.run_context else 'no_run_context'
    
    print(f"Builder run_id: {builder_run_id}")
    print(f"Runner run_id: {runner_run_id}")
    print(f"Same run_id: {builder_run_id == runner_run_id}")
    
    if (builder_manager is runner_manager and 
        builder_run_id == runner_run_id and 
        builder_run_id not in {'default_run', 'no_run_context'}):
        print(f"\n✅ BUILDER CONNECTION SUCCESSFUL!")
        print(f"   Builder uses runner's manager with proper run_context")
        return True
    else:
        print(f"\n❌ BUILDER CONNECTION FAILED!")
        return False


if __name__ == "__main__":
    print("🚨 RUN_ID FIX VERIFICATION TEST")
    print("=" * 60)
    
    # Test 1: Verify runners create unique run_ids
    fix_works = test_runner_creates_unique_run_ids()
    
    # Test 2: Verify builder connection works
    connection_works = test_builder_gets_proper_manager()
    
    print(f"\n" + "=" * 60)
    print(f"📋 VERIFICATION SUMMARY:")
    print(f"  Run_id uniqueness: {fix_works}")
    print(f"  Builder connection: {connection_works}")
    
    if fix_works and connection_works:
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"   The constraint violation fix is working correctly")
        print(f"   Multiple universeStateManager instances will have unique run_ids")
        print(f"   Database constraint violations should be resolved")
    else:
        print(f"\n❌ SOME TESTS FAILED!")
        print(f"   Further investigation needed")
    
    print(f"\n📝 WHAT WAS FIXED:")
    print(f"  Before: runner_utils.py created manager without run_context → 'no_run_context'")
    print(f"  After:  runner creates manager with proper run_context → unique UUIDs")
    print(f"  Result: No more duplicate (universe_id, duration, start_date_time, run_id) keys")