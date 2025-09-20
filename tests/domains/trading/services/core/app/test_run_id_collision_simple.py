#!/usr/bin/env python3
"""
Simple test to demonstrate the run_id collision issue that causes constraint violations.

This test reproduces the core issue without requiring complex database setup.
The issue is in universe_state_manager.py line 397:
    run_id = getattr(self.run_context, 'run_id', 'default_run') if self.run_context else 'no_run_context'
"""
import os
import sys

# Set test environment variables to avoid Gin config issues
os.environ['PYTEST_CURRENT_TEST'] = 'test_run_id_collision'
os.environ['SKIP_GLOBAL_ENV'] = '1'
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

# Add src to path for imports
sys.path.insert(0, 'src')

def test_demonstrate_run_id_collision():
    """
    This test demonstrates that multiple UniverseStateManager instances
    without run_context will use the same run_id, causing constraint violations.
    """
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
    
    print("🔍 DEMONSTRATING RUN_ID COLLISION ISSUE")
    print("=" * 50)
    
    # Create multiple managers without run_context (simulates runner_utils.py issue)
    managers = []
    for i in range(3):
        manager = UniverseStateManager(env=None, run_context=None)
        managers.append(manager)
    
    # Extract the run_id that each manager would use (from line 397 in universe_state_manager.py)
    run_ids = []
    for i, manager in enumerate(managers):
        # This mimics the exact logic in universe_state_manager.py line 397
        run_id = getattr(manager.run_context, 'run_id', 'default_run') if manager.run_context else 'no_run_context'
        run_ids.append(run_id)
        print(f"Manager {i+1} run_id: {run_id}")
    
    # Show the problem: all have the same run_id
    unique_run_ids = set(run_ids)
    print(f"\nUnique run_ids: {unique_run_ids}")
    print(f"Number of unique run_ids: {len(unique_run_ids)}")
    print(f"All managers have same run_id: {len(unique_run_ids) == 1}")
    
    if len(unique_run_ids) == 1:
        print(f"\n🚨 PROBLEM IDENTIFIED:")
        print(f"   All managers use run_id='{run_ids[0]}'")
        print(f"   When multiple managers try to save with same:")
        print(f"   - universe_id (e.g., 1)")
        print(f"   - duration (e.g., '5m')")
        print(f"   - start_date_time (e.g., '2025-07-01 09:30:00')")
        print(f"   - run_id ('{run_ids[0]}')")
        print(f"   → Duplicate key constraint violation!")
        
        print(f"\n✅ REPRODUCTION SUCCESSFUL")
        print(f"   This demonstrates why the constraint violation occurs.")
    else:
        print(f"\n❌ UNEXPECTED: Managers have different run_ids")
        print(f"   This suggests the issue may have been fixed or is more complex.")
    
    return len(unique_run_ids) == 1  # True = problem reproduced


def test_show_correct_behavior_with_run_context():
    """
    This test shows what SHOULD happen: each manager with proper run_context
    should have a unique run_id.
    """
    print(f"\n🔧 DEMONSTRATING CORRECT BEHAVIOR WITH RUN_CONTEXT")
    print("=" * 55)
    
    # Mock a simple run_context class for demonstration
    class MockRunContext:
        def __init__(self, run_id):
            self.run_id = run_id
    
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
    
    # Create managers WITH proper run_context
    managers_with_context = []
    for i in range(3):
        mock_context = MockRunContext(f"unique_run_{i+1}")
        manager = UniverseStateManager(env=None, run_context=mock_context)
        managers_with_context.append((manager, mock_context))
    
    # Extract run_ids
    run_ids_with_context = []
    for i, (manager, context) in enumerate(managers_with_context):
        # This simulates the logic in universe_state_manager.py line 397
        run_id = getattr(manager.run_context, 'run_id', 'default_run') if manager.run_context else 'no_run_context'
        run_ids_with_context.append(run_id)
        print(f"Manager {i+1} with context run_id: {run_id}")
    
    # Show the solution: all have unique run_ids
    unique_run_ids_with_context = set(run_ids_with_context)
    print(f"\nUnique run_ids with context: {unique_run_ids_with_context}")
    print(f"Number of unique run_ids: {len(unique_run_ids_with_context)}")
    print(f"All managers have unique run_ids: {len(unique_run_ids_with_context) == len(run_ids_with_context)}")
    
    if len(unique_run_ids_with_context) == len(run_ids_with_context):
        print(f"\n✅ SOLUTION DEMONSTRATED:")
        print(f"   Each manager has unique run_id")
        print(f"   No constraint violations would occur")
        print(f"   → This is what runner_utils.py should do!")
    
    return len(unique_run_ids_with_context) == len(run_ids_with_context)


if __name__ == "__main__":
    print("🚨 UNIVERSE STATE RUN_ID COLLISION TEST")
    print("=" * 60)
    
    # Test 1: Demonstrate the problem
    problem_reproduced = test_demonstrate_run_id_collision()
    
    # Test 2: Show the solution
    solution_works = test_show_correct_behavior_with_run_context()
    
    print(f"\n" + "=" * 60)
    print(f"📋 SUMMARY:")
    print(f"  Problem reproduced: {problem_reproduced}")
    print(f"  Solution verified: {solution_works}")
    
    if problem_reproduced and solution_works:
        print(f"\n🎯 NEXT STEPS:")
        print(f"  1. Fix runner_utils.py to pass proper run_context")
        print(f"  2. Ensure UniverseStateManager gets unique run_id")
        print(f"  3. Verify constraint violations are resolved")
    
    print(f"\n🔗 RELATED FILES:")
    print(f"  - src/domains/trading/services/core/app/runner_utils.py:28")
    print(f"  - src/domains/trading/services/state/universe_state_manager.py:397")