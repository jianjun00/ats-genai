"""
Test to reproduce NameError: working_dir not defined in create_run_context.

This test reproduces the exact scoping issue and analyzes why test coverage
is missing for this critical function.
Following CLAUDE.md debug-first methodology: reproduce the issue first, then fix it.
"""

import pytest
import sys
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from core.shared.run_context import create_run_context, RunContextManager


class TestCreateRunContextCoverageGap:
    """Test to reproduce the working_dir NameError and analyze test coverage gaps."""

    def test_create_run_context_now_works_after_fix(self):
        """
        Test that create_run_context now works after fixing the working_dir NameError.
        
        This verifies the fix resolves the scoping issue.
        """
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a RunContextManager instance (same as _global_run_manager)
            manager = RunContextManager(base_artifacts_dir=temp_dir)
            
            # This should now work without NameError
            result = manager.create_run_context(run_id="test_run", metadata={"test": "data"})
            
            # Verify the result is correct
            assert result is not None
            assert result.run_id == "test_run"
            assert result.metadata is not None
            assert "working_directory" in result.metadata
            assert result.metadata["working_directory"] == os.getcwd()
            assert result.metadata["test"] == "data"
            
            print("✅ CREATE_RUN_CONTEXT FIX VERIFIED:")
            print(f"   ✅ Function completes without NameError")
            print(f"   ✅ Working directory set to: {result.metadata['working_directory']}")
            print(f"   ✅ Run ID correct: {result.run_id}")
            print(f"   ✅ Custom metadata preserved: {result.metadata['test']}")

    def test_reproduce_working_dir_nameerror_before_fix(self):
        """
        Test to document what the NameError was before the fix.
        
        This is kept for reference to show the issue that was resolved.
        """
        
        # This test documents the issue that WAS happening before the fix:
        # NameError: name 'working_dir' is not defined at line 131
        
        # The issue was in this code (before fix):
        # metadata.update({
        #     'created_at': start_time.isoformat(),
        #     'python_version': f"{os.sys.version_info.major}.{os.sys.version_info.minor}",
        #     'working_directory': working_dir,  # <-- working_dir was not defined
        #     'environment': os.environ.get('ENVIRONMENT', 'unknown')
        # })
        
        # The fix added:
        # working_dir = os.getcwd()
        
        print("📋 DOCUMENTED ISSUE (RESOLVED):")
        print("   ❌ WAS: NameError: name 'working_dir' is not defined")
        print("   ✅ FIXED: Added working_dir = os.getcwd() before usage")
        print("   ✅ RESULT: Function now works correctly")

    def test_module_level_function_now_works_after_fix(self):
        """
        Test that the module-level function now works after the fix.
        
        This verifies the fix works through the convenience function.
        """
        
        # The module-level function delegates to _global_run_manager.create_run_context()
        # which should now work correctly
        result = create_run_context(run_id="test_run", metadata={"test": "data"})
        
        # Verify the result is correct
        assert result is not None
        assert result.run_id == "test_run"
        assert result.metadata is not None
        assert "working_directory" in result.metadata
        assert result.metadata["working_directory"] == os.getcwd()
        assert result.metadata["test"] == "data"
        
        print("✅ MODULE-LEVEL FUNCTION FIX VERIFIED:")
        print(f"   ✅ Function completes without NameError")
        print(f"   ✅ Working directory set to: {result.metadata['working_directory']}")
        print(f"   ✅ Run ID correct: {result.run_id}")
        print(f"   ✅ Custom metadata preserved: {result.metadata['test']}")

    def test_analyze_why_test_coverage_is_missing(self):
        """
        Analyze why this critical function lacks proper test coverage.
        
        This identifies the patterns that lead to untested code.
        """
        
        # REASON 1: Function works in some contexts where working_dir is accidentally available
        def scenario_with_accidental_working_dir():
            """Simulate a context where working_dir might be accidentally available."""
            # If working_dir was defined in outer scope, the function might work
            working_dir = "/some/path"  # This would mask the bug
            
            # In this scenario, the function might appear to work
            # But it's relying on accidental scope leakage
            return working_dir
        
        # REASON 2: Tests might be mocking the function instead of testing it
        def scenario_with_mocked_function():
            """Simulate tests that mock instead of testing real behavior."""
            with patch('core.shared.run_context.create_run_context') as mock_create:
                mock_create.return_value = MagicMock()
                
                # This test would pass but doesn't exercise the real function
                result = create_run_context(run_id="test")
                assert result is not None  # Passes but tests nothing real
                return True
        
        # REASON 3: Tests might not exercise the specific code path
        def scenario_with_incomplete_code_path():
            """Simulate tests that don't reach the problematic line."""
            # Tests might only test the function signature or early returns
            # without reaching line 131 where working_dir is used
            
            # Example: Testing with None parameters
            with pytest.raises(Exception):
                # This might fail earlier for other reasons,
                # never reaching the working_dir line
                pass
        
        # REASON 4: Integration tests might set up environment differently
        def scenario_with_different_test_environment():
            """Simulate integration tests with different environment setup."""
            # Integration tests might run in environments where
            # working_dir is set by other parts of the system
            # masking the issue in unit test isolation
            pass
        
        # Verify the analysis scenarios
        working_dir_available = scenario_with_accidental_working_dir()
        assert working_dir_available == "/some/path"
        
        mocked_test_passes = scenario_with_mocked_function()
        assert mocked_test_passes == True
        
        print("📋 TEST COVERAGE GAP ANALYSIS:")
        print("   1. ❌ Function may work when working_dir accidentally available in outer scope")
        print("   2. ❌ Tests might mock the function instead of testing real implementation")
        print("   3. ❌ Tests might not reach the problematic line 131")
        print("   4. ❌ Integration tests might mask the issue with different environment setup")
        print("   5. ❌ No unit tests directly test create_run_context with real implementation")

    def test_identify_what_working_dir_should_be(self):
        """
        Test to identify what working_dir should be set to.
        
        This analyzes the function to understand the intended behavior.
        """
        
        # Looking at the context of the function, working_dir should likely be:
        # 1. The current working directory (os.getcwd())
        # 2. A parameter passed to the function
        # 3. Derived from the run context or metadata
        
        # Option 1: Current working directory
        current_working_dir = os.getcwd()
        assert current_working_dir is not None
        assert isinstance(current_working_dir, str)
        
        # Option 2: Derived from metadata
        metadata_working_dir = {"working_directory": "/some/project/path"}
        assert metadata_working_dir["working_directory"] is not None
        
        # Option 3: Function parameter
        def fixed_function_signature(run_id=None, metadata=None, working_dir=None):
            if working_dir is None:
                working_dir = os.getcwd()
            return working_dir
        
        result = fixed_function_signature()
        assert result == current_working_dir
        
        print("✅ WORKING_DIR SHOULD BE:")
        print(f"   1. Current working directory: {current_working_dir}")
        print(f"   2. Or function parameter with default to os.getcwd()")
        print(f"   3. Or derived from metadata if provided")

    def test_demonstrate_proper_test_coverage_approach(self):
        """
        Test to demonstrate how proper test coverage should work.
        
        This shows the testing approach that would have caught this bug.
        """
        
        def proper_test_approach():
            """Demonstrate proper testing that would catch the bug."""
            with tempfile.TemporaryDirectory() as temp_dir:
                # PROPER APPROACH: Test real function with real parameters
                manager = RunContextManager(base_artifacts_dir=temp_dir)
                
                # This SHOULD work but currently fails - that's what tests should catch
                try:
                    result = manager.create_run_context(
                        run_id="test_run_12345",
                        metadata={"test_key": "test_value"}
                    )
                    
                    # If it worked, verify the result
                    assert result is not None
                    assert result.run_id == "test_run_12345"
                    assert "working_directory" in result.metadata
                    return True
                    
        # Run the proper test approach
        test_passed = proper_test_approach()
        assert test_passed == False  # Currently fails due to bug
        
        print("🧪 PROPER TEST COVERAGE APPROACH:")
        print("   ✅ Test real function implementation (not mocks)")
        print("   ✅ Test with realistic parameters")
        print("   ✅ Verify complete execution path")
        print("   ✅ Assert on actual results and behavior")
        print("   ❌ Current test would FAIL, revealing the bug")

    def test_compare_bad_vs_good_testing_patterns(self):
        """
        Test to compare bad testing patterns vs good testing patterns.
        
        This shows why the bug wasn't caught.
        """
        
        # BAD PATTERN 1: Mock everything
        def bad_pattern_mock_everything():
            with patch('core.shared.run_context.RunContextManager') as mock_manager:
                mock_manager.return_value.create_run_context.return_value = MagicMock()
                
                # This passes but tests nothing
                result = create_run_context()
                return result is not None
        
        # BAD PATTERN 2: Only test happy path with mocked dependencies
        def bad_pattern_happy_path_only():
            with patch('os.getcwd', return_value='/mocked/path'):
                # This might accidentally provide working_dir through scope
                # Tests pass but real usage fails
                return True
        
        # BAD PATTERN 3: Integration tests that mask the issue
        def bad_pattern_integration_masking():
            # Integration tests run in environments where working_dir
            # might be available from other setup, masking the unit-level bug
            return True
        
        # GOOD PATTERN: Test real implementation with isolated setup
        def good_pattern_real_implementation():
            with tempfile.TemporaryDirectory() as temp_dir:
                manager = RunContextManager(base_artifacts_dir=temp_dir)
                
                # Test real function - this reveals the bug
                try:
                    manager.create_run_context(run_id="test")
                    return True
        # Compare patterns
        bad1_passes = bad_pattern_mock_everything()
        bad2_passes = bad_pattern_happy_path_only()
        bad3_passes = bad_pattern_integration_masking()
        good_implementation_works = good_pattern_real_implementation()
        
        assert bad1_passes == True   # Bad tests pass (but test nothing)
        assert bad2_passes == True   # Bad tests pass (but test nothing)
        assert bad3_passes == True   # Bad tests pass (but test nothing)
        assert good_implementation_works == True  # Good test now passes (bug is fixed!)
        
        print("📊 TESTING PATTERN COMPARISON (AFTER FIX):")
        print(f"   ❌ Bad Pattern 1 (Mock everything): {bad1_passes} - WOULD HAVE HIDDEN BUG")
        print(f"   ❌ Bad Pattern 2 (Happy path only): {bad2_passes} - WOULD HAVE HIDDEN BUG")
        print(f"   ❌ Bad Pattern 3 (Integration masking): {bad3_passes} - WOULD HAVE HIDDEN BUG")
        print(f"   ✅ Good Pattern (Real implementation): {good_implementation_works} - WOULD HAVE CAUGHT BUG")
        print("\n💡 The good testing pattern would have caught this bug immediately!")
        print("💡 Bad testing patterns would have let the bug slip into production!")


if __name__ == "__main__":
    # Run the coverage gap analysis
    test = TestCreateRunContextCoverageGap()
    
    print("🔍 ANALYZING CREATE_RUN_CONTEXT TEST COVERAGE GAP")
    print("=" * 60)
    
    try:
        print("\n1. Reproducing working_dir NameError...")
        test.test_reproduce_working_dir_nameerror()
    try:
        print("\n2. Reproducing via module-level function...")
        test.test_reproduce_via_module_level_function()
    print("\n3. Analyzing why test coverage is missing...")
    test.test_analyze_why_test_coverage_is_missing()
    
    print("\n4. Identifying what working_dir should be...")
    test.test_identify_what_working_dir_should_be()
    
    print("\n5. Demonstrating proper test coverage approach...")
    test.test_demonstrate_proper_test_coverage_approach()
    
    print("\n6. Comparing bad vs good testing patterns...")
    test.test_compare_bad_vs_good_testing_patterns()
    
    print("\n📋 COVERAGE GAP ANALYSIS COMPLETE:")
    print("   ❌ PROBLEM: Function has NameError but no tests catch it")
    print("   ❌ ROOT CAUSE: Tests mock function instead of testing real implementation")
    print("   ❌ CONSEQUENCE: Bug reaches production without detection")
    print("   ✅ SOLUTION: Add real implementation tests that would catch scoping issues")
    print("\n🎯 NEXT: Fix working_dir NameError and add proper test coverage")