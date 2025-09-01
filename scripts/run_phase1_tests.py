#!/usr/bin/env python3
"""
Phase 1 Test Runner
Validates all Phase 1 implementations for multi-scale sequence modeling.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_test_suite(test_file: str) -> tuple[bool, str]:
    """Run a specific test suite and return success status and output."""
    try:
        cmd = [
            sys.executable, "-m", "pytest", 
            test_file, 
            "-v", 
            "--tb=short",
            "--disable-warnings"
        ]
        
        # Set PYTHONPATH to include src directory
        env = os.environ.copy()
        env['PYTHONPATH'] = 'src'
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            cwd=Path(__file__).parent.parent,
            env=env
        )
        
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, f"Error running {test_file}: {str(e)}"

def main():
    """Run all Phase 1 test suites."""
    print("🚀 Running Phase 1 Test Validation")
    print("=" * 50)
    
    test_suites = [
        "tests/storage/test_multi_scale_sequence.py",
        "tests/storage/test_hdf5_multi_scale_cache.py", 
        "tests/events/test_event_integration.py",
        "tests/models/attention/test_cross_scale_attention.py"
    ]
    
    results = {}
    total_passed = 0
    total_failed = 0
    
    for test_suite in test_suites:
        print(f"\n📋 Running {test_suite}...")
        success, output = run_test_suite(test_suite)
        results[test_suite] = (success, output)
        
        if success:
            total_passed += 1
            print(f"✅ {test_suite} - PASSED")
        else:
            total_failed += 1
            print(f"❌ {test_suite} - FAILED")
            # Print first few lines of error for debugging
            error_lines = output.split('\n')[:10]
            print("Error details:")
            for line in error_lines:
                print(f"   {line}")
    
    print("\n" + "=" * 50)
    print(f"📊 PHASE 1 TEST RESULTS:")
    print(f"✅ Passed: {total_passed}")
    print(f"❌ Failed: {total_failed}")
    print(f"📈 Success Rate: {total_passed}/{len(test_suites)} ({100*total_passed/len(test_suites):.1f}%)")
    
    # Print detailed results for failed tests
    if total_failed > 0:
        print("\n🔍 DETAILED FAILURE ANALYSIS:")
        for test_suite, (success, output) in results.items():
            if not success:
                print(f"\n❌ {test_suite}:")
                print(output)
    
    return total_failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)