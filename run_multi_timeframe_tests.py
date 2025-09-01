#!/usr/bin/env python3
"""
Test runner for multi-timeframe functionality.

Runs all tests related to the multi-timeframe training data generation enhancement,
including UniverseStateManager integration and TrainingDataJobRunner functionality.
"""

import sys
import os
import subprocess
import unittest
from pathlib import Path

def run_tests():
    """Run all multi-timeframe related tests."""
    
    print("🧪 MULTI-TIMEFRAME FUNCTIONALITY TEST SUITE")
    print("=" * 60)
    
    # Test files to run
    test_files = [
        'tests/test_multi_timeframe_universe_state_manager.py',
        'tests/test_multi_timeframe_training_data_job_runner.py',
        'test_multi_timeframe_validation.py'
    ]
    
    all_passed = True
    total_tests = 0
    
    for test_file in test_files:
        print(f"\n📋 Running {test_file}...")
        print("-" * 40)
        
        if not os.path.exists(test_file):
            print(f"❌ Test file not found: {test_file}")
            all_passed = False
            continue
        
        try:
            # Run the test file
            result = subprocess.run(
                [sys.executable, test_file], 
                capture_output=True, 
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            # Print output
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            
            # Check result
            if result.returncode == 0:
                print(f"✅ {test_file} passed")
                # Try to extract test count from output
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if 'tests passed' in line or 'All tests passed' in line:
                        try:
                            test_count = int([x for x in line.split() if x.isdigit()][-1])
                            total_tests += test_count
                            break
                        except:
                            pass
            else:
                print(f"❌ {test_file} failed (exit code: {result.returncode})")
                all_passed = False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ {test_file} timed out")
            all_passed = False
        except Exception as e:
            print(f"💥 Error running {test_file}: {e}")
            all_passed = False
    
    print("\n" + "=" * 60)
    print("📊 MULTI-TIMEFRAME TEST SUMMARY")
    print("=" * 60)
    
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print(f"✅ Total tests executed: {total_tests}")
        print("✅ Multi-timeframe functionality is working correctly")
        print("✅ UniverseStateManager integration verified")
        print("✅ TrainingDataJobRunner multi-timeframe features verified")
        print("✅ Gin configuration compliance verified")
        print("\n🎯 Ready for multi-timeframe training data generation!")
        return True
    else:
        print("💥 SOME TESTS FAILED!")
        print("❌ Multi-timeframe functionality has issues")
        print("❌ Check the test output above for details")
        print("❌ Fix issues before proceeding with training data generation")
        return False

def run_specific_test_categories():
    """Run specific categories of tests for targeted validation."""
    
    print("\n🔍 TARGETED TEST CATEGORIES")
    print("=" * 60)
    
    categories = {
        "Universe State Manager": {
            "description": "Tests UniverseStateManager.get_lag_prices() with time_interval parameter",
            "command": [sys.executable, "tests/test_multi_timeframe_universe_state_manager.py"]
        },
        "Training Data Job Runner": {
            "description": "Tests TrainingDataJobRunner multi-timeframe feature extraction",
            "command": [sys.executable, "tests/test_multi_timeframe_training_data_job_runner.py"]
        },
        "Feature Validation": {
            "description": "Validates feature structure and gin config compliance",
            "command": [sys.executable, "test_multi_timeframe_validation.py"]
        }
    }
    
    for category, info in categories.items():
        print(f"\n📂 {category}:")
        print(f"   {info['description']}")
        
        try:
            result = subprocess.run(info['command'], capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                print(f"   ✅ PASSED")
            else:
                print(f"   ❌ FAILED")
                if result.stderr:
                    print(f"   Error: {result.stderr.strip()}")
        except Exception as e:
            print(f"   💥 ERROR: {e}")

def validate_test_environment():
    """Validate that the test environment is properly set up."""
    
    print("🔧 VALIDATING TEST ENVIRONMENT")
    print("=" * 40)
    
    # Check Python path
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    
    # Check required modules
    required_modules = ['pandas', 'numpy', 'unittest', 'pytest']
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"✅ {module} available")
        except ImportError:
            print(f"❌ {module} missing")
            missing_modules.append(module)
    
    # Check test files exist
    test_files = [
        'tests/test_multi_timeframe_universe_state_manager.py',
        'tests/test_multi_timeframe_training_data_job_runner.py',
        'test_multi_timeframe_validation.py'
    ]
    
    missing_files = []
    for test_file in test_files:
        if os.path.exists(test_file):
            print(f"✅ {test_file} found")
        else:
            print(f"❌ {test_file} missing")
            missing_files.append(test_file)
    
    # Check source files
    src_files = [
        'src/state/universe_state_manager.py',
        'src/app/training_data_job_runner.py'
    ]
    
    for src_file in src_files:
        if os.path.exists(src_file):
            print(f"✅ {src_file} found")
        else:
            print(f"❌ {src_file} missing")
            missing_files.append(src_file)
    
    if missing_modules or missing_files:
        print(f"\n❌ Environment validation failed!")
        print(f"Missing modules: {missing_modules}")
        print(f"Missing files: {missing_files}")
        return False
    else:
        print(f"\n✅ Environment validation passed!")
        return True

if __name__ == "__main__":
    print("🚀 MULTI-TIMEFRAME TEST RUNNER")
    print("Testing the enhanced multi-timeframe training data generation functionality")
    print("")
    
    # Validate environment first
    if not validate_test_environment():
        print("🛑 Environment validation failed. Fix issues before running tests.")
        sys.exit(1)
    
    # Run all tests
    success = run_tests()
    
    # Run targeted categories for more detailed validation
    run_specific_test_categories()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)