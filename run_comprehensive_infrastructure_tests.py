#!/usr/bin/env python3
"""
Comprehensive test runner for training data generation infrastructure.

This script runs all the comprehensive tests created to detect and prevent 
regressions of the issues found during AAPL training data generation debugging.

Usage:
    python run_comprehensive_infrastructure_tests.py
    python run_comprehensive_infrastructure_tests.py --fast     # Skip slow tests
    python run_comprehensive_infrastructure_tests.py --category imports  # Run specific category
"""

import argparse
import sys
import subprocess
import time
from pathlib import Path


def run_test_suite(test_file, category_name, fast_mode=False):
    """Run a specific test suite and return results."""
    
    print(f"\n🔍 Running {category_name} tests...")
    print(f"   Test file: {test_file}")
    
    start_time = time.time()
    
    # Run pytest with appropriate flags
    cmd = [
        'python3', '-m', 'pytest', 
        str(test_file),
        '-v', 
        '--tb=short',
        '--no-header'
    ]
    
    if fast_mode:
        cmd.extend(['-x'])  # Stop on first failure in fast mode
        
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent,
        env={'PYTHONPATH': 'src'}
    )
    
    duration = time.time() - start_time
    
    if result.returncode == 0:
        print(f"✅ {category_name} tests PASSED ({duration:.1f}s)")
        return True, duration, result.stdout
    else:
        print(f"❌ {category_name} tests FAILED ({duration:.1f}s)")
        print(f"   Error output:")
        for line in result.stderr.split('\n'):
            if line.strip():
                print(f"     {line}")
        return False, duration, result.stderr
        
def main():
    """Main test runner."""
    
    parser = argparse.ArgumentParser(
        description="Run comprehensive infrastructure tests for training data generation"
    )
    parser.add_argument(
        '--fast', 
        action='store_true',
        help="Run in fast mode (skip slow tests, stop on first failure)"
    )
    parser.add_argument(
        '--category',
        choices=['imports', 'firstrate', 'database', 'config', 'data', 'performance', 'all'],
        default='all',
        help="Run specific test category"
    )
    parser.add_argument(
        '--list-issues',
        action='store_true',
        help="List all issues that these tests cover"
    )
    
    args = parser.parse_args()
    
    if args.list_issues:
        print_covered_issues()
        return 0
    
    # Define test suites
    test_suites = {
        'imports': {
            'file': 'tests/integration/test_training_data_infrastructure_comprehensive.py',
            'name': 'Core Infrastructure & Imports',
            'description': 'Tests import dependencies, FirstRate adapter, UniverseStateManager methods, feature extraction'
        },
        'database': {
            'file': 'tests/integration/test_database_constraints_regression.py', 
            'name': 'Database Constraints & UUID System',
            'description': 'Tests database constraints, UUID deduplication, concurrent run handling'
        },
        'config': {
            'file': 'tests/integration/test_configuration_enum_regression.py',
            'name': 'Configuration & Enum Usage',
            'description': 'Tests enum usage, Gin config, environment variables, undefined variable detection'
        },
        'data': {
            'file': 'tests/integration/test_data_validation_comprehensive.py',
            'name': 'Data Validation & File System',
            'description': 'Tests parquet files, OHLCV validation, volume preservation, path resolution'
        },
        'performance': {
            'file': 'tests/integration/test_performance_regression_endtoend.py',
            'name': 'Performance & End-to-End Integration', 
            'description': 'Tests performance, memory usage, regression detection, complete pipeline'
        }
    }
    
    # Determine which tests to run
    if args.category == 'all':
        tests_to_run = test_suites
    else:
        tests_to_run = {args.category: test_suites[args.category]}
    
    print("🚀 Training Data Generation Infrastructure Test Suite")
    print("=" * 60)
    print(f"Running {len(tests_to_run)} test suite(s)")
    if args.fast:
        print("🏃 Fast mode enabled - stopping on first failure")
    print()
    
    # Run tests
    results = {}
    total_duration = 0
    all_passed = True
    
    for category, suite_info in tests_to_run.items():
        test_file = Path(suite_info['file'])
        
        if not test_file.exists():
            print(f"⚠️  Test file not found: {test_file}")
            results[category] = (False, 0, f"File not found: {test_file}")
            all_passed = False
            continue
            
        passed, duration, output = run_test_suite(
            test_file, 
            suite_info['name'],
            args.fast
        )
        
        results[category] = (passed, duration, output)
        total_duration += duration
        
        if not passed:
            all_passed = False
            if args.fast:
                print("🛑 Fast mode - stopping on first failure")
                break
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed_count = sum(1 for passed, _, _ in results.values() if passed)
    total_count = len(results)
    
    for category, (passed, duration, output) in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        suite_name = test_suites[category]['name']
        print(f"{status} {suite_name:<45} ({duration:5.1f}s)")
    
    print("-" * 60)
    print(f"Total: {passed_count}/{total_count} test suites passed")
    print(f"Duration: {total_duration:.1f} seconds")
    
    if all_passed:
        print("\n🎉 All infrastructure tests PASSED!")
        print("   Training data generation infrastructure is healthy.")
        return 0
    else:
        print(f"\n💥 {total_count - passed_count} test suite(s) FAILED!")
        print("   Training data generation infrastructure has issues.")
        
        # Show first failure details
        for category, (passed, duration, output) in results.items():
            if not passed:
                print(f"\n❌ First failure in {test_suites[category]['name']}:")
                print("   " + "\n   ".join(output.split('\n')[:10]))  # First 10 lines
                break
        
        return 1


def print_covered_issues():
    """Print all issues covered by these tests."""
    
    print("🐛 Issues Covered by Comprehensive Infrastructure Tests")
    print("=" * 60)
    
    issues = {
        "Import Dependencies": [
            "Missing DailyPriceMarketDataManager module",
            "Missing FileBasedMinuteMarketDataManager module", 
            "Broken import paths and module resolution",
            "PYTHONPATH configuration issues"
        ],
        "Data Processing": [
            "FirstRate adapter using wrong file structure (complete.parquet vs monthly files)",
            "Volume data loss in get_minute_ohlc_batch method",
            "Dict/DataFrame compatibility issue in universe_state_builder",
            "None volume values causing float() conversion errors"
        ],
        "Database Constraints": [
            "Unique constraint missing universe_state_interval_id",
            "Duplicate key violations on concurrent runs",
            "UUID deduplication system failures",
            "Foreign key relationship validation"
        ],
        "Configuration Issues": [
            "StorageBackend enum usage (StorageBackend.FILE vs 'file')",
            "Undefined variables (enable_run_isolation, run_context)",
            "Gin configuration loading and validation",
            "Environment variable resolution"
        ],
        "Missing Methods": [
            "UniverseStateManager missing get_lead_prices method",
            "UniverseStateManager missing get_lagged_signals method", 
            "Method signature validation and return types"
        ],
        "Data Validation": [
            "OHLC data consistency rules",
            "Volume data type preservation",
            "Parquet file structure validation",
            "File path resolution and naming conventions"
        ],
        "Performance & Regression": [
            "Memory leaks during data processing",
            "Import performance degradation",
            "Feature extraction performance",
            "End-to-end pipeline integration",
            "Regression detection for all fixed issues"
        ],
        "Error Handling": [
            "Missing parquet file handling",
            "Malformed data graceful degradation", 
            "API timeout and connection failures",
            "Git merge conflict detection"
        ]
    }
    
    for category, issue_list in issues.items():
        print(f"\n📂 {category}:")
        for issue in issue_list:
            print(f"   • {issue}")
    
    print(f"\nTotal: {sum(len(issues) for issues in issues.values())} specific issues covered")
    print("\n✨ These tests prevent regressions and ensure infrastructure stability.")


if __name__ == "__main__":
    sys.exit(main())