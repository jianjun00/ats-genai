#!/usr/bin/env python3
"""
Comprehensive Earnings Quality Test Suite

Main test runner for all earnings data quality tests.
Provides unified test execution and reporting.
"""

import pytest
import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def run_earnings_quality_tests():
    """Run all earnings quality tests with detailed reporting"""

    # Test files to run
    test_files = [
        'tests/events/test_eps_extraction.py',
        'tests/events/test_earnings_quality_monitor.py',
        'tests/events/test_historical_backfill.py',
        'tests/events/test_database_validation.py',
        'tests/events/test_earnings_pipeline_e2e.py'
    ]

    # Verify test files exist
    missing_files = []
    for test_file in test_files:
        if not Path(test_file).exists():
            missing_files.append(test_file)

    if missing_files:
        print("❌ Missing test files:")
        for file in missing_files:
            print(f"   - {file}")
        return False

    print("🧪 Running Comprehensive Earnings Quality Test Suite")
    print("=" * 60)

    # Run tests with verbose output
    pytest_args = [
        '-v',                    # Verbose output
        '--tb=short',           # Short traceback format
        '--color=yes',          # Colored output
        '-x',                   # Stop on first failure
        '--durations=10',       # Show 10 slowest tests
    ] + test_files

    # Run the tests
    exit_code = pytest.main(pytest_args)

    if exit_code == 0:
        print("\n✅ All earnings quality tests passed!")
        print("🎯 Ready for production deployment")
        return True
    else:
        print(f"\n❌ Tests failed with exit code: {exit_code}")
        return False

def run_specific_test_category(category: str):
    """Run tests for a specific category"""

    category_map = {
        'extraction': 'tests/events/test_eps_extraction.py',
        'quality': 'tests/events/test_earnings_quality_monitor.py',
        'backfill': 'tests/events/test_historical_backfill.py',
        'database': 'tests/events/test_database_validation.py',
        'e2e': 'tests/events/test_earnings_pipeline_e2e.py'
    }

    if category not in category_map:
        print(f"❌ Unknown category: {category}")
        print(f"Available categories: {list(category_map.keys())}")
        return False

    test_file = category_map[category]
    print(f"🧪 Running {category} tests: {test_file}")

    exit_code = pytest.main(['-v', '--tb=short', test_file])
    return exit_code == 0

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Earnings Quality Test Suite")
    parser.add_argument('--category', type=str, help='Run specific test category')
    parser.add_argument('--list-categories', action='store_true', help='List available test categories')

    args = parser.parse_args()

    if args.list_categories:
        print("Available test categories:")
        print("  - extraction: EPS extraction logic tests")
        print("  - quality: Quality monitoring tests")
        print("  - backfill: Historical backfill tests")
        print("  - database: Database validation tests")
        print("  - e2e: End-to-end pipeline tests")
        sys.exit(0)

    if args.category:
        success = run_specific_test_category(args.category)
    else:
        success = run_earnings_quality_tests()

    sys.exit(0 if success else 1)