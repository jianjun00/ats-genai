#!/usr/bin/env python3
"""
Comprehensive test runner for hourly training data generation.

Runs all unit tests, integration tests, and end-to-end tests to verify
that the hourly training data generation works correctly.
"""

import sys
import os
import unittest
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

def run_unit_tests():
    """Run unit tests for hourly aggregation logic."""
    print("🧪 Running Unit Tests")
    print("=" * 50)

    # Import and run unit tests
    from test_hourly_aggregation import TestHourlyAggregation, TestHourlyDataFrameGeneration

    suite = unittest.TestSuite()

    # Add unit tests
    suite.addTest(unittest.makeSuite(TestHourlyAggregation))
    suite.addTest(unittest.makeSuite(TestHourlyDataFrameGeneration))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()

def run_integration_tests():
    """Run integration tests with FileBasedMinuteManager."""
    print("\n🔗 Running Integration Tests")
    print("=" * 50)

    from test_integration import TestHourlyTrainingDataIntegration, TestAsyncHourlyGeneration

    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestHourlyTrainingDataIntegration))
    suite.addTest(unittest.makeSuite(TestAsyncHourlyGeneration))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()

def run_universe_state_tests():
    """Run universe state builder integration tests."""
    print("\n🌟 Running Universe State Builder Tests")
    print("=" * 50)

    from test_universe_state_integration import TestUniverseStateBuilderIntegration

    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUniverseStateBuilderIntegration))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()

async def run_end_to_end_tests():
    """Run end-to-end tests with real test data."""
    print("\n🚀 Running End-to-End Tests with Real Data")
    print("=" * 50)

    from test_with_real_data import TestHourlyGenerationWithRealData

    # Set up test class
    TestHourlyGenerationWithRealData.setUpClass()

    try:
        test_instance = TestHourlyGenerationWithRealData()

        # Run async tests
        async_tests = [
            'test_end_to_end_with_real_minute_data',
            'test_multiple_symbols_real_data',
            'test_hourly_aggregation_accuracy'
        ]

        all_passed = True

        for test_name in async_tests:
            print(f"\n📋 Running {test_name}...")
            try:
                test_instance.setUp()
                await getattr(test_instance, test_name)()
                print(f"✅ {test_name} PASSED")
            except Exception as e:
                print(f"❌ {test_name} FAILED: {e}")
                all_passed = False

        # Run sync test
        print(f"\n📋 Running test_real_data_file_structure...")
        try:
            test_instance.setUp()
            test_instance.test_real_data_file_structure()
            print(f"✅ test_real_data_file_structure PASSED")
        except Exception as e:
            print(f"❌ test_real_data_file_structure FAILED: {e}")
            all_passed = False

        return all_passed

    finally:
        # Clean up
        TestHourlyGenerationWithRealData.tearDownClass()

def run_performance_validation():
    """Run performance validation tests."""
    print("\n⚡ Running Performance Validation")
    print("=" * 50)

    import time
    from test_with_real_data import TestHourlyGenerationWithRealData

    # Quick performance test
    TestHourlyGenerationWithRealData.setUpClass()

    try:
        test_instance = TestHourlyGenerationWithRealData()
        test_instance.setUp()

        # Time the aggregation process
        from storage.file_based_minute_manager import FileBasedMinuteManager

        async def performance_test():
            minute_manager = FileBasedMinuteManager(base_path=str(test_instance.minute_data_path))

            start_time = time.time()

            minute_data = await minute_manager.get_minute_data(
                symbol='AAPL',
                start_date=test_instance.config.start_date,
                end_date=test_instance.config.end_date
            )

            load_time = time.time() - start_time

            if minute_data is not None and not minute_data.empty:
                start_time = time.time()

                hourly_rows = test_instance.runner._aggregate_minutes_to_hourly(
                    minute_data, 'AAPL', universe_manager=None
                )

                aggregation_time = time.time() - start_time

                print(f"📊 Performance Results:")
                print(f"   Minute data points: {len(minute_data)}")
                print(f"   Hourly rows generated: {len(hourly_rows)}")
                print(f"   Data loading time: {load_time:.3f}s")
                print(f"   Aggregation time: {aggregation_time:.3f}s")
                print(f"   Total processing time: {load_time + aggregation_time:.3f}s")

                # Performance thresholds
                if load_time > 5.0:
                    print(f"⚠️  Data loading took {load_time:.3f}s (threshold: 5.0s)")
                    return False

                if aggregation_time > 2.0:
                    print(f"⚠️  Aggregation took {aggregation_time:.3f}s (threshold: 2.0s)")
                    return False

                print(f"✅ Performance validation PASSED")
                return True
            else:
                print(f"❌ No minute data available for performance test")
                return False

        return asyncio.run(performance_test())

    finally:
        TestHourlyGenerationWithRealData.tearDownClass()

def main():
    """Run all tests and provide comprehensive results."""
    print("🎯 Comprehensive Hourly Training Data Generation Test Suite")
    print("=" * 70)

    results = {
        'unit_tests': False,
        'integration_tests': False,
        'universe_state_tests': False,
        'end_to_end_tests': False,
        'performance_validation': False
    }

    try:
        # Run unit tests
        results['unit_tests'] = run_unit_tests()

        # Run integration tests
        results['integration_tests'] = run_integration_tests()

        # Run universe state tests
        results['universe_state_tests'] = run_universe_state_tests()

        # Run end-to-end tests
        results['end_to_end_tests'] = asyncio.run(run_end_to_end_tests())

        # Run performance validation
        results['performance_validation'] = run_performance_validation()

    except Exception as e:
        print(f"\n💥 Test execution failed: {e}")
        return False

    # Print summary
    print(f"\n📋 Test Results Summary")
    print("=" * 50)

    all_passed = True
    for test_category, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        test_name = test_category.replace('_', ' ').title()
        print(f"   {test_name:<25} {status}")
        if not passed:
            all_passed = False

    print(f"\n" + "=" * 50)

    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("\n✅ Hourly training data generation is working correctly:")
        print("   • Unit tests validate core aggregation logic")
        print("   • Integration tests verify FileBasedMinuteManager compatibility")
        print("   • Universe state tests confirm indicator integration")
        print("   • End-to-end tests validate complete pipeline with real data")
        print("   • Performance validation ensures acceptable speeds")
        print("\n🚀 Ready for production use!")
        return True
    else:
        print("❌ SOME TESTS FAILED!")
        print("\n⚠️  Please review failed tests before deploying to production.")
        print("   Failed test categories need to be fixed.")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)