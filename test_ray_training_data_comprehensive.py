#!/usr/bin/env python3
"""
Comprehensive Test Runner for Ray-Enhanced Training Data Generation

Runs all Ray training data tests and provides consolidated test results.
This serves as a single entry point for validating Ray enhancements.
"""

import subprocess
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_test_suite(test_file: str, description: str) -> bool:
    """Run a test suite and return success status."""
    logger.info(f"🧪 Running {description}...")
    logger.info(f"   Test file: {test_file}")
    
    try:
        # Run the test file directly with Python
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per test suite
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {description} - PASSED")
            if result.stdout:
                # Show key results
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'PASSED' in line or 'FAILED' in line or 'ERROR' in line:
                        logger.info(f"   {line.strip()}")
            return True
        else:
            logger.error(f"❌ {description} - FAILED")
            if result.stdout:
                logger.error("STDOUT:")
                logger.error(result.stdout)
            if result.stderr:
                logger.error("STDERR:")
                logger.error(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {description} - TIMEOUT (exceeded 5 minutes)")
        return False
    except Exception as e:
        logger.error(f"💥 {description} - ERROR: {e}")
        return False


def main():
    """Run comprehensive Ray training data test suite."""
    logger.info("🚀 Starting Comprehensive Ray Training Data Test Suite")
    logger.info("=" * 80)
    
    # Define test suites
    test_suites = [
        {
            'file': 'tests/unit/test_ray_parallel_training_data_callback.py',
            'description': 'Unit Tests - Ray Parallel Processing'
        },
        {
            'file': 'tests/integration/test_ray_training_data_integration.py', 
            'description': 'Integration Tests - End-to-End Workflows'
        },
        {
            'file': 'tests/performance/test_ray_training_data_performance.py',
            'description': 'Performance Tests - Throughput & Scalability'
        },
        {
            'file': 'tests/regression/test_ray_training_data_regression.py',
            'description': 'Regression Tests - Backward Compatibility'
        }
    ]
    
    # Track results
    results = []
    total_tests = len(test_suites)
    
    # Run each test suite
    for i, suite in enumerate(test_suites, 1):
        logger.info(f"\n📋 Test Suite {i}/{total_tests}: {suite['description']}")
        logger.info("-" * 60)
        
        # Check if test file exists
        test_path = Path(suite['file'])
        if not test_path.exists():
            logger.error(f"❌ Test file not found: {suite['file']}")
            results.append(False)
            continue
        
        # Run the test suite
        success = run_test_suite(suite['file'], suite['description'])
        results.append(success)
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("📊 TEST SUITE SUMMARY")
    logger.info("=" * 80)
    
    passed_count = sum(results)
    failed_count = total_tests - passed_count
    
    for i, (suite, success) in enumerate(zip(test_suites, results)):
        status = "✅ PASSED" if success else "❌ FAILED"
        logger.info(f"{i+1}. {suite['description']}: {status}")
    
    logger.info("-" * 80)
    logger.info(f"Total Tests: {total_tests}")
    logger.info(f"Passed: {passed_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info(f"Success Rate: {(passed_count/total_tests)*100:.1f}%")
    
    if failed_count == 0:
        logger.info("🎉 ALL RAY TRAINING DATA TESTS PASSED!")
        logger.info("✅ Ray enhancements are ready for production use")
        return True
    else:
        logger.error("🚨 SOME TESTS FAILED!")
        logger.error(f"❌ {failed_count} test suite(s) need attention before deployment")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)