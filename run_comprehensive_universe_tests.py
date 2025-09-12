#!/usr/bin/env python3
"""
Run Comprehensive Universe Analytics Tests
Executes all test suites using real stock examples and market data
"""

import subprocess
import sys
import os
from datetime import datetime

def run_test_suite(test_file, description):
    """Run a test suite and return results"""
    print(f"\n{'='*60}")
    print(f"🧪 {description}")
    print(f"📁 {test_file}")
    print("="*60)

    start_time = datetime.now()

    try:
        if 'playwright' in test_file:
            # Playwright tests need special handling
            result = subprocess.run([
                'python3', '-m', 'pytest', test_file, '-v', '--tb=short', '-s'
            ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')
        else:
            # Regular pytest
            result = subprocess.run([
                'python3', '-m', 'pytest', test_file, '-v', '--tb=short'
            ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

        duration = (datetime.now() - start_time).total_seconds()

        if result.returncode == 0:
            print("✅ PASSED")
            print(f"⏱️  Duration: {duration:.2f}s")
            if result.stdout:
                # Show key results
                lines = result.stdout.split('\n')
                for line in lines:
                    if '✅' in line or 'PASSED' in line or 'passed' in line:
                        print(f"   {line}")
        else:
            print("❌ FAILED")
            print(f"⏱️  Duration: {duration:.2f}s")
            if result.stdout:
                print("\nSTDOUT:")
                print(result.stdout)
            if result.stderr:
                print("\nSTDERR:")
                print(result.stderr)

        return result.returncode == 0, duration

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False, 0

def check_services_running():
    """Check if required services are running"""
    print("🔍 Checking required services...")

    # Check if analytics service is running on port 4000 (integration)
    try:
        result = subprocess.run([
            'curl', '-s', 'http://localhost:4000/health'
        ], capture_output=True, text=True, timeout=5)

        if result.returncode == 0:
            print("✅ Integration analytics service (port 4000) is running")
        else:
            print("❌ Integration analytics service (port 4000) not responding")
            print("   Run: python3 scripts/run_intg.py start --service analytics")
            return False
    except Exception as e:
        print(f"❌ Failed to check analytics service: {e}")
        return False

    # Check database connectivity
    try:
        result = subprocess.run([
            'python3', 'scripts/run_intg.py', 'query', '--query', 'SELECT 1'
        ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

        if result.returncode == 0:
            print("✅ Integration database is accessible")
        else:
            print("❌ Integration database not accessible")
            return False
    except Exception as e:
        print(f"❌ Failed to check database: {e}")
        return False

    return True

def main():
    """Run comprehensive test suite for Universe Analytics"""
    print("🚀 COMPREHENSIVE UNIVERSE ANALYTICS TEST SUITE")
    print("🎯 Testing real stock examples and market dynamics")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check prerequisites
    if not check_services_running():
        print("\n❌ Prerequisites not met. Please start required services.")
        return 1

    # Define test suites
    test_suites = [
        {
            'file': 'tests/unit/test_universe_business_logic.py',
            'description': 'Unit Tests - Business Logic & Market Dynamics',
            'category': 'unit'
        },
        {
            'file': 'tests/integration/test_universe_simple.py',
            'description': 'Integration Tests - Database & API Validation',
            'category': 'integration'
        },
        {
            'file': 'tests/browser_tests/test_universe_analytics_playwright.py',
            'description': 'Browser Tests - End-to-End UI Functionality',
            'category': 'e2e'
        }
    ]

    # Run all test suites
    results = []
    total_duration = 0

    for suite in test_suites:
        success, duration = run_test_suite(suite['file'], suite['description'])
        results.append({
            'name': suite['description'],
            'category': suite['category'],
            'success': success,
            'duration': duration
        })
        total_duration += duration

    # Print summary
    print(f"\n{'='*60}")
    print("📊 TEST SUITE SUMMARY")
    print("="*60)

    passed_count = sum(1 for r in results if r['success'])
    total_count = len(results)

    for result in results:
        status = "✅ PASSED" if result['success'] else "❌ FAILED"
        print(f"{status} | {result['category'].upper():<12} | {result['duration']:>6.2f}s | {result['name']}")

    print("-"*60)
    print(f"📈 Overall Results: {passed_count}/{total_count} test suites passed")
    print(f"⏱️  Total Duration: {total_duration:.2f}s")

    if passed_count == total_count:
        print("\n🎉 ALL TESTS PASSED! Universe Analytics is fully validated.")
        print("\n🔍 Test Coverage Summary:")
        print("   ✅ Real stock examples (AAPL, TSLA, PTON, BYND, SMCI)")
        print("   ✅ Market dynamics (COVID impact, AI boom, hype cycles)")
        print("   ✅ Business logic (volume thresholds, IPO dates, exits)")
        print("   ✅ Database integrity (A-Z coverage, 665+ active members)")
        print("   ✅ API functionality (universe list, member retrieval)")
        print("   ✅ UI functionality (selection, filtering, display)")
        print("   ✅ Historical tracking (entry/exit patterns)")
        print("   ✅ Error handling (invalid inputs, edge cases)")
        print("\n🚀 Universe Analytics is production-ready!")
        return 0
    else:
        print(f"\n❌ {total_count - passed_count} test suite(s) failed. Review output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())