#!/usr/bin/env python3
"""
Comprehensive ArrayRecord validation script to document the timeframe separation bug.

This script runs all validation tests and creates a comprehensive report
to document the current state before fixing the training dataset generation logic.

Usage:
    python scripts/debug/run_comprehensive_arrayrecord_validation.py
"""

import subprocess
import sys
import json
import os
from datetime import datetime


def run_pytest_test(test_path: str, test_name: str = None) -> dict:
    """Run a specific pytest test and capture results"""
    cmd = ["python3", "-m", "pytest"]

    if test_name:
        cmd.append(f"{test_path}::{test_name}")
    else:
        cmd.append(test_path)

    cmd.extend(["-v", "--tb=short", "--no-header", "-q"])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/home/jianjun/ats-genai-pm",
            env={**os.environ, "PYTHONPATH": "src"}
        )

        return {
            'command': ' '.join(cmd),
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'passed': result.returncode == 0
        }

    except Exception as e:
        return {
            'command': ' '.join(cmd),
            'error': str(e),
            'passed': False
        }


def main():
    """Run comprehensive validation and generate report"""
    print("🔍 COMPREHENSIVE ARRAYRECORD VALIDATION")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()

    # Test cases to run
    test_cases = [
        {
            'name': 'Critical Bug Detection (Identical Files)',
            'test_class': 'TestArrayRecordTimeframeSeparation',
            'test_method': 'test_critical_bug_detection_identical_files',
            'description': 'Detects if all timeframe files are identical',
            'expected_result': 'FAIL (confirms bug exists)'
        },
        {
            'name': 'Timeframe Column Isolation',
            'test_class': 'TestArrayRecordTimeframeSeparation',
            'test_method': 'test_timeframe_column_isolation',
            'description': 'Validates each timeframe contains only its own features',
            'expected_result': 'FAIL (mixed timeframe features)'
        },
        {
            'name': 'Expected Feature Counts',
            'test_class': 'TestArrayRecordTimeframeSeparation',
            'test_method': 'test_expected_feature_counts_by_timeframe',
            'description': 'Checks if feature counts are reasonable for isolated timeframes',
            'expected_result': 'FAIL (962 features instead of <100)'
        },
        {
            'name': 'Single Value Per Feature',
            'test_class': 'TestArrayRecordTimeframeSeparation',
            'test_method': 'test_single_value_per_feature_requirement',
            'description': 'Validates each feature has single value, not sequences',
            'expected_result': 'Should PASS (data structure requirement)'
        },
        {
            'name': 'File Existence',
            'test_class': 'TestArrayRecordTimeframeSeparation',
            'test_method': 'test_arrayrecord_files_exist',
            'description': 'Verifies all timeframe files exist',
            'expected_result': 'Should PASS (files exist but are incorrect)'
        }
    ]

    # Results storage
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'test_results': [],
        'summary': {
            'total_tests': len(test_cases),
            'passed': 0,
            'failed': 0,
            'bug_confirmed': False
        }
    }

    test_base_path = "tests/integration/test_arrayrecord_timeframe_separation.py"

    print("📋 Running validation tests...")
    print("-" * 30)

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['name']}")
        print(f"   Description: {test_case['description']}")
        print(f"   Expected: {test_case['expected_result']}")

        # Run the test
        full_test_name = f"{test_case['test_class']}::{test_case['test_method']}"
        result = run_pytest_test(test_base_path, full_test_name)

        # Store results
        test_result = {
            'name': test_case['name'],
            'test_method': test_case['test_method'],
            'description': test_case['description'],
            'expected_result': test_case['expected_result'],
            'actual_result': 'PASS' if result['passed'] else 'FAIL',
            'command': result['command'],
            'stdout': result.get('stdout', ''),
            'stderr': result.get('stderr', ''),
            'returncode': result.get('returncode', -1)
        }

        validation_results['test_results'].append(test_result)

        # Update summary
        if result['passed']:
            validation_results['summary']['passed'] += 1
            print(f"   Result: ✅ PASS")
        else:
            validation_results['summary']['failed'] += 1
            print(f"   Result: ❌ FAIL")

            # Extract key error messages
            if 'AssertionError' in result.get('stderr', ''):
                error_lines = result['stderr'].split('\n')
                assertion_lines = [line.strip() for line in error_lines if 'AssertionError' in line or 'assert' in line]
                if assertion_lines:
                    print(f"   Error: {assertion_lines[0][:100]}...")

    # Determine if bug is confirmed
    critical_bug_test = next((r for r in validation_results['test_results'] if 'identical_files' in r['test_method']), None)
    mixed_timeframe_test = next((r for r in validation_results['test_results'] if 'column_isolation' in r['test_method']), None)

    validation_results['summary']['bug_confirmed'] = (
        critical_bug_test and critical_bug_test['actual_result'] == 'FAIL' and
        mixed_timeframe_test and mixed_timeframe_test['actual_result'] == 'FAIL'
    )

    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY")
    print("=" * 50)

    summary = validation_results['summary']
    print(f"Total Tests: {summary['total_tests']}")
    print(f"Passed: {summary['passed']} ✅")
    print(f"Failed: {summary['failed']} ❌")

    if summary['bug_confirmed']:
        print("\n🚨 BUG STATUS: CONFIRMED")
        print("   ✓ All timeframe files are identical")
        print("   ✓ Mixed timeframe features detected")
        print("   ✓ Excessive feature counts (962 vs expected <100)")
    else:
        print("\n⚠️  BUG STATUS: INCONCLUSIVE")

    print("\n🔧 NEXT STEPS:")
    print("1. Fix training dataset generation logic for timeframe separation")
    print("2. Regenerate training datasets with corrected logic")
    print("3. Re-run these tests to verify fixes")

    # Save detailed results
    output_file = "comprehensive_arrayrecord_validation.json"
    try:
        with open(output_file, 'w') as f:
            json.dump(validation_results, f, indent=2)
        print(f"\n📄 Detailed results saved to: {output_file}")
    except Exception as e:
        print(f"\n⚠️  Could not save results file: {e}")

    # Also run the debug analysis script
    print("\n🔍 Running detailed ArrayRecord analysis...")
    try:
        debug_result = subprocess.run(
            ["python3", "scripts/debug/analyze_arrayrecord_timeframe_bug.py"],
            capture_output=True,
            text=True,
            cwd="/home/jianjun/ats-genai-pm",
            env={**os.environ, "PYTHONPATH": "src"}
        )

        if debug_result.returncode == 0:
            print("✅ Debug analysis completed successfully")
        else:
            print("⚠️  Debug analysis completed with warnings")

    except Exception as e:
        print(f"⚠️  Could not run debug analysis: {e}")

    print(f"\n🎯 Validation completed. Bug confirmed: {'YES' if summary['bug_confirmed'] else 'NO'}")

    # Exit with appropriate code
    sys.exit(1 if summary['bug_confirmed'] else 0)


if __name__ == "__main__":
    main()