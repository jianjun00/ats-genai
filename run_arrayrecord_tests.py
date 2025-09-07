#!/usr/bin/env python3
"""
ArrayRecord Test Runner

Executes the comprehensive test suite for ArrayRecord integration fixes.
Provides organized test execution with clear reporting of results.

Based on test suite documented in PRD: ArrayRecord Training Data System (September 4, 2025)
"""

import subprocess
import sys
import time
from pathlib import Path


class TestRunner:
    def __init__(self):
        self.test_results = {}
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
        self.skipped_tests = 0

    def run_test_file(self, test_file, description):
        """Run a single test file and capture results."""
        print(f"\n{'='*80}")
        print(f"🧪 Running: {description}")
        print(f"   File: {test_file}")
        print(f"{'='*80}")

        start_time = time.time()

        try:
            result = subprocess.run([
                sys.executable, "-m", "pytest",
                str(test_file),
                "-v",
                "--tb=short",
                "--color=yes"
            ], capture_output=True, text=True, timeout=300)

            duration = time.time() - start_time

            self.test_results[test_file] = {
                'description': description,
                'returncode': result.returncode,
                'duration': duration,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

            # Parse pytest output for counts
            stdout_lines = result.stdout.split('\n')
            for line in stdout_lines:
                if 'passed' in line and ('failed' in line or 'error' in line or 'skipped' in line):
                    # Parse summary line like "2 passed, 1 skipped in 1.23s"
                    self._parse_test_counts(line)
                    break

            if result.returncode == 0:
                print(f"✅ PASSED ({duration:.2f}s)")
                return True
            else:
                print(f"❌ FAILED ({duration:.2f}s)")
                if result.stderr:
                    print("STDERR:")
                    print(result.stderr[:500])  # First 500 chars
                return False

        except subprocess.TimeoutExpired:
            print(f"⏰ TIMEOUT after 5 minutes")
            self.test_results[test_file] = {
                'description': description,
                'returncode': -1,
                'duration': 300,
                'stdout': '',
                'stderr': 'Test timed out'
            }
            return False
        except Exception as e:
            print(f"💥 ERROR: {e}")
            self.test_results[test_file] = {
                'description': description,
                'returncode': -2,
                'duration': 0,
                'stdout': '',
                'stderr': str(e)
            }
            return False

    def _parse_test_counts(self, summary_line):
        """Parse pytest summary line to extract test counts."""
        # Example: "2 passed, 1 skipped in 1.23s"
        words = summary_line.split()
        for i, word in enumerate(words):
            if word == 'passed' and i > 0:
                self.passed_tests += int(words[i-1])
                self.total_tests += int(words[i-1])
            elif word == 'failed' and i > 0:
                self.failed_tests += int(words[i-1])
                self.total_tests += int(words[i-1])
            elif word == 'skipped' and i > 0:
                self.skipped_tests += int(words[i-1])
                self.total_tests += int(words[i-1])

    def print_summary(self):
        """Print comprehensive test execution summary."""
        print(f"\n{'='*80}")
        print(f"🏁 TEST EXECUTION SUMMARY")
        print(f"{'='*80}")

        total_files = len(self.test_results)
        passed_files = sum(1 for r in self.test_results.values() if r['returncode'] == 0)
        failed_files = total_files - passed_files

        print(f"📊 Test Files: {passed_files}/{total_files} passed")
        print(f"📊 Individual Tests: {self.passed_tests} passed, {self.failed_tests} failed, {self.skipped_tests} skipped")

        print(f"\n📋 Detailed Results:")
        for test_file, result in self.test_results.items():
            status = "✅ PASS" if result['returncode'] == 0 else "❌ FAIL"
            duration = f"{result['duration']:.2f}s"
            print(f"  {status} {test_file.name:40} ({duration:>8}) - {result['description']}")

        if failed_files > 0:
            print(f"\n🔍 Failure Details:")
            for test_file, result in self.test_results.items():
                if result['returncode'] != 0:
                    print(f"\n❌ {test_file.name}:")
                    if result['stderr']:
                        print(f"   Error: {result['stderr'][:200]}...")
                    if 'FAILED' in result['stdout']:
                        # Extract failed test names
                        lines = result['stdout'].split('\n')
                        failed_lines = [line for line in lines if 'FAILED' in line]
                        for line in failed_lines[:3]:  # Show first 3 failures
                            print(f"   {line.strip()}")

    def run_all_tests(self):
        """Run all ArrayRecord integration tests."""

        print("🚀 ArrayRecord Integration Test Suite")
        print("=" * 80)
        print("Testing all critical fixes documented in PRD:")
        print("- ArrayRecord API compatibility")
        print("- JSON datetime serialization")
        print("- Database schema consistency")
        print("- API endpoint patterns")
        print("- TSLA data path resolution")
        print("- End-to-end EDA integration")
        print("=" * 80)

        test_files = [
            # Unit tests (fast)
            (Path("tests/unit/test_json_datetime_serialization.py"),
             "JSON Datetime Serialization Tests"),

            # Integration tests (require services)
            (Path("tests/integration/test_arrayrecord_api_compatibility.py"),
             "ArrayRecord API Compatibility Tests"),

            (Path("tests/integration/test_database_schema_consistency.py"),
             "Database Schema Consistency Tests"),

            (Path("tests/integration/test_api_endpoint_patterns.py"),
             "API Endpoint Pattern Tests"),

            (Path("tests/integration/test_tsla_data_path_resolution.py"),
             "TSLA Data Path Resolution Tests"),

            # End-to-end tests (full system)
            (Path("tests/integration/test_eda_arrayrecord_integration.py"),
             "End-to-End EDA ArrayRecord Integration Tests"),
        ]

        # Verify test files exist
        missing_files = []
        for test_file, description in test_files:
            if not test_file.exists():
                missing_files.append(str(test_file))

        if missing_files:
            print(f"❌ Missing test files:")
            for file in missing_files:
                print(f"   {file}")
            return False

        # Run tests in order
        all_passed = True
        for test_file, description in test_files:
            success = self.run_test_file(test_file, description)
            if not success:
                all_passed = False

        # Print comprehensive summary
        self.print_summary()

        # Final result
        if all_passed:
            print(f"\n🎉 ALL TESTS PASSED! ArrayRecord integration is working correctly.")
        else:
            print(f"\n⚠️ Some tests failed. Review the issues above.")

        return all_passed


def main():
    """Main test runner entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run ArrayRecord integration tests")
    parser.add_argument("--fast", action="store_true", help="Run only fast unit tests")
    parser.add_argument("--integration", action="store_true", help="Run only integration tests")
    parser.add_argument("--file", help="Run specific test file")
    args = parser.parse_args()

    runner = TestRunner()

    if args.file:
        # Run specific file
        test_file = Path(args.file)
        if not test_file.exists():
            print(f"❌ Test file not found: {test_file}")
            return 1

        success = runner.run_test_file(test_file, f"Manual test: {test_file.name}")
        return 0 if success else 1

    elif args.fast:
        # Run only unit tests
        test_file = Path("tests/unit/test_json_datetime_serialization.py")
        success = runner.run_test_file(test_file, "JSON Datetime Serialization Tests (Fast)")
        runner.print_summary()
        return 0 if success else 1

    elif args.integration:
        # Run only integration tests
        integration_tests = [
            (Path("tests/integration/test_arrayrecord_api_compatibility.py"),
             "ArrayRecord API Compatibility Tests"),
            (Path("tests/integration/test_database_schema_consistency.py"),
             "Database Schema Consistency Tests"),
            (Path("tests/integration/test_api_endpoint_patterns.py"),
             "API Endpoint Pattern Tests"),
        ]

        all_passed = True
        for test_file, description in integration_tests:
            if test_file.exists():
                success = runner.run_test_file(test_file, description)
                if not success:
                    all_passed = False

        runner.print_summary()
        return 0 if all_passed else 1

    else:
        # Run all tests
        success = runner.run_all_tests()
        return 0 if success else 1


if __name__ == "__main__":
    exit(main())