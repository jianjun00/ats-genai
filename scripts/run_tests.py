#!/usr/bin/env python3
"""
Comprehensive test runner for the Multi-Modal News Prediction System
Provides organized test execution with coverage reporting and categorization.
"""

import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestRunner:
    """Organized test execution and reporting"""

    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.tests_dir = self.project_root / "tests"

        # Test categories and their locations
        self.test_categories = {
            'unit': [
                'events/test_economic_events_classifier.py',
                'training/test_multimodal_dataset_generator.py'
            ],
            'integration': [
                'integration/test_news_multimodal_integration.py'
            ],
            'performance': [
                'performance/test_multimodal_performance.py'
            ],
            'all_existing': [
                # Existing comprehensive tests
                'market_data/eod/',
                'market_data/agent/',
                'secmaster/',
                'signals/',
                'dao/',
                'core/',
                'config/'
            ]
        }

    def run_category(self, category: str, verbose: bool = False, coverage: bool = False) -> bool:
        """Run tests for a specific category"""
        if category not in self.test_categories:
            print(f"❌ Unknown test category: {category}")
            print(f"Available categories: {', '.join(self.test_categories.keys())}")
            return False

        test_paths = self.test_categories[category]

        if not test_paths:
            print(f"⚠️  No tests defined for category: {category}")
            return True

        # Build pytest command
        cmd = ['python', '-m', 'pytest']

        # Add verbosity
        if verbose:
            cmd.append('-v')
        else:
            cmd.append('-q')

        # Add coverage if requested
        if coverage:
            cmd.extend(['--cov=src', '--cov-report=html', '--cov-report=term'])

        # Add test markers for filtering
        if category == 'unit':
            cmd.extend(['-m', 'unit or not (integration or performance)'])
        elif category == 'integration':
            cmd.extend(['-m', 'integration'])
        elif category == 'performance':
            cmd.extend(['-m', 'performance'])

        # Add test paths
        for path in test_paths:
            full_path = self.tests_dir / path
            if full_path.exists():
                cmd.append(str(full_path))
            else:
                print(f"⚠️  Test path not found: {full_path}")

        # Additional pytest options
        cmd.extend([
            '--tb=short',           # Short traceback format
            '--maxfail=10',         # Stop after 10 failures
            '--timeout=300',        # 5 minute timeout per test
            '--disable-warnings'    # Reduce noise
        ])

        print(f"🚀 Running {category} tests...")
        print(f"Command: {' '.join(cmd)}")
        print("-" * 50)

        start_time = time.time()

        try:
            # Set environment variables for testing
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.project_root / 'src')
            env['TESTING'] = '1'

            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                env=env,
                capture_output=False,  # Show output in real-time
                text=True
            )

            end_time = time.time()
            duration = end_time - start_time

            if result.returncode == 0:
                print("-" * 50)
                print(f"✅ {category} tests passed ({duration:.1f}s)")
                return True
            else:
                print("-" * 50)
                print(f"❌ {category} tests failed ({duration:.1f}s)")
                return False

        except Exception as e:
            print(f"❌ Error running {category} tests: {e}")
            return False

    def run_specific_test(self, test_path: str, verbose: bool = False) -> bool:
        """Run a specific test file"""
        full_path = self.tests_dir / test_path

        if not full_path.exists():
            print(f"❌ Test file not found: {full_path}")
            return False

        cmd = ['python', '-m', 'pytest']

        if verbose:
            cmd.extend(['-v', '--tb=long'])
        else:
            cmd.extend(['-q', '--tb=short'])

        cmd.extend([
            str(full_path),
            '--timeout=300',
            '--disable-warnings'
        ])

        print(f"🎯 Running specific test: {test_path}")
        print("-" * 50)

        try:
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.project_root / 'src')
            env['TESTING'] = '1'

            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                env=env,
                text=True
            )

            return result.returncode == 0

        except Exception as e:
            print(f"❌ Error running test: {e}")
            return False

    def check_test_dependencies(self) -> bool:
        """Check if required test dependencies are available"""
        required_packages = [
            'pytest',
            'pytest-asyncio',
            'pytest-cov',
            'pytest-timeout',
            'psutil'
        ]

        missing_packages = []

        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
            except ImportError:
                missing_packages.append(package)

        if missing_packages:
            print("❌ Missing required test dependencies:")
            for package in missing_packages:
                print(f"   - {package}")
            print("\nInstall with: pip install " + " ".join(missing_packages))
            return False

        return True

    def validate_test_structure(self) -> bool:
        """Validate that test files are properly structured"""
        print("🔍 Validating test structure...")

        issues = []

        # Check that test files exist
        for category, paths in self.test_categories.items():
            if category == 'all_existing':
                continue  # Skip existing tests validation

            for path in paths:
                full_path = self.tests_dir / path
                if not full_path.exists():
                    issues.append(f"Missing test file: {path}")

        # Check for common test file issues
        for category, paths in self.test_categories.items():
            if category == 'all_existing':
                continue

            for path in paths:
                full_path = self.tests_dir / path
                if full_path.exists():
                    try:
                        with open(full_path, 'r') as f:
                            content = f.read()

                        # Check for basic test structure
                        if 'def test_' not in content and 'class Test' not in content:
                            issues.append(f"No test functions found in: {path}")

                        # Check for imports
                        if 'import pytest' not in content:
                            issues.append(f"Missing pytest import in: {path}")

                    except Exception as e:
                        issues.append(f"Error reading test file {path}: {e}")

        if issues:
            print("⚠️  Test structure issues found:")
            for issue in issues:
                print(f"   - {issue}")
            return False
        else:
            print("✅ Test structure validation passed")
            return True

    def generate_test_report(self, results: Dict[str, bool]) -> None:
        """Generate a comprehensive test report"""
        print("\n" + "="*60)
        print("📊 TEST EXECUTION SUMMARY")
        print("="*60)

        total_categories = len(results)
        passed_categories = sum(1 for success in results.values() if success)

        print(f"Total test categories: {total_categories}")
        print(f"Passed categories: {passed_categories}")
        print(f"Failed categories: {total_categories - passed_categories}")
        print(f"Success rate: {passed_categories/total_categories*100:.1f}%")

        print("\nDetailed results:")
        for category, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"  {category:15} {status}")

        if all(results.values()):
            print("\n🎉 All tests passed! Multi-modal system is ready.")
        else:
            print("\n⚠️  Some tests failed. Please review the failures above.")

        print("="*60)


def main():
    """Main test runner entry point"""
    parser = argparse.ArgumentParser(description="Run Multi-Modal System Tests")

    parser.add_argument(
        'category',
        nargs='?',
        choices=['unit', 'integration', 'performance', 'all_existing', 'all'],
        default='unit',
        help='Test category to run (default: unit)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )

    parser.add_argument(
        '--coverage', '-c',
        action='store_true',
        help='Enable coverage reporting'
    )

    parser.add_argument(
        '--specific', '-s',
        type=str,
        help='Run specific test file (relative to tests/)'
    )

    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate test structure only'
    )

    parser.add_argument(
        '--check-deps',
        action='store_true',
        help='Check test dependencies only'
    )

    args = parser.parse_args()

    # Determine project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    runner = TestRunner(str(project_root))

    # Check dependencies first
    if args.check_deps or args.validate:
        if not runner.check_test_dependencies():
            sys.exit(1)

        if args.check_deps:
            print("✅ All test dependencies are available")
            return

    # Validate test structure
    if args.validate:
        if runner.validate_test_structure():
            print("✅ Test structure is valid")
        else:
            sys.exit(1)
        return

    # Run specific test
    if args.specific:
        success = runner.run_specific_test(args.specific, args.verbose)
        sys.exit(0 if success else 1)

    # Run test categories
    if args.category == 'all':
        # Run all categories
        results = {}
        categories_to_run = ['unit', 'integration', 'performance']

        for category in categories_to_run:
            print(f"\n{'='*20} {category.upper()} TESTS {'='*20}")
            results[category] = runner.run_category(category, args.verbose, args.coverage)

        runner.generate_test_report(results)

        # Exit with failure if any category failed
        if not all(results.values()):
            sys.exit(1)
    else:
        # Run single category
        success = runner.run_category(args.category, args.verbose, args.coverage)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()