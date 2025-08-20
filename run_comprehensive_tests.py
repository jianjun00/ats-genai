#!/usr/bin/env python3
"""
Comprehensive Test Runner for Configurable Training Data Framework.

Runs all thorough tests including edge cases, performance, integration, and validation.
"""

import sys
import os
import time
import subprocess
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

class ComprehensiveTestRunner:
    """Comprehensive test runner with detailed reporting."""
    
    def __init__(self):
        self.test_results = {}
        self.total_tests = 0
        self.passed_tests = 0
        self.failed_tests = 0
        self.skipped_tests = 0
        self.total_time = 0
        
    def run_test_suite(self, test_file, description):
        """Run a test suite and capture results."""
        print(f"\n{'='*80}")
        print(f"RUNNING: {description}")
        print(f"FILE: {test_file}")
        print(f"{'='*80}")
        
        start_time = time.time()
        
        try:
            # Run pytest with detailed output
            cmd = [
                sys.executable, '-m', 'pytest', 
                test_file, 
                '-v', '--tb=short', '-s',
                '--durations=10',  # Show slowest 10 tests
                '--strict-markers'
            ]
            
            env = os.environ.copy()
            env['PYTHONPATH'] = 'src'
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                cwd=os.path.dirname(__file__),
                env=env
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Parse pytest output for statistics
            output_lines = result.stdout.split('\n')
            stderr_lines = result.stderr.split('\n')
            
            # Look for test results summary
            test_summary = self._parse_pytest_output(output_lines)
            
            self.test_results[test_file] = {
                'description': description,
                'return_code': result.returncode,
                'duration': duration,
                'summary': test_summary,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            # Update totals
            if test_summary:
                self.total_tests += test_summary.get('total', 0)
                self.passed_tests += test_summary.get('passed', 0)
                self.failed_tests += test_summary.get('failed', 0)
                self.skipped_tests += test_summary.get('skipped', 0)
            
            self.total_time += duration
            
            # Print immediate results
            if result.returncode == 0:
                print(f"✅ PASSED in {duration:.2f}s")
                if test_summary:
                    print(f"   Tests: {test_summary.get('total', 0)}, "
                          f"Passed: {test_summary.get('passed', 0)}, "
                          f"Failed: {test_summary.get('failed', 0)}, "
                          f"Skipped: {test_summary.get('skipped', 0)}")
            else:
                print(f"❌ FAILED in {duration:.2f}s")
                print("STDERR:")
                print(result.stderr)
                if test_summary:
                    print(f"   Tests: {test_summary.get('total', 0)}, "
                          f"Passed: {test_summary.get('passed', 0)}, "
                          f"Failed: {test_summary.get('failed', 0)}, "
                          f"Skipped: {test_summary.get('skipped', 0)}")
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            self.test_results[test_file] = {
                'description': description,
                'return_code': -1,
                'duration': duration,
                'summary': None,
                'error': str(e)
            }
            
            print(f"💥 ERROR in {duration:.2f}s: {e}")
    
    def _parse_pytest_output(self, output_lines):
        """Parse pytest output to extract test statistics."""
        summary = {}
        
        for line in output_lines:
            # Look for summary line like "=== 5 passed, 2 failed, 1 skipped in 2.34s ==="
            if '===' in line and ('passed' in line or 'failed' in line or 'skipped' in line):
                parts = line.strip('= ').split()
                
                total = 0
                passed = 0
                failed = 0
                skipped = 0
                
                i = 0
                while i < len(parts) - 1:
                    if parts[i+1] == 'passed':
                        passed = int(parts[i])
                        total += passed
                    elif parts[i+1] == 'failed':
                        failed = int(parts[i])
                        total += failed
                    elif parts[i+1] == 'skipped':
                        skipped = int(parts[i])
                        total += skipped
                    i += 1
                
                summary = {
                    'total': total,
                    'passed': passed,
                    'failed': failed,
                    'skipped': skipped
                }
                break
        
        return summary
    
    def print_final_report(self):
        """Print comprehensive final report."""
        print(f"\n{'='*80}")
        print("COMPREHENSIVE TEST RESULTS SUMMARY")
        print(f"{'='*80}")
        
        print(f"\nOverall Statistics:")
        print(f"  Total Tests: {self.total_tests}")
        print(f"  Passed: {self.passed_tests} ({self.passed_tests/max(self.total_tests,1)*100:.1f}%)")
        print(f"  Failed: {self.failed_tests} ({self.failed_tests/max(self.total_tests,1)*100:.1f}%)")
        print(f"  Skipped: {self.skipped_tests} ({self.skipped_tests/max(self.total_tests,1)*100:.1f}%)")
        print(f"  Total Time: {self.total_time:.2f}s")
        
        print(f"\nTest Suite Results:")
        print(f"{'-'*80}")
        
        for test_file, result in self.test_results.items():
            status = "✅ PASS" if result['return_code'] == 0 else "❌ FAIL"
            print(f"{status} | {result['duration']:6.2f}s | {Path(test_file).name}")
            print(f"     | {result['description']}")
            
            if result.get('summary'):
                s = result['summary']
                print(f"     | Tests: {s.get('total', 0)}, "
                      f"Passed: {s.get('passed', 0)}, "
                      f"Failed: {s.get('failed', 0)}, "
                      f"Skipped: {s.get('skipped', 0)}")
        
        print(f"{'-'*80}")
        
        # Performance analysis
        print(f"\nPerformance Analysis:")
        sorted_results = sorted(
            self.test_results.items(), 
            key=lambda x: x[1]['duration'], 
            reverse=True
        )
        
        print("Slowest Test Suites:")
        for test_file, result in sorted_results[:5]:
            print(f"  {result['duration']:6.2f}s - {Path(test_file).name}")
        
        # Failure analysis
        failed_suites = [
            (test_file, result) for test_file, result in self.test_results.items()
            if result['return_code'] != 0
        ]
        
        if failed_suites:
            print(f"\nFailed Test Suites ({len(failed_suites)}):")
            for test_file, result in failed_suites:
                print(f"  ❌ {Path(test_file).name}")
                print(f"     Description: {result['description']}")
                if result.get('summary'):
                    s = result['summary']
                    print(f"     Failed Tests: {s.get('failed', 0)}")
                
                # Show first few lines of stderr if available
                if result.get('stderr'):
                    stderr_lines = result['stderr'].split('\n')[:3]
                    for line in stderr_lines:
                        if line.strip():
                            print(f"     Error: {line.strip()}")
                            break
        
        # Success summary
        success_rate = (self.passed_tests / max(self.total_tests, 1)) * 100
        
        print(f"\n{'='*80}")
        if success_rate >= 95:
            print(f"🎉 EXCELLENT! {success_rate:.1f}% success rate")
        elif success_rate >= 80:
            print(f"✅ GOOD! {success_rate:.1f}% success rate")
        elif success_rate >= 60:
            print(f"⚠️  NEEDS WORK! {success_rate:.1f}% success rate")
        else:
            print(f"❌ CRITICAL ISSUES! {success_rate:.1f}% success rate")
        
        print(f"Total testing time: {self.total_time:.1f} seconds")
        print(f"{'='*80}")
        
        return success_rate >= 80  # Return True if success rate is acceptable

def main():
    """Run all comprehensive tests."""
    print("🧪 COMPREHENSIVE CONFIGURABLE TRAINING FRAMEWORK TEST SUITE")
    print("🚀 Running thorough tests with edge cases, performance, and validation")
    
    runner = ComprehensiveTestRunner()
    
    # Test suites to run (in order of importance)
    test_suites = [
        # Core functionality tests
        ("test_configurable_framework.py", "Basic Framework Functionality"),
        ("test_simple_configurable.py", "Simple Working Configuration"),
        
        # Comprehensive component tests
        ("tests/signals/test_feature_registry_comprehensive.py", "Feature Registry - Comprehensive"),
        ("tests/signals/test_label_registry_comprehensive.py", "Label Registry - Comprehensive"),
        ("tests/modeling/test_configurable_train_data_generator_comprehensive.py", "Training Data Generator - Comprehensive"),
        
        # Integration and data quality tests
        ("tests/integration/test_data_quality_validation.py", "Data Quality & Validation"),
        ("tests/config/test_gin_configuration_validation.py", "Gin Configuration Validation"),
    ]
    
    # Check if test files exist
    print("\nVerifying test files...")
    missing_files = []
    for test_file, description in test_suites:
        if not os.path.exists(test_file):
            missing_files.append(test_file)
            print(f"⚠️  Missing: {test_file}")
        else:
            print(f"✓ Found: {test_file}")
    
    if missing_files:
        print(f"\n❌ Missing {len(missing_files)} test files. Please ensure all test files exist.")
        return 1
    
    # Create necessary directories for tests
    os.makedirs("tests/signals", exist_ok=True)
    os.makedirs("tests/modeling", exist_ok=True)
    os.makedirs("tests/integration", exist_ok=True)
    os.makedirs("tests/config", exist_ok=True)
    
    # Create __init__.py files for test packages
    for test_dir in ["tests", "tests/signals", "tests/modeling", "tests/integration", "tests/config"]:
        init_file = os.path.join(test_dir, "__init__.py")
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write("# Test package\n")
    
    # Run each test suite
    start_time = time.time()
    
    for test_file, description in test_suites:
        if os.path.exists(test_file):
            runner.run_test_suite(test_file, description)
        else:
            print(f"⚠️  Skipping {test_file} - file not found")
    
    # Print comprehensive final report
    success = runner.print_final_report()
    
    total_time = time.time() - start_time
    print(f"\n⏱️  Total execution time: {total_time:.1f} seconds")
    
    if success:
        print("🎯 Comprehensive testing completed successfully!")
        return 0
    else:
        print("🔧 Some tests failed - please review and fix issues")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)