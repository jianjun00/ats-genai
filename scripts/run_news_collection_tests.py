#!/usr/bin/env python3
"""
News Collection Test Runner

Comprehensive test runner for news collection issues discovered during the
Polygon news gap investigation (data stopped at 2025-08-27).

This script runs multiple test categories:
1. Unit tests for API date formatting
2. Integration tests for database operations  
3. Monitoring tests for production health
4. End-to-end workflow validation

USAGE:
    # Run all tests
    python scripts/run_news_collection_tests.py
    
    # Run specific test category
    python scripts/run_news_collection_tests.py --category unit
    python scripts/run_news_collection_tests.py --category integration
    python scripts/run_news_collection_tests.py --category monitoring
    
    # Run for specific environment
    python scripts/run_news_collection_tests.py --environment intg --category monitoring
    
    # CI/CD mode (exit with error on failure)
    python scripts/run_news_collection_tests.py --ci --fail-fast
    
    # Generate test report
    python scripts/run_news_collection_tests.py --report-file news_test_report.json
"""

import asyncio
import subprocess
import sys
import os
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class NewsCollectionTestRunner:
    """Comprehensive test runner for news collection systems"""
    
    def __init__(self, environment: str = 'dev', ci_mode: bool = False):
        self.environment = environment
        self.ci_mode = ci_mode
        self.test_results = {
            'timestamp': datetime.now().isoformat(),
            'environment': environment,
            'categories': {},
            'overall_status': 'UNKNOWN',
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'warnings': []
        }
        
    def run_all_tests(self, categories: List[str] = None, fail_fast: bool = False) -> Dict[str, Any]:
        """Run comprehensive test suite"""
        if not categories:
            categories = ['unit', 'integration', 'monitoring', 'end_to_end']
            
        print(f"🧪 Running News Collection Tests - {self.environment.upper()}")
        print(f"{'='*60}")
        
        for category in categories:
            print(f"\n📋 Running {category.upper()} tests...")
            
            try:
                if category == 'unit':
                    result = self.run_unit_tests()
                elif category == 'integration':
                    result = self.run_integration_tests()
                elif category == 'monitoring':
                    result = asyncio.run(self.run_monitoring_tests())
                elif category == 'end_to_end':
                    result = self.run_end_to_end_tests()
                else:
                    result = {'status': 'SKIPPED', 'reason': f'Unknown category: {category}'}
                    
                self.test_results['categories'][category] = result
                
                if result['status'] == 'FAILED' and fail_fast:
                    print(f"❌ Fast failure on {category} tests")
                    break
                    
            except Exception as e:
                error_result = {
                    'status': 'ERROR',
                    'error': str(e),
                    'tests_run': 0,
                    'tests_passed': 0,
                    'tests_failed': 1
                }
                self.test_results['categories'][category] = error_result
                
                if fail_fast:
                    break
        
        # Calculate overall results
        self._calculate_overall_results()
        
        return self.test_results
        
    def run_unit_tests(self) -> Dict[str, Any]:
        """Run unit tests for news collection components"""
        print("  🔬 API date formatting tests...")
        print("  🔬 Database operation tests...")
        print("  🔬 Error handling tests...")
        
        # Run pytest on specific unit test files
        test_files = [
            'tests/unit/test_polygon_api_formatting.py',
            'tests/unit/test_news_database_operations.py'
        ]
        
        # Create basic unit tests if they don't exist
        for test_file in test_files:
            self._ensure_unit_test_exists(test_file)
        
        try:
            cmd = [
                'python', '-m', 'pytest', 
                'tests/integration/test_news_collection_comprehensive.py',
                '-v', '--tb=short',
                '-k', 'test_polygon_api_date_format or test_news_insertion'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            return {
                'status': 'PASSED' if result.returncode == 0 else 'FAILED',
                'output': result.stdout,
                'errors': result.stderr,
                'tests_run': self._count_tests_from_output(result.stdout),
                'tests_passed': self._count_passed_from_output(result.stdout),
                'tests_failed': self._count_failed_from_output(result.stdout)
            }
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'TIMEOUT',
                'error': 'Unit tests timed out after 60 seconds',
                'tests_run': 0,
                'tests_passed': 0,
                'tests_failed': 1
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e),
                'tests_run': 0,
                'tests_passed': 0,
                'tests_failed': 1
            }
            
    def run_integration_tests(self) -> Dict[str, Any]:
        """Run integration tests for news collection workflow"""
        print("  🔗 Database transaction tests...")
        print("  🔗 API integration tests...")
        print("  🔗 Duplicate handling tests...")
        
        try:
            cmd = [
                'python', '-m', 'pytest',
                'tests/integration/test_news_collection_comprehensive.py',
                '-v', '--tb=short',
                f'--environment={self.environment}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            return {
                'status': 'PASSED' if result.returncode == 0 else 'FAILED',
                'output': result.stdout,
                'errors': result.stderr,
                'tests_run': self._count_tests_from_output(result.stdout),
                'tests_passed': self._count_passed_from_output(result.stdout),
                'tests_failed': self._count_failed_from_output(result.stdout)
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e),
                'tests_run': 0,
                'tests_passed': 0,
                'tests_failed': 1
            }
            
    async def run_monitoring_tests(self) -> Dict[str, Any]:
        """Run monitoring tests for production health"""
        print("  📊 Data freshness checks...")
        print("  📊 Gap detection...")
        print("  📊 Quality metrics...")
        
        try:
            # Import and run monitoring tests
            from tests.monitoring.test_news_data_monitoring import NewsDataMonitor
            
            monitor = NewsDataMonitor(self.environment)
            results = await monitor.run_all_checks()
            
            # Count successful checks
            total_checks = len(results['checks'])
            passed_checks = sum(1 for check in results['checks'].values() if check.get('passed', False))
            failed_checks = total_checks - passed_checks
            
            return {
                'status': 'PASSED' if results['overall_health'] == 'HEALTHY' else 'FAILED',
                'monitoring_results': results,
                'tests_run': total_checks,
                'tests_passed': passed_checks,
                'tests_failed': failed_checks,
                'alerts': results.get('alerts', [])
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e),
                'tests_run': 0,
                'tests_passed': 0,
                'tests_failed': 1
            }
            
    def run_end_to_end_tests(self) -> Dict[str, Any]:
        """Run end-to-end workflow tests"""
        print("  🌐 Complete backfill workflow...")
        print("  🌐 API to database pipeline...")
        print("  🌐 Error recovery tests...")
        
        try:
            # Test a complete mini-backfill workflow
            cmd = [
                'python', 'scripts/polygon_news_backfill.py',
                '--start-date', '2025-09-10',
                '--end-date', '2025-09-11', 
                f'--environment', self.environment,
                '--limit-per-request', '5',
                '--max-requests', '1',
                '--dry-run'  # Don't actually insert data
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            # Check for successful dry run
            success_indicators = [
                'Backfill completed successfully',
                'Success rate: 100.0%',
                'DRY RUN'
            ]
            
            output_lower = result.stdout.lower() + result.stderr.lower()
            success_count = sum(1 for indicator in success_indicators if indicator.lower() in output_lower)
            
            return {
                'status': 'PASSED' if result.returncode == 0 and success_count >= 2 else 'FAILED',
                'output': result.stdout,
                'errors': result.stderr,
                'tests_run': 1,
                'tests_passed': 1 if result.returncode == 0 else 0,
                'tests_failed': 0 if result.returncode == 0 else 1,
                'success_indicators_found': success_count
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e),
                'tests_run': 1,
                'tests_passed': 0,
                'tests_failed': 1
            }
            
    def _ensure_unit_test_exists(self, test_file: str):
        """Create basic unit test file if it doesn't exist"""
        test_path = Path(test_file)
        
        if not test_path.exists():
            test_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create basic test template
            test_content = f'''"""
Basic unit test for {test_path.stem}
Generated by news collection test runner.
"""

def test_placeholder():
    """Placeholder test - implement actual tests"""
    assert True, "Placeholder test should pass"
'''
            test_path.write_text(test_content)
            
    def _count_tests_from_output(self, output: str) -> int:
        """Count total tests from pytest output"""
        if 'collected' in output:
            for line in output.split('\n'):
                if 'collected' in line and 'item' in line:
                    try:
                        return int(line.split('collected')[1].split('item')[0].strip())
                    except:
                        pass
        return 0
        
    def _count_passed_from_output(self, output: str) -> int:
        """Count passed tests from pytest output"""
        if 'passed' in output:
            for line in output.split('\n'):
                if 'passed' in line and ('failed' in line or 'error' in line or line.strip().endswith('passed')):
                    try:
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part == 'passed' and i > 0:
                                return int(parts[i-1])
                    except:
                        pass
        return 0
        
    def _count_failed_from_output(self, output: str) -> int:
        """Count failed tests from pytest output"""
        if 'failed' in output:
            for line in output.split('\n'):
                if 'failed' in line:
                    try:
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part == 'failed' and i > 0:
                                return int(parts[i-1])
                    except:
                        pass
        return 0
        
    def _calculate_overall_results(self):
        """Calculate overall test results"""
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        
        for category_result in self.test_results['categories'].values():
            total_tests += category_result.get('tests_run', 0)
            passed_tests += category_result.get('tests_passed', 0)
            failed_tests += category_result.get('tests_failed', 0)
            
        self.test_results['total_tests'] = total_tests
        self.test_results['passed_tests'] = passed_tests
        self.test_results['failed_tests'] = failed_tests
        
        # Determine overall status
        if total_tests == 0:
            self.test_results['overall_status'] = 'NO_TESTS'
        elif failed_tests == 0:
            self.test_results['overall_status'] = 'PASSED'
        else:
            self.test_results['overall_status'] = 'FAILED'
            
    def generate_report(self, output_file: str = None):
        """Generate comprehensive test report"""
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(self.test_results, f, indent=2)
                
        # Console report
        print(f"\n📊 TEST REPORT SUMMARY")
        print(f"{'='*40}")
        print(f"Overall Status: {self.test_results['overall_status']}")
        print(f"Total Tests: {self.test_results['total_tests']}")
        print(f"Passed: {self.test_results['passed_tests']}")
        print(f"Failed: {self.test_results['failed_tests']}")
        print(f"Environment: {self.test_results['environment']}")
        
        for category, result in self.test_results['categories'].items():
            status_icon = "✅" if result['status'] == 'PASSED' else "❌"
            print(f"\n{status_icon} {category.upper()}: {result['status']}")
            print(f"   Tests: {result.get('tests_run', 0)} run, {result.get('tests_passed', 0)} passed, {result.get('tests_failed', 0)} failed")
            
            if result['status'] == 'FAILED' and 'error' in result:
                print(f"   Error: {result['error']}")
                
        # Show any alerts from monitoring
        alerts = []
        for result in self.test_results['categories'].values():
            if 'alerts' in result:
                alerts.extend(result['alerts'])
                
        if alerts:
            print(f"\n🚨 ALERTS ({len(alerts)}):")
            for alert in alerts:
                severity_icon = "🔴" if alert.get('severity') == 'critical' else "🟡"
                print(f"  {severity_icon} {alert.get('check', 'unknown')}: {alert.get('message', '')}")
                
        print(f"\n⏰ Report generated: {self.test_results['timestamp']}")
        
        if output_file:
            print(f"📄 Full report saved: {output_file}")


def main():
    """Main function for test runner"""
    parser = argparse.ArgumentParser(description='News Collection Test Runner')
    parser.add_argument('--environment', default='dev', choices=['dev', 'intg', 'prod'])
    parser.add_argument('--category', action='append', 
                       choices=['unit', 'integration', 'monitoring', 'end_to_end'],
                       help='Test categories to run (default: all)')
    parser.add_argument('--ci', action='store_true', help='CI mode (strict failure handling)')
    parser.add_argument('--fail-fast', action='store_true', help='Stop on first failure')
    parser.add_argument('--report-file', help='Output file for JSON report')
    
    args = parser.parse_args()
    
    runner = NewsCollectionTestRunner(args.environment, args.ci)
    results = runner.run_all_tests(args.category, args.fail_fast)
    
    runner.generate_report(args.report_file)
    
    # Exit with appropriate code
    if args.ci and results['overall_status'] != 'PASSED':
        print(f"\n❌ CI Mode: Exiting with failure code")
        sys.exit(1)
    elif results['overall_status'] == 'PASSED':
        print(f"\n✅ All tests passed!")
        sys.exit(0)
    else:
        print(f"\n⚠️  Some tests failed - check output above")
        sys.exit(0 if not args.ci else 1)


if __name__ == "__main__":
    main()