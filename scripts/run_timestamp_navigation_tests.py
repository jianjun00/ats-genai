#!/usr/bin/env python3
"""
Test Runner for Timestamp-Based Multi-Timeframe Navigation

Runs comprehensive test suite in proper order:
1. Unit tests (mock/isolated testing)
2. Integration tests (API endpoint testing)  
3. API contract tests (contract compliance)
4. Performance tests (load and stress testing)
5. Playwright tests (end-to-end browser testing)

Usage:
    python scripts/run_timestamp_navigation_tests.py [--type all|unit|integration|contract|performance|playwright]
    
Examples:
    python scripts/run_timestamp_navigation_tests.py                    # Run all tests
    python scripts/run_timestamp_navigation_tests.py --type unit        # Run only unit tests  
    python scripts/run_timestamp_navigation_tests.py --type playwright  # Run only Playwright tests
"""

import sys
import os
import argparse
import subprocess
import time
import requests
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / 'src'))

class TimestampNavigationTestRunner:
    """Test runner for timestamp navigation system."""
    
    def __init__(self):
        self.base_url = "http://localhost:3001"
        self.project_root = project_root
        self.analytics_service_running = False
        
    def check_analytics_service(self):
        """Check if analytics service is running."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            self.analytics_service_running = response.status_code == 200
            if self.analytics_service_running:
                print("✅ Analytics service is running")
            else:
                print(f"⚠️ Analytics service responded with {response.status_code}")
        except requests.ConnectionError:
            self.analytics_service_running = False
            print("❌ Analytics service is not accessible")
        except Exception as e:
            self.analytics_service_running = False
            print(f"❌ Error checking analytics service: {e}")
        
        return self.analytics_service_running
    
    def start_analytics_service(self):
        """Start analytics service if not running."""
        if not self.analytics_service_running:
            print("🔧 Starting analytics service...")
            try:
                result = subprocess.run([
                    "python", "scripts/run_dev.py", "start", "--service", "analytics"
                ], cwd=self.project_root, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    print("✅ Analytics service started")
                    # Wait a bit for service to be ready
                    time.sleep(5)
                    return self.check_analytics_service()
                else:
                    print(f"❌ Failed to start analytics service: {result.stderr}")
                    return False
            except subprocess.TimeoutExpired:
                print("⏰ Timeout starting analytics service")
                return False
            except Exception as e:
                print(f"❌ Error starting analytics service: {e}")
                return False
        
        return True
    
    def run_unit_tests(self):
        """Run unit tests."""
        print("\n" + "="*60)
        print("🧪 RUNNING UNIT TESTS")
        print("="*60)
        
        test_file = self.project_root / "tests/unit/test_timestamp_based_navigation.py"
        
        if not test_file.exists():
            print(f"❌ Unit test file not found: {test_file}")
            return False
        
        try:
            result = subprocess.run([
                "python", "-m", "pytest", str(test_file), "-v", "--tb=short", "-s"
            ], cwd=self.project_root, timeout=300)  # 5 minute timeout
            
            success = result.returncode == 0
            if success:
                print("✅ Unit tests passed")
            else:
                print("❌ Unit tests failed")
            
            return success
            
        except subprocess.TimeoutExpired:
            print("⏰ Unit tests timed out")
            return False
        except Exception as e:
            print(f"❌ Error running unit tests: {e}")
            return False
    
    def run_integration_tests(self):
        """Run integration tests."""
        print("\n" + "="*60)
        print("🔗 RUNNING INTEGRATION TESTS")
        print("="*60)
        
        if not self.analytics_service_running:
            print("⚠️ Analytics service not running - integration tests may fail")
        
        test_file = self.project_root / "tests/integration/test_timestamp_navigation_integration.py"
        
        if not test_file.exists():
            print(f"❌ Integration test file not found: {test_file}")
            return False
        
        try:
            result = subprocess.run([
                "python", "-m", "pytest", str(test_file), "-v", "--tb=short", "-s"
            ], cwd=self.project_root, timeout=600)  # 10 minute timeout
            
            success = result.returncode == 0
            if success:
                print("✅ Integration tests passed")
            else:
                print("❌ Integration tests failed")
            
            return success
            
        except subprocess.TimeoutExpired:
            print("⏰ Integration tests timed out")
            return False
        except Exception as e:
            print(f"❌ Error running integration tests: {e}")
            return False
    
    def run_contract_tests(self):
        """Run API contract tests."""
        print("\n" + "="*60)
        print("📋 RUNNING API CONTRACT TESTS")
        print("="*60)
        
        if not self.analytics_service_running:
            print("⚠️ Analytics service not running - contract tests will fail")
            return False
        
        test_file = self.project_root / "tests/integration/test_api_contract_timestamp_synchronization.py"
        
        if not test_file.exists():
            print(f"❌ Contract test file not found: {test_file}")
            return False
        
        try:
            result = subprocess.run([
                "python", "-m", "pytest", str(test_file), "-v", "--tb=short", "-s"
            ], cwd=self.project_root, timeout=600)  # 10 minute timeout
            
            success = result.returncode == 0
            if success:
                print("✅ API contract tests passed")
            else:
                print("❌ API contract tests failed")
            
            return success
            
        except subprocess.TimeoutExpired:
            print("⏰ API contract tests timed out")
            return False
        except Exception as e:
            print(f"❌ Error running API contract tests: {e}")
            return False
    
    def run_performance_tests(self):
        """Run performance tests."""
        print("\n" + "="*60)
        print("⏱️ RUNNING PERFORMANCE TESTS")
        print("="*60)
        
        if not self.analytics_service_running:
            print("⚠️ Analytics service not running - performance tests will fail")
            return False
        
        test_file = self.project_root / "tests/performance/test_timestamp_navigation_performance.py"
        
        if not test_file.exists():
            print(f"❌ Performance test file not found: {test_file}")
            return False
        
        # Install required packages for performance testing
        try:
            subprocess.run([
                "pip", "install", "aiohttp", "psutil"
            ], capture_output=True, check=True)
        except subprocess.CalledProcessError:
            print("⚠️ Could not install performance test dependencies")
        
        try:
            result = subprocess.run([
                "python", "-m", "pytest", str(test_file), "-v", "--tb=short", "-s"
            ], cwd=self.project_root, timeout=900)  # 15 minute timeout
            
            success = result.returncode == 0
            if success:
                print("✅ Performance tests passed")
            else:
                print("❌ Performance tests failed")
            
            return success
            
        except subprocess.TimeoutExpired:
            print("⏰ Performance tests timed out")
            return False
        except Exception as e:
            print(f"❌ Error running performance tests: {e}")
            return False
    
    def run_playwright_tests(self):
        """Run Playwright end-to-end tests."""
        print("\n" + "="*60)
        print("🎭 RUNNING PLAYWRIGHT TESTS")
        print("="*60)
        
        if not self.analytics_service_running:
            print("⚠️ Analytics service not running - Playwright tests will fail")
            return False
        
        test_file = self.project_root / "tests/browser_tests/test_timestamp_navigation_playwright.py"
        
        if not test_file.exists():
            print(f"❌ Playwright test file not found: {test_file}")
            return False
        
        # Check if Playwright is installed
        try:
            subprocess.run(["playwright", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("🔧 Installing Playwright...")
            try:
                subprocess.run(["pip", "install", "playwright"], check=True)
                subprocess.run(["playwright", "install", "chromium"], check=True)
            except subprocess.CalledProcessError:
                print("❌ Failed to install Playwright")
                return False
        
        try:
            result = subprocess.run([
                "python", "-m", "pytest", str(test_file), "-v", "--tb=short", "-s"
            ], cwd=self.project_root, timeout=900)  # 15 minute timeout
            
            success = result.returncode == 0
            if success:
                print("✅ Playwright tests passed")
            else:
                print("❌ Playwright tests failed")
            
            return success
            
        except subprocess.TimeoutExpired:
            print("⏰ Playwright tests timed out")
            return False
        except Exception as e:
            print(f"❌ Error running Playwright tests: {e}")
            return False
    
    def run_all_tests(self):
        """Run all test types in proper order."""
        print("🚀 RUNNING COMPREHENSIVE TIMESTAMP NAVIGATION TEST SUITE")
        print("="*80)
        
        test_results = {}
        
        # 1. Unit tests (can run without service)
        print("📦 Phase 1: Unit Tests")
        test_results['unit'] = self.run_unit_tests()
        
        # 2. Service dependency check and startup
        print("\n📦 Phase 2: Service Setup")
        service_ready = self.check_analytics_service()
        if not service_ready:
            print("🔧 Attempting to start analytics service...")
            service_ready = self.start_analytics_service()
        
        if service_ready:
            # 3. Integration tests
            print("\n📦 Phase 3: Integration Tests")
            test_results['integration'] = self.run_integration_tests()
            
            # 4. API contract tests
            print("\n📦 Phase 4: API Contract Tests")
            test_results['contract'] = self.run_contract_tests()
            
            # 5. Performance tests
            print("\n📦 Phase 5: Performance Tests")
            test_results['performance'] = self.run_performance_tests()
            
            # 6. Playwright tests
            print("\n📦 Phase 6: End-to-End Tests")
            test_results['playwright'] = self.run_playwright_tests()
        
        else:
            print("❌ Cannot run service-dependent tests without analytics service")
            test_results.update({
                'integration': False,
                'contract': False,
                'performance': False,
                'playwright': False
            })
        
        # Final summary
        print("\n" + "="*80)
        print("📊 FINAL TEST RESULTS SUMMARY")
        print("="*80)
        
        total_tests = len(test_results)
        passed_tests = sum(1 for result in test_results.values() if result)
        
        for test_type, result in test_results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"  {test_type.upper():15} {status}")
        
        print(f"\n🎯 Overall Result: {passed_tests}/{total_tests} test suites passed")
        
        if passed_tests == total_tests:
            print("🎉 ALL TESTS PASSED! Timestamp navigation system is ready!")
            return True
        else:
            print("⚠️ Some tests failed. Review the results above.")
            return False

def main():
    """Main test runner entry point."""
    parser = argparse.ArgumentParser(description="Run timestamp navigation tests")
    parser.add_argument('--type', choices=['all', 'unit', 'integration', 'contract', 'performance', 'playwright'], 
                       default='all', help='Type of tests to run')
    
    args = parser.parse_args()
    
    runner = TimestampNavigationTestRunner()
    
    if args.type == 'all':
        success = runner.run_all_tests()
    elif args.type == 'unit':
        success = runner.run_unit_tests()
    elif args.type == 'integration':
        runner.check_analytics_service() or runner.start_analytics_service()
        success = runner.run_integration_tests()
    elif args.type == 'contract':
        runner.check_analytics_service() or runner.start_analytics_service()
        success = runner.run_contract_tests()
    elif args.type == 'performance':
        runner.check_analytics_service() or runner.start_analytics_service()
        success = runner.run_performance_tests()
    elif args.type == 'playwright':
        runner.check_analytics_service() or runner.start_analytics_service()
        success = runner.run_playwright_tests()
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()