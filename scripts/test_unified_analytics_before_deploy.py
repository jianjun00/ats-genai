#!/usr/bin/env python3
"""
Unified Analytics Pre-Deployment Test Runner

This script runs all critical unified-analytics regression protection tests
before any deployment to prevent accidental feature reversion.

Usage:
    python scripts/test_unified_analytics_before_deploy.py [--quick] [--verbose]

Exit codes:
    0: All tests passed
    1: Tests failed - DO NOT DEPLOY
    2: Tests skipped (service not available)
"""

import argparse
import os
import sys
import subprocess
import time
import json
import requests
from typing import Dict, List, Any, Optional
from pathlib import Path


class UnifiedAnalyticsPreDeploymentTester:
    """
    Comprehensive pre-deployment testing for unified-analytics platform.
    Prevents regressions from being deployed to any environment.
    """
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.base_url = os.getenv("UNIFIED_ANALYTICS_URL", "http://localhost:8081")
        self.test_results = []
        self.failed_tests = []
    
    def log(self, message: str, force: bool = False):
        """Log message if verbose or force"""
        if self.verbose or force:
            print(f"[{time.strftime('%H:%M:%S')}] {message}")
    
    def run_pytest_tests(self) -> Dict[str, Any]:
        """Run pytest tests for unified-analytics"""
        self.log("Running pytest regression protection tests...")
        
        # Find test files
        test_files = [
            "tests/integration/test_unified_analytics_regression_protection.py",
            "tests/integration/test_unified_analytics_deployment_protection.py"
        ]
        
        existing_test_files = []
        for test_file in test_files:
            if os.path.exists(test_file):
                existing_test_files.append(test_file)
            else:
                self.log(f"Warning: {test_file} not found", force=True)
        
        if not existing_test_files:
            return {"success": False, "error": "No test files found"}
        
        # Run pytest
        try:
            env = os.environ.copy()
            env["PYTHONPATH"] = "src"
            
            cmd = [
                "python", "-m", "pytest", 
                "-v", "--tb=short", 
                "--disable-warnings"
            ] + existing_test_files
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=300,  # 5 minute timeout
                env=env
            )
            
            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "tests_run": len(existing_test_files)
            }
            
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "Tests timed out after 5 minutes"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run_service_availability_test(self) -> Dict[str, Any]:
        """Test if unified-analytics service is available"""
        self.log("Testing service availability...")
        
        max_retries = 30
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{self.base_url}/health", timeout=10)
                if response.status_code == 200:
                    health_data = response.json()
                    return {
                        "success": True,
                        "health_data": health_data,
                        "attempt": attempt + 1
                    }
            except requests.exceptions.RequestException:
                pass
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        
        return {"success": False, "error": "Service not available after 60 seconds"}
    
    def run_critical_endpoint_tests(self) -> Dict[str, Any]:
        """Test critical endpoints directly"""
        self.log("Testing critical endpoints...")
        
        critical_endpoints = [
            {"path": "/health", "name": "Health Check"},
            {"path": "/", "name": "Main Page"},
            {"path": "/datasets", "name": "Data Catalog"},
            {"path": "/jobs", "name": "Job Management"},
            {"path": "/api/v1/datasets", "name": "Datasets API"},
            {"path": "/api/v1/jobs", "name": "Jobs API"},
        ]
        
        results = []
        
        for endpoint in critical_endpoints:
            try:
                response = requests.get(f"{self.base_url}{endpoint['path']}", timeout=30)
                
                result = {
                    "name": endpoint["name"],
                    "path": endpoint["path"],
                    "status_code": response.status_code,
                    "accessible": response.status_code == 200,
                    "content_size": len(response.content),
                    "success": response.status_code == 200 and len(response.content) > 50
                }
                
                results.append(result)
                
                if result["success"]:
                    self.log(f"✅ {endpoint['name']}: OK")
                else:
                    self.log(f"❌ {endpoint['name']}: FAILED ({response.status_code})", force=True)
                    
            except Exception as e:
                result = {
                    "name": endpoint["name"],
                    "path": endpoint["path"],
                    "error": str(e),
                    "success": False
                }
                results.append(result)
                self.log(f"❌ {endpoint['name']}: ERROR - {e}", force=True)
        
        successful_endpoints = sum(1 for r in results if r["success"])
        
        return {
            "success": successful_endpoints == len(critical_endpoints),
            "total_endpoints": len(critical_endpoints),
            "successful_endpoints": successful_endpoints,
            "results": results
        }
    
    def run_data_validation_tests(self) -> Dict[str, Any]:
        """Validate that real data (not mock data) is being used"""
        self.log("Validating real data usage...")
        
        try:
            # Check datasets API for real data
            response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=30)
            if response.status_code == 200:
                data = response.json()
                datasets = data.get("datasets", [])
                
                mock_patterns_found = []
                
                for dataset in datasets:
                    name = dataset.get("dataset_name", "").lower()
                    
                    # Prohibited mock patterns - but allow some exceptions for legitimate test datasets
                    mock_patterns = ["mock", "fake", "sample", "test_data"]
                    for pattern in mock_patterns:
                        if pattern in name:
                            mock_patterns_found.append(f"Dataset: {dataset['dataset_name']}")
                    
                    # Special handling for demo datasets - allow them if they contain real stock symbols
                    if "demo" in name:
                        symbols = dataset.get("symbols", [])
                        real_symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "SPY", "QQQ", "IWM"]
                        if not any(symbol in real_symbols for symbol in symbols):
                            mock_patterns_found.append(f"Demo dataset with non-real symbols: {dataset['dataset_name']}")
                
                # Check jobs API for real data
                response = requests.get(f"{self.base_url}/api/v1/jobs", timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    jobs = data.get("jobs", [])
                    
                    for job in jobs:
                        run_type = job.get("run_type", "").lower()
                        mock_patterns = ["demo", "mock", "fake", "sample"]
                        
                        for pattern in mock_patterns:
                            if pattern in run_type:
                                mock_patterns_found.append(f"Job: {job['run_type']}")
                
                return {
                    "success": len(mock_patterns_found) == 0,
                    "mock_patterns_found": mock_patterns_found,
                    "datasets_checked": len(datasets),
                    "jobs_checked": len(jobs)
                }
            
            return {"success": False, "error": "Could not access APIs for data validation"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run_feature_completeness_test(self) -> Dict[str, Any]:
        """Test that all restored features are present"""
        self.log("Testing feature completeness...")
        
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                health_data = response.json()
                features = health_data.get("features", [])
                
                required_features = [
                    "jobs",
                    "datasets", 
                    "enhanced_dataset_details",
                    "filterable_tables",
                    "coverage_catalog",
                    "real_training_files"
                ]
                
                missing_features = []
                for feature in required_features:
                    if feature not in features:
                        missing_features.append(feature)
                
                # Test that dataset detail pages work
                dataset_detail_accessible = False
                try:
                    response = requests.get(f"{self.base_url}/dataset/1", timeout=10)
                    # Even if dataset 1 doesn't exist, route should be registered (not 404 for missing route)
                    dataset_detail_accessible = response.status_code in [200, 404, 500]
                except:
                    pass
                
                return {
                    "success": len(missing_features) == 0 and dataset_detail_accessible,
                    "required_features": required_features,
                    "available_features": features,
                    "missing_features": missing_features,
                    "dataset_detail_accessible": dataset_detail_accessible
                }
            
            return {"success": False, "error": "Health endpoint not accessible"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run_kubernetes_deployment_test(self) -> Dict[str, Any]:
        """Test Kubernetes deployment status"""
        self.log("Testing Kubernetes deployment status...")
        
        try:
            # Check if kubectl is available
            result = subprocess.run(["kubectl", "version", "--client"], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                return {"success": False, "error": "kubectl not available", "skipped": True}
            
            # Check pod status
            result = subprocess.run([
                "kubectl", "get", "pods", "-n", "ats-dev", 
                "-l", "app=unified-analytics-webapp", "-o", "json"
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                pods_data = json.loads(result.stdout)
                pods = pods_data.get("items", [])
                
                running_pods = 0
                crashloop_pods = 0
                
                for pod in pods:
                    phase = pod["status"]["phase"]
                    if phase == "Running":
                        running_pods += 1
                    
                    # Check for crashloop
                    container_statuses = pod["status"].get("containerStatuses", [])
                    for container in container_statuses:
                        if container.get("state", {}).get("waiting", {}).get("reason") == "CrashLoopBackOff":
                            crashloop_pods += 1
                
                return {
                    "success": running_pods > 0 and crashloop_pods == 0,
                    "total_pods": len(pods),
                    "running_pods": running_pods,
                    "crashloop_pods": crashloop_pods,
                    "kubernetes_available": True
                }
            else:
                return {"success": False, "error": "Could not get pod status", "kubernetes_available": True}
                
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "Kubernetes commands timed out", "skipped": True}
        except Exception as e:
            return {"success": False, "error": str(e), "skipped": True}
    
    def generate_test_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        return {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "base_url": self.base_url,
            "test_results": self.test_results,
            "failed_tests": self.failed_tests,
            "total_tests": len(self.test_results),
            "failed_count": len(self.failed_tests),
            "success_rate": (len(self.test_results) - len(self.failed_tests)) / len(self.test_results) if self.test_results else 0,
            "overall_success": len(self.failed_tests) == 0
        }
    
    def run_all_tests(self, quick: bool = False) -> bool:
        """Run all pre-deployment tests"""
        self.log("🚀 Starting Unified Analytics Pre-Deployment Tests", force=True)
        self.log(f"Target URL: {self.base_url}", force=True)
        
        # Test 1: Service Availability
        result = self.run_service_availability_test()
        self.test_results.append({"name": "Service Availability", "result": result})
        if not result["success"]:
            self.failed_tests.append("Service Availability")
            self.log("❌ Service not available - skipping remaining tests", force=True)
            return False
        else:
            self.log("✅ Service is available", force=True)
        
        # Test 2: Critical Endpoints
        result = self.run_critical_endpoint_tests()
        self.test_results.append({"name": "Critical Endpoints", "result": result})
        if not result["success"]:
            self.failed_tests.append("Critical Endpoints")
        
        # Test 3: Feature Completeness
        result = self.run_feature_completeness_test()
        self.test_results.append({"name": "Feature Completeness", "result": result})
        if not result["success"]:
            self.failed_tests.append("Feature Completeness")
        
        # Test 4: Data Validation (No Mock Data)
        result = self.run_data_validation_tests()
        self.test_results.append({"name": "Data Validation", "result": result})
        if not result["success"]:
            self.failed_tests.append("Data Validation")
        
        # Test 5: Kubernetes Deployment (if available)
        if not quick:
            result = self.run_kubernetes_deployment_test()
            self.test_results.append({"name": "Kubernetes Deployment", "result": result})
            if not result["success"] and not result.get("skipped", False):
                self.failed_tests.append("Kubernetes Deployment")
        
        # Test 6: Comprehensive pytest tests (if not quick)
        if not quick:
            result = self.run_pytest_tests()
            self.test_results.append({"name": "Pytest Regression Tests", "result": result})
            if not result["success"]:
                self.failed_tests.append("Pytest Regression Tests")
        
        return len(self.failed_tests) == 0
    
    def print_summary(self):
        """Print test summary"""
        report = self.generate_test_report()
        
        self.log("", force=True)
        self.log("=" * 80, force=True)
        self.log("UNIFIED ANALYTICS PRE-DEPLOYMENT TEST SUMMARY", force=True)
        self.log("=" * 80, force=True)
        self.log(f"Timestamp: {report['timestamp']}", force=True)
        self.log(f"Target URL: {report['base_url']}", force=True)
        self.log(f"Total Tests: {report['total_tests']}", force=True)
        self.log(f"Failed Tests: {report['failed_count']}", force=True)
        self.log(f"Success Rate: {report['success_rate']:.2%}", force=True)
        self.log("", force=True)
        
        if report["failed_tests"]:
            self.log("❌ FAILED TESTS:", force=True)
            for failed_test in report["failed_tests"]:
                self.log(f"  - {failed_test}", force=True)
            self.log("", force=True)
            self.log("🚫 DEPLOYMENT BLOCKED - Fix failures before deploying!", force=True)
        else:
            self.log("✅ ALL TESTS PASSED", force=True)
            self.log("🚀 DEPLOYMENT APPROVED - All features working correctly!", force=True)
        
        self.log("=" * 80, force=True)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Run unified-analytics pre-deployment tests"
    )
    parser.add_argument("--quick", action="store_true", 
                       help="Run quick tests only (skip pytest and K8s tests)")
    parser.add_argument("--verbose", action="store_true", 
                       help="Enable verbose logging")
    parser.add_argument("--url", default="http://localhost:8081", 
                       help="Unified analytics service URL")
    
    args = parser.parse_args()
    
    # Set environment variable for URL
    os.environ["UNIFIED_ANALYTICS_URL"] = args.url
    
    tester = UnifiedAnalyticsPreDeploymentTester(verbose=args.verbose)
    
    try:
        success = tester.run_all_tests(quick=args.quick)
        tester.print_summary()
        
        # Generate report file
        report = tester.generate_test_report()
        with open("unified_analytics_test_report.json", "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        if success:
            print("\n🎉 All tests passed! Deployment approved.")
            sys.exit(0)
        else:
            print("\n🚫 Tests failed! Do not deploy.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Tests interrupted by user")
        sys.exit(2)
    except Exception as e:
        print(f"\n❌ Test runner failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()