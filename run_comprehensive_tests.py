#!/usr/bin/env python3
"""
Comprehensive test runner for UUID + Cache + Training Pipeline system.

This script runs all comprehensive tests and generates detailed reports:
1. UniverseStateManager rolling cache tests
2. UniverseStateBuilder multi-timeframe tests  
3. TrainingDataCallback end-to-end tests
4. Complete pipeline integration tests
5. Performance benchmarks
6. Memory usage analysis
7. Data integrity validation

Usage:
    python run_comprehensive_tests.py [--fast] [--component COMPONENT] [--report-dir DIR]
    
    --fast: Skip slow tests (performance, memory tests)
    --component: Run tests for specific component (manager|builder|callback|pipeline)
    --report-dir: Directory to save test reports (default: test_reports)
"""

import argparse
import subprocess
import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import psutil


class ComprehensiveTestRunner:
    """Runner for comprehensive test suites."""

    def __init__(self, report_dir: str = "test_reports"):
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(exist_ok=True)
        self.results = {}
        self.start_time = None
        
    def run_test_suite(self, test_file: str, test_class: str = None, fast_mode: bool = False) -> Dict[str, Any]:
        """Run a specific test suite and return results."""
        print(f"\n🔍 Running {test_file}...")
        
        # Construct pytest command
        cmd = ["python3", "-m", "pytest", f"tests/integration/{test_file}", "-v", "--tb=short"]
        
        if test_class:
            cmd.append(f"-k {test_class}")
            
        if fast_mode:
            # Skip performance and memory tests in fast mode
            cmd.extend(["-m", "not slow"])
        
        # Note: JSON reporting removed - using basic pytest output
        json_report = None
        
        start_time = time.time()
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            end_time = time.time()
            
            return {
                'file': test_file,
                'class': test_class,
                'duration': end_time - start_time,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'success': result.returncode == 0,
                'json_report': str(json_report) if json_report and json_report.exists() else None
            }
            
        except Exception as e:
            return {
                'file': test_file,
                'class': test_class,
                'duration': 0,
                'returncode': -1,
                'stdout': '',
                'stderr': str(e),
                'success': False,
                'json_report': None
            }

    def run_universe_state_manager_tests(self, fast_mode: bool = False) -> Dict[str, Any]:
        """Run UniverseStateManager comprehensive tests."""
        print("\n🏗️ Testing UniverseStateManager (Rolling Cache Functionality)")
        
        results = {}
        test_file = "test_universe_state_manager_comprehensive.py"
        
        # Test different aspects
        test_classes = [
            "TestUniverseStateManagerRollingCache",
            "TestUniverseStateManagerDatabaseIntegration"
        ]
        
        for test_class in test_classes:
            result = self.run_test_suite(test_file, test_class, fast_mode)
            results[test_class] = result
            
            if result['success']:
                print(f"   ✅ {test_class} - PASSED ({result['duration']:.2f}s)")
            else:
                print(f"   ❌ {test_class} - FAILED ({result['duration']:.2f}s)")
                
        return results

    def run_universe_state_builder_tests(self, fast_mode: bool = False) -> Dict[str, Any]:
        """Run UniverseStateBuilder comprehensive tests."""
        print("\n🔄 Testing UniverseStateBuilder (Multi-Timeframe Processing)")
        
        results = {}
        test_file = "test_universe_state_builder_comprehensive.py"
        
        test_classes = [
            "TestUniverseStateBuilderMultiTimeframe",
            "TestUniverseStateBuilderPerformance",
            "TestUniverseStateBuilderEdgeCases"
        ]
        
        for test_class in test_classes:
            if fast_mode and "Performance" in test_class:
                print(f"   ⏩ {test_class} - SKIPPED (fast mode)")
                continue
                
            result = self.run_test_suite(test_file, test_class, fast_mode)
            results[test_class] = result
            
            if result['success']:
                print(f"   ✅ {test_class} - PASSED ({result['duration']:.2f}s)")
            else:
                print(f"   ❌ {test_class} - FAILED ({result['duration']:.2f}s)")
                
        return results

    def run_training_data_callback_tests(self, fast_mode: bool = False) -> Dict[str, Any]:
        """Run TrainingDataCallback comprehensive tests."""
        print("\n📊 Testing TrainingDataCallback (End-to-End Workflows)")
        
        results = {}
        test_file = "test_training_data_callback_comprehensive.py"
        
        test_classes = [
            "TestTrainingDataCallbackArrayRecord",
            "TestTrainingDataCallbackDatabaseIntegration", 
            "TestTrainingDataCallbackEndToEnd",
            "TestTrainingDataCallbackPerformance"
        ]
        
        for test_class in test_classes:
            if fast_mode and "Performance" in test_class:
                print(f"   ⏩ {test_class} - SKIPPED (fast mode)")
                continue
                
            result = self.run_test_suite(test_file, test_class, fast_mode)
            results[test_class] = result
            
            if result['success']:
                print(f"   ✅ {test_class} - PASSED ({result['duration']:.2f}s)")
            else:
                print(f"   ❌ {test_class} - FAILED ({result['duration']:.2f}s)")
                
        return results

    def run_pipeline_integration_tests(self, fast_mode: bool = False) -> Dict[str, Any]:
        """Run complete pipeline integration tests."""
        print("\n🔗 Testing Complete Pipeline Integration (UUID + Cache + Training)")
        
        results = {}
        test_file = "test_complete_pipeline_integration.py"
        
        test_classes = [
            "TestCompleteUUIDCacheTrainingPipeline",
            "TestPipelineDataIntegrity"
        ]
        
        for test_class in test_classes:
            result = self.run_test_suite(test_file, test_class, fast_mode)
            results[test_class] = result
            
            if result['success']:
                print(f"   ✅ {test_class} - PASSED ({result['duration']:.2f}s)")
            else:
                print(f"   ❌ {test_class} - FAILED ({result['duration']:.2f}s)")
                
        return results

    def generate_summary_report(self) -> Dict[str, Any]:
        """Generate comprehensive summary report."""
        total_duration = time.time() - self.start_time
        
        summary = {
            'test_run': {
                'timestamp': datetime.now().isoformat(),
                'total_duration': total_duration,
                'components_tested': list(self.results.keys()),
                'total_test_classes': sum(len(component) for component in self.results.values()),
            },
            'system_info': {
                'python_version': sys.version,
                'platform': sys.platform,
                'memory_gb': round(psutil.virtual_memory().total / (1024**3), 2),
                'cpu_count': psutil.cpu_count(),
            },
            'results': self.results,
            'summary': {}
        }
        
        # Calculate summary statistics
        total_classes = 0
        passed_classes = 0
        total_test_duration = 0
        
        for component, classes in self.results.items():
            component_passed = 0
            component_total = len(classes)
            component_duration = 0
            
            for class_name, result in classes.items():
                total_classes += 1
                total_test_duration += result['duration']
                component_duration += result['duration']
                
                if result['success']:
                    passed_classes += 1
                    component_passed += 1
            
            summary['summary'][component] = {
                'total_classes': component_total,
                'passed_classes': component_passed,
                'success_rate': (component_passed / component_total * 100) if component_total > 0 else 0,
                'duration': component_duration
            }
        
        summary['summary']['overall'] = {
            'total_classes': total_classes,
            'passed_classes': passed_classes,
            'success_rate': (passed_classes / total_classes * 100) if total_classes > 0 else 0,
            'total_duration': total_test_duration,
            'setup_overhead': total_duration - total_test_duration
        }
        
        return summary

    def save_reports(self, summary: Dict[str, Any]):
        """Save comprehensive reports."""
        # Save JSON summary
        summary_file = self.report_dir / "comprehensive_test_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Save markdown report
        markdown_file = self.report_dir / "comprehensive_test_report.md"
        with open(markdown_file, 'w') as f:
            self.write_markdown_report(f, summary)
        
        print(f"\n📋 Reports saved:")
        print(f"   📄 JSON Summary: {summary_file}")
        print(f"   📝 Markdown Report: {markdown_file}")

    def write_markdown_report(self, f, summary: Dict[str, Any]):
        """Write markdown formatted report."""
        f.write("# Comprehensive Test Report\n\n")
        f.write(f"**Generated:** {summary['test_run']['timestamp']}\n")
        f.write(f"**Total Duration:** {summary['test_run']['total_duration']:.2f}s\n\n")
        
        # Overall summary
        overall = summary['summary']['overall']
        f.write("## Overall Summary\n\n")
        f.write(f"- **Test Classes:** {overall['total_classes']}\n")
        f.write(f"- **Passed:** {overall['passed_classes']}\n")
        f.write(f"- **Success Rate:** {overall['success_rate']:.1f}%\n")
        f.write(f"- **Test Duration:** {overall['total_duration']:.2f}s\n\n")
        
        # Component results
        f.write("## Component Results\n\n")
        
        for component, classes in self.results.items():
            component_summary = summary['summary'][component]
            status_emoji = "✅" if component_summary['success_rate'] == 100 else "⚠️" if component_summary['success_rate'] > 50 else "❌"
            
            f.write(f"### {status_emoji} {component.replace('_', ' ').title()}\n\n")
            f.write(f"- **Success Rate:** {component_summary['success_rate']:.1f}%\n")
            f.write(f"- **Duration:** {component_summary['duration']:.2f}s\n")
            f.write(f"- **Classes:** {component_summary['passed_classes']}/{component_summary['total_classes']}\n\n")
            
            for class_name, result in classes.items():
                status = "✅ PASSED" if result['success'] else "❌ FAILED"
                f.write(f"- `{class_name}`: {status} ({result['duration']:.2f}s)\n")
            
            f.write("\n")
        
        # System information
        f.write("## System Information\n\n")
        f.write(f"- **Python:** {summary['system_info']['python_version']}\n")
        f.write(f"- **Platform:** {summary['system_info']['platform']}\n")
        f.write(f"- **Memory:** {summary['system_info']['memory_gb']} GB\n")
        f.write(f"- **CPUs:** {summary['system_info']['cpu_count']}\n\n")

    def run_all_tests(self, fast_mode: bool = False, components: List[str] = None):
        """Run all comprehensive tests."""
        self.start_time = time.time()
        
        print("🚀 Starting Comprehensive Test Suite")
        print("=" * 50)
        
        components = components or ['manager', 'builder', 'callback', 'pipeline']
        
        if 'manager' in components:
            self.results['universe_state_manager'] = self.run_universe_state_manager_tests(fast_mode)
            
        if 'builder' in components:
            self.results['universe_state_builder'] = self.run_universe_state_builder_tests(fast_mode)
            
        if 'callback' in components:
            self.results['training_data_callback'] = self.run_training_data_callback_tests(fast_mode)
            
        if 'pipeline' in components:
            self.results['pipeline_integration'] = self.run_pipeline_integration_tests(fast_mode)
        
        # Generate and save reports
        summary = self.generate_summary_report()
        self.save_reports(summary)
        
        # Print final summary
        overall = summary['summary']['overall']
        print("\n" + "=" * 50)
        print("🏁 Test Suite Complete")
        print(f"📊 Results: {overall['passed_classes']}/{overall['total_classes']} ({overall['success_rate']:.1f}%)")
        print(f"⏱️  Duration: {overall['total_duration']:.2f}s")
        
        if overall['success_rate'] == 100:
            print("🎉 All tests passed! System is ready for production.")
            return 0
        elif overall['success_rate'] >= 80:
            print("⚠️  Most tests passed. Review failures before deployment.")
            return 1
        else:
            print("❌ Significant test failures. System needs attention.")
            return 2


def main():
    parser = argparse.ArgumentParser(description='Run comprehensive tests for UUID + Cache + Training Pipeline')
    parser.add_argument('--fast', action='store_true', help='Skip slow performance tests')
    parser.add_argument('--component', choices=['manager', 'builder', 'callback', 'pipeline'], 
                       help='Run tests for specific component only')
    parser.add_argument('--report-dir', default='test_reports', help='Directory for test reports')
    
    args = parser.parse_args()
    
    # Set up test environment
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(current_dir, 'src')
    os.environ['PYTHONPATH'] = src_path
    
    # Run tests
    runner = ComprehensiveTestRunner(args.report_dir)
    components = [args.component] if args.component else None
    
    exit_code = runner.run_all_tests(args.fast, components)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()