#!/usr/bin/env python3
"""
Test Suite Optimization Script

Analyzes the ATS platform test suite to identify optimization opportunities:
- Slow running tests
- Duplicate test patterns
- Inefficient test data usage
- Missing test parallelization opportunities

Usage:
    python scripts/optimize_test_suite.py --analyze
    python scripts/optimize_test_suite.py --fix-slow-tests
    python scripts/optimize_test_suite.py --report
"""

import os
import ast
import re
import json
import subprocess
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
from collections import defaultdict, Counter
import argparse


@dataclass
class TestMetrics:
    """Test file metrics for optimization analysis."""
    file_path: str
    test_count: int
    line_count: int
    imports_count: int
    fixtures_count: int
    slow_markers: int
    database_usage: bool
    api_usage: bool
    large_data_generation: bool
    duplicate_patterns: List[str]
    execution_time: Optional[float] = None


class TestSuiteAnalyzer:
    """Analyzes the test suite for optimization opportunities."""

    def __init__(self, test_directory: str = "tests"):
        self.test_dir = Path(test_directory)
        self.metrics: List[TestMetrics] = []
        self.common_patterns: Dict[str, int] = defaultdict(int)
        self.slow_tests: List[str] = []

    def analyze_test_suite(self) -> Dict[str, any]:
        """Perform comprehensive analysis of the test suite."""
        print("🔍 Analyzing ATS Platform Test Suite...")

        # Find all test files
        test_files = list(self.test_dir.rglob("test_*.py"))
        print(f"Found {len(test_files)} test files")

        # Analyze each test file
        for test_file in test_files:
            metrics = self._analyze_test_file(test_file)
            self.metrics.append(metrics)
        return self._generate_analysis_report()

    def _analyze_test_file(self, file_path: Path) -> TestMetrics:
        """Analyze a single test file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        tree = ast.parse(content)
        analyzer = TestFileASTAnalyzer()
        analyzer.visit(tree)

        metrics = TestMetrics(
            file_path=str(file_path),
            test_count=analyzer.test_count,
            line_count=len(content.splitlines()),
            imports_count=analyzer.import_count,
            fixtures_count=analyzer.fixture_count,
            slow_markers=analyzer.slow_markers,
            database_usage=analyzer.database_usage,
            api_usage=analyzer.api_usage,
            large_data_generation=analyzer.large_data_generation,
            duplicate_patterns=self._find_duplicate_patterns(content)
        )

        return metrics

    def _analyze_file_text(self, file_path: Path, content: str) -> TestMetrics:
        """Fallback text-based analysis."""
        lines = content.splitlines()

        return TestMetrics(
            file_path=str(file_path),
            test_count=len([line for line in lines if re.match(r'\s*def test_', line)]),
            line_count=len(lines),
            imports_count=len([line for line in lines if line.strip().startswith('import ')]),
            fixtures_count=len([line for line in lines if '@pytest.fixture' in line]),
            slow_markers=len([line for line in lines if 'pytest.mark.slow' in line]),
            database_usage='database' in content.lower() or 'db_' in content or 'conn' in content,
            api_usage='requests' in content or 'aiohttp' in content or 'api' in content.lower(),
            large_data_generation='range(1000' in content or 'range(10000' in content,
            duplicate_patterns=self._find_duplicate_patterns(content)
        )

    def _find_duplicate_patterns(self, content: str) -> List[str]:
        """Find duplicate code patterns in test content."""
        patterns = []

        # Common duplicate patterns
        duplicate_checks = [
            (r'mock\.patch\([\'"][^\'"]+"[\'"]\)', 'mock.patch pattern'),
            (r'assert .+ == .+', 'assert equality pattern'),
            (r'with pytest\.raises\(.+\):', 'pytest.raises pattern'),
            (r'@pytest\.fixture\s*\([^)]*\)', 'pytest.fixture pattern'),
            (r'async def test_', 'async test function pattern')
        ]

        for pattern, description in duplicate_checks:
            matches = re.findall(pattern, content)
            if len(matches) > 3:  # If pattern appears more than 3 times
                patterns.append(f"{description} ({len(matches)} times)")
                self.common_patterns[description] += len(matches)

        return patterns

    def _generate_analysis_report(self) -> Dict[str, any]:
        """Generate comprehensive analysis report."""
        total_tests = sum(m.test_count for m in self.metrics)
        total_lines = sum(m.line_count for m in self.metrics)

        # Identify problematic files
        large_files = [m for m in self.metrics if m.line_count > 500]
        test_heavy_files = [m for m in self.metrics if m.test_count > 20]
        slow_test_files = [m for m in self.metrics if m.slow_markers > 0]

        # Calculate averages
        avg_tests_per_file = total_tests / len(self.metrics) if self.metrics else 0
        avg_lines_per_file = total_lines / len(self.metrics) if self.metrics else 0

        return {
            'summary': {
                'total_files': len(self.metrics),
                'total_tests': total_tests,
                'total_lines': total_lines,
                'avg_tests_per_file': round(avg_tests_per_file, 2),
                'avg_lines_per_file': round(avg_lines_per_file, 2)
            },
            'optimization_opportunities': {
                'large_files': len(large_files),
                'test_heavy_files': len(test_heavy_files),
                'slow_test_files': len(slow_test_files),
                'files_with_db_usage': sum(1 for m in self.metrics if m.database_usage),
                'files_with_api_usage': sum(1 for m in self.metrics if m.api_usage),
                'files_with_large_data_gen': sum(1 for m in self.metrics if m.large_data_generation)
            },
            'common_patterns': dict(self.common_patterns),
            'recommendations': self._generate_recommendations(large_files, test_heavy_files, slow_test_files)
        }

    def _generate_recommendations(self, large_files: List[TestMetrics],
                                test_heavy_files: List[TestMetrics],
                                slow_test_files: List[TestMetrics]) -> List[str]:
        """Generate optimization recommendations."""
        recommendations = []

        if large_files:
            recommendations.append(
                f"📝 {len(large_files)} files have >500 lines. Consider splitting into smaller modules."
            )

        if test_heavy_files:
            recommendations.append(
                f"🧪 {len(test_heavy_files)} files have >20 tests. Consider using parameterized tests."
            )

        if slow_test_files:
            recommendations.append(
                f"⏱️  {len(slow_test_files)} files have slow tests. Consider mocking or test data factories."
            )

        if self.common_patterns.get('mock.patch pattern', 0) > 50:
            recommendations.append(
                "🔧 High mock.patch usage detected. Consider consolidating into fixtures."
            )

        if sum(1 for m in self.metrics if m.database_usage) > 20:
            recommendations.append(
                "🗄️  Many tests use database. Consider mock database helpers or test database."
            )

        if sum(1 for m in self.metrics if m.large_data_generation) > 5:
            recommendations.append(
                "📊 Large data generation detected. Consider using test data factories."
            )

        return recommendations

    def measure_test_execution_times(self) -> Dict[str, float]:
        """Measure execution times for all tests."""
        print("⏱️  Measuring test execution times...")

        # Run pytest with timing
        result = subprocess.run([
            'python', '-m', 'pytest',
            '--tb=no', '-v', '--durations=0',
            str(self.test_dir)
        ], capture_output=True, text=True, timeout=300)

        # Parse timing results
        timing_data = self._parse_pytest_timing(result.stdout)
        return timing_data

    def _parse_pytest_timing(self, output: str) -> Dict[str, float]:
        """Parse pytest timing output."""
        timing_data = {}

        # Look for timing information in pytest output
        timing_pattern = r'(\d+\.\d+)s call\s+(.+?):'
        matches = re.findall(timing_pattern, output)

        for time_str, test_name in matches:
            timing_data[test_name] = float(time_str)
        return timing_data

    def generate_optimization_script(self, output_file: str):
        """Generate a script to apply optimizations."""
        print(f"📝 Generating optimization script: {output_file}")

        script_content = f"""#!/usr/bin/env python3
'''
Auto-generated test suite optimization script
Generated by: scripts/optimize_test_suite.py
Date: {__import__('datetime').datetime.now().isoformat()}

This script applies identified optimizations to the ATS test suite.
'''

import os
import shutil
from pathlib import Path

def optimize_test_suite():
    '''Apply test suite optimizations.'''

    print("🚀 Applying ATS Test Suite Optimizations...")

    # 1. Install enhanced conftest.py
    enhanced_conftest = Path('tests/conftest_enhanced.py')
    target_conftest = Path('tests/conftest.py')

    if enhanced_conftest.exists():
        if target_conftest.exists():
            shutil.copy(target_conftest, 'tests/conftest_backup.py')
        shutil.copy(enhanced_conftest, target_conftest)
        print("✅ Enhanced conftest.py installed")

    # 2. Create test utilities directory
    utils_dir = Path('tests/utils')
    utils_dir.mkdir(exist_ok=True)

    if not (utils_dir / '__init__.py').exists():
        (utils_dir / '__init__.py').write_text('')

    print("✅ Test utilities directory created")

    # 3. Update pytest.ini with optimized settings
    pytest_ini_content = '''[tool:pytest]
addopts =
    -ra
    --strict-markers
    --strict-config
    --disable-warnings
    --tb=short
    --durations=10
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
    database: Tests requiring database
    api: Tests calling external APIs
    regression: Regression tests
filterwarnings =
    ignore::DeprecationWarning
    ignore::PendingDeprecationWarning
'''

    Path('pytest.ini').write_text(pytest_ini_content)
    print("✅ pytest.ini optimized")

    # 4. Performance recommendations
    print("\\n📊 OPTIMIZATION RECOMMENDATIONS:")
    print("1. Use 'tests/conftest_enhanced.py' fixtures to reduce duplication")
    print("2. Use 'tests/utils/test_patterns.py' helpers for common patterns")
    print("3. Run tests with: pytest -n auto (parallel execution)")
    print("4. Use markers to categorize tests: @pytest.mark.unit")
    print("5. Mock database connections for unit tests")

    print("\\n✅ Test suite optimization complete!")

if __name__ == "__main__":
    optimize_test_suite()
"""

        Path(output_file).write_text(script_content)
        print(f"✅ Optimization script created: {output_file}")


class TestFileASTAnalyzer(ast.NodeVisitor):
    """AST visitor for analyzing test file structure."""

    def __init__(self):
        self.test_count = 0
        self.import_count = 0
        self.fixture_count = 0
        self.slow_markers = 0
        self.database_usage = False
        self.api_usage = False
        self.large_data_generation = False

    def visit_FunctionDef(self, node):
        if node.name.startswith('test_'):
            self.test_count += 1

        # Check for slow test markers
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Attribute):
                if hasattr(decorator.value, 'attr') and decorator.value.attr == 'mark':
                    if hasattr(decorator, 'attr') and decorator.attr == 'slow':
                        self.slow_markers += 1

        # Check for fixture decorators
        for decorator in node.decorator_list:
            if (isinstance(decorator, ast.Name) and decorator.id == 'pytest.fixture') or \
               (isinstance(decorator, ast.Attribute) and
                hasattr(decorator.value, 'attr') and decorator.value.attr == 'fixture'):
                self.fixture_count += 1

        self.generic_visit(node)

    def visit_Import(self, node):
        self.import_count += 1

        # Check for database/API related imports
        for alias in node.names:
            if any(keyword in alias.name.lower() for keyword in ['database', 'db', 'sql', 'postgres']):
                self.database_usage = True
            if any(keyword in alias.name.lower() for keyword in ['requests', 'aiohttp', 'api']):
                self.api_usage = True

        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        self.import_count += 1

        if node.module:
            module_lower = node.module.lower()
            if any(keyword in module_lower for keyword in ['database', 'db', 'sql', 'postgres']):
                self.database_usage = True
            if any(keyword in module_lower for keyword in ['requests', 'aiohttp', 'api']):
                self.api_usage = True

        self.generic_visit(node)

    def visit_Call(self, node):
        # Check for large data generation patterns
        if isinstance(node.func, ast.Name) and node.func.id == 'range':
            if node.args and isinstance(node.args[0], ast.Constant):
                if node.args[0].value >= 1000:
                    self.large_data_generation = True

        self.generic_visit(node)


def main():
    """Main function to run test suite analysis."""
    parser = argparse.ArgumentParser(description="Optimize ATS Platform Test Suite")
    parser.add_argument('--analyze', action='store_true', help='Analyze test suite')
    parser.add_argument('--timing', action='store_true', help='Measure test execution times')
    parser.add_argument('--report', action='store_true', help='Generate optimization report')
    parser.add_argument('--fix', action='store_true', help='Generate optimization script')
    parser.add_argument('--output', default='test_optimization_report.json', help='Output file')

    args = parser.parse_args()

    analyzer = TestSuiteAnalyzer()

    if args.analyze or not any([args.timing, args.report, args.fix]):
        # Run analysis
        analysis = analyzer.analyze_test_suite()

        print("\n" + "="*80)
        print("ATS PLATFORM TEST SUITE ANALYSIS")
        print("="*80)

        print(f"\n📊 SUMMARY:")
        summary = analysis['summary']
        print(f"  Total files: {summary['total_files']}")
        print(f"  Total tests: {summary['total_tests']}")
        print(f"  Total lines: {summary['total_lines']:,}")
        print(f"  Avg tests/file: {summary['avg_tests_per_file']}")
        print(f"  Avg lines/file: {summary['avg_lines_per_file']:.0f}")

        print(f"\n🎯 OPTIMIZATION OPPORTUNITIES:")
        opportunities = analysis['optimization_opportunities']
        print(f"  Large files (>500 lines): {opportunities['large_files']}")
        print(f"  Test-heavy files (>20 tests): {opportunities['test_heavy_files']}")
        print(f"  Files with slow tests: {opportunities['slow_test_files']}")
        print(f"  Files using database: {opportunities['files_with_db_usage']}")
        print(f"  Files calling APIs: {opportunities['files_with_api_usage']}")

        print(f"\n💡 RECOMMENDATIONS:")
        for recommendation in analysis['recommendations']:
            print(f"  {recommendation}")

        # Save full report
        with open(args.output, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"\n✅ Full analysis saved to: {args.output}")

    if args.timing:
        timing_data = analyzer.measure_test_execution_times()
        if timing_data:
            slow_tests = sorted(timing_data.items(), key=lambda x: x[1], reverse=True)[:10]
            print(f"\n⏱️  TOP 10 SLOWEST TESTS:")
            for test_name, duration in slow_tests:
                print(f"  {duration:.2f}s - {test_name}")

    if args.fix:
        analyzer.generate_optimization_script('scripts/apply_test_optimizations.py')

    print(f"\n🚀 Test suite analysis complete!")


if __name__ == "__main__":
    main()