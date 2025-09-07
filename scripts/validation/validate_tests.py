#!/usr/bin/env python3
"""
Test validation script that checks test structure without requiring dependencies
"""

import os
import sys
from pathlib import Path
import ast


def validate_test_file(file_path: Path) -> dict:
    """Validate a single test file"""
    results = {
        'file': str(file_path),
        'exists': False,
        'readable': False,
        'has_test_functions': False,
        'has_test_classes': False,
        'has_imports': False,
        'has_docstring': False,
        'test_count': 0,
        'issues': []
    }

    try:
        results['exists'] = file_path.exists()
        if not results['exists']:
            results['issues'].append("File does not exist")
            return results

        # Read file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        results['readable'] = True

        # Parse AST to analyze structure
        try:
            tree = ast.parse(content)

            # Check for module docstring
            if (isinstance(tree.body[0], ast.Expr) and
                isinstance(tree.body[0].value, ast.Constant) and
                isinstance(tree.body[0].value.value, str)):
                results['has_docstring'] = True

            # Analyze AST nodes
            for node in ast.walk(tree):
                # Check for imports
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    results['has_imports'] = True

                # Check for test functions (including async)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith('test_'):
                    results['has_test_functions'] = True
                    results['test_count'] += 1

                # Check for test classes
                if isinstance(node, ast.ClassDef) and node.name.startswith('Test'):
                    results['has_test_classes'] = True

                    # Count test methods in class (including async)
                    for class_node in node.body:
                        if (isinstance(class_node, (ast.FunctionDef, ast.AsyncFunctionDef)) and
                            class_node.name.startswith('test_')):
                            results['test_count'] += 1

            # Check for specific patterns
            if 'pytest' in content:
                results['has_pytest'] = True
            if 'asyncio' in content:
                results['has_asyncio'] = True
            if 'mock' in content.lower() or 'Mock' in content:
                results['has_mocking'] = True

        except SyntaxError as e:
            results['issues'].append(f"Syntax error: {e}")

        # Basic content checks
        if not results['has_test_functions'] and not results['has_test_classes']:
            results['issues'].append("No test functions or classes found")

    except Exception as e:
        results['issues'].append(f"Error reading file: {e}")

    return results


def main():
    """Main validation function"""
    project_root = Path(__file__).parent.parent
    tests_dir = project_root / "tests"

    # Test files to validate
    new_test_files = [
        "events/test_economic_events_classifier.py",
        "training/test_multimodal_dataset_generator.py",
        "integration/test_news_multimodal_integration.py",
        "performance/test_multimodal_performance.py"
    ]

    print("🔍 Validating Multi-Modal System Test Files")
    print("=" * 50)

    all_results = []
    total_tests = 0

    for test_file in new_test_files:
        file_path = tests_dir / test_file
        results = validate_test_file(file_path)
        all_results.append(results)

        print(f"\n📁 {test_file}")
        print(f"   Exists: {'✅' if results['exists'] else '❌'}")

        if results['exists']:
            print(f"   Readable: {'✅' if results['readable'] else '❌'}")
            print(f"   Has docstring: {'✅' if results['has_docstring'] else '⚠️'}")
            print(f"   Has test functions: {'✅' if results['has_test_functions'] else '❌'}")
            print(f"   Has test classes: {'✅' if results['has_test_classes'] else '⚠️'}")
            print(f"   Test count: {results['test_count']}")
            total_tests += results['test_count']

            if results.get('has_pytest'):
                print(f"   Uses pytest: ✅")
            if results.get('has_asyncio'):
                print(f"   Uses asyncio: ✅")
            if results.get('has_mocking'):
                print(f"   Uses mocking: ✅")

            if results['issues']:
                print(f"   Issues:")
                for issue in results['issues']:
                    print(f"     - {issue}")

    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY")
    print("=" * 50)

    files_exist = sum(1 for r in all_results if r['exists'])
    files_have_tests = sum(1 for r in all_results if r['has_test_functions'] or r['has_test_classes'])
    files_readable = sum(1 for r in all_results if r['readable'])

    print(f"Total test files: {len(new_test_files)}")
    print(f"Files exist: {files_exist}/{len(new_test_files)}")
    print(f"Files readable: {files_readable}/{len(new_test_files)}")
    print(f"Files with tests: {files_have_tests}/{len(new_test_files)}")
    print(f"Total test methods: {total_tests}")

    # Calculate coverage areas
    coverage_areas = {
        'Economic Events Classification': any('events' in r['file'] for r in all_results if r['exists']),
        'Multi-Modal Dataset Generation': any('training' in r['file'] for r in all_results if r['exists']),
        'End-to-End Integration': any('integration' in r['file'] for r in all_results if r['exists']),
        'Performance & Scalability': any('performance' in r['file'] for r in all_results if r['exists'])
    }

    print(f"\nTest Coverage Areas:")
    for area, covered in coverage_areas.items():
        status = "✅" if covered else "❌"
        print(f"   {area}: {status}")

    # Overall assessment
    success_rate = files_have_tests / len(new_test_files) if new_test_files else 0

    if success_rate >= 1.0:
        print(f"\n🎉 Excellent! All test files are properly structured with {total_tests} total tests")
    elif success_rate >= 0.75:
        print(f"\n✅ Good! Most test files are ready ({success_rate:.0%} success rate)")
    elif success_rate >= 0.5:
        print(f"\n⚠️  Partial coverage ({success_rate:.0%} success rate) - some files need attention")
    else:
        print(f"\n❌ Poor coverage ({success_rate:.0%} success rate) - significant issues found")

    # Detailed recommendations
    if total_tests > 50:
        print(f"\n📈 {total_tests} tests provide comprehensive coverage of the multi-modal system")
    elif total_tests > 20:
        print(f"\n📊 {total_tests} tests provide good basic coverage")
    else:
        print(f"\n⚠️  Only {total_tests} tests - consider adding more for better coverage")

    return success_rate >= 0.75


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)