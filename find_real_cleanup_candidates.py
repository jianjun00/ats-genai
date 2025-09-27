#!/usr/bin/env python3
"""
Find Real Cleanup Candidates

This script finds actual cleanup candidates by examining files that exist
and identifying safe patterns for removal.
"""

import os
import ast
import json
from pathlib import Path
from typing import List, Dict, Set


def find_unused_imports_and_functions():
    """Find unused imports and obvious dead code in existing files"""

    cleanup_candidates = []

    # Common patterns that are safe to remove
    safe_patterns = [
        'test_*',  # Test files
        'debug_*',  # Debug files
        'demo_*',   # Demo files
        '*_backup*', # Backup files
        '*_old*',   # Old files
        '*sample*', # Sample files
        '*example*' # Example files
    ]

    print("🔍 Scanning for real cleanup candidates...")

    # 1. Find obvious file-level candidates
    root_files = list(Path('.').glob('*.py'))
    test_files = []
    demo_files = []
    debug_files = []

    for file_path in root_files:
        name = file_path.name
        if any(pattern.replace('*', '') in name for pattern in ['test_', 'debug_', 'demo_']):
            if 'test_' in name:
                test_files.append(str(file_path))
            elif 'debug_' in name:
                debug_files.append(str(file_path))
            elif 'demo_' in name:
                demo_files.append(str(file_path))

    # 2. Find unused script files in root directory
    script_files = [f for f in root_files if f.suffix == '.py' and f.stat().st_size < 10000]  # Small scripts

    print(f"📁 Found potential file-level candidates:")
    print(f"   Test files: {len(test_files)}")
    print(f"   Debug files: {len(debug_files)}")
    print(f"   Demo files: {len(demo_files)}")
    print(f"   Small scripts: {len(script_files)}")

    # Create specific recommendations
    recommendations = []

    # Check some specific files that might be cleanup candidates
    specific_files = [
        'analyze_universe_differences.py',
        'debug_analytics_content.py',
        'debug_analytics_service.py',
        'debug_arrayrecord_api.py',
        'debug_comprehensive_features.py',
        'demo_time_navigation.py',
        'fingpt_alternative_demo.py',
        'fingpt_llama_demo.py',
        'simple_news_signal_test.py',
        'test_observability_setup.py',
        'train_aapl_limited_real.py',
        'train_real_aapl_simple.py'
    ]

    for file_name in specific_files:
        file_path = Path(file_name)
        if file_path.exists():
            size_kb = file_path.stat().st_size / 1024
            recommendations.append({
                'name': file_name,
                'type': 'file',
                'category': 'debug/demo/test_script',
                'size_kb': size_kb,
                'reason': 'Development/debug script not needed in production',
                'action': 'remove_file',
                'safety': 'high'
            })

    # Look for duplicate or backup files
    backup_patterns = ['*backup*', '*_old.py', '*_bak.py', '*.py.backup']
    for pattern in backup_patterns:
        for file_path in Path('.').glob(pattern):
            if file_path.is_file():
                size_kb = file_path.stat().st_size / 1024
                recommendations.append({
                    'name': str(file_path),
                    'type': 'file',
                    'category': 'backup_file',
                    'size_kb': size_kb,
                    'reason': 'Backup file no longer needed',
                    'action': 'remove_file',
                    'safety': 'high'
                })

    # Look for empty or minimal files
    for file_path in Path('src').rglob('*.py'):
        if file_path.stat().st_size < 100:  # Very small files
            with open(file_path) as f:
                content = f.read().strip()
            if not content or content == '# TODO: Implement' or content.count('\n') < 3:
                recommendations.append({
                    'name': str(file_path),
                    'type': 'file',
                    'category': 'empty_or_minimal',
                    'size_kb': file_path.stat().st_size / 1024,
                    'reason': 'Empty or minimal implementation',
                    'action': 'remove_file',
                    'safety': 'medium'
                })
    return recommendations


def find_unused_functions_in_specific_files():
    """Find unused functions in specific files"""

    # Look for obvious dead code patterns in key files
    candidates = []

    # Check analytics service for unused private methods
    analytics_file = Path('src/services/analytics_service.py')
    if analytics_file.exists():
        with open(analytics_file) as f:
            content = f.read()

        # Parse AST to find methods
        tree = ast.parse(content)

        # Look for private methods that might be unused
        private_methods = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith('_'):
                private_methods.append(node.name)

        # These are likely unused based on naming patterns
        likely_unused = [method for method in private_methods
                       if any(pattern in method for pattern in
                            ['_debug_', '_test_', '_sample_', '_demo_'])]

        for method in likely_unused:
            candidates.append({
                'name': f'src.services.analytics_service.{method}',
                'type': 'function',
                'category': 'private_debug_method',
                'file': str(analytics_file),
                'reason': 'Private debug/test method likely unused',
                'action': 'remove_function',
                'safety': 'medium'
            })

    return candidates


def analyze_import_usage():
    """Find unused imports - common source of cleanup"""

    candidates = []

    # Look for files with obviously unused imports
    py_files = list(Path('src').rglob('*.py'))[:20]  # Sample for demo

    for file_path in py_files:
        with open(file_path) as f:
            content = f.read()

        # Look for imports that are clearly unused (simple heuristic)
        lines = content.split('\n')
        imports = []

        for line in lines:
            line = line.strip()
            if line.startswith('import ') or line.startswith('from '):
                # Extract imported names
                if ' as ' in line:
                    # Handle "import x as y"
                    imported_name = line.split(' as ')[-1].strip()
                elif line.startswith('from '):
                    # Handle "from x import y"
                    parts = line.split(' import ')
                    if len(parts) > 1:
                        imported_name = parts[1].split(',')[0].strip()
                    else:
                        continue
                else:
                    # Handle "import x"
                    imported_name = line.replace('import ', '').split('.')[0].strip()

                # Check if imported name is used in the file
                if imported_name and len(imported_name) > 2:  # Avoid short names
                    usage_count = content.count(imported_name)
                    if usage_count <= 1:  # Only the import line itself
                        imports.append({
                            'line': line,
                            'imported_name': imported_name,
                            'usage_count': usage_count
                        })

        if imports:
            candidates.append({
                'name': str(file_path),
                'type': 'file_with_unused_imports',
                'category': 'unused_imports',
                'unused_imports': len(imports),
                'imports': imports[:3],  # Sample
                'reason': f'{len(imports)} potentially unused imports',
                'action': 'remove_unused_imports',
                'safety': 'high'
            })

    return candidates


def main():
    """Main analysis"""
    print("🎯 Finding Real Cleanup Candidates")
    print("=" * 50)

    # 1. File-level candidates
    file_candidates = find_unused_imports_and_functions()

    # 2. Function-level candidates
    function_candidates = find_unused_functions_in_specific_files()

    # 3. Import-level candidates
    import_candidates = analyze_import_usage()

    all_candidates = file_candidates + function_candidates + import_candidates

    # Sort by safety and impact
    all_candidates.sort(key=lambda x: (x['safety'] == 'high', x.get('size_kb', 0)), reverse=True)

    # Take top 15 for immediate action
    top_candidates = all_candidates[:15]

    print(f"\n🎯 TOP {len(top_candidates)} CLEANUP CANDIDATES")
    print("=" * 50)

    by_category = {}
    total_size_kb = 0

    for candidate in top_candidates:
        category = candidate['category']
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(candidate)
        total_size_kb += candidate.get('size_kb', 0)

    for category, items in by_category.items():
        print(f"\n📁 {category.upper().replace('_', ' ')} ({len(items)} items):")
        for item in items:
            name = item['name']
            if len(name) > 60:
                name = name[:57] + "..."
            print(f"   ✅ {name}")
            print(f"      {item['reason']} ({item.get('size_kb', 0):.1f} KB)")

    print(f"\n💾 Total cleanup potential: {total_size_kb:.1f} KB")
    print(f"🛡️ Safety levels: {len([c for c in top_candidates if c['safety'] == 'high'])} high, {len([c for c in top_candidates if c['safety'] == 'medium'])} medium")

    # Save results
    output_file = "real_cleanup_candidates.json"
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'total_candidates': len(top_candidates),
                'total_size_kb': total_size_kb,
                'by_category': {k: len(v) for k, v in by_category.items()},
                'safety_breakdown': {
                    'high': len([c for c in top_candidates if c['safety'] == 'high']),
                    'medium': len([c for c in top_candidates if c['safety'] == 'medium'])
                }
            },
            'candidates': top_candidates
        }, f, indent=2)

    print(f"\n📄 Detailed analysis saved: {output_file}")

    # Generate specific removal commands
    print(f"\n🚀 IMMEDIATE ACTIONS:")
    high_safety_files = [c for c in top_candidates if c['safety'] == 'high' and c['type'] == 'file']

    if high_safety_files:
        print("📝 Safe file removals:")
        for candidate in high_safety_files[:5]:  # Top 5
            print(f"   rm {candidate['name']}")

    return top_candidates


if __name__ == "__main__":
    main()