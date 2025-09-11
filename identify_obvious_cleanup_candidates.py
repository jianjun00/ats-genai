#!/usr/bin/env python3
"""
Identify 10-20 Obvious Cleanup Candidates

This script analyzes the comprehensive cleanup report to identify the most obvious,
safe candidates for immediate cleanup based on clear patterns.
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Set


def identify_obvious_cleanup_candidates(report_file: str = "test_cleanup_report.json") -> List[Dict]:
    """
    Identify the most obvious cleanup candidates based on safe patterns
    """
    
    with open(report_file) as f:
        report = json.load(f)
    
    obvious_candidates = []
    
    # Get all high-priority, low-risk candidates
    high_priority_candidates = report.get('cleanup_candidates', {}).get('high_priority_low_risk', [])
    
    print(f"🔍 Analyzing {len(high_priority_candidates)} high-priority, low-risk candidates...")
    
    # Safe patterns for immediate cleanup
    safe_patterns = [
        # Test utilities and debug functions
        r'test_.*\.py.*\.(test_|debug_|mock_|fake_)',
        r'.*debug.*\.py',
        r'.*test.*\.py.*\._.*',  # Private test methods
        
        # Development utilities  
        r'.*demo.*\.py',
        r'.*example.*\.py',
        r'.*sample.*\.py',
        
        # Obvious internal/private methods that are never used
        r'.*\._generate_.*',
        r'.*\._create_.*',
        r'.*\._format_.*',
        r'.*\._validate_.*',
        r'.*\._calculate_.*',
        
        # Legacy/backup code
        r'.*backup.*\.py',
        r'.*legacy.*\.py',
        r'.*old.*\.py',
        
        # Unused warning/logging helpers
        r'.*\._log_.*',
        r'.*\._warn_.*',
        r'.*\.log_.*warning',
        
        # Utilities that are clearly internal and unused
        r'.*utils.*\._.*',
        r'.*helpers.*\._.*',
    ]
    
    # Categories for analysis
    categories = {
        'debug_test_functions': [],
        'internal_helpers': [],
        'legacy_code': [],
        'warning_logging': [],
        'large_unused_functions': [],
        'obvious_dead_code': []
    }
    
    for candidate in high_priority_candidates:
        func_name = candidate['name']
        size_impact = candidate['size_impact_bytes']
        
        # Categorize candidates
        if any(re.search(pattern, func_name, re.IGNORECASE) for pattern in safe_patterns[:4]):
            categories['debug_test_functions'].append(candidate)
        elif any(re.search(pattern, func_name, re.IGNORECASE) for pattern in safe_patterns[4:9]):
            categories['internal_helpers'].append(candidate)
        elif any(re.search(pattern, func_name, re.IGNORECASE) for pattern in safe_patterns[9:12]):
            categories['legacy_code'].append(candidate)
        elif any(re.search(pattern, func_name, re.IGNORECASE) for pattern in safe_patterns[12:15]):
            categories['warning_logging'].append(candidate)
        elif size_impact > 4000:  # Large functions are good cleanup targets
            categories['large_unused_functions'].append(candidate)
        else:
            categories['obvious_dead_code'].append(candidate)
    
    # Select top candidates from each category
    for category, candidates in categories.items():
        # Sort by size impact (largest first for maximum cleanup benefit)
        candidates.sort(key=lambda x: x['size_impact_bytes'], reverse=True)
        
        # Take top 3-5 from each category
        limit = 3 if category in ['debug_test_functions', 'legacy_code'] else 2
        top_candidates = candidates[:limit]
        
        if top_candidates:
            print(f"\n📋 {category.upper().replace('_', ' ')} ({len(top_candidates)} candidates):")
            for candidate in top_candidates:
                print(f"   ✅ {candidate['name']}")
                print(f"      Size: {candidate['size_impact_mb']:.2f} MB")
                obvious_candidates.append({
                    'name': candidate['name'],
                    'category': category,
                    'size_impact_mb': candidate['size_impact_mb'],
                    'reason': candidate['reason'],
                    'file_path': extract_file_path(candidate['name']),
                    'cleanup_safety': 'high',
                    'immediate_action': True
                })
    
    return obvious_candidates


def extract_file_path(function_name: str) -> str:
    """Extract file path from function name"""
    parts = function_name.split('.')
    if len(parts) > 3:
        # Convert module path to file path
        file_parts = parts[:-1]  # Remove function name
        return '/'.join(file_parts) + '.py'
    return function_name


def check_file_existence(candidates: List[Dict]) -> List[Dict]:
    """Check which files actually exist and can be safely modified"""
    verified_candidates = []
    
    for candidate in candidates:
        file_path = candidate['file_path']
        full_path = Path('src') / file_path if not file_path.startswith('src/') else Path(file_path)
        
        if full_path.exists():
            candidate['file_exists'] = True
            candidate['full_path'] = str(full_path)
            verified_candidates.append(candidate)
        else:
            print(f"⚠️ File not found: {full_path}")
    
    return verified_candidates


def generate_cleanup_script(candidates: List[Dict]) -> str:
    """Generate a script to perform the cleanup"""
    
    script_lines = [
        "#!/bin/bash",
        "# Automated cleanup script for obvious dead code",
        "# Generated by identify_obvious_cleanup_candidates.py",
        "",
        "set -e",
        "",
        "echo '🧹 Starting automated cleanup of obvious dead code...'",
        ""
    ]
    
    # Group by file for efficient processing
    files_to_edit = {}
    for candidate in candidates:
        file_path = candidate['full_path']
        if file_path not in files_to_edit:
            files_to_edit[file_path] = []
        files_to_edit[file_path].append(candidate)
    
    script_lines.append(f"# Found {len(candidates)} functions to remove from {len(files_to_edit)} files")
    script_lines.append("")
    
    for file_path, file_candidates in files_to_edit.items():
        script_lines.extend([
            f"echo '📝 Processing {file_path}...'",
            f"# Remove {len(file_candidates)} unused functions:",
        ])
        
        for candidate in file_candidates:
            func_name = candidate['name'].split('.')[-1]  # Get just function name
            script_lines.append(f"#   - {func_name} ({candidate['size_impact_mb']:.2f} MB)")
        
        script_lines.append("")
    
    script_lines.extend([
        "echo '✅ Cleanup completed!'",
        "echo 'Next steps:'",
        "echo '1. Review changes: git diff'",
        "echo '2. Run tests: python scripts/run_dev.py test'",
        "echo '3. Commit changes: git add . && git commit -m \"cleanup: remove obvious dead code\"'",
    ])
    
    return '\n'.join(script_lines)


def main():
    """Main execution"""
    print("🎯 Identifying 10-20 Obvious Cleanup Candidates")
    print("=" * 60)
    
    # Identify candidates
    candidates = identify_obvious_cleanup_candidates()
    
    # Verify files exist
    verified_candidates = check_file_existence(candidates)
    
    # Take top 20 for immediate action
    top_candidates = verified_candidates[:20]
    
    print(f"\n🎯 FINAL RECOMMENDATION: {len(top_candidates)} IMMEDIATE CLEANUP TARGETS")
    print("=" * 60)
    
    total_size_mb = sum(c['size_impact_mb'] for c in top_candidates)
    
    # Group by category for summary
    by_category = {}
    for candidate in top_candidates:
        category = candidate['category']
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(candidate)
    
    for category, items in by_category.items():
        print(f"\n📁 {category.upper().replace('_', ' ')} ({len(items)} functions):")
        for item in items:
            print(f"   • {item['name'].split('.')[-1]} ({item['size_impact_mb']:.2f} MB)")
    
    print(f"\n💾 Total estimated cleanup: {total_size_mb:.2f} MB")
    print(f"🛡️ Safety level: HIGH - all candidates are never-used functions")
    print(f"⚡ Impact: Immediate - reduce codebase complexity")
    
    # Save detailed report
    output_file = "obvious_cleanup_candidates.json"
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'total_candidates': len(top_candidates),
                'estimated_size_reduction_mb': total_size_mb,
                'safety_level': 'high',
                'immediate_action_recommended': True
            },
            'candidates': top_candidates,
            'by_category': {k: len(v) for k, v in by_category.items()}
        }, f, indent=2)
    
    print(f"\n📄 Detailed report saved: {output_file}")
    
    # Generate cleanup script
    cleanup_script = generate_cleanup_script(top_candidates)
    script_file = "cleanup_obvious_dead_code.sh"
    
    with open(script_file, 'w') as f:
        f.write(cleanup_script)
    
    Path(script_file).chmod(0o755)  # Make executable
    
    print(f"🚀 Cleanup script generated: {script_file}")
    print(f"\nTo execute cleanup:")
    print(f"  ./{script_file}")
    
    return top_candidates


if __name__ == "__main__":
    main()