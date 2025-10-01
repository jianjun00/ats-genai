#!/usr/bin/env python3
"""
Duplicate Test File Detection Script

Systematically identifies all duplicate test basenames across the test suite
to prevent pytest import mismatch errors.

Usage:
    python scripts/detect_duplicate_test_files.py
    python scripts/detect_duplicate_test_files.py --fix-duplicates
"""

import os
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple
import argparse


def find_all_test_files(test_root: Path) -> List[Path]:
    """Find all Python test files."""
    test_files = []
    for path in test_root.rglob("*.py"):
        if path.name.startswith("test_") or path.name.endswith("_test.py"):
            test_files.append(path)
    return test_files


def group_by_basename(test_files: List[Path]) -> Dict[str, List[Path]]:
    """Group test files by basename."""
    basename_map = defaultdict(list)
    for path in test_files:
        basename_map[path.name].append(path)
    return basename_map


def find_duplicates(basename_map: Dict[str, List[Path]]) -> Dict[str, List[Path]]:
    """Filter to only files with duplicate basenames."""
    return {
        basename: paths
        for basename, paths in basename_map.items()
        if len(paths) > 1 and basename != "__init__.py" and basename != "conftest.py"
    }


def analyze_duplicates(duplicates: Dict[str, List[Path]]) -> None:
    """Analyze and report duplicate test files."""
    if not duplicates:
        print("✅ No duplicate test basenames found!")
        return

    print(f"🔍 DUPLICATE TEST FILE ANALYSIS")
    print("=" * 80)
    print(f"Found {len(duplicates)} duplicate basenames affecting {sum(len(paths) for paths in duplicates.values())} files\n")

    # Sort by number of duplicates (descending)
    sorted_duplicates = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)

    for basename, paths in sorted_duplicates:
        print(f"📄 {basename} ({len(paths)} copies)")
        
        # Get file sizes
        file_info = []
        for path in sorted(paths):
            size = path.stat().st_size
            try:
                rel_path = path.relative_to(Path.cwd())
            except ValueError:
                rel_path = path  # Use absolute path if relative fails
            file_info.append((rel_path, size))
        
        # Display sorted by size (largest first)
        file_info.sort(key=lambda x: x[1], reverse=True)
        
        for rel_path, size in file_info:
            size_kb = size / 1024
            print(f"   {size:>8,} bytes ({size_kb:>6.1f} KB) - {rel_path}")
        
        # Check if files are identical
        if len(paths) == 2:
            content1 = paths[0].read_bytes()
            content2 = paths[1].read_bytes()
            if content1 == content2:
                print(f"   ⚠️  FILES ARE IDENTICAL - safe to remove smaller/wrong location")
            else:
                print(f"   ⚠️  FILES DIFFER - manual review required")
        
        print()


def recommend_actions(duplicates: Dict[str, List[Path]]) -> List[Tuple[str, Path, Path]]:
    """Recommend which duplicates to remove."""
    recommendations = []
    
    for basename, paths in duplicates.items():
        if len(paths) == 2:
            # Check if identical
            content1 = paths[0].read_bytes()
            content2 = paths[1].read_bytes()
            
            if content1 == content2:
                # Keep the one in the more specific/correct location
                # Preference: domains/ > infrastructure/ > services/ > core/
                paths_sorted = sorted(paths, key=lambda p: (
                    0 if 'domains/' in str(p) else
                    1 if 'infrastructure/' in str(p) else
                    2 if 'services/' in str(p) else
                    3 if 'core/' in str(p) else
                    4
                ))
                
                keep = paths_sorted[0]
                remove = paths_sorted[1]
                recommendations.append(("IDENTICAL", keep, remove))
        
        elif len(paths) > 2:
            # Multiple copies - needs manual review
            largest = max(paths, key=lambda p: p.stat().st_size)
            recommendations.append(("MANUAL_REVIEW", largest, None))
    
    return recommendations


def print_recommendations(recommendations: List[Tuple[str, Path, Path]]) -> None:
    """Print actionable recommendations."""
    print("\n" + "=" * 80)
    print("🎯 RECOMMENDED ACTIONS")
    print("=" * 80)
    
    identical_count = sum(1 for r in recommendations if r[0] == "IDENTICAL")
    manual_count = sum(1 for r in recommendations if r[0] == "MANUAL_REVIEW")
    
    print(f"\n{identical_count} identical duplicates (safe to auto-remove)")
    print(f"{manual_count} cases requiring manual review\n")
    
    if identical_count > 0:
        print("✅ Safe to remove (identical files):")
        for action, keep, remove in recommendations:
            if action == "IDENTICAL":
                try:
                    remove_rel = remove.relative_to(Path.cwd())
                    keep_rel = keep.relative_to(Path.cwd())
                except ValueError:
                    remove_rel = remove
                    keep_rel = keep
                print(f"   rm {remove_rel}")
                print(f"      (keeping {keep_rel})")
    
    if manual_count > 0:
        print("\n⚠️  Requires manual review (multiple copies or different content):")
        for action, largest, _ in recommendations:
            if action == "MANUAL_REVIEW":
                try:
                    largest_rel = largest.relative_to(Path.cwd())
                except ValueError:
                    largest_rel = largest
                print(f"   {largest_rel} - review all copies manually")


def generate_removal_script(recommendations: List[Tuple[str, Path, Path]], output_file: Path) -> None:
    """Generate a shell script to remove safe duplicates."""
    identical = [(k, r) for a, k, r in recommendations if a == "IDENTICAL"]
    
    if not identical:
        print("\n⚠️  No safe removals identified - manual review required for all duplicates")
        return
    
    script_content = """#!/bin/bash
# Auto-generated script to remove duplicate test files
# Generated by scripts/detect_duplicate_test_files.py

set -e  # Exit on error

echo "🗑️  Removing duplicate test files..."
echo "=" * 60

"""
    
    for keep, remove in identical:
        try:
            keep_rel = keep.relative_to(Path.cwd())
            remove_rel = remove.relative_to(Path.cwd())
        except ValueError:
            keep_rel = keep
            remove_rel = remove
        script_content += f"""
# Remove duplicate of {keep_rel}
echo "Removing {remove_rel}..."
rm "{remove}"
"""
    
    script_content += """
echo ""
echo "✅ Duplicate removal complete!"
echo "Run: pytest --collect-only to verify no import mismatch errors"
"""
    
    output_file.write_text(script_content)
    output_file.chmod(0o755)
    
    print(f"\n📝 Generated removal script: {output_file}")
    print(f"   Review and run: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Detect duplicate test files")
    parser.add_argument("--generate-script", action="store_true",
                       help="Generate shell script to remove safe duplicates")
    parser.add_argument("--test-root", default="tests",
                       help="Root directory for test files (default: tests)")
    args = parser.parse_args()
    
    test_root = Path(args.test_root)
    
    if not test_root.exists():
        print(f"❌ Test root directory not found: {test_root}")
        sys.exit(1)
    
    # Find all test files
    print(f"🔍 Scanning {test_root} for test files...")
    test_files = find_all_test_files(test_root)
    print(f"   Found {len(test_files)} test files\n")
    
    # Group by basename and find duplicates
    basename_map = group_by_basename(test_files)
    duplicates = find_duplicates(basename_map)
    
    # Analyze and report
    analyze_duplicates(duplicates)
    
    if not duplicates:
        sys.exit(0)
    
    # Generate recommendations
    recommendations = recommend_actions(duplicates)
    print_recommendations(recommendations)
    
    # Generate removal script if requested
    if args.generate_script:
        script_path = Path("scripts/remove_duplicate_tests.sh")
        generate_removal_script(recommendations, script_path)
    else:
        print("\n💡 TIP: Run with --generate-script to create an automated removal script")
    
    # Exit with error code if duplicates found
    sys.exit(1)


if __name__ == "__main__":
    main()