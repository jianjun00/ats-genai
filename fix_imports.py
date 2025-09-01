#!/usr/bin/env python3
"""
Script to fix sys.path.insert import patterns across the codebase.

This script identifies files using sys.path.insert and converts them
to proper relative imports where possible.
"""

import os
import re
from pathlib import Path
from typing import List, Tuple

def find_files_with_sys_path_insert(src_dir: Path) -> List[Path]:
    """Find all Python files containing sys.path.insert."""
    files_with_issues = []
    
    for py_file in src_dir.rglob("*.py"):
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if 'sys.path.insert' in content:
                    files_with_issues.append(py_file)
        except Exception as e:
            print(f"Warning: Could not read {py_file}: {e}")
    
    return files_with_issues

def fix_import_patterns(file_path: Path, src_root: Path) -> bool:
    """
    Fix sys.path.insert patterns in a single file.
    
    Returns:
        True if file was modified, False otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        content = original_content
        modified = False
        
        # Pattern 1: sys.path.insert(0, '/workspace/src')
        if "sys.path.insert(0, '/workspace/src')" in content:
            # Remove the sys.path.insert line
            content = re.sub(
                r'sys\.path\.insert\(0, [\'\"]/workspace/src[\'\"]\)\s*\n?',
                '',
                content
            )
            modified = True
        
        # Pattern 2: sys.path.insert with relative paths
        pattern = r'sys\.path\.insert\(0, os\.path\.join\(os\.path\.dirname\(__file__\), [^)]+\)\)'
        if re.search(pattern, content):
            content = re.sub(pattern + r'\s*\n?', '', content)
            modified = True
        
        # Pattern 3: if '/workspace/src' not in sys.path blocks
        if_pattern = r'if [\'\"]/workspace/src[\'\"] not in sys\.path:\s*\n\s*sys\.path\.insert\(0, [\'\"]/workspace/src[\'\"]\)\s*\n?'
        if re.search(if_pattern, content):
            content = re.sub(if_pattern, '', content)
            modified = True
        
        # Clean up any remaining sys import if not used
        if 'sys.path' not in content and 'sys.argv' not in content and 'sys.exit' not in content:
            # Check if sys is still imported but not used
            lines = content.split('\n')
            new_lines = []
            for line in lines:
                if line.strip() == 'import sys' and 'sys.' not in '\n'.join(lines):
                    continue  # Remove unused sys import
                new_lines.append(line)
            content = '\n'.join(new_lines)
            modified = True
        
        if modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Fixed imports in {file_path.relative_to(src_root)}")
            return True
        
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
    
    return False

def main():
    """Main execution function."""
    src_dir = Path(__file__).parent / "src"
    
    if not src_dir.exists():
        print(f"Error: Source directory {src_dir} does not exist")
        return
    
    print("🔍 Finding files with sys.path.insert patterns...")
    problematic_files = find_files_with_sys_path_insert(src_dir)
    
    if not problematic_files:
        print("✅ No files with sys.path.insert patterns found")
        return
    
    print(f"Found {len(problematic_files)} files with import issues:")
    for file_path in problematic_files:
        print(f"  - {file_path.relative_to(src_dir.parent)}")
    
    print("\n🔧 Fixing import patterns...")
    fixed_count = 0
    
    for file_path in problematic_files:
        if fix_import_patterns(file_path, src_dir.parent):
            fixed_count += 1
    
    print(f"\n📊 Summary:")
    print(f"  - Files examined: {len(problematic_files)}")
    print(f"  - Files fixed: {fixed_count}")
    print(f"  - Files remaining: {len(problematic_files) - fixed_count}")
    
    if fixed_count > 0:
        print("\n⚠️ Note: Fixed files may need manual review for:")
        print("  - Import statement corrections")
        print("  - Relative import path adjustments") 
        print("  - Testing to ensure functionality is preserved")

if __name__ == "__main__":
    main()