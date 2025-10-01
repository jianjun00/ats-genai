#!/usr/bin/env python3
"""
Fix Path import issues systematically.

Finds files that use Path(__file__) but don't have the proper import,
and adds the missing pathlib import.
"""

import os
import re
import subprocess
import sys

def find_files_using_path_without_import():
    """Find files using Path but missing import."""
    # Find files using Path
    result = subprocess.run([
        'grep', '-r', 'Path(__file__)', 'tests/', '--include=*.py'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        return []
    
    files_using_path = set()
    for line in result.stdout.strip().split('\n'):
        if ':' in line:
            file_path = line.split(':')[0]
            files_using_path.add(file_path)
    
    # Filter out files that already import Path
    files_missing_import = []
    for file_path in files_using_path:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                if 'from pathlib import Path' not in content and 'import pathlib' not in content:
                    files_missing_import.append(file_path)
        except FileNotFoundError:
            continue
    
    return sorted(files_missing_import)

def add_path_import(file_path):
    """Add Path import to a file."""
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Find the best place to insert the import
        insert_line = 0
        in_docstring = False
        docstring_char = None
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # Handle docstrings
            if stripped.startswith('"""') or stripped.startswith("'''"):
                if not in_docstring:
                    in_docstring = True
                    docstring_char = stripped[:3]
                    if stripped.count(docstring_char) >= 2:  # Single line docstring
                        in_docstring = False
                        insert_line = i + 1
                elif stripped.endswith(docstring_char):
                    in_docstring = False
                    insert_line = i + 1
                continue
                
            if in_docstring:
                continue
                
            # Skip shebang and encoding
            if stripped.startswith('#'):
                insert_line = i + 1
                continue
                
            # If we find an import, insert here
            if stripped.startswith('import ') or stripped.startswith('from '):
                # Check if Path is already imported somehow
                if 'pathlib' in stripped and 'Path' in stripped:
                    return False  # Already has Path import
                # Insert after other imports
                insert_line = i + 1
                continue
                
            # If we hit non-import code, insert before it
            if stripped and not stripped.startswith('#'):
                break
        
        # Insert the import
        lines.insert(insert_line, 'from pathlib import Path\n')
        
        # Write back
        with open(file_path, 'w') as f:
            f.writelines(lines)
        
        return True
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function."""
    print("Finding files missing Path imports...")
    
    files_to_fix = find_files_using_path_without_import()
    
    if not files_to_fix:
        print("No files need Path import fixes!")
        return 0
    
    print(f"Found {len(files_to_fix)} files missing Path imports:")
    for file_path in files_to_fix:
        print(f"  {file_path}")
    
    print(f"\nFixing {len(files_to_fix)} files...")
    fixed_count = 0
    
    for file_path in files_to_fix:
        if add_path_import(file_path):
            print(f"✅ Fixed: {file_path}")
            fixed_count += 1
        else:
            print(f"❌ Failed: {file_path}")
    
    print(f"\n🎉 Successfully fixed {fixed_count}/{len(files_to_fix)} files!")
    return 0

if __name__ == "__main__":
    sys.exit(main())