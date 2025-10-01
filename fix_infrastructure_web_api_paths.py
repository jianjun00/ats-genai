#!/usr/bin/env python3
"""
Fix infrastructure.web.api import path issues systematically.

Changes incorrect imports from:
  from infrastructure.web.api.* import *
To:
  from infrastructure.web.web_services.api.* import *
"""

import os
import re
import subprocess
import sys

def find_files_with_incorrect_web_api_imports():
    """Find all files importing from infrastructure.web.api incorrectly."""
    result = subprocess.run([
        'grep', '-r', 'from infrastructure\\.web\\.api', 'tests/', '--include=*.py'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        return []
    
    files = set()
    for line in result.stdout.strip().split('\n'):
        if ':' in line:
            file_path = line.split(':')[0]
            files.add(file_path)
    
    return sorted(files)

def fix_web_api_imports(file_path):
    """Fix web api imports in a file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Track if we made changes
        changes_made = False
        original_content = content
        
        # Fix all infrastructure.web.api imports
        patterns = [
            (r'from infrastructure\.web\.api\.([^\s]+) import', 
             r'from infrastructure.web.web_services.api.\1 import'),
            (r'import infrastructure\.web\.api\.([^\s]+)', 
             r'import infrastructure.web.web_services.api.\1'),
        ]
        
        for pattern, replacement in patterns:
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                changes_made = True
        
        # Write back if changes were made
        if changes_made:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Main function."""
    print("Finding files with incorrect infrastructure.web.api imports...")
    
    files_to_fix = find_files_with_incorrect_web_api_imports()
    
    if not files_to_fix:
        print("No files need infrastructure.web.api import fixes!")
        return 0
    
    print(f"Found {len(files_to_fix)} files with incorrect imports:")
    for file_path in files_to_fix:
        print(f"  {file_path}")
    
    print(f"\nFixing {len(files_to_fix)} files...")
    fixed_count = 0
    
    for file_path in files_to_fix:
        if fix_web_api_imports(file_path):
            print(f"✅ Fixed: {file_path}")
            fixed_count += 1
        else:
            print(f"❌ No changes: {file_path}")
    
    print(f"\n🎉 Successfully fixed {fixed_count}/{len(files_to_fix)} files!")
    return 0

if __name__ == "__main__":
    sys.exit(main())