#!/usr/bin/env python3
"""
Fix Malformed Import Syntax Errors

The comprehensive script introduced syntax errors by creating malformed import
statements with literal \\n characters instead of actual newlines. This script
systematically fixes all these syntax errors.
"""

import os
import re
from pathlib import Path

def fix_malformed_imports(file_path):
    """Fix malformed import statements with literal \\n characters."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Fix malformed import statements with literal \n
        patterns_to_fix = [
            # Pattern: import module\nimport other_module
            (r'import ([^\\]+)\\nimport', r'import \1\nimport'),
            # Pattern: import module\n#!/usr/bin/env python3
            (r'import ([^\\]+)\\n#!/', r'import \1\n#!/'),
            # Pattern: Other variations with \n in imports
            (r'\\nimport', r'\nimport'),
            (r'\\nfrom', r'\nfrom'),
            (r'\\n#!/', r'\n#!/'),
        ]
        
        changes_made = False
        for pattern, replacement in patterns_to_fix:
            new_content = re.sub(pattern, replacement, content)
            if new_content != content:
                content = new_content
                changes_made = True
        
        # Write back if changes were made
        if changes_made:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True, "Fixed malformed import syntax"
        
        return False, "No changes needed"
        
    except Exception as e:
        return False, f"Error: {e}"

def find_files_with_syntax_errors():
    """Find all Python files that likely have syntax errors."""
    test_dirs = ['tests/', 'src/']
    files_with_issues = []
    
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            for file_path in Path(test_dir).rglob('*.py'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        first_line = f.readline()
                        # Check for malformed imports in first line
                        if '\\n' in first_line and ('import' in first_line or '#!/' in first_line):
                            files_with_issues.append(str(file_path))
                except Exception:
                    continue
    
    return files_with_issues

def main():
    """Main fix workflow."""
    print("🔧 FIXING MALFORMED IMPORT SYNTAX ERRORS")
    print("=" * 50)
    
    # Find files with syntax issues
    print("🔍 Finding files with malformed import syntax...")
    files_with_issues = find_files_with_syntax_errors()
    
    if not files_with_issues:
        print("✅ No files found with malformed import syntax")
        return
    
    print(f"📊 Found {len(files_with_issues)} files with potential syntax issues")
    
    # Fix each file
    fixed_count = 0
    failed_count = 0
    
    for file_path in files_with_issues:
        success, message = fix_malformed_imports(file_path)
        if success:
            print(f"✅ {file_path}: {message}")
            fixed_count += 1
        else:
            print(f"❌ {file_path}: {message}")
            failed_count += 1
    
    print(f"\n📊 RESULTS:")
    print(f"✅ Fixed: {fixed_count} files")
    print(f"❌ Failed: {failed_count} files")
    
    if fixed_count > 0:
        print(f"\n🎯 Next: Test collection to verify syntax fixes")
        print("Run: export PYTHONPATH=src && python3 -m pytest --collect-only")

if __name__ == "__main__":
    main()