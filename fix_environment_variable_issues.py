#!/usr/bin/env python3
"""
Fix environment variable issues in tests.

Finds tests that require ENVIRONMENT variable and adds proper setup
or modifies them to use test-specific environment configuration.
"""

import os
import re
import subprocess
import sys

def find_files_with_environment_errors():
    """Find files that fail due to missing ENVIRONMENT variable."""
    problem_files = []
    
    # Common patterns that indicate environment issues
    patterns_to_check = [
        r'env = Environment\(EnvironmentType\.TEST\)',
        r'Environment\(',
        r'from.*environment.*import',
        r'environment\.get_db_connection',
    ]
    
    result = subprocess.run([
        'find', 'tests/', '-name', '*.py', '-type', 'f'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        return []
    
    test_files = result.stdout.strip().split('\n')
    
    for file_path in test_files:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check if file has environment-related code
            has_env_code = any(re.search(pattern, content) for pattern in patterns_to_check)
            
            if has_env_code:
                # Quick test if this file has environment issues
                test_result = subprocess.run([
                    'python3', '-m', 'pytest', '--collect-only', file_path
                ], capture_output=True, text=True, env={'PYTHONPATH': 'src'})
                
                if ('RuntimeError' in test_result.stderr and 
                    'ENVIRONMENT variable must be set' in test_result.stderr):
                    problem_files.append(file_path)
                    
        except Exception:
            continue
    
    return problem_files

def fix_environment_in_file(file_path):
    """Fix environment issues in a file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Track if we made changes
        changes_made = False
        
        # Strategy 1: Add environment variable setup at the top
        if ('import os' not in content and 
            'Environment(' in content and 
            'ENVIRONMENT variable must be set' not in content):
            
            # Find where to insert the environment setup
            lines = content.split('\n')
            insert_line = 0
            
            # Find the right place after imports
            for i, line in enumerate(lines):
                if (line.strip().startswith('from ') or 
                    line.strip().startswith('import ') or
                    line.strip().startswith('#') or
                    line.strip() == '' or
                    line.strip().startswith('"""') or
                    line.strip().startswith("'''")):
                    insert_line = i + 1
                else:
                    break
            
            # Insert environment setup
            env_setup = [
                '',
                '# Set up test environment',
                'import os',
                'os.environ.setdefault("ENVIRONMENT", "test")',
                ''
            ]
            
            for j, setup_line in enumerate(env_setup):
                lines.insert(insert_line + j, setup_line)
            
            content = '\n'.join(lines)
            changes_made = True
        
        # Strategy 2: For files that already import os, just add the setdefault
        elif ('import os' in content and 
              'os.environ.setdefault("ENVIRONMENT"' not in content and
              'Environment(' in content):
            
            lines = content.split('\n')
            
            # Find the line with 'import os'
            for i, line in enumerate(lines):
                if 'import os' in line:
                    # Insert after this import
                    lines.insert(i + 1, 'os.environ.setdefault("ENVIRONMENT", "test")')
                    changes_made = True
                    break
            
            content = '\n'.join(lines)
        
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
    print("Finding files with ENVIRONMENT variable issues...")
    
    files_to_fix = find_files_with_environment_errors()
    
    if not files_to_fix:
        print("No files need environment variable fixes!")
        return 0
    
    print(f"Found {len(files_to_fix)} files with environment issues:")
    for file_path in files_to_fix:
        print(f"  {file_path}")
    
    print(f"\nFixing {len(files_to_fix)} files...")
    fixed_count = 0
    
    for file_path in files_to_fix:
        if fix_environment_in_file(file_path):
            print(f"✅ Fixed: {file_path}")
            fixed_count += 1
        else:
            print(f"❌ No changes: {file_path}")
    
    print(f"\n🎉 Successfully fixed {fixed_count}/{len(files_to_fix)} files!")
    return 0

if __name__ == "__main__":
    sys.exit(main())