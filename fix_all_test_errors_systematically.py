#!/usr/bin/env python3
"""
Comprehensive test error fixer - systematically fix ALL remaining test errors.

This script identifies and fixes multiple categories of test import errors:
1. Missing SecmasterDAO -> SecMasterDAO class name fixes
2. Environment variable setup issues  
3. Missing optional dependency imports
4. Other common import patterns
"""

import os
import re
import subprocess
import sys
from collections import defaultdict

def get_all_test_files():
    """Get all test files."""
    result = subprocess.run(['find', 'tests/', '-name', '*.py', '-type', 'f'], 
                           capture_output=True, text=True)
    return result.stdout.strip().split('\n') if result.returncode == 0 else []

def fix_secmaster_dao_issues():
    """Fix SecmasterDAO -> SecMasterDAO class name issues."""
    print("🔧 Fixing SecmasterDAO -> SecMasterDAO class name issues...")
    
    result = subprocess.run([
        'grep', '-r', 'SecmasterDAO', 'tests/', '--include=*.py'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print("   No SecmasterDAO issues found")
        return 0
    
    files_to_fix = set()
    for line in result.stdout.strip().split('\n'):
        if ':' in line:
            file_path = line.split(':')[0]
            files_to_fix.add(file_path)
    
    fixed_count = 0
    for file_path in files_to_fix:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Fix the class name
            if 'SecmasterDAO' in content:
                content = content.replace('SecmasterDAO', 'SecMasterDAO')
                
                with open(file_path, 'w') as f:
                    f.write(content)
                
                print(f"   ✅ Fixed SecmasterDAO in: {file_path}")
                fixed_count += 1
                
        except Exception as e:
            print(f"   ❌ Error fixing {file_path}: {e}")
    
    print(f"   🎉 Fixed {fixed_count} files with SecmasterDAO issues")
    return fixed_count

def fix_environment_setup_issues():
    """Fix Environment setup issues in test files."""
    print("🔧 Fixing Environment setup issues...")
    
    # Find files that use Environment but might have issues
    patterns = [
        r'Environment\(EnvironmentType\.',
        r'env = Environment\(',
    ]
    
    files_to_check = []
    for pattern in patterns:
        result = subprocess.run([
            'grep', '-r', pattern, 'tests/', '--include=*.py'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if ':' in line:
                    file_path = line.split(':')[0]
                    files_to_check.append(file_path)
    
    fixed_count = 0
    for file_path in set(files_to_check):
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            changes_made = False
            
            # Fix Environment constructor calls without env_type parameter
            if re.search(r'Environment\(EnvironmentType\.', content):
                content = re.sub(
                    r'Environment\(EnvironmentType\.(\w+)\)',
                    r'Environment(env_type=EnvironmentType.\1)',
                    content
                )
                changes_made = True
            
            # Add environment variable setup if missing
            if ('Environment(' in content and 
                'os.environ.setdefault("ENVIRONMENT"' not in content and
                'import os' not in content):
                
                lines = content.split('\n')
                # Find where to insert imports
                insert_line = 0
                for i, line in enumerate(lines):
                    if (line.strip().startswith('import ') or 
                        line.strip().startswith('from ') or
                        line.strip().startswith('#') or
                        line.strip() == ''):
                        insert_line = i + 1
                    else:
                        break
                
                # Insert os import and environment setup
                env_setup = [
                    'import os',
                    '# Set up test environment',
                    'os.environ.setdefault("ENVIRONMENT", "test")',
                    ''
                ]
                
                for j, setup_line in enumerate(env_setup):
                    lines.insert(insert_line + j, setup_line)
                
                content = '\n'.join(lines)
                changes_made = True
            
            if changes_made:
                with open(file_path, 'w') as f:
                    f.write(content)
                
                print(f"   ✅ Fixed Environment setup in: {file_path}")
                fixed_count += 1
                
        except Exception as e:
            print(f"   ❌ Error fixing {file_path}: {e}")
    
    print(f"   🎉 Fixed {fixed_count} files with Environment setup issues")
    return fixed_count

def fix_optional_dependency_imports():
    """Fix missing optional dependency imports."""
    print("🔧 Fixing optional dependency imports...")
    
    # Common optional dependencies that should be handled with try/except
    optional_deps = {
        'sklearn': 'scikit-learn',
        'playwright': 'playwright',
        'matplotlib': 'matplotlib',
        'seaborn': 'seaborn',
        'plotly': 'plotly',
        'transformers': 'transformers',
        'torch': 'torch',
        'tensorflow': 'tensorflow'
    }
    
    fixed_count = 0
    
    for dep_name, package_name in optional_deps.items():
        # Find files importing this dependency directly
        result = subprocess.run([
            'grep', '-r', f'import {dep_name}', 'tests/', '--include=*.py'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            files_with_dep = set()
            for line in result.stdout.strip().split('\n'):
                if ':' in line:
                    file_path = line.split(':')[0]
                    files_with_dep.add(file_path)
            
            for file_path in files_with_dep:
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                    
                    # Check if it already has try/except or pytest.importorskip
                    if (f'try:.*import {dep_name}' in content or 
                        f'pytest.importorskip("{dep_name}")' in content):
                        continue
                    
                    # Replace direct import with optional import
                    import_pattern = f'import {dep_name}'
                    if import_pattern in content:
                        replacement = f'''try:
    import {dep_name}
except ImportError:
    {dep_name} = None
    pytest.skip(f"Skipping test because {package_name} is not installed")'''
                        
                        content = content.replace(import_pattern, replacement)
                        
                        # Ensure pytest is imported
                        if 'import pytest' not in content:
                            content = 'import pytest\n' + content
                        
                        with open(file_path, 'w') as f:
                            f.write(content)
                        
                        print(f"   ✅ Fixed {dep_name} import in: {file_path}")
                        fixed_count += 1
                        
                except Exception as e:
                    print(f"   ❌ Error fixing {file_path}: {e}")
    
    print(f"   🎉 Fixed {fixed_count} files with optional dependency issues")
    return fixed_count

def fix_common_missing_imports():
    """Fix common missing import issues."""
    print("🔧 Fixing common missing imports...")
    
    common_missing = {
        'Path(__file__)': 'from pathlib import Path',
        'datetime.': 'from datetime import datetime',
        'asyncio.': 'import asyncio',
        'logging.': 'import logging',
        'json.': 'import json',
        'sys.path': 'import sys',
        'os.environ': 'import os',
    }
    
    fixed_count = 0
    
    for usage, import_stmt in common_missing.items():
        result = subprocess.run([
            'grep', '-r', usage, 'tests/', '--include=*.py'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            files_with_usage = set()
            for line in result.stdout.strip().split('\n'):
                if ':' in line:
                    file_path = line.split(':')[0]
                    files_with_usage.add(file_path)
            
            for file_path in files_with_usage:
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                    
                    # Check if import is already present
                    if import_stmt.split()[-1] in content.split('\\n')[0:20]:  # Check first 20 lines
                        continue
                    
                    # Add the missing import
                    lines = content.split('\\n')
                    
                    # Find where to insert
                    insert_line = 0
                    for i, line in enumerate(lines):
                        if line.strip().startswith('import ') or line.strip().startswith('from '):
                            insert_line = i + 1
                        elif line.strip() and not line.strip().startswith('#'):
                            break
                    
                    lines.insert(insert_line, import_stmt)
                    content = '\\n'.join(lines)
                    
                    with open(file_path, 'w') as f:
                        f.write(content)
                    
                    print(f"   ✅ Added missing import to: {file_path}")
                    fixed_count += 1
                    
                except Exception as e:
                    print(f"   ❌ Error fixing {file_path}: {e}")
    
    print(f"   🎉 Fixed {fixed_count} files with missing import issues")
    return fixed_count

def main():
    """Main comprehensive fixing function."""
    print("🚀 COMPREHENSIVE TEST ERROR FIXING - SYSTEMATIC APPROACH")
    print("=" * 70)
    
    total_fixed = 0
    
    # Fix all categories systematically
    total_fixed += fix_secmaster_dao_issues()
    total_fixed += fix_environment_setup_issues()
    total_fixed += fix_optional_dependency_imports()
    total_fixed += fix_common_missing_imports()
    
    print()
    print(f"🎉 TOTAL FILES FIXED: {total_fixed}")
    print("=" * 70)
    
    # Test overall progress
    print("🧪 Testing overall progress...")
    result = subprocess.run([
        'python3', '-m', 'pytest', '--collect-only', 'tests/', '-q'
    ], capture_output=True, text=True, env={'PYTHONPATH': 'src'})
    
    # Count errors and tests
    error_count = result.stderr.count('ERROR tests/')
    
    # Extract test count from output
    if 'tests collected' in result.stdout:
        test_count = result.stdout.split('tests collected')[0].split()[-1]
    else:
        test_count = "unknown"
    
    print(f"📊 CURRENT STATUS:")
    print(f"   Tests collected: {test_count}")
    print(f"   Errors remaining: {error_count}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())