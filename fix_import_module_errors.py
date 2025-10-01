#!/usr/bin/env python3
"""
Fix Import and Module Errors

Focus on fixing ModuleNotFoundError and ImportError issues to maximize
test collection, leaving syntax errors for later focused fixing.
"""

import subprocess
import re
import os
from collections import defaultdict
from pathlib import Path

def get_import_module_errors():
    """Get detailed import and module errors from pytest collection."""
    print("🔍 Collecting import and module errors...")
    
    try:
        result = subprocess.run([
            'python3', '-m', 'pytest', '--collect-only', '--tb=short'
        ], capture_output=True, text=True, timeout=120,
        env={'PYTHONPATH': 'src'})
        
        output = result.stdout + result.stderr
        return output
    except subprocess.TimeoutExpired:
        print("⚠️ Collection timed out")
        return ""
    except Exception as e:
        print(f"❌ Error: {e}")
        return ""

def parse_import_errors(output):
    """Parse import and module errors from pytest output."""
    errors = []
    
    # Find ERROR sections
    error_sections = re.findall(r'ERROR (.*?)\n(.*?)(?=ERROR|!!!!|====)', output, re.DOTALL)
    
    for file_path, error_content in error_sections:
        # Skip syntax errors - focus on import/module errors
        if 'SyntaxError' in error_content:
            continue
            
        # Extract import/module errors
        if 'ModuleNotFoundError' in error_content or 'ImportError' in error_content:
            # Extract the specific error message
            lines = error_content.strip().split('\n')
            error_msg = ""
            
            for line in lines:
                if 'ModuleNotFoundError:' in line or 'ImportError:' in line:
                    error_msg = line.strip()
                    break
            
            if error_msg:
                errors.append({
                    'file': file_path,
                    'error': error_msg,
                    'type': 'ModuleNotFoundError' if 'ModuleNotFoundError' in error_msg else 'ImportError'
                })
    
    return errors

def categorize_import_errors(errors):
    """Categorize import errors by type and pattern."""
    categories = defaultdict(list)
    
    for error in errors:
        error_msg = error['error']
        
        # Categorize by specific patterns
        if "No module named 'core'" in error_msg:
            categories['core_module_missing'].append(error)
        elif "No module named 'domains'" in error_msg:
            categories['domains_module_missing'].append(error)
        elif "No module named 'infrastructure'" in error_msg:
            categories['infrastructure_module_missing'].append(error)
        elif "cannot import name" in error_msg:
            categories['import_name_error'].append(error)
        elif "attempted relative import" in error_msg:
            categories['relative_import_error'].append(error)
        else:
            categories['other_import_error'].append(error)
    
    return categories

def fix_core_module_imports(files):
    """Fix core module import issues."""
    fixed_count = 0
    
    for error in files:
        file_path = error['file']
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Common core module fixes
            content = re.sub(r'from core\.config\.logging_config', 'from core.platform.config.logging_config', content)
            content = re.sub(r'from core\.config\.environment', 'from core.platform.config.environment', content)
            content = re.sub(r'from core\.config\.gin_loader', 'from core.platform.config.gin_loader', content)
            
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixed_count += 1
                print(f"✅ Fixed core imports: {file_path}")
                
        except Exception as e:
            print(f"❌ Error fixing {file_path}: {e}")
    
    return fixed_count

def fix_domains_module_imports(files):
    """Fix domains module import issues."""
    fixed_count = 0
    
    for error in files:
        file_path = error['file']
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Add PYTHONPATH setup if missing
            if 'sys.path.insert' not in content and 'domains.' in content:
                # Add sys path setup at the top
                lines = content.split('\n')
                
                # Find import section
                import_index = 0
                for i, line in enumerate(lines):
                    if line.strip().startswith('import ') or line.strip().startswith('from '):
                        import_index = i
                        break
                
                # Insert sys.path setup before imports
                sys_path_setup = [
                    'import sys',
                    'from pathlib import Path',
                    "sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))",
                    ''
                ]
                
                lines = lines[:import_index] + sys_path_setup + lines[import_index:]
                content = '\n'.join(lines)
            
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixed_count += 1
                print(f"✅ Fixed domains imports: {file_path}")
                
        except Exception as e:
            print(f"❌ Error fixing {file_path}: {e}")
    
    return fixed_count

def fix_import_name_errors(files):
    """Fix 'cannot import name' errors."""
    fixed_count = 0
    
    for error in files:
        file_path = error['file']
        error_msg = error['error']
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Extract the problematic import
            if "cannot import name" in error_msg:
                # Parse error message to find what name is missing
                match = re.search(r"cannot import name '([^']+)'", error_msg)
                if match:
                    missing_name = match.group(1)
                    
                    # Common fixes for missing names
                    if missing_name == 'InstrumentService':
                        content = re.sub(r'from.*import.*InstrumentService', 
                                       'from domains.instruments.services.impl.instrument_service_cached import CachedInstrumentService as InstrumentService', 
                                       content)
                    elif missing_name == 'SecmasterDAO':
                        content = re.sub(r'SecmasterDAO', 'SecMasterDAO', content)
            
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixed_count += 1
                print(f"✅ Fixed import name error: {file_path}")
                
        except Exception as e:
            print(f"❌ Error fixing {file_path}: {e}")
    
    return fixed_count

def main():
    """Main workflow."""
    print("🔧 FIXING IMPORT AND MODULE ERRORS")
    print("=" * 50)
    
    # Get current errors
    output = get_import_module_errors()
    
    if not output:
        print("❌ Could not collect error information")
        return
    
    # Parse and categorize errors
    errors = parse_import_errors(output)
    categories = categorize_import_errors(errors)
    
    print(f"\n📊 Found {len(errors)} import/module errors:")
    for category, files in categories.items():
        print(f"  • {category}: {len(files)} errors")
    
    # Fix each category
    total_fixed = 0
    
    if 'core_module_missing' in categories:
        print(f"\n🔧 Fixing core module imports ({len(categories['core_module_missing'])} files)...")
        total_fixed += fix_core_module_imports(categories['core_module_missing'])
    
    if 'domains_module_missing' in categories:
        print(f"\n🔧 Fixing domains module imports ({len(categories['domains_module_missing'])} files)...")
        total_fixed += fix_domains_module_imports(categories['domains_module_missing'])
    
    if 'import_name_error' in categories:
        print(f"\n🔧 Fixing import name errors ({len(categories['import_name_error'])} files)...")
        total_fixed += fix_import_name_errors(categories['import_name_error'])
    
    print(f"\n📊 RESULTS:")
    print(f"✅ Fixed: {total_fixed} files")
    
    if total_fixed > 0:
        print(f"\n🎯 Testing collection after fixes...")
        try:
            result = subprocess.run([
                'python3', '-m', 'pytest', '--collect-only', '--tb=no'
            ], capture_output=True, text=True, timeout=60,
            env={'PYTHONPATH': 'src'})
            
            output = result.stdout + result.stderr
            if 'collected' in output:
                matches = re.search(r'(\d+) tests collected.*?(\d+) errors', output)
                if matches:
                    tests_collected = matches.group(1)
                    errors_remaining = matches.group(2)
                    print(f"📊 Progress: {tests_collected} tests collected, {errors_remaining} errors remaining")
                
        except Exception as e:
            print(f"⚠️ Could not run test collection: {e}")

if __name__ == "__main__":
    main()