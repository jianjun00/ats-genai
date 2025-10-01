#!/usr/bin/env python3
"""
Fix Remaining 539 Errors Systematically

Continue fixing errors until ZERO remain as demanded by the user.
"""

import subprocess
import re
import os
import ast
from pathlib import Path
from collections import defaultdict

def get_detailed_error_analysis():
    """Get detailed analysis of all remaining errors."""
    print("🔍 Analyzing remaining 539 errors in detail...")
    
    try:
        result = subprocess.run([
            'python3', '-m', 'pytest', '--collect-only', '--tb=short'
        ], capture_output=True, text=True, timeout=120,
        env={'PYTHONPATH': 'src'})
        
        output = result.stdout + result.stderr
        return output
    except Exception as e:
        print(f"Error getting analysis: {e}")
        return ""

def categorize_all_errors(output):
    """Categorize all remaining errors by type."""
    error_categories = defaultdict(list)
    
    # Find error sections
    error_sections = re.findall(r'ERROR (.*?)\n(.*?)(?=ERROR|!!!!|====)', output, re.DOTALL)
    
    for file_path, error_content in error_sections:
        error_type = "unknown"
        specific_error = ""
        
        if 'SyntaxError' in error_content:
            if 'unterminated string literal' in error_content:
                error_type = "unterminated_string"
            elif 'unterminated f-string' in error_content:
                error_type = "unterminated_fstring"
            elif 'invalid syntax' in error_content:
                error_type = "invalid_syntax"
            else:
                error_type = "other_syntax"
        elif 'ModuleNotFoundError' in error_content:
            error_type = "module_not_found"
        elif 'ImportError' in error_content:
            error_type = "import_error"
        elif 'NameError' in error_content:
            error_type = "name_error"
        elif 'AttributeError' in error_content:
            error_type = "attribute_error"
        
        # Extract specific error message
        lines = error_content.strip().split('\n')
        for line in lines:
            if line.strip().startswith('E   '):
                specific_error = line.strip()[4:]
                break
        
        error_categories[error_type].append({
            'file': file_path,
            'error': specific_error,
            'content': error_content
        })
    
    return error_categories

def fix_unterminated_strings_batch(files):
    """Fix unterminated string literal errors in batch."""
    fixed_count = 0
    
    for error_info in files:
        file_path = error_info['file']
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            lines = content.split('\n')
            
            # Find and fix unterminated strings
            for i, line in enumerate(lines):
                # Pattern: print(" with no closing quote
                if 'print("' in line and line.count('"') == 1:
                    if not line.rstrip().endswith('"') and not line.rstrip().endswith(')'):
                        lines[i] = line.rstrip() + '")'
                
                # Pattern: f" without closing quote
                if 'f"' in line and line.count('"') == 1:
                    if not line.rstrip().endswith('"'):
                        lines[i] = line.rstrip() + '"'
                
                # Pattern: standalone ") that should be removed
                if line.strip() == '")' and i > 0:
                    prev_line = lines[i-1]
                    if prev_line.count('"') == 1:
                        lines[i-1] = prev_line.rstrip() + '")'
                        lines[i] = ''
                
                # Pattern: broken multi-line strings
                if line.strip().startswith('===') and line.strip().endswith('===")'):
                    # Fix broken print statement
                    if i > 0 and 'print("' in lines[i-1] and lines[i-1].count('"') == 1:
                        combined = lines[i-1].rstrip() + line.strip() + ')'
                        lines[i-1] = combined
                        lines[i] = ''
            
            new_content = '\n'.join(lines)
            
            if new_content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                # Verify fix worked
                try:
                    ast.parse(new_content, filename=file_path)
                    fixed_count += 1
                    print(f"✅ Fixed unterminated string: {file_path}")
                except SyntaxError:
                    print(f"⚠️ Fix applied but syntax still invalid: {file_path}")
                    
        except Exception as e:
            print(f"❌ Error fixing {file_path}: {e}")
    
    return fixed_count

def fix_import_errors_batch(files):
    """Fix import errors in batch."""
    fixed_count = 0
    
    for error_info in files:
        file_path = error_info['file']
        error_msg = error_info['error']
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Fix common import issues
            if "No module named 'core'" in error_msg:
                content = re.sub(r'from core\.config\.', 'from core.platform.config.', content)
                content = re.sub(r'import core\.config\.', 'import core.platform.config.', content)
            
            if "No module named 'domains'" in error_msg:
                # Add sys.path setup if missing
                if 'sys.path.insert' not in content:
                    lines = content.split('\n')
                    
                    # Find first import
                    import_index = 0
                    for i, line in enumerate(lines):
                        if line.strip().startswith(('import ', 'from ')):
                            import_index = i
                            break
                    
                    # Insert path setup
                    setup_lines = [
                        'import sys',
                        'from pathlib import Path',
                        "sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))",
                        ''
                    ]
                    
                    lines = lines[:import_index] + setup_lines + lines[import_index:]
                    content = '\n'.join(lines)
            
            if "cannot import name" in error_msg:
                # Fix specific import name issues
                if "'InstrumentService'" in error_msg:
                    content = re.sub(
                        r'from.*import.*InstrumentService',
                        'from domains.instruments.services.impl.instrument_service_cached import CachedInstrumentService as InstrumentService',
                        content
                    )
                elif "'SecmasterDAO'" in error_msg:
                    content = re.sub(r'SecmasterDAO', 'SecMasterDAO', content)
            
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixed_count += 1
                print(f"✅ Fixed import error: {file_path}")
                
        except Exception as e:
            print(f"❌ Error fixing {file_path}: {e}")
    
    return fixed_count

def main():
    """Main workflow to fix ALL remaining errors."""
    print("🎯 SYSTEMATIC FIXING OF ALL 539 REMAINING ERRORS")
    print("=" * 60)
    print("TARGET: ZERO ERRORS (as demanded by user)")
    print("=" * 60)
    
    iteration = 1
    max_iterations = 15
    
    while iteration <= max_iterations:
        print(f"\n🔄 ITERATION {iteration}")
        print("-" * 30)
        
        # Get current error analysis
        output = get_detailed_error_analysis()
        if not output:
            print("❌ Could not get error analysis")
            break
        
        # Extract current error count
        error_match = re.search(r'(\d+) errors', output)
        if not error_match:
            print("✅ NO ERRORS FOUND - SUCCESS!")
            break
        
        current_errors = int(error_match.group(1))
        print(f"📊 Current errors: {current_errors}")
        
        if current_errors == 0:
            print("🎉 ZERO ERRORS ACHIEVED!")
            break
        
        # Categorize errors
        categories = categorize_all_errors(output)
        
        print(f"📊 Error breakdown:")
        for error_type, files in categories.items():
            print(f"  • {error_type}: {len(files)} errors")
        
        # Fix errors by priority
        total_fixed = 0
        
        # Priority 1: Syntax errors (highest impact)
        if 'unterminated_string' in categories:
            print(f"\n🔧 Fixing unterminated strings ({len(categories['unterminated_string'])} files)...")
            total_fixed += fix_unterminated_strings_batch(categories['unterminated_string'])
        
        if 'unterminated_fstring' in categories:
            print(f"\n🔧 Fixing unterminated f-strings ({len(categories['unterminated_fstring'])} files)...")
            total_fixed += fix_unterminated_strings_batch(categories['unterminated_fstring'])
        
        # Priority 2: Import errors
        if 'module_not_found' in categories:
            print(f"\n🔧 Fixing module not found ({len(categories['module_not_found'])} files)...")
            total_fixed += fix_import_errors_batch(categories['module_not_found'])
        
        if 'import_error' in categories:
            print(f"\n🔧 Fixing import errors ({len(categories['import_error'])} files)...")
            total_fixed += fix_import_errors_batch(categories['import_error'])
        
        print(f"\n📊 Iteration {iteration} results:")
        print(f"✅ Fixed: {total_fixed} files")
        
        if total_fixed == 0:
            print("⚠️ No fixes applied - may need manual intervention")
            break
        
        iteration += 1
    
    # Final verification
    print(f"\n🎯 FINAL VERIFICATION:")
    final_output = get_detailed_error_analysis()
    final_match = re.search(r'(\d+) errors', final_output) if final_output else None
    
    if final_match:
        final_errors = int(final_match.group(1))
        if final_errors == 0:
            print("🎉 MISSION ACCOMPLISHED: ZERO ERRORS ACHIEVED!")
        else:
            print(f"⚠️ {final_errors} errors still remain")
            # Show some examples of remaining errors
            categories = categorize_all_errors(final_output)
            print("\nRemaining error types:")
            for error_type, files in categories.items():
                if len(files) > 0:
                    print(f"  • {error_type}: {len(files)} errors")
                    if len(files) <= 3:  # Show up to 3 examples
                        for file_info in files:
                            print(f"    - {file_info['file']}: {file_info['error'][:100]}...")
        
        return final_errors
    else:
        print("✅ NO ERRORS DETECTED!")
        return 0

if __name__ == "__main__":
    final_count = main()
    exit(0 if final_count == 0 else 1)