#!/usr/bin/env python3
"""
Fix Final Syntax Errors

Comprehensive script to fix ALL remaining syntax errors by:
1. Finding all files that still have syntax errors
2. Applying targeted fixes for each error type
3. Verifying fixes work
"""

import os
import re
import subprocess
import ast
from pathlib import Path

def get_syntax_error_files():
    """Find all files that still have syntax errors."""
    print("🔍 Finding files with remaining syntax errors...")
    
    files_with_errors = []
    test_dirs = ['tests/', 'src/']
    
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            for file_path in Path(test_dir).rglob('*.py'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Try to compile the file
                    ast.parse(content, filename=str(file_path))
                    
                except SyntaxError as e:
                    files_with_errors.append((str(file_path), str(e), e.lineno))
                except Exception:
                    continue
    
    return files_with_errors

def fix_unterminated_strings(content):
    """Fix unterminated string literals."""
    lines = content.split('\n')
    fixed_lines = []
    
    for i, line in enumerate(lines):
        # Check for unterminated strings
        if 'print("' in line and not line.rstrip().endswith('"') and not line.rstrip().endswith("')"):
            # Simple case: add missing quote
            if line.count('"') % 2 == 1:  # Odd number of quotes
                line = line.rstrip() + '"'
        
        # Fix other common string issues
        if 'f"' in line and not line.rstrip().endswith('"') and not line.rstrip().endswith("')"):
            if line.count('"') % 2 == 1:
                line = line.rstrip() + '"'
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def fix_malformed_imports_aggressive(content):
    """More aggressive fixing of import issues."""
    # Fix remaining \\n patterns
    content = re.sub(r'\\n', '\n', content)
    
    # Fix cases where import lines got mangled
    content = re.sub(r'\nimport\s*\n', '\n', content)
    content = re.sub(r'\nfrom\s*\n', '\n', content)
    
    # Fix double import statements
    content = re.sub(r'(import [^\n]+)\s+import', r'\1\nimport', content)
    content = re.sub(r'(from [^\n]+)\s+from', r'\1\nfrom', content)
    
    return content

def fix_indentation_issues(content):
    """Fix basic indentation issues."""
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        # Fix common indentation problems
        if line.strip() and not line.startswith(' ') and not line.startswith('\t'):
            # Check if this should be indented (after def, class, if, etc.)
            if len(fixed_lines) > 0:
                prev_line = fixed_lines[-1].strip()
                if prev_line.endswith(':') and line.strip() not in ['"""', "'''", 'pass']:
                    line = '    ' + line.strip()
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def fix_quote_issues(content):
    """Fix quote and string issues."""
    # Fix unclosed triple quotes
    if content.count('"""') % 2 == 1:
        content = content + '\n"""'
    
    if content.count("'''") % 2 == 1:
        content = content + "\n'''"
    
    return content

def fix_syntax_error_file(file_path, error_message, line_no):
    """Fix syntax errors in a specific file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply fixes based on error type
        if 'unterminated string literal' in error_message:
            content = fix_unterminated_strings(content)
        
        if 'unexpected character after line continuation' in error_message:
            content = fix_malformed_imports_aggressive(content)
        
        if 'expected an indented block' in error_message:
            content = fix_indentation_issues(content)
        
        if 'EOF while scanning' in error_message:
            content = fix_quote_issues(content)
        
        # Apply general fixes
        content = fix_malformed_imports_aggressive(content)
        
        # Write back if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True, "Fixed syntax errors"
        
        return False, "No changes made"
        
    except Exception as e:
        return False, f"Error fixing file: {e}"

def verify_syntax_fix(file_path):
    """Verify that the syntax fix worked."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        ast.parse(content, filename=file_path)
        return True
    except SyntaxError:
        return False
    except Exception:
        return False

def main():
    """Main fix workflow."""
    print("🔧 FIXING ALL FINAL SYNTAX ERRORS")
    print("=" * 50)
    
    # Find all files with syntax errors
    files_with_errors = get_syntax_error_files()
    
    if not files_with_errors:
        print("✅ No syntax errors found!")
        return
    
    print(f"📊 Found {len(files_with_errors)} files with syntax errors")
    
    # Group by error type for analysis
    error_types = {}
    for file_path, error_message, line_no in files_with_errors:
        error_type = error_message.split('(')[0].strip()
        if error_type not in error_types:
            error_types[error_type] = []
        error_types[error_type].append(file_path)
    
    print("\n📊 Error type breakdown:")
    for error_type, files in error_types.items():
        print(f"  • {error_type}: {len(files)} files")
    
    # Fix each file
    fixed_count = 0
    failed_count = 0
    
    print(f"\n🔧 Fixing files...")
    
    for file_path, error_message, line_no in files_with_errors:
        success, message = fix_syntax_error_file(file_path, error_message, line_no)
        
        if success:
            # Verify the fix worked
            if verify_syntax_fix(file_path):
                fixed_count += 1
                if fixed_count % 50 == 0:
                    print(f"✅ Fixed {fixed_count} files so far...")
            else:
                failed_count += 1
        else:
            failed_count += 1
    
    print(f"\n📊 FINAL RESULTS:")
    print(f"✅ Successfully fixed: {fixed_count} files")
    print(f"❌ Failed to fix: {failed_count} files")
    
    if fixed_count > 0:
        print(f"\n🎯 Testing collection after fixes...")
        # Quick test to see improvement
        try:
            result = subprocess.run([
                'python3', '-m', 'pytest', '--collect-only', '--tb=no'
            ], capture_output=True, text=True, timeout=60,
            env={'PYTHONPATH': 'src'})
            
            output = result.stdout + result.stderr
            if 'collected' in output:
                # Extract numbers
                import re
                matches = re.search(r'(\d+) tests collected.*?(\d+) errors', output)
                if matches:
                    tests_collected = matches.group(1)
                    errors_remaining = matches.group(2)
                    print(f"📊 Progress: {tests_collected} tests collected, {errors_remaining} errors remaining")
                
        except Exception as e:
            print(f"⚠️ Could not run test collection: {e}")

if __name__ == "__main__":
    main()