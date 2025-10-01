#!/usr/bin/env python3
"""
Fix ALL String Syntax Errors

Systematically find and fix every single string literal syntax error
to achieve ZERO errors as requested by the user.
"""

import os
import re
import ast
import subprocess
from pathlib import Path

def find_all_syntax_error_files():
    """Find every single file with syntax errors."""
    print("🔍 Finding ALL files with syntax errors...")
    
    syntax_error_files = []
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
                    syntax_error_files.append((str(file_path), str(e), e.lineno))
                except Exception:
                    continue
    
    return syntax_error_files

def fix_broken_string_patterns(content):
    """Fix all known broken string patterns."""
    lines = content.split('\n')
    fixed_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Pattern 1: print("" followed by content on next line
        if 'print(""' in line and i + 1 < len(lines):
            next_line = lines[i + 1]
            if next_line.strip() and not next_line.strip().startswith('print'):
                # Combine the lines properly
                content_match = re.search(r'print\(""\s*$', line)
                if content_match:
                    # Extract content from next line
                    content_part = next_line.strip()
                    if content_part.endswith('")'):
                        content_part = content_part[:-2]  # Remove ending ")
                    elif content_part.endswith('"'):
                        content_part = content_part[:-1]  # Remove ending "
                    
                    # Create fixed line
                    fixed_line = line.replace('print(""', f'print("{content_part}"')
                    fixed_lines.append(fixed_line)
                    i += 2  # Skip next line
                    continue
        
        # Pattern 2: f.write(f"something" followed by newline and ")
        if 'f.write(f"' in line and line.count('"') == 2 and not line.rstrip().endswith(')'):
            if i + 1 < len(lines) and lines[i + 1].strip() == '")':
                # Add missing \n and close properly
                line = line.rstrip() + '\n")'
                i += 2  # Skip the separate ") line
                fixed_lines.append(line)
                continue
        
        # Pattern 3: Unterminated f-strings
        if 'f"' in line and line.count('"') % 2 == 1:
            # Add missing closing quote
            line = line.rstrip() + '"'
        
        # Pattern 4: Unterminated regular strings
        if 'print("' in line and line.count('"') == 1:
            line = line.rstrip() + '"'
        
        # Pattern 5: Broken multi-line strings
        if line.strip() == '")' and i > 0:
            prev_line = fixed_lines[-1] if fixed_lines else ""
            if 'f"' in prev_line and prev_line.count('"') == 1:
                # Fix previous line instead
                fixed_lines[-1] = prev_line.rstrip() + '")'
                i += 1  # Skip this ") line
                continue
        
        fixed_lines.append(line)
        i += 1
    
    return '\n'.join(fixed_lines)

def fix_syntax_error_file(file_path, error_message, line_no):
    """Fix syntax errors in a specific file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply comprehensive string fixes
        content = fix_broken_string_patterns(content)
        
        # Additional targeted fixes based on error message
        if 'unterminated string literal' in error_message:
            # Fix line-specific issues around the error line
            lines = content.split('\n')
            if 0 <= line_no - 1 < len(lines):
                error_line = lines[line_no - 1]
                
                # Fix specific patterns
                if error_line.strip() == '")':
                    # Look for unclosed string in previous lines
                    for j in range(max(0, line_no - 5), line_no - 1):
                        if j < len(lines):
                            prev_line = lines[j]
                            if ('f"' in prev_line or 'print("' in prev_line) and prev_line.count('"') == 1:
                                lines[j] = prev_line.rstrip() + '")'
                                lines[line_no - 1] = ''  # Remove the standalone ")
                                break
                
                elif 'f"' in error_line and error_line.count('"') == 1:
                    lines[line_no - 1] = error_line.rstrip() + '"'
                
                elif 'print("' in error_line and error_line.count('"') == 1:
                    lines[line_no - 1] = error_line.rstrip() + '"'
            
            content = '\n'.join(lines)
        
        # Fix unclosed triple quotes
        if content.count('"""') % 2 == 1:
            content = content.rstrip() + '\n"""'
        
        if content.count("'''") % 2 == 1:
            content = content.rstrip() + "\n'''"
        
        # Write back if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True, "Fixed string syntax errors"
        
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

def get_current_error_count():
    """Get current error count from pytest collection."""
    try:
        result = subprocess.run([
            'python3', '-m', 'pytest', '--collect-only', '--tb=no'
        ], capture_output=True, text=True, timeout=60,
        env={'PYTHONPATH': 'src'})
        
        output = result.stdout + result.stderr
        if 'errors' in output:
            import re
            matches = re.search(r'(\d+) errors', output)
            if matches:
                return int(matches.group(1))
        return 0
    except Exception:
        return -1

def main():
    """Main workflow - fix ALL errors until ZERO remain."""
    print("🔧 FIXING ALL STRING SYNTAX ERRORS UNTIL ZERO REMAIN")
    print("=" * 60)
    
    iteration = 1
    max_iterations = 10  # Prevent infinite loops
    
    while iteration <= max_iterations:
        print(f"\n🔄 ITERATION {iteration}")
        print("-" * 30)
        
        # Find current syntax errors
        syntax_error_files = find_all_syntax_error_files()
        
        if not syntax_error_files:
            print("✅ NO SYNTAX ERRORS FOUND!")
            break
        
        print(f"📊 Found {len(syntax_error_files)} files with syntax errors")
        
        # Fix each file
        fixed_count = 0
        failed_count = 0
        
        for file_path, error_message, line_no in syntax_error_files:
            success, message = fix_syntax_error_file(file_path, error_message, line_no)
            
            if success and verify_syntax_fix(file_path):
                fixed_count += 1
                if fixed_count <= 10:  # Show first 10 fixes
                    print(f"✅ Fixed: {file_path}")
            else:
                failed_count += 1
                if failed_count <= 5:  # Show first 5 failures
                    print(f"❌ Failed: {file_path} - {message}")
        
        print(f"\n📊 Iteration {iteration} Results:")
        print(f"✅ Fixed: {fixed_count} files")
        print(f"❌ Failed: {failed_count} files")
        
        # Check total error count
        total_errors = get_current_error_count()
        if total_errors == 0:
            print(f"\n🎉 SUCCESS: ZERO ERRORS ACHIEVED!")
            break
        elif total_errors > 0:
            print(f"📊 Total errors remaining: {total_errors}")
        
        iteration += 1
    
    # Final verification
    print(f"\n🎯 FINAL VERIFICATION:")
    final_errors = get_current_error_count()
    
    if final_errors == 0:
        print("🎉 MISSION ACCOMPLISHED: ZERO TEST ERRORS!")
    else:
        print(f"⚠️ {final_errors} errors still remain - need additional fixing")
    
    return final_errors

if __name__ == "__main__":
    final_error_count = main()
    exit(0 if final_error_count == 0 else 1)