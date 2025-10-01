#!/usr/bin/env python3
"""
Fix String Literal Issues

Target specific string literal problems that were introduced by the comprehensive script.
"""

import os
import re
import ast
from pathlib import Path

def fix_broken_print_statements(content):
    """Fix broken print statements and string literals."""
    lines = content.split('\n')
    fixed_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Fix: print(" followed by content on next line
        if 'print("' in line and line.count('"') == 1 and not line.rstrip().endswith('"'):
            # Check if next line continues the string
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                # If next line looks like it continues the string
                if not next_line.strip().startswith('print') and next_line.strip():
                    # Combine the lines
                    combined = line.rstrip() + next_line.strip() + '"'
                    fixed_lines.append(combined)
                    i += 2  # Skip next line since we combined it
                    continue
        
        # Fix: f" or f' without closing quote on same line
        if re.search(r'f["\']', line) and line.count('"') == 1 and not line.rstrip().endswith('"'):
            line = line.rstrip() + '"'
        
        # Fix: print(" with missing quote at end
        if 'print("' in line and line.count('"') == 1:
            line = line.rstrip() + '"'
        
        # Fix: print(f" with missing quote
        if 'print(f"' in line and line.count('"') == 1:
            line = line.rstrip() + '"'
        
        fixed_lines.append(line)
        i += 1
    
    return '\n'.join(fixed_lines)

def fix_malformed_multiline_strings(content):
    """Fix malformed multiline strings."""
    # Fix cases where """ got broken
    content = re.sub(r'"""\s*\n\s*"""', '"""Original content"""', content)
    
    # Fix unclosed triple quotes
    if content.count('"""') % 2 == 1:
        content = content.rstrip() + '\n"""'
    
    if content.count("'''") % 2 == 1:
        content = content.rstrip() + "\n'''"
    
    return content

def fix_broken_f_strings(content):
    """Fix broken f-strings."""
    # Fix f" without closing quote
    content = re.sub(r'f"([^"]*?)$', r'f"\1"', content, flags=re.MULTILINE)
    
    # Fix broken f-string patterns
    content = re.sub(r'f"([^"]*?)\n([^"]*?)"', r'f"\1\2"', content)
    
    return content

def fix_syntax_issues_in_file(file_path):
    """Fix syntax issues in a specific file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply fixes in order
        content = fix_broken_print_statements(content)
        content = fix_malformed_multiline_strings(content)
        content = fix_broken_f_strings(content)
        
        # Additional general fixes
        # Fix broken lines where content got split
        content = re.sub(r'print\(\s*\n\s*"', 'print("', content)
        content = re.sub(r'f\s*\n\s*"', 'f"', content)
        
        # Write back if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True, "Fixed string literal issues"
        
        return False, "No changes needed"
        
    except Exception as e:
        return False, f"Error: {e}"

def find_files_with_string_errors():
    """Find files with string literal syntax errors."""
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
                    if 'string literal' in str(e) or 'f-string' in str(e):
                        files_with_errors.append(str(file_path))
                except Exception:
                    continue
    
    return files_with_errors

def main():
    """Main fix workflow."""
    print("🔧 FIXING STRING LITERAL SYNTAX ERRORS")
    print("=" * 50)
    
    # Find files with string literal errors
    files_with_errors = find_files_with_string_errors()
    
    if not files_with_errors:
        print("✅ No string literal errors found!")
        return
    
    print(f"📊 Found {len(files_with_errors)} files with string literal errors")
    
    # Fix each file
    fixed_count = 0
    failed_count = 0
    
    for file_path in files_with_errors:
        success, message = fix_syntax_issues_in_file(file_path)
        if success:
            # Verify the fix worked
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                ast.parse(content, filename=file_path)
                print(f"✅ {file_path}: {message}")
                fixed_count += 1
            except SyntaxError:
                print(f"⚠️ {file_path}: Fix applied but syntax still invalid")
                failed_count += 1
        else:
            print(f"❌ {file_path}: {message}")
            failed_count += 1
    
    print(f"\n📊 RESULTS:")
    print(f"✅ Fixed: {fixed_count} files")
    print(f"❌ Failed: {failed_count} files")
    
    if fixed_count > 0:
        print(f"\n🎯 Next: Test collection to verify fixes")

if __name__ == "__main__":
    main()