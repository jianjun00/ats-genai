#!/usr/bin/env python3
"""
Fix All Remaining Syntax Errors

Comprehensive script to find and fix ALL remaining syntax errors including:
1. Literal \\n characters in imports
2. Malformed import statements
3. Any other syntax issues from the comprehensive script
"""

import os
import re
import subprocess
from pathlib import Path

def find_syntax_error_files():
    """Find all files with syntax errors by actually testing them."""
    print("🔍 Finding files with syntax errors by testing compilation...")
    
    files_with_errors = []
    test_dirs = ['tests/', 'src/']
    
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            for file_path in Path(test_dir).rglob('*.py'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Try to compile the file
                    compile(content, str(file_path), 'exec')
                    
                except SyntaxError as e:
                    files_with_errors.append((str(file_path), str(e)))
                except Exception:
                    # Skip files we can't read
                    continue
    
    return files_with_errors

def fix_literal_newlines_in_content(content):
    """Fix literal \\n characters that should be actual newlines."""
    # Pattern 1: \\nfrom or \\nimport at start of line or after other content
    content = re.sub(r'\\n(from|import)', r'\n\1', content)
    
    # Pattern 2: \\n#!/usr/bin/env python3
    content = re.sub(r'\\n#!/', r'\n#!/', content)
    
    # Pattern 3: Other \\n patterns that should be newlines
    content = re.sub(r'([^\\])\\n([a-zA-Z_])', r'\1\n\2', content)
    
    # Pattern 4: At beginning of line
    content = re.sub(r'^\\n', '\n', content, flags=re.MULTILINE)
    
    return content

def fix_syntax_error_file(file_path, error_message):
    """Fix syntax errors in a specific file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply comprehensive fixes
        content = fix_literal_newlines_in_content(content)
        
        # Additional specific fixes based on error patterns
        if 'unexpected character after line continuation character' in error_message:
            # Fix \\n issues more aggressively
            content = re.sub(r'\\n', '\n', content)
        
        if 'invalid syntax' in error_message:
            # Fix common invalid syntax patterns
            content = re.sub(r'([^"])\\n([^"])', r'\1\n\2', content)
        
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
        compile(content, file_path, 'exec')
        return True
    except SyntaxError:
        return False
    except Exception:
        return False

def main():
    """Main fix workflow."""
    print("🔧 FIXING ALL REMAINING SYNTAX ERRORS")
    print("=" * 50)
    
    # Find all files with syntax errors
    files_with_errors = find_syntax_error_files()
    
    if not files_with_errors:
        print("✅ No syntax errors found!")
        return
    
    print(f"📊 Found {len(files_with_errors)} files with syntax errors")
    
    # Fix each file
    fixed_count = 0
    failed_count = 0
    
    for file_path, error_message in files_with_errors:
        print(f"\n🔧 Fixing: {file_path}")
        print(f"   Error: {error_message}")
        
        success, message = fix_syntax_error_file(file_path, error_message)
        
        if success:
            # Verify the fix worked
            if verify_syntax_fix(file_path):
                print(f"✅ {file_path}: {message} (verified)")
                fixed_count += 1
            else:
                print(f"⚠️ {file_path}: Fix applied but syntax still invalid")
                failed_count += 1
        else:
            print(f"❌ {file_path}: {message}")
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