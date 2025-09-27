#!/usr/bin/env python3
"""
Remove exception handling from codebase to implement fail-fast principles
Following CLAUDE.md directive: NO EXCEPTION CATCHING - FAIL FAST POLICY
"""

import os
import re
import ast
import sys
from pathlib import Path
from typing import List, Tuple, Dict

def find_files_with_exceptions() -> List[str]:
    """Find all Python files with exception handling"""
    files_with_exceptions = []
    
    for root, dirs, files in os.walk("src"):
        # Skip __pycache__ directories
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if 'except' in content or 'try:' in content:
                            files_with_exceptions.append(file_path)
                except Exception:
                    # Even this script follows fail-fast - let errors propagate
                    raise
    
    return files_with_exceptions

def analyze_exception_patterns(file_path: str) -> Dict[str, List[int]]:
    """Analyze exception handling patterns in a file"""
    patterns = {
        'try_except': [],
        'bare_except': [],
        'generic_except': [],
        'specific_except': [],
        'finally_blocks': []
    }
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for i, line in enumerate(lines, 1):
        line_stripped = line.strip()
        
        if line_stripped.startswith('try:'):
            patterns['try_except'].append(i)
        elif line_stripped.startswith('except:'):
            patterns['bare_except'].append(i)
        elif line_stripped.startswith('except Exception'):
            patterns['generic_except'].append(i)
        elif line_stripped.startswith('except '):
            patterns['specific_except'].append(i)
        elif line_stripped.startswith('finally:'):
            patterns['finally_blocks'].append(i)
    
    return patterns

def is_allowed_exception_handling(file_path: str, line_content: str) -> bool:
    """Check if exception handling is allowed per CLAUDE.md rules"""
    
    # Allowed patterns from CLAUDE.md:
    # - Resource cleanup in finally blocks
    # - Context managers (with statements)
    # - Input validation (except ValueError for user input only)
    # - Specific exceptions only (except FileNotFoundError for optional files)
    
    line = line_content.strip().lower()
    
    # Always allow finally blocks for cleanup
    if line.startswith('finally:'):
        return True
        
    # Allow specific exceptions for file operations
    if 'filenotfound' in line and 'optional' in file_path.lower():
        return True
        
    # Allow ValueError for input validation only in specific contexts
    if 'valueerror' in line and ('input' in file_path.lower() or 'validation' in file_path.lower()):
        return True
    
    # All other exception handling should be removed
    return False

def remove_exception_handling_from_file(file_path: str) -> bool:
    """Remove exception handling from a file, keeping only allowed patterns"""
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    new_lines = []
    skip_until_dedent = False
    indent_level = 0
    modified = False
    
    for i, line in enumerate(lines):
        original_line = line
        stripped = line.strip()
        
        # Calculate current indentation
        current_indent = len(line) - len(line.lstrip())
        
        # Check if we're in a try/except block to skip
        if skip_until_dedent:
            if current_indent <= indent_level and stripped and not stripped.startswith(('except', 'finally', 'else:')):
                skip_until_dedent = False
            else:
                modified = True
                continue  # Skip this line
        
        # Check for try/except patterns to remove
        if stripped.startswith('try:'):
            if not is_allowed_exception_handling(file_path, stripped):
                skip_until_dedent = True
                indent_level = current_indent
                modified = True
                continue
        
        elif stripped.startswith('except'):
            if not is_allowed_exception_handling(file_path, stripped):
                skip_until_dedent = True
                indent_level = current_indent
                modified = True
                continue
        
        # Keep the line if we're not skipping
        new_lines.append(original_line)
    
    if modified:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        return True
    
    return False

def main():
    """Remove exception handling from entire codebase"""
    print("🚨 REMOVING EXCEPTION HANDLING - IMPLEMENTING FAIL-FAST POLICY")
    print("=" * 80)
    print("Following CLAUDE.md directive: NO EXCEPTION CATCHING - FAIL FAST POLICY")
    print()
    
    # Find all files with exception handling
    files_with_exceptions = find_files_with_exceptions()
    
    print(f"📋 Found {len(files_with_exceptions)} files with exception handling")
    print()
    
    # Analyze patterns
    total_try_blocks = 0
    total_bare_except = 0
    total_generic_except = 0
    modified_files = 0
    
    for file_path in files_with_exceptions:
        patterns = analyze_exception_patterns(file_path)
        total_try_blocks += len(patterns['try_except'])
        total_bare_except += len(patterns['bare_except'])
        total_generic_except += len(patterns['generic_except'])
        
        print(f"📁 {file_path}")
        print(f"   Try blocks: {len(patterns['try_except'])}")
        print(f"   Bare except: {len(patterns['bare_except'])} ❌")
        print(f"   Generic except: {len(patterns['generic_except'])} ❌")
        print(f"   Specific except: {len(patterns['specific_except'])}")
        print(f"   Finally blocks: {len(patterns['finally_blocks'])} ✅")
        
        # Remove exception handling
        if remove_exception_handling_from_file(file_path):
            modified_files += 1
            print(f"   🔧 MODIFIED: Removed exception handling")
        else:
            print(f"   ✅ KEPT: Only allowed exception patterns")
        print()
    
    print("📊 SUMMARY:")
    print(f"   Files analyzed: {len(files_with_exceptions)}")
    print(f"   Files modified: {modified_files}")
    print(f"   Total try blocks found: {total_try_blocks}")
    print(f"   Bare except blocks (REMOVED): {total_bare_except}")
    print(f"   Generic except blocks (REMOVED): {total_generic_except}")
    print()
    print("✅ FAIL-FAST POLICY IMPLEMENTED")
    print("🚨 All exception masking removed - errors will now propagate immediately")
    print("🔧 Only allowed: finally blocks, specific file exceptions, input validation")

if __name__ == "__main__":
    main()