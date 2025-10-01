#!/usr/bin/env python3
"""
Systematic Analysis of Current Test Collection Errors

After the comprehensive fix, we need to identify and categorize the remaining
1267 test collection errors to fix them systematically.
"""

import subprocess
import re
import sys
from collections import defaultdict, Counter
from pathlib import Path

def get_collection_errors():
    """Get all test collection errors with details."""
    print("🔍 Collecting detailed error information...")
    
    try:
        result = subprocess.run([
            'python3', '-m', 'pytest', '--collect-only', '--tb=no'
        ], capture_output=True, text=True, timeout=120, 
        env={'PYTHONPATH': 'src'})
        
        output = result.stdout + result.stderr
        return output
    except subprocess.TimeoutExpired:
        print("⚠️ Collection timed out, using partial results")
        return ""
    except Exception as e:
        print(f"❌ Error running collection: {e}")
        return ""

def analyze_errors(output):
    """Analyze error patterns systematically."""
    print("\n📊 SYSTEMATIC ERROR ANALYSIS")
    print("=" * 50)
    
    errors = []
    error_patterns = defaultdict(list)
    
    # Extract individual error entries
    error_sections = re.findall(r'ERROR (.*?)\n(.*?)(?=ERROR|!!!!|====)', output, re.DOTALL)
    
    for file_path, error_content in error_sections:
        # Extract the actual error message
        error_lines = error_content.strip().split('\n')
        
        # Find the main error (usually starts with 'E   ')
        main_error = ""
        for line in error_lines:
            if line.strip().startswith('E   '):
                main_error = line.strip()[4:]  # Remove 'E   '
                break
        
        if not main_error and error_lines:
            main_error = error_lines[-1].strip()
        
        errors.append({
            'file': file_path,
            'error': main_error,
            'full_content': error_content
        })
        
        # Categorize by error type
        if 'ModuleNotFoundError' in main_error:
            error_patterns['ModuleNotFoundError'].append((file_path, main_error))
        elif 'ImportError' in main_error:
            error_patterns['ImportError'].append((file_path, main_error))
        elif 'NameError' in main_error:
            error_patterns['NameError'].append((file_path, main_error))
        elif 'AttributeError' in main_error:
            error_patterns['AttributeError'].append((file_path, main_error))
        elif 'SyntaxError' in main_error:
            error_patterns['SyntaxError'].append((file_path, main_error))
        else:
            error_patterns['Other'].append((file_path, main_error))
    
    return errors, error_patterns

def print_error_summary(error_patterns):
    """Print comprehensive error summary."""
    total_errors = sum(len(errors) for errors in error_patterns.values())
    
    print(f"\n🎯 ERROR SUMMARY: {total_errors} total errors")
    print("=" * 50)
    
    for error_type, errors in sorted(error_patterns.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"\n{error_type}: {len(errors)} errors")
        print("-" * 30)
        
        # Group by specific error message
        error_groups = defaultdict(list)
        for file_path, error_msg in errors:
            error_groups[error_msg].append(file_path)
        
        for error_msg, files in sorted(error_groups.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  • {error_msg}")
            print(f"    {len(files)} files: {files[:3]}{'...' if len(files) > 3 else ''}")
            print()

def identify_fix_priorities(error_patterns):
    """Identify which errors to fix first for maximum impact."""
    print("\n🎯 FIX PRIORITY RANKING")
    print("=" * 50)
    
    # Count occurrences of each specific error
    all_errors = []
    for error_type, errors in error_patterns.items():
        for file_path, error_msg in errors:
            all_errors.append(error_msg)
    
    error_counts = Counter(all_errors)
    
    print("Top 10 most frequent errors:")
    for i, (error_msg, count) in enumerate(error_counts.most_common(10), 1):
        print(f"{i:2d}. {error_msg} ({count} files)")
    
    return error_counts

def suggest_systematic_fixes(error_patterns):
    """Suggest systematic fixes for error categories."""
    print("\n🔧 SYSTEMATIC FIX SUGGESTIONS")
    print("=" * 50)
    
    for error_type, errors in error_patterns.items():
        if len(errors) == 0:
            continue
            
        print(f"\n{error_type} ({len(errors)} errors):")
        
        if error_type == 'ModuleNotFoundError':
            print("  → Create automated script to fix import paths")
            print("  → Add missing __init__.py files")
            print("  → Update PYTHONPATH configuration")
            
        elif error_type == 'ImportError':
            print("  → Fix incorrect class/function names in imports")
            print("  → Add missing dependencies")
            print("  → Handle circular import issues")
            
        elif error_type == 'NameError':
            print("  → Add missing variable definitions")
            print("  → Fix undefined function/class references")
            print("  → Add missing imports")
            
        elif error_type == 'AttributeError':
            print("  → Fix incorrect attribute access")
            print("  → Update class interface usage")
            print("  → Handle missing method implementations")
            
        elif error_type == 'SyntaxError':
            print("  → Fix malformed code syntax")
            print("  → Repair import statement formatting")
            print("  → Fix indentation and punctuation")

def main():
    """Main analysis workflow."""
    print("🚨 SYSTEMATIC TEST ERROR ANALYSIS")
    print("Analyzing current test collection errors...")
    
    # Get current error output
    output = get_collection_errors()
    
    if not output:
        print("❌ Could not collect error information")
        return
    
    # Analyze error patterns
    errors, error_patterns = analyze_errors(output)
    
    # Print comprehensive analysis
    print_error_summary(error_patterns)
    error_counts = identify_fix_priorities(error_patterns)
    suggest_systematic_fixes(error_patterns)
    
    print(f"\n📋 NEXT STEPS:")
    print("1. Fix high-frequency errors first (maximum impact)")
    print("2. Create automated scripts for each error type")
    print("3. Apply fixes systematically and verify progress")
    print("4. Repeat until zero errors remain")
    
    return error_patterns, error_counts

if __name__ == "__main__":
    main()