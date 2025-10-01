#!/usr/bin/env python3
"""
Fix InstrumentService import issues systematically.

Changes incorrect imports from:
  from domains.instruments.services.impl.instrument_service_cached import InstrumentService
To:
  from domains.instruments.services.impl.instrument_service_cached import CachedInstrumentService

And updates usage:
  InstrumentService(...) -> CachedInstrumentService(...)
"""

import os
import re
import subprocess
import sys

def find_files_with_incorrect_imports():
    """Find all files importing InstrumentService from cached service."""
    result = subprocess.run([
        'grep', '-r', 'from.*instrument_service_cached.*import.*InstrumentService', 
        'tests/', '--include=*.py'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        return []
    
    files = set()
    for line in result.stdout.strip().split('\n'):
        if ':' in line:
            file_path = line.split(':')[0]
            files.add(file_path)
    
    return sorted(files)

def fix_file(file_path):
    """Fix imports and usage in a single file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Track if we made changes
        changes_made = False
        original_content = content
        
        # Fix the import statement
        import_pattern = r'from domains\.instruments\.services\.impl\.instrument_service_cached import InstrumentService'
        import_replacement = 'from domains.instruments.services.impl.instrument_service_cached import CachedInstrumentService'
        
        if re.search(import_pattern, content):
            content = re.sub(import_pattern, import_replacement, content)
            changes_made = True
        
        # Fix usage patterns
        usage_patterns = [
            (r'\bInstrumentService\(', 'CachedInstrumentService('),
            (r'return InstrumentService\(', 'return CachedInstrumentService('),
            (r'= InstrumentService\(', '= CachedInstrumentService('),
        ]
        
        for pattern, replacement in usage_patterns:
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                changes_made = True
        
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
    print("Finding files with incorrect InstrumentService imports...")
    
    files_to_fix = find_files_with_incorrect_imports()
    
    if not files_to_fix:
        print("No files need InstrumentService import fixes!")
        return 0
    
    print(f"Found {len(files_to_fix)} files with incorrect imports:")
    for file_path in files_to_fix:
        print(f"  {file_path}")
    
    print(f"\nFixing {len(files_to_fix)} files...")
    fixed_count = 0
    
    for file_path in files_to_fix:
        if fix_file(file_path):
            print(f"✅ Fixed: {file_path}")
            fixed_count += 1
        else:
            print(f"❌ No changes: {file_path}")
    
    print(f"\n🎉 Successfully fixed {fixed_count}/{len(files_to_fix)} files!")
    return 0

if __name__ == "__main__":
    sys.exit(main())