#!/usr/bin/env python3
"""
Script to fix relative imports in source files after directory restructuring.
"""

import os
import sys
from pathlib import Path

# Enhanced mapping for relative imports in source files
SRC_IMPORT_MAPPINGS = {
    # Relative imports that need src. prefix
    'from infrastructure.': 'from src.infrastructure.',
    'import infrastructure.': 'import src.infrastructure.',
    'from domains.': 'from src.domains.',
    'import domains.': 'import src.domains.',
    'from core.': 'from src.core.',
    'import core.': 'import src.core.',
    
    # Remove any double src. prefixes
    'from src.src.': 'from src.',
    'import src.src.': 'import src.',
}

def update_file_imports(file_path: Path) -> bool:
    """Update imports in a single source file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply source import mappings
        for old_import, new_import in SRC_IMPORT_MAPPINGS.items():
            content = content.replace(old_import, new_import)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Fixed: {file_path}")
            return True
        
        return False
    
    except Exception as e:
        print(f"❌ Error updating {file_path}: {e}")
        return False

def main():
    """Main function to fix source imports."""
    
    # Find all Python files in src/
    src_dir = Path("src")
    if not src_dir.exists():
        print("❌ src/ directory not found")
        return 1
    
    python_files = list(src_dir.rglob("*.py"))
    
    updated_count = 0
    total_count = len(python_files)
    
    print(f"Processing {total_count} Python files in src/...")
    
    for py_file in python_files:
        if update_file_imports(py_file):
            updated_count += 1
    
    print(f"\n🎉 Fixed {updated_count} source files out of {total_count} total files")
    return 0

if __name__ == "__main__":
    sys.exit(main())