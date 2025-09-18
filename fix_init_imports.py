#!/usr/bin/env python3
"""
Script to fix __init__.py files after directory restructuring.
Updates relative imports to use proper src. prefixes.
"""

import os
import sys
from pathlib import Path

# Mapping for fixing __init__.py imports
INIT_IMPORT_MAPPINGS = {
    # Data quality agents
    'from domains.data_quality.agents.': 'from src.domains.data_quality.agents.',
    'from domains.data_quality.services.': 'from src.domains.data_quality.services.',
    
    # Analytics
    'from domains.analytics.events.': 'from src.domains.analytics.events.',
    'from domains.analytics.services.': 'from src.domains.analytics.services.',
    
    # Trading
    'from domains.trading.signals.': 'from src.domains.trading.signals.',
    'from domains.trading.services.': 'from src.domains.trading.services.',
    
    # ML
    'from domains.ml.legacy.': 'from src.domains.ml.legacy.',
    'from domains.ml.services.': 'from src.domains.ml.services.',
    
    # Infrastructure  
    'from infrastructure.interfaces.': 'from src.infrastructure.interfaces.',
    'from infrastructure.monitoring.': 'from src.infrastructure.monitoring.',
    'from infrastructure.jobs.': 'from src.infrastructure.jobs.',
    'from infrastructure.tools.': 'from src.infrastructure.tools.',
    'from infrastructure.web.': 'from src.infrastructure.web.',
    'from infrastructure.data.': 'from src.infrastructure.data.',
    
    # Core
    'from core.shared.': 'from src.core.shared.',
    
    # Remove any double src. prefixes that might have been added
    'from src.src.': 'from src.',
    'import src.src.': 'import src.',
}

def fix_init_file(file_path: Path) -> bool:
    """Fix imports in a single __init__.py file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply init import mappings
        for old_import, new_import in INIT_IMPORT_MAPPINGS.items():
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
    """Main function to fix __init__.py imports."""
    
    # Find all __init__.py files in src/
    src_dir = Path("src")
    if not src_dir.exists():
        print("❌ src/ directory not found")
        return 1
    
    init_files = list(src_dir.rglob("__init__.py"))
    
    updated_count = 0
    total_count = len(init_files)
    
    print(f"Found {total_count} __init__.py files to check...")
    
    for init_file in init_files:
        if fix_init_file(init_file):
            updated_count += 1
    
    print(f"\n🎉 Fixed {updated_count} __init__.py files out of {total_count} total files")
    return 0

if __name__ == "__main__":
    sys.exit(main())