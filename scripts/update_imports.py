#!/usr/bin/env python3
"""
Ultra-aggressive import update script for clean architecture refactoring.
Updates all import statements to match new domain-driven structure.
"""

import os
import re
from pathlib import Path

class ImportUpdater:
    def __init__(self):
        self.src_dir = Path(__file__).parent
        self.import_mapping = self._create_import_mapping()
        self.files_updated = 0
        self.imports_updated = 0
        
    def _create_import_mapping(self):
        """Create mapping from old imports to new domain-driven imports"""
        return {
            # DAO imports to domain repositories
            r'from dao\.(.+) import': self._map_dao_import,
            r'import dao\.(.+)': self._map_dao_import_direct,
            
            # Service imports to domain services
            r'from market_data\.(.+) import': r'from domains.market_data.services.\1 import',
            r'from ml\.(.+) import': r'from domains.ml.services.\1 import',
            r'from analytics\.(.+) import': r'from domains.analytics.services.\1 import',
            r'from portfolio\.(.+) import': r'from domains.trading.services.\1 import',
            r'from universe\.(.+) import': r'from domains.trading.services.\1 import',
            r'from signals\.(.+) import': r'from domains.trading.services.\1 import',
            r'from secmaster\.(.+) import': r'from domains.instruments.services.\1 import',
            
            # Infrastructure imports
            r'from config\.(.+) import': r'from shared.utils.\1 import',
            r'from utils\.(.+) import': r'from shared.utils.\1 import',
            r'from monitoring\.(.+) import': r'from infrastructure.monitoring.\1 import',
            r'from storage\.(.+) import': r'from infrastructure.storage.\1 import',
            
            # API imports to interfaces
            r'from api\.(.+) import': r'from interfaces.rest_api.\1 import',
            
            # Core imports to shared
            r'from core\.utils\.(.+) import': r'from shared.utils.\1 import',
            r'from core\.exceptions\.(.+) import': r'from shared.exceptions.\1 import',
        }
    
    def _map_dao_import(self, match):
        """Map DAO imports to appropriate domain repositories"""
        dao_name = match.group(1)
        
        # Market data DAOs
        if any(keyword in dao_name for keyword in ['daily_prices', 'fundamentals']):
            return f'from domains.market_data.repositories.{dao_name} import'
        
        # Instrument DAOs
        elif any(keyword in dao_name for keyword in ['instrument', 'exchange', 'secmaster']):
            return f'from domains.instruments.repositories.{dao_name} import'
            
        # Trading DAOs
        elif any(keyword in dao_name for keyword in ['universe', 'factor']):
            return f'from domains.trading.repositories.{dao_name} import'
            
        # ML DAOs
        elif any(keyword in dao_name for keyword in ['training']):
            return f'from domains.ml.repositories.{dao_name} import'
            
        # Analytics DAOs
        elif any(keyword in dao_name for keyword in ['economic', 'events']):
            return f'from domains.analytics.repositories.{dao_name} import'
            
        # Infrastructure DAOs
        else:
            return f'from infrastructure.database.repositories.{dao_name} import'
    
    def _map_dao_import_direct(self, match):
        """Map direct DAO imports"""
        dao_name = match.group(1)
        
        # Market data DAOs
        if any(keyword in dao_name for keyword in ['daily_prices', 'fundamentals']):
            return f'import domains.market_data.repositories.{dao_name}'
        
        # Instrument DAOs
        elif any(keyword in dao_name for keyword in ['instrument', 'exchange', 'secmaster']):
            return f'import domains.instruments.repositories.{dao_name}'
            
        # Trading DAOs
        elif any(keyword in dao_name for keyword in ['universe', 'factor']):
            return f'import domains.trading.repositories.{dao_name}'
            
        # ML DAOs
        elif any(keyword in dao_name for keyword in ['training']):
            return f'import domains.ml.repositories.{dao_name}'
            
        # Analytics DAOs
        elif any(keyword in dao_name for keyword in ['economic', 'events']):
            return f'import domains.analytics.repositories.{dao_name}'
            
        # Infrastructure DAOs
        else:
            return f'import infrastructure.database.repositories.{dao_name}'
    
    def update_file(self, file_path):
        """Update imports in a single file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            file_imports_updated = 0
            
            # Apply each import mapping
            for pattern, replacement in self.import_mapping.items():
                if callable(replacement):
                    # Handle dynamic replacements
                    def replace_func(match):
                        return replacement(match)
                    new_content, count = re.subn(pattern, replace_func, content)
                    content = new_content
                    file_imports_updated += count
                else:
                    # Handle static replacements
                    content, count = re.subn(pattern, replacement, content)
                    file_imports_updated += count
            
            # Write back if changes were made
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.files_updated += 1
                self.imports_updated += file_imports_updated
                print(f"✅ Updated {file_imports_updated} imports in {file_path}")
            
        except Exception as e:
            print(f"❌ Error updating {file_path}: {e}")
    
    def update_all_files(self):
        """Update imports in all Python files in the new structure"""
        print("🚀 Starting ultra-aggressive import updates...")
        
        # Update files in domain directories
        for domain_dir in ['domains', 'infrastructure', 'interfaces', 'shared']:
            domain_path = self.src_dir / domain_dir
            if domain_path.exists():
                for py_file in domain_path.rglob('*.py'):
                    if py_file.name != '__init__.py':
                        self.update_file(py_file)
        
        print(f"\n🎯 Import update complete:")
        print(f"   Files updated: {self.files_updated}")
        print(f"   Imports updated: {self.imports_updated}")

if __name__ == "__main__":
    updater = ImportUpdater()
    updater.update_all_files()