#!/usr/bin/env python3
"""
Update test imports to match the new domain-driven architecture.
"""

import os
import re
from pathlib import Path

class TestImportUpdater:
    def __init__(self):
        self.root_dir = Path(__file__).parent
        self.files_updated = 0
        self.imports_updated = 0
        
    def update_test_imports(self, file_path):
        """Update imports in a test file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Update imports to match new src structure
            patterns = [
                # DAO imports to domain repositories
                (r'from dao\.(.+) import', self._map_dao_test_import),
                (r'import dao\.(.+)', self._map_dao_test_import_direct),
                
                # Service imports
                (r'from market_data\.(.+) import', r'from domains.market_data.services.\1 import'),
                (r'from analytics\.(.+) import', r'from domains.analytics.services.\1 import'),
                (r'from secmaster\.(.+) import', r'from domains.instruments.services.\1 import'),
                (r'from universe\.(.+) import', r'from domains.trading.services.\1 import'),
                (r'from portfolio\.(.+) import', r'from domains.trading.services.\1 import'),
                (r'from signals\.(.+) import', r'from domains.trading.services.\1 import'),
                (r'from ml\.(.+) import', r'from domains.ml.services.\1 import'),
                (r'from modeling\.(.+) import', r'from domains.ml.services.\1 import'),
                
                # Infrastructure imports
                (r'from config\.(.+) import', r'from shared.utils.\1 import'),
                (r'from utils\.(.+) import', r'from shared.utils.\1 import'),
                
                # API imports
                (r'from api\.(.+) import', r'from interfaces.rest_api.\1 import'),
                
                # Src prefix updates for PYTHONPATH usage
                (r'from src\.dao\.(.+) import', self._map_src_dao_import),
                (r'from src\.market_data\.(.+) import', r'from domains.market_data.services.\1 import'),
                (r'from src\.analytics\.(.+) import', r'from domains.analytics.services.\1 import'),
                (r'from src\.config\.(.+) import', r'from shared.utils.\1 import'),
                (r'from src\.api\.(.+) import', r'from interfaces.rest_api.\1 import'),
            ]
            
            for pattern, replacement in patterns:
                if callable(replacement):
                    def replace_func(match):
                        return replacement(match)
                    content, count = re.subn(pattern, replace_func, content)
                else:
                    content, count = re.subn(pattern, replacement, content)
                
                if count > 0:
                    self.imports_updated += count
            
            # Write back if changed
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.files_updated += 1
                print(f"✅ Updated {file_path}")
                
        except Exception as e:
            print(f"❌ Error updating {file_path}: {e}")
    
    def _map_dao_test_import(self, match):
        """Map DAO test imports to domain repositories"""
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
    
    def _map_dao_test_import_direct(self, match):
        """Map direct DAO test imports"""
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
    
    def _map_src_dao_import(self, match):
        """Map src.dao imports"""
        dao_name = match.group(1)
        return self._map_dao_test_import(type('Match', (), {'group': lambda self, n: dao_name})())
    
    def update_all_test_files(self):
        """Update all test files"""
        print("🚀 Starting test import updates...")
        
        # Update files in tests/domains
        tests_dir = self.root_dir / "tests"
        for py_file in tests_dir.rglob('*.py'):
            if py_file.name != '__init__.py' and '__pycache__' not in str(py_file):
                self.update_test_imports(py_file)
        
        print(f"\n🎯 Test import update complete:")
        print(f"   Files updated: {self.files_updated}")
        print(f"   Imports updated: {self.imports_updated}")

if __name__ == "__main__":
    updater = TestImportUpdater()
    updater.update_all_test_files()