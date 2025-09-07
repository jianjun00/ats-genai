#!/usr/bin/env python3
"""
Fix common test issues after architectural refactoring.
"""

import os
import re
from pathlib import Path

class TestFixer:
    def __init__(self):
        self.root_dir = Path(__file__).parent
        self.files_fixed = 0
        self.issues_fixed = 0

    def fix_async_tests(self, file_path):
        """Add pytest.mark.asyncio to async test functions"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content

            # Add asyncio marker to async test functions
            pattern = r'^(\s*)(async def test_[^(]+\([^)]*\):)'
            def add_asyncio_marker(match):
                indent = match.group(1)
                func_def = match.group(2)
                return f'{indent}@pytest.mark.asyncio\n{indent}{func_def}'

            content = re.sub(pattern, add_asyncio_marker, content, flags=re.MULTILINE)

            # Fix old patch paths for DAO imports
            old_patterns = [
                (r'patch\([\'"]dao\.([^.]+)\.asyncpg\.create_pool[\'"]', r'patch(\'domains.market_data.repositories.\1.asyncpg.create_pool\''),
                (r'patch\([\'"]dao\.instrument([^.]+)\.asyncpg\.create_pool[\'"]', r'patch(\'domains.instruments.repositories.instrument\1.asyncpg.create_pool\''),
                (r'patch\([\'"]dao\.universe([^.]+)\.asyncpg\.create_pool[\'"]', r'patch(\'domains.trading.repositories.universe\1.asyncpg.create_pool\''),
                (r'patch\([\'"]dao\.([^.]+_dao)\.asyncpg\.create_pool[\'"]', self._map_dao_patch),
            ]

            for pattern, replacement in old_patterns:
                if callable(replacement):
                    def replace_func(match):
                        return replacement(match)
                    content = re.sub(pattern, replace_func, content)
                else:
                    content = re.sub(pattern, replacement, content)

            # Write back if changed
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.files_fixed += 1
                self.issues_fixed += len(re.findall(r'@pytest\.mark\.asyncio', content))
                print(f"✅ Fixed {file_path}")

        except Exception as e:
            print(f"❌ Error fixing {file_path}: {e}")

    def _map_dao_patch(self, match):
        """Map DAO patch paths to correct domains"""
        dao_name = match.group(1)

        # Market data DAOs
        if any(keyword in dao_name for keyword in ['daily_prices', 'fundamentals']):
            return f'patch(\'domains.market_data.repositories.{dao_name}.asyncpg.create_pool\''

        # Instrument DAOs
        elif any(keyword in dao_name for keyword in ['instrument', 'exchange', 'secmaster']):
            return f'patch(\'domains.instruments.repositories.{dao_name}.asyncpg.create_pool\''

        # Trading DAOs
        elif any(keyword in dao_name for keyword in ['universe', 'factor']):
            return f'patch(\'domains.trading.repositories.{dao_name}.asyncpg.create_pool\''

        # ML DAOs
        elif any(keyword in dao_name for keyword in ['training']):
            return f'patch(\'domains.ml.repositories.{dao_name}.asyncpg.create_pool\''

        # Analytics DAOs
        elif any(keyword in dao_name for keyword in ['economic', 'events']):
            return f'patch(\'domains.analytics.repositories.{dao_name}.asyncpg.create_pool\''

        # Infrastructure DAOs
        else:
            return f'patch(\'infrastructure.database.repositories.{dao_name}.asyncpg.create_pool\''

    def fix_all_test_files(self):
        """Fix all test files"""
        print("🚀 Starting test fixes...")

        # Fix files in tests directory
        tests_dir = self.root_dir / "tests"
        for py_file in tests_dir.rglob('*.py'):
            if py_file.name != '__init__.py' and '__pycache__' not in str(py_file):
                if 'test_' in py_file.name:
                    self.fix_async_tests(py_file)

        print(f"\n🎯 Test fixes complete:")
        print(f"   Files fixed: {self.files_fixed}")
        print(f"   Issues fixed: {self.issues_fixed}")

if __name__ == "__main__":
    fixer = TestFixer()
    fixer.fix_all_test_files()