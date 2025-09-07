#!/usr/bin/env python3
"""
Automated Migration Script for Shared Utilities Integration

This script safely migrates files to use the new shared utilities for:
- API key management (vendor_api_keys.py)
- Database connections (database_connections.py)
- Backfill framework (backfill_framework.py)

Usage:
    python scripts/migrate_to_shared_utilities.py --phase 1 --dry-run
    python scripts/migrate_to_shared_utilities.py --file src/infrastructure/vendor/polygon/services/populate_instrument_polygon.py
    python scripts/migrate_to_shared_utilities.py --phase 1 --execute
"""

import os
import re
import argparse
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SharedUtilitiesMigrator:
    """Automated migration tool for shared utilities integration"""

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.changes_made = []
        self.backup_dir = Path("migration_backups")

    def create_backup(self, file_path: Path) -> Path:
        """Create backup of file before migration"""
        if not self.backup_dir.exists():
            self.backup_dir.mkdir()

        backup_path = self.backup_dir / f"{file_path.name}.backup"
        if not self.dry_run:
            shutil.copy2(file_path, backup_path)
        logger.info(f"Backup created: {backup_path}")
        return backup_path

    def migrate_api_keys(self, file_path: Path) -> List[str]:
        """Migrate API key patterns to shared utilities"""
        changes = []

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content

        # Pattern 1: Replace vendor-specific imports
        vendor_imports = {
            r'from vendor\.polygon\.utils import.*POLYGON_API_KEY':
                'from shared.utils.vendor_api_keys import get_polygon_api_key',
            r'from vendor\.tiingo\.utils import.*TIINGO_API_KEY':
                'from shared.utils.vendor_api_keys import get_tiingo_api_key',
            r'from vendor\.eodhd\.utils import.*EODHD_API_KEY':
                'from shared.utils.vendor_api_keys import get_eodhd_api_key',
        }

        for old_pattern, new_import in vendor_imports.items():
            if re.search(old_pattern, content):
                content = re.sub(old_pattern, new_import, content)
                changes.append(f"Replaced vendor import with shared utility import")

        # Pattern 2: Replace API key assignments
        api_key_assignments = {
            r'POLYGON_API_KEY\s*=\s*os\.environ\.get\([^)]+\)[^;]*':
                'POLYGON_API_KEY = get_polygon_api_key()',
            r'TIINGO_API_KEY\s*=\s*os\.environ\.get\([^)]+\)[^;]*':
                'TIINGO_API_KEY = get_tiingo_api_key()',
            r'EODHD_API_KEY\s*=\s*os\.environ\.get\([^)]+\)[^;]*':
                'EODHD_API_KEY = get_eodhd_api_key()',
        }

        for old_pattern, new_assignment in api_key_assignments.items():
            if re.search(old_pattern, content):
                content = re.sub(old_pattern, new_assignment, content)
                changes.append(f"Simplified API key assignment")

        # Pattern 3: Remove complex fallback logic
        fallback_patterns = [
            r'\s*if not POLYGON_API_KEY:[^}]+}?\s*',
            r'\s*if not TIINGO_API_KEY:[^}]+}?\s*',
            r'\s*if not EODHD_API_KEY:[^}]+}?\s*',
        ]

        for pattern in fallback_patterns:
            if re.search(pattern, content, re.MULTILINE | re.DOTALL):
                content = re.sub(pattern, '', content, flags=re.MULTILINE | re.DOTALL)
                changes.append("Removed complex fallback logic (now handled by shared utility)")

        # Write changes if not dry run
        if changes and not self.dry_run:
            self.create_backup(file_path)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

        return changes

    def migrate_database_connections(self, file_path: Path) -> List[str]:
        """Migrate database connection patterns to shared utilities"""
        changes = []

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Pattern 1: Replace complex database imports
        db_import_patterns = {
            r'from shared\.data_handling\.utils\.database import Database':
                'from shared.utils.database_connections import get_database_pool, get_table_name',
        }

        for old_pattern, new_import in db_import_patterns.items():
            if re.search(old_pattern, content):
                if 'from shared.utils.database_connections import' not in content:
                    content = re.sub(old_pattern, new_import, content)
                    changes.append("Added shared database utilities import")

        # Pattern 2: Replace complex connection pool creation
        pool_patterns = {
            r'pool\s*=\s*await\s+Database\.create_connection_pool\([^)]+\)':
                'pool = await get_database_pool(environment)',
            r'asyncpg\.create_pool\([^)]+\)':
                'get_database_pool(environment)',
        }

        for old_pattern, new_assignment in pool_patterns.items():
            if re.search(old_pattern, content):
                # Need to be more careful with complex patterns - this is a simplified version
                changes.append("Database connection pattern identified for manual review")

        return changes

    def migrate_backfill_stats(self, file_path: Path) -> List[str]:
        """Migrate custom statistics classes to shared framework"""
        changes = []

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Pattern 1: Identify custom stats classes
        if re.search(r'@dataclass.*class.*Stats:', content, re.MULTILINE | re.DOTALL):
            changes.append("Custom statistics class identified - consider migrating to BackfillStats")

        # Pattern 2: Look for rate limiting logic
        if re.search(r'time\.sleep|asyncio\.sleep.*\d+', content):
            changes.append("Rate limiting logic found - consider using VendorRateLimiters")

        return changes

    def migrate_file(self, file_path: Path) -> Dict[str, List[str]]:
        """Migrate a single file to use shared utilities"""
        logger.info(f"{'[DRY RUN] ' if self.dry_run else ''}Migrating: {file_path}")

        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return {}

        results = {}

        # API Key Migration
        api_changes = self.migrate_api_keys(file_path)
        if api_changes:
            results['API Keys'] = api_changes

        # Database Migration
        db_changes = self.migrate_database_connections(file_path)
        if db_changes:
            results['Database'] = db_changes

        # Backfill Framework Migration
        stats_changes = self.migrate_backfill_stats(file_path)
        if stats_changes:
            results['Statistics'] = stats_changes

        return results

# Priority files for migration
PHASE_1_FILES = [
    "src/infrastructure/vendor/polygon/services/populate_instrument_polygon.py",
    "src/infrastructure/vendor/tiingo/services/populate_instrument_tiingo.py",
    "src/infrastructure/vendor/eodhd/services/populate_instrument_eodhd.py",
    "src/infrastructure/vendor/polygon/services/dividend_polygon.py",
    "src/infrastructure/vendor/tiingo/services/dividend_tiingo.py",
    "src/infrastructure/vendor/polygon/services/populate_market_cap_polygon.py",
    "src/infrastructure/vendor/tiingo/services/populate_market_cap_tiingo.py",
    "src/infrastructure/vendor/polygon/services/adv_mktcap_polygon.py",
]

PHASE_2_FILES = [
    "scripts/populate_30year_eodhd_minute_bars.py",
    "src/domains/market_data/services/vendor_adapters/news/turbo_news_backfill.py",
    "src/domains/market_data/services/vendor_adapters/news/comprehensive_news_backfill.py",
]

def main():
    parser = argparse.ArgumentParser(description="Migrate files to use shared utilities")
    parser.add_argument("--phase", type=int, choices=[1, 2], help="Migration phase to execute")
    parser.add_argument("--file", help="Specific file to migrate")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Preview changes without executing")
    parser.add_argument("--execute", action="store_true", help="Execute the migration (overrides dry-run)")

    args = parser.parse_args()

    # Determine dry run mode
    dry_run = args.dry_run and not args.execute

    migrator = SharedUtilitiesMigrator(dry_run=dry_run)

    # Determine files to migrate
    files_to_migrate = []
    if args.file:
        files_to_migrate = [Path(args.file)]
    elif args.phase == 1:
        files_to_migrate = [Path(f) for f in PHASE_1_FILES]
    elif args.phase == 2:
        files_to_migrate = [Path(f) for f in PHASE_2_FILES]
    else:
        parser.print_help()
        return

    logger.info(f"{'DRY RUN: ' if dry_run else ''}Migrating {len(files_to_migrate)} files")

    total_changes = 0
    for file_path in files_to_migrate:
        results = migrator.migrate_file(file_path)

        if results:
            print(f"\n📁 {file_path}")
            for category, changes in results.items():
                print(f"  {category}:")
                for change in changes:
                    print(f"    ✅ {change}")
                total_changes += len(changes)
        else:
            print(f"📁 {file_path} - No migrations needed")

    print(f"\n{'🔍 PREVIEW: ' if dry_run else '✅ COMPLETED: '}{total_changes} changes identified across {len(files_to_migrate)} files")

    if dry_run:
        print("\n💡 Run with --execute to apply changes")
    else:
        print(f"\n📂 Backups created in: {migrator.backup_dir}")

if __name__ == "__main__":
    main()