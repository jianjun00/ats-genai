#!/usr/bin/env python3
"""
Test Directory Migration Script.

Migrates existing tests to new organized structure following 7-item rule.
Run with --dry-run first to see planned moves.
"""

import os
import shutil
from pathlib import Path

class TestMigrator:
    def __init__(self, dry_run=True):
        self.old_tests_dir = Path('tests')
        self.new_tests_dir = Path('tests_new')
        self.dry_run = dry_run
        self.migrations = 0

    def migrate_core_tests(self):
        """Migrate core/ related tests."""
        mappings = [
            ('tests/core', 'tests_new/core'),
            ('tests/dao', 'tests_new/core/dao'),
            ('tests/config', 'tests_new/core/platform'),
            ('tests/database', 'tests_new/core/platform'),
            ('tests/utils', 'tests_new/core/shared')
        ]
        self._execute_mappings(mappings)

    def migrate_signals_tests(self):
        """Migrate signals/ related tests."""
        mappings = [
            ('tests/signals', 'tests_new/signals'),
            ('tests/indicators', 'tests_new/signals'),
            ('tests/technical', 'tests_new/signals')
        ]
        self._execute_mappings(mappings)

    def migrate_services_tests(self):
        """Migrate services/ related tests."""
        mappings = [
            ('tests/services', 'tests_new/services'),
            ('tests/analytics', 'tests_new/services'),
            ('tests/api', 'tests_new/services')
        ]
        self._execute_mappings(mappings)

    def migrate_domains_tests(self):
        """Migrate domains/ related tests."""
        mappings = [
            ('tests/domains', 'tests_new/domains'),
            ('tests/market_data', 'tests_new/domains/market_data'),
            ('tests/ml', 'tests_new/domains/ml'),
            ('tests/vendor', 'tests_new/domains/vendors')
        ]
        self._execute_mappings(mappings)

    def migrate_integration_tests(self):
        """Migrate integration tests."""
        mappings = [
            ('tests/integration', 'tests_new/integration'),
            ('tests/e2e', 'tests_new/integration'),
            ('tests/browser_tests', 'tests_new/integration')
        ]
        self._execute_mappings(mappings)

    def migrate_unit_tests(self):
        """Migrate pure unit tests."""
        mappings = [
            ('tests/unit', 'tests_new/unit')
        ]
        self._execute_mappings(mappings)

    def _execute_mappings(self, mappings):
        """Execute a list of directory mappings."""
        for old_path, new_path in mappings:
            old_full = Path(old_path)
            new_full = Path(new_path)

            if old_full.exists():
                print(f"{'[DRY-RUN] ' if self.dry_run else ''}Moving {old_path} -> {new_path}")

                if not self.dry_run:
                    new_full.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(old_full), str(new_full))

                self.migrations += 1

    def run_migration(self):
        """Run complete migration."""
        print(f"Starting test migration ({'DRY-RUN' if self.dry_run else 'LIVE'})...")

        self.migrate_core_tests()
        self.migrate_signals_tests()
        self.migrate_services_tests()
        self.migrate_domains_tests()
        self.migrate_integration_tests()
        self.migrate_unit_tests()

        print(f"\nMigration complete: {self.migrations} directories processed")

        if not self.dry_run:
            print("\nNext steps:")
            print("1. Update CI/CD to use tests_new/")
            print("2. Update IDE test discovery settings")
            print("3. Remove old tests/ directory")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--execute', action='store_true', help='Actually perform migration')
    args = parser.parse_args()

    migrator = TestMigrator(dry_run=not args.execute)
    migrator.run_migration()
