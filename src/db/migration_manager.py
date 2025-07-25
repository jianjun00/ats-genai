"""
Database Migration Manager for versioned schema changes.

This module handles:
- Running migrations in order
- Tracking applied migrations
- Environment-aware migrations (test_, intg_, prod_ prefixes)
- Rollback capabilities
"""

import asyncio
import asyncpg
import os
import hashlib
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
from config.environment import get_environment

class MigrationManager:
    def __init__(self, db_url: str = None):
        self.env = get_environment()
        self.db_url = db_url or self.env.get_database_url()
        self.migrations_dir = Path(__file__).parent / "migrations"
        
        # Determine environment based on database URL
        if db_url:
            if "intg_trading_db" in db_url:
                from config.environment import Environment, EnvironmentType
                self.env = Environment(EnvironmentType.INTEGRATION)
            elif "prod_trading_db" in db_url:
                from config.environment import Environment, EnvironmentType
                self.env = Environment(EnvironmentType.PRODUCTION)
            # else use current environment for test_trading_db
        
        # Extract table prefix from environment
        sample_table = self.env.get_table_name("sample")
        self.table_prefix = sample_table.replace("sample", "")
    
    def _get_migration_files(self) -> List[Tuple[int, str, Path]]:
        """Get all migration files sorted by version number."""
        migrations = []
        for file_path in self.migrations_dir.glob("*.sql"):
            match = re.match(r"(\d{3})_(.+)\.sql", file_path.name)
            if match:
                version = int(match.group(1))
                description = match.group(2).replace("_", " ")
                migrations.append((version, description, file_path))
        
        return sorted(migrations, key=lambda x: x[0])
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of migration file."""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    async def get_current_version(self) -> int:
        """Get the current database version."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                # Create db_version table if it doesn't exist
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_prefix}db_version (
                        id SERIAL PRIMARY KEY,
                        version INTEGER NOT NULL UNIQUE,
                        description TEXT NOT NULL,
                        applied_at TIMESTAMPTZ DEFAULT now(),
                        checksum TEXT,
                        migration_file TEXT
                    )
                """)
                
                # Get current version
                result = await conn.fetchval(f"""
                    SELECT COALESCE(MAX(version), -1) 
                    FROM {self.table_prefix}db_version
                """)
                return result if result is not None else -1
        finally:
            await pool.close()
    
    async def apply_migration(self, version: int, description: str, file_path: Path) -> bool:
        """Apply a single migration. Executes the entire migration file as a single SQL script to support complex PostgreSQL constructs (e.g., dollar-quoted functions)."""
        print(f"[DEBUG] Applying migration version={version}, description='{description}', file_path='{file_path}'")
        # Ensure version table exists before migration (fix for concurrency)
        await self.get_current_version()
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Read migration file
                    with open(file_path, 'r') as f:
                        migration_sql = f.read()
                    print(f"[DEBUG] Raw migration SQL for version {version}:")
                    print(migration_sql)
                    prefixed_sql = self._apply_table_prefixes(migration_sql)
                    if version == 1:
                        print(f"[DEBUG] --- Prefixed migration SQL for version 1 (initial schema) ---")
                        print(prefixed_sql)
                        print(f"[DEBUG] --- END Prefixed migration SQL for version 1 ---")
                    else:
                        print(f"[DEBUG] Prefixed migration SQL for version {version}:")
                        print(prefixed_sql)
                    # Execute the entire migration as a single script
                    try:
                        await conn.execute(prefixed_sql)
                    except Exception as exec_sql_exc:
                        print(f"[ERROR] Exception executing migration SQL for version {version}: {exec_sql_exc}")
                        print(f"[DEBUG] --- Failing SQL for version {version} ---")
                        print(prefixed_sql)
                        print(f"[DEBUG] --- END Failing SQL for version {version} ---")
                        import traceback
                        traceback.print_exc()
                        raise
                
                    # Record migration (skip for version 0 as it records itself)
                    if version != 0:
                        # Check if this version already exists
                        exists = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM {self.table_prefix}db_version WHERE version = $1
                        """, version)
                        if exists:
                            print(f"[WARN] Migration version {version} already exists in db_version. Skipping insert.")
                            return False
                        print(f"[DEBUG] About to insert migration version {version} into db_version")
                        checksum = self._calculate_checksum(file_path)
                        await conn.execute(f"""
                            INSERT INTO {self.table_prefix}db_version 
                            (version, description, checksum, migration_file)
                            VALUES ($1, $2, $3, $4)
                        """, version, description, checksum, file_path.name)
                        print(f"[DEBUG] Successfully inserted migration version {version} into db_version")
                    else:
                        # For version 0, just update the checksum since the migration inserts itself
                        checksum = self._calculate_checksum(file_path)
                        await conn.execute(f"""
                            UPDATE {self.table_prefix}db_version 
                            SET checksum = $1
                            WHERE version = 0
                        """, checksum)
                    
                    print(f"Applied migration {version:03d}: {description}")
                    return True
        except Exception as e:
            print(f"[ERROR] Failed to apply migration {version:03d}: {e}")
            print(f"[DEBUG] Transaction rollback triggered for migration version {version}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            await pool.close()
    
    def _apply_table_prefixes(self, sql: str) -> str:
        """Apply environment-specific table prefixes to all tables in the SQL, dynamically extracting table names."""
        if not self.table_prefix:
            return sql

        # 1. Dynamically extract all table names from CREATE TABLE statements
        create_table_pattern = re.compile(
            r'CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?!' + re.escape(self.table_prefix) + r')(\w+)',
            flags=re.IGNORECASE
        )
        table_names = set(match.group(1) for match in create_table_pattern.finditer(sql))

        # Always include db_version for version tracking
        table_names.add('db_version')

        # 1b. Add a comprehensive static list of known tables
        static_table_names = set([
            # Core tables
            'events', 'daily_prices', 'vendors', 'instruments', 'instrument_aliases',
            'instrument_metadata', 'instrument_polygon', 'status_code', 'daily_prices_tiingo',
            'daily_prices_polygon', 'daily_market_cap', 'universe', 'universe_membership',
            'fundamentals', 'db_version',
            # Test and migration utility tables
            'users', 'posts', 'test_migration_table', 'concurrent_test', 'test_complex',
            'test_rollback', 'first_table', 'middle_table', 'last_table', 'duplicate_test', 'test_idempotent',
            # All tables found in migrations (ensure these are always prefixed)
            'universe_membership_changes', 'stock_splits', 'dividends',
        ])
        table_names.update(static_table_names)
        
        # 2. Prefix all CREATE TABLE statements
        def prefix_create_table(match):
            prefix = '' if match.group(2).startswith(self.table_prefix) else self.table_prefix
            return match.group(1) + prefix + match.group(2)
        sql = re.sub(
            r'(CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+)(\w+)',
            prefix_create_table,
            sql,
            flags=re.IGNORECASE
        )

        # 3. Prefix all CREATE INDEX statements
        def prefix_create_index(match):
            prefix = '' if match.group(3).startswith(self.table_prefix) else self.table_prefix
            return f'{match.group(1)}{match.group(2)} ON {prefix}{match.group(3)}'
        sql = re.sub(
            r'(CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+)(\w+)\s+ON\s+(\w+)',
            prefix_create_index,
            sql,
            flags=re.IGNORECASE
        )

        # 4. Prefix all table references in statements (including foreign keys, inserts, etc)
        for table in sorted(table_names, key=lambda x: -len(x)):
            # Avoid double prefixing
            'instrument_metadata', 'instrument_polygon', 'status_code', 'daily_prices_tiingo',
            'daily_prices_polygon', 'daily_market_cap', 'universe', 'universe_membership',
            'fundamentals', 'db_version',
            # Test and migration utility tables
            'users', 'posts', 'test_migration_table', 'concurrent_test', 'test_complex',
            pattern = rf'(?<!{re.escape(self.table_prefix)})\b{re.escape(table)}\b(?!\w)'
            replacement = f'{self.table_prefix}{table}'
            sql = re.sub(pattern, replacement, sql)

        return sql
    
    def _get_backup_file(self):
        db_name = self.env.get_database_config()['database']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = Path(self.migrations_dir) / 'backups'
        backup_dir.mkdir(exist_ok=True)
        backup_file = backup_dir / f"{db_name}_backup_{timestamp}.dump"
        return str(backup_file)

    def _run_pg_dump(self, backup_file):
        db_cfg = self.env.get_database_config()
        cmd = [
            'pg_dump', '-Fc',
            '-h', db_cfg['host'],
            '-p', str(db_cfg['port']),
            '-U', db_cfg['user'],
            '-d', db_cfg['database'],
            '-f', backup_file
        ]
        env = os.environ.copy()
        if db_cfg.get('password'):
            env['PGPASSWORD'] = db_cfg['password']
        print(f"[INFO] Backing up database to {backup_file} ...")
        subprocess.check_call(cmd, env=env)
        print(f"[INFO] Backup complete.")

    def _run_pg_restore(self, backup_file):
        db_cfg = self.env.get_database_config()
        cmd = [
            'pg_restore', '-c',
            '-h', db_cfg['host'],
            '-p', str(db_cfg['port']),
            '-U', db_cfg['user'],
            '-d', db_cfg['database'],
            backup_file
        ]
        env = os.environ.copy()
        if db_cfg.get('password'):
            env['PGPASSWORD'] = db_cfg['password']
        print(f"[INFO] Restoring database from {backup_file} ...")
        subprocess.check_call(cmd, env=env)
        print(f"[INFO] Restore complete.")

    async def migrate_to_latest(self) -> bool:
        """Apply all pending migrations with automatic backup and restore."""
        current_version = await self.get_current_version()
        migrations = self._get_migration_files()
        
        pending_migrations = [
            (v, d, p) for v, d, p in migrations 
            if v > current_version
        ]
        
        if not pending_migrations:
            print(f"Database is up to date (version {current_version})")
            return True
        
        print(f"Current version: {current_version}")
        print(f"Applying {len(pending_migrations)} pending migrations...")
        
        backup_file = self._get_backup_file()
        try:
            self._run_pg_dump(backup_file)
        except Exception as e:
            print(f"[ERROR] Database backup failed: {e}")
            return False
        
        success = True
        for version, description, file_path in pending_migrations:
            try:
                if not await self.apply_migration(version, description, file_path):
                    raise Exception(f"Migration {version} failed")
            except Exception as migration_exc:
                print(f"[ERROR] Migration failed. Attempting to restore database from backup...")
                try:
                    self._run_pg_restore(backup_file)
                except Exception as restore_exc:
                    print(f"[CRITICAL] Database restore failed: {restore_exc}")
                print(f"[INFO] Fix the migration SQL and re-run migrations.")
                success = False
                break
        
        if success:
            final_version = await self.get_current_version()
            print(f"Migration completed. Current version: {final_version}")
        
        return success
    
    async def validate_migrations(self) -> bool:
        """Validate that applied migrations haven't been modified."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                applied_migrations = await conn.fetch(f"""
                    SELECT version, checksum, migration_file 
                    FROM {self.table_prefix}db_version 
                    ORDER BY version
                """)
                
                for record in applied_migrations:
                    file_path = self.migrations_dir / record['migration_file']
                    if file_path.exists():
                        current_checksum = self._calculate_checksum(file_path)
                        if current_checksum != record['checksum']:
                            print(f"WARNING: Migration {record['version']:03d} has been modified!")
                            return False
                
                print("All applied migrations are valid")
                return True
        finally:
            await pool.close()


async def main():
    """CLI interface for migration management."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python migration_manager.py [migrate|validate|version]")
        return
    
    command = sys.argv[1]
    manager = MigrationManager()
    
    if command == "migrate":
        await manager.migrate_to_latest()
    elif command == "validate":
        await manager.validate_migrations()
    elif command == "version":
        version = await manager.get_current_version()
        print(f"Current database version: {version}")
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())
