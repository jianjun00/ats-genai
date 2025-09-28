"""
Migration 0025: Add run_id column to all interval-related tables

This migration adds a run_id column to all interval tables:
- instrument_interval
- factor_interval
- instrument_indicator_interval

Changes:
1. Add run_id column to each interval table
2. Add index on run_id for performance
3. Set default run_id for existing records
"""

import asyncio
import asyncpg
from typing import Dict, List
import logging


class Migration0025:
    """Add run_id column to all interval-related tables."""

    def __init__(self):
        self.migration_id = "0025"
        self.description = "Add run_id column to all interval tables"
        self.logger = logging.getLogger(__name__)

    async def up(self, connection: asyncpg.Connection, environment_prefix: str) -> None:
        """Apply the migration."""

        tables = [
            f"{environment_prefix}_instrument_interval",
            f"{environment_prefix}_factor_interval",
            f"{environment_prefix}_instrument_indicator_interval"
        ]

        self.logger.info(f"Starting migration {self.migration_id}: {self.description}")
        self.logger.info(f"Environment: {environment_prefix}")

        for table_name in tables:
            await self.add_run_id_to_table(connection, table_name)

    async def add_run_id_to_table(self, connection: asyncpg.Connection, table_name: str) -> None:
        """Add run_id column to a specific table."""

        # Check if run_id column already exists
        check_column_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1 AND column_name = 'run_id'
        """

        existing_column = await connection.fetchval(check_column_query, table_name)

        if existing_column:
            self.logger.info(f"Column run_id already exists in {table_name}, skipping")
            return

        # Check if table exists
        table_exists_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = $1
        )
        """

        table_exists = await connection.fetchval(table_exists_query, table_name)

        if not table_exists:
            self.logger.warning(f"Table {table_name} does not exist, skipping")
            return

        # Step 1: Add run_id column with default value for existing records
        self.logger.info(f"Adding run_id column to {table_name}")
        add_column_query = f"""
        ALTER TABLE {table_name}
        ADD COLUMN run_id VARCHAR(255) DEFAULT 'legacy_run_pre_0025'
        """
        await connection.execute(add_column_query)

        # Step 2: Add index on run_id for performance
        self.logger.info(f"Adding index on run_id for {table_name}")
        add_index_query = f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_run_id
        ON {table_name} (run_id)
        """
        await connection.execute(add_index_query)

        # Step 3: Update column comment for documentation
        comment_query = f"""
        COMMENT ON COLUMN {table_name}.run_id IS
        'Unique run identifier to allow multiple training runs to process same intervals'
        """
        await connection.execute(comment_query)

        self.logger.info(f"Successfully added run_id column to {table_name}")

    async def down(self, connection: asyncpg.Connection, environment_prefix: str) -> None:
        """Rollback the migration."""

        tables = [
            f"{environment_prefix}_instrument_interval",
            f"{environment_prefix}_factor_interval",
            f"{environment_prefix}_instrument_indicator_interval"
        ]

        self.logger.info(f"Rolling back migration {self.migration_id}")

        for table_name in tables:
            # Drop index
            await connection.execute(f"""
            DROP INDEX IF EXISTS idx_{table_name}_run_id
            """)

            # Drop column
            await connection.execute(f"""
            ALTER TABLE {table_name} DROP COLUMN IF EXISTS run_id
            """)

            self.logger.info(f"Successfully rolled back run_id from {table_name}")

    def get_migration_info(self) -> Dict[str, str]:
        """Get migration metadata."""
        return {
            "id": self.migration_id,
            "description": self.description,
            "tables_affected": ["instrument_interval", "factor_interval", "instrument_indicator_interval"],
            "breaking_changes": "Adds run_id column to interval tables",
            "rollback_safe": "Yes, but will lose run_id data"
        }


async def apply_migration_to_environment(db_config: Dict[str, str], environment_prefix: str) -> bool:
    """Apply migration to a specific environment."""
    migration = Migration0025()

    # Connect to database
    conn = await asyncpg.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

    # Apply migration
    await migration.up(conn, environment_prefix)

    print(f"✅ Migration {migration.migration_id} applied successfully to {environment_prefix}")
    return True

    print(f"❌ Failed to apply migration {migration.migration_id} to {environment_prefix}: {e}")
    return False


async def main():
    """Main migration application function."""

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("🚀 Applying Migration 0025: Add run_id to all interval tables")
    print("=" * 60)

    # Environment configurations
    environments = {
        'dev': {
            'host': 'localhost',
            'port': '3432',
            'user': 'postgres',
            'password': 'dev_password',
            'database': 'dev_db',
            'prefix': 'dev'
        },
        'intg': {
            'host': 'localhost',
            'port': '4432',
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db',
            'prefix': 'intg'
        }
    }

    results = {}

    # Apply to each environment
    for env_name, config in environments.items():
        print(f"\n📋 Applying migration to {env_name.upper()} environment...")
        success = await apply_migration_to_environment(config, config['prefix'])
        results[env_name] = success
    print(f"\n🎯 Migration Summary:")
    print("=" * 30)

    all_successful = True
    for env_name, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{env_name.upper()}: {status}")
        if not success:
            all_successful = False

    if all_successful:
        print(f"\n🎉 Migration 0025 applied successfully to all environments!")
        print(f"   - run_id column added to all interval tables")
        print(f"   - Indexes created for performance")
    else:
        print(f"\n⚠️ Some environments failed. Check logs and retry failed environments.")

    return 0 if all_successful else 1


if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)