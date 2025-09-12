"""
Migration 0024: Add run_id column to interval-related tables

This migration adds a run_id column to interval tables and updates the primary key
to include run_id, allowing multiple runs to process the same time intervals
without unique constraint violations.

Changes:
1. Add run_id column to universe_state_interval table
2. Update unique constraint to include run_id
3. Add index on run_id for performance
4. Set default run_id for existing records
"""

import asyncio
import asyncpg
from typing import Dict, List
import logging


class Migration0024:
    """Add run_id column to interval-related tables."""
    
    def __init__(self):
        self.migration_id = "0024"
        self.description = "Add run_id column to interval tables"
        self.logger = logging.getLogger(__name__)

    async def up(self, connection: asyncpg.Connection, environment_prefix: str) -> None:
        """Apply the migration."""
        
        table_name = f"{environment_prefix}_universe_state_interval"
        
        self.logger.info(f"Starting migration {self.migration_id}: {self.description}")
        self.logger.info(f"Environment: {environment_prefix}")
        
        # Check if run_id column already exists
        check_column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = $1 AND column_name = 'run_id'
        """
        
        existing_column = await connection.fetchval(check_column_query, table_name)
        
        if existing_column:
            self.logger.info(f"Column run_id already exists in {table_name}, skipping migration")
            return
        
        try:
            # Step 1: Add run_id column with default value for existing records
            self.logger.info(f"Adding run_id column to {table_name}")
            add_column_query = f"""
            ALTER TABLE {table_name} 
            ADD COLUMN run_id VARCHAR(255) NOT NULL DEFAULT 'legacy_run_pre_0024'
            """
            await connection.execute(add_column_query)
            
            # Step 2: Drop existing unique constraint
            self.logger.info(f"Dropping existing unique constraint on {table_name}")
            drop_constraint_query = f"""
            ALTER TABLE {table_name} 
            DROP CONSTRAINT IF EXISTS {table_name}_universe_id_duration_start_date_key
            """
            await connection.execute(drop_constraint_query)
            
            # Step 3: Create new unique constraint including run_id
            self.logger.info(f"Creating new unique constraint with run_id on {table_name}")
            add_constraint_query = f"""
            ALTER TABLE {table_name} 
            ADD CONSTRAINT {table_name}_universe_id_duration_start_date_run_id_key 
            UNIQUE (universe_id, duration, start_date_time, run_id)
            """
            await connection.execute(add_constraint_query)
            
            # Step 4: Add index on run_id for performance
            self.logger.info(f"Adding index on run_id for {table_name}")
            add_index_query = f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_run_id 
            ON {table_name} (run_id)
            """
            await connection.execute(add_index_query)
            
            # Step 5: Add composite index for common queries
            self.logger.info(f"Adding composite index for common queries on {table_name}")
            add_composite_index_query = f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_run_id_start_date 
            ON {table_name} (run_id, start_date_time)
            """
            await connection.execute(add_composite_index_query)
            
            # Step 6: Update column comment for documentation
            comment_query = f"""
            COMMENT ON COLUMN {table_name}.run_id IS 
            'Unique run identifier to allow multiple training runs to process same intervals'
            """
            await connection.execute(comment_query)
            
            self.logger.info(f"Successfully added run_id column to {table_name}")
            
            # Log the new table structure
            structure_query = f"""
            SELECT column_name, data_type, is_nullable, column_default 
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
            """
            
            columns = await connection.fetch(structure_query, table_name)
            self.logger.info(f"Updated table structure for {table_name}:")
            for col in columns:
                self.logger.info(f"  {col['column_name']} ({col['data_type']}) - "
                               f"nullable: {col['is_nullable']}, default: {col['column_default']}")
            
        except Exception as e:
            self.logger.error(f"Error in migration {self.migration_id}: {e}")
            raise

    async def down(self, connection: asyncpg.Connection, environment_prefix: str) -> None:
        """Rollback the migration."""
        
        table_name = f"{environment_prefix}_universe_state_interval"
        
        self.logger.info(f"Rolling back migration {self.migration_id}")
        
        try:
            # Step 1: Drop new constraints and indexes
            self.logger.info(f"Dropping new constraints and indexes from {table_name}")
            
            await connection.execute(f"""
            DROP INDEX IF EXISTS idx_{table_name}_run_id
            """)
            
            await connection.execute(f"""
            DROP INDEX IF EXISTS idx_{table_name}_run_id_start_date
            """)
            
            await connection.execute(f"""
            ALTER TABLE {table_name} 
            DROP CONSTRAINT IF EXISTS {table_name}_universe_id_duration_start_date_run_id_key
            """)
            
            # Step 2: Recreate original unique constraint
            self.logger.info(f"Recreating original unique constraint on {table_name}")
            await connection.execute(f"""
            ALTER TABLE {table_name} 
            ADD CONSTRAINT {table_name}_universe_id_duration_start_date_key 
            UNIQUE (universe_id, duration, start_date_time)
            """)
            
            # Step 3: Drop run_id column
            self.logger.info(f"Dropping run_id column from {table_name}")
            await connection.execute(f"""
            ALTER TABLE {table_name} DROP COLUMN IF EXISTS run_id
            """)
            
            self.logger.info(f"Successfully rolled back migration {self.migration_id}")
            
        except Exception as e:
            self.logger.error(f"Error rolling back migration {self.migration_id}: {e}")
            raise

    def get_migration_info(self) -> Dict[str, str]:
        """Get migration metadata."""
        return {
            "id": self.migration_id,
            "description": self.description,
            "tables_affected": ["universe_state_interval"],
            "breaking_changes": "Updates unique constraint to include run_id",
            "rollback_safe": "Yes, but will lose run_id data"
        }


async def apply_migration_to_environment(db_config: Dict[str, str], environment_prefix: str) -> bool:
    """Apply migration to a specific environment."""
    migration = Migration0024()
    
    try:
        # Connect to database
        conn = await asyncpg.connect(
            host=db_config['host'],
            port=db_config['port'], 
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        
        try:
            # Apply migration
            await migration.up(conn, environment_prefix)
            
            # Update migration tracking table
            await conn.execute(f"""
            INSERT INTO {environment_prefix}_migrations (migration_id, description, applied_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (migration_id) DO NOTHING
            """, migration.migration_id, migration.description)
            
            print(f"✅ Migration {migration.migration_id} applied successfully to {environment_prefix}")
            return True
            
        finally:
            await conn.close()
            
    except Exception as e:
        print(f"❌ Failed to apply migration {migration.migration_id} to {environment_prefix}: {e}")
        return False


async def main():
    """Main migration application function."""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 Applying Migration 0024: Add run_id to interval tables")
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
        try:
            success = await apply_migration_to_environment(config, config['prefix'])
            results[env_name] = success
        except Exception as e:
            print(f"❌ Unexpected error in {env_name}: {e}")
            results[env_name] = False
    
    # Summary
    print(f"\n🎯 Migration Summary:")
    print("=" * 30)
    
    all_successful = True
    for env_name, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{env_name.upper()}: {status}")
        if not success:
            all_successful = False
    
    if all_successful:
        print(f"\n🎉 Migration 0024 applied successfully to all environments!")
        print(f"   - run_id column added to universe_state_interval tables")
        print(f"   - Unique constraints updated to include run_id")
        print(f"   - Indexes created for performance")
    else:
        print(f"\n⚠️ Some environments failed. Check logs and retry failed environments.")
    
    return 0 if all_successful else 1


if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)