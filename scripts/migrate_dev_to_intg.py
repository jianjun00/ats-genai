#!/usr/bin/env python3
"""
Database Migration Script: dev_db to intg_db
Copies all tables from dev_ prefix to intg_ prefix and synchronizes sequences
"""

import asyncio
import asyncpg
import os
import sys
import argparse
from typing import List, Tuple, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection configurations
DEV_DB_CONFIG = {
    'host': os.getenv('DEV_DB_HOST', 'postgres.ats-dev.svc.cluster.local'),
    'port': int(os.getenv('DEV_DB_PORT', 5432)),
    'user': os.getenv('DEV_DB_USER', 'postgres'),
    'password': os.getenv('DEV_DB_PASSWORD', 'dev_password'),
    'database': os.getenv('DEV_DB_NAME', 'dev_db')
}

INTG_DB_CONFIG = {
    'host': os.getenv('INTG_DB_HOST', 'postgres.ats-intg.svc.cluster.local'),
    'port': int(os.getenv('INTG_DB_PORT', 5432)),
    'user': os.getenv('INTG_DB_USER', 'postgres'),
    'password': os.getenv('INTG_DB_PASSWORD', 'intg_password'),
    'database': os.getenv('INTG_DB_NAME', 'intg_db')
}

class DatabaseMigrator:
    def __init__(self, dev_config: Dict, intg_config: Dict):
        self.dev_config = dev_config
        self.intg_config = intg_config
        self.dev_conn = None
        self.intg_conn = None

    async def connect(self):
        """Establish connections to both databases"""
        try:
            logger.info("Connecting to dev database...")
            self.dev_conn = await asyncpg.connect(**self.dev_config)
            logger.info("✅ Connected to dev database")
            
            logger.info("Connecting to intg database...")
            self.intg_conn = await asyncpg.connect(**self.intg_config)
            logger.info("✅ Connected to intg database")
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise

    async def disconnect(self):
        """Close database connections"""
        if self.dev_conn:
            await self.dev_conn.close()
        if self.intg_conn:
            await self.intg_conn.close()

    async def get_dev_tables(self) -> List[str]:
        """Get all tables with dev_ prefix"""
        query = """
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE 'dev_%'
        ORDER BY tablename
        """
        rows = await self.dev_conn.fetch(query)
        tables = [row['tablename'] for row in rows]
        logger.info(f"Found {len(tables)} dev_ tables: {', '.join(tables)}")
        return tables

    async def get_table_structure(self, table_name: str) -> str:
        """Get CREATE TABLE statement for a table"""
        # Get table columns and types
        query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = $1 
        AND table_schema = 'public'
        ORDER BY ordinal_position
        """
        columns = await self.dev_conn.fetch(query, table_name)
        
        # Get constraints
        constraint_query = """
        SELECT 
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage ccu 
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_name = $1 
        AND tc.table_schema = 'public'
        """
        constraints = await self.dev_conn.fetch(constraint_query, table_name)
        
        # Build CREATE TABLE statement
        intg_table_name = table_name.replace('dev_', 'intg_', 1)
        columns_sql = []
        
        for col in columns:
            col_def = f"{col['column_name']} {col['data_type']}"
            
            if col['character_maximum_length']:
                col_def += f"({col['character_maximum_length']})"
            
            if col['is_nullable'] == 'NO':
                col_def += " NOT NULL"
                
            if col['column_default']:
                # Handle sequence defaults - update to intg_ prefix
                default_val = col['column_default']
                if 'dev_' in default_val:
                    default_val = default_val.replace('dev_', 'intg_')
                col_def += f" DEFAULT {default_val}"
                
            columns_sql.append(col_def)
        
        # Add constraints
        for constraint in constraints:
            if constraint['constraint_type'] == 'PRIMARY KEY':
                columns_sql.append(f"PRIMARY KEY ({constraint['column_name']})")
            elif constraint['constraint_type'] == 'FOREIGN KEY':
                foreign_table = constraint['foreign_table_name']
                if foreign_table.startswith('dev_'):
                    foreign_table = foreign_table.replace('dev_', 'intg_', 1)
                columns_sql.append(
                    f"FOREIGN KEY ({constraint['column_name']}) "
                    f"REFERENCES {foreign_table}({constraint['foreign_column_name']})"
                )
        
        create_sql = f"CREATE TABLE {intg_table_name} (\n  " + ",\n  ".join(columns_sql) + "\n)"
        return create_sql

    async def get_sequences(self, table_name: str) -> List[Tuple[str, str]]:
        """Get sequences associated with a table"""
        query = """
        SELECT 
            s.sequence_name,
            c.column_name
        FROM information_schema.sequences s
        JOIN pg_class cl ON cl.relname = s.sequence_name
        JOIN pg_depend d ON d.objid = cl.oid
        JOIN pg_class t ON d.refobjid = t.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        JOIN information_schema.columns c ON c.table_name = t.relname AND c.column_name = a.attname
        WHERE t.relname = $1
        """
        rows = await self.dev_conn.fetch(query, table_name)
        return [(row['sequence_name'], row['column_name']) for row in rows]

    async def create_sequence(self, dev_sequence: str, intg_sequence: str):
        """Create sequence in intg database with same current value"""
        # Get current value from dev sequence
        current_val = await self.dev_conn.fetchval(f"SELECT last_value FROM {dev_sequence}")
        
        # Create sequence in intg database
        await self.intg_conn.execute(f"DROP SEQUENCE IF EXISTS {intg_sequence} CASCADE")
        await self.intg_conn.execute(f"CREATE SEQUENCE {intg_sequence} START WITH {current_val}")
        
        logger.info(f"✅ Created sequence {intg_sequence} with value {current_val}")

    async def copy_table_data(self, dev_table: str, intg_table: str):
        """Copy all data from dev table to intg table"""
        # Get row count
        count = await self.dev_conn.fetchval(f"SELECT COUNT(*) FROM {dev_table}")
        if count == 0:
            logger.info(f"⚠️  Table {dev_table} is empty, skipping data copy")
            return
            
        logger.info(f"📊 Copying {count} rows from {dev_table} to {intg_table}")
        
        # Get all data
        rows = await self.dev_conn.fetch(f"SELECT * FROM {dev_table}")
        
        if not rows:
            return
            
        # Get column names
        columns = list(rows[0].keys())
        
        # Build INSERT statement
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        insert_sql = f"INSERT INTO {intg_table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            batch_values = [list(row.values()) for row in batch]
            await self.intg_conn.executemany(insert_sql, batch_values)
            logger.info(f"  📝 Inserted batch {i//batch_size + 1}: {len(batch)} rows")

    async def migrate_table(self, dev_table: str):
        """Migrate a single table from dev to intg"""
        intg_table = dev_table.replace('dev_', 'intg_', 1)
        
        logger.info(f"🔄 Migrating {dev_table} → {intg_table}")
        
        try:
            # 1. Handle sequences first
            sequences = await self.get_sequences(dev_table)
            for dev_seq, column in sequences:
                intg_seq = dev_seq.replace('dev_', 'intg_', 1)
                await self.create_sequence(dev_seq, intg_seq)
            
            # 2. Get table structure and create intg table
            create_sql = await self.get_table_structure(dev_table)
            
            # Drop existing table
            await self.intg_conn.execute(f"DROP TABLE IF EXISTS {intg_table} CASCADE")
            
            # Create new table
            await self.intg_conn.execute(create_sql)
            logger.info(f"✅ Created table structure for {intg_table}")
            
            # 3. Copy data
            await self.copy_table_data(dev_table, intg_table)
            
            # 4. Update sequences to current max value
            for dev_seq, column in sequences:
                intg_seq = dev_seq.replace('dev_', 'intg_', 1)
                max_val = await self.intg_conn.fetchval(f"SELECT COALESCE(MAX({column}), 0) FROM {intg_table}")
                if max_val > 0:
                    await self.intg_conn.execute(f"SELECT setval('{intg_seq}', {max_val})")
                    logger.info(f"🔢 Set {intg_seq} to {max_val}")
            
            logger.info(f"✅ Successfully migrated {dev_table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to migrate {dev_table}: {e}")
            raise

    async def verify_migration(self):
        """Verify that migration was successful"""
        logger.info("🔍 Verifying migration...")
        
        # Get table counts
        dev_tables = await self.get_dev_tables()
        
        verification_results = []
        
        for dev_table in dev_tables:
            intg_table = dev_table.replace('dev_', 'intg_', 1)
            
            # Check if intg table exists
            intg_exists = await self.intg_conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )
            """, intg_table)
            
            if not intg_exists:
                verification_results.append(f"❌ {intg_table} does not exist")
                continue
            
            # Compare row counts
            dev_count = await self.dev_conn.fetchval(f"SELECT COUNT(*) FROM {dev_table}")
            intg_count = await self.intg_conn.fetchval(f"SELECT COUNT(*) FROM {intg_table}")
            
            if dev_count == intg_count:
                verification_results.append(f"✅ {dev_table} → {intg_table}: {dev_count} rows")
            else:
                verification_results.append(f"❌ {dev_table} → {intg_table}: {dev_count} vs {intg_count} rows")
        
        logger.info("📊 Migration Verification Results:")
        for result in verification_results:
            logger.info(f"  {result}")
        
        return all("✅" in result for result in verification_results)

    async def run_migration(self, tables: List[str] = None, dry_run: bool = False):
        """Run the complete migration process"""
        await self.connect()
        
        try:
            dev_tables = await self.get_dev_tables()
            
            if not dev_tables:
                logger.warning("⚠️  No dev_ tables found in source database")
                return
            
            if tables:
                # Filter to specific tables
                dev_tables = [t for t in dev_tables if t in tables]
                if not dev_tables:
                    logger.error(f"❌ No matching tables found for: {tables}")
                    return
            
            logger.info(f"🚀 Starting migration of {len(dev_tables)} tables")
            logger.info(f"📋 Tables to migrate: {', '.join(dev_tables)}")
            
            if dry_run:
                logger.info("🔍 DRY RUN MODE - No changes will be made")
                for table in dev_tables:
                    intg_table = table.replace('dev_', 'intg_', 1)
                    count = await self.dev_conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    sequences = await self.get_sequences(table)
                    logger.info(f"  📊 {table} → {intg_table}: {count} rows, {len(sequences)} sequences")
                return
            
            # Migrate each table
            for table in dev_tables:
                await self.migrate_table(table)
            
            # Verify migration
            success = await self.verify_migration()
            
            if success:
                logger.info("🎉 Migration completed successfully!")
            else:
                logger.error("❌ Migration completed with errors - check verification results")
                
        finally:
            await self.disconnect()

async def main():
    parser = argparse.ArgumentParser(description='Migrate dev_db tables to intg_db with prefix change')
    parser.add_argument('--tables', nargs='+', help='Specific tables to migrate (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without making changes')
    parser.add_argument('--dev-host', default=DEV_DB_CONFIG['host'], help='Dev database host')
    parser.add_argument('--intg-host', default=INTG_DB_CONFIG['host'], help='Integration database host')
    
    args = parser.parse_args()
    
    # Update configs with command line args
    dev_config = DEV_DB_CONFIG.copy()
    intg_config = INTG_DB_CONFIG.copy()
    
    dev_config['host'] = args.dev_host
    intg_config['host'] = args.intg_host
    
    migrator = DatabaseMigrator(dev_config, intg_config)
    
    try:
        await migrator.run_migration(args.tables, args.dry_run)
    except KeyboardInterrupt:
        logger.info("🛑 Migration interrupted by user")
    except Exception as e:
        logger.error(f"💥 Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())