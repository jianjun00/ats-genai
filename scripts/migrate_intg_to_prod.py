#!/usr/bin/env python3
"""
Migration script to create production database and copy data from integration environment.
This script will:
1. Create prod_trading_db database
2. Create all tables with prod_ prefixes
3. Copy all data from integration environment to production environment
"""
import asyncio
import asyncpg
import sys
import os
from typing import List, Dict, Any
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.environment import EnvironmentType, set_environment, get_environment
from db.environment_migration import EnvironmentMigration


class IntegrationToProductionMigrator:
    """Handles migration from integration to production environment."""
    
    def __init__(self):
        self.migration_log = []
        
        # Set up integration environment (source)
        set_environment(EnvironmentType.INTEGRATION)
        self.intg_env = get_environment()
        self.intg_db_url = self.intg_env.get_database_url()
        
        # Set up production environment (target) - use same connection pattern as test/intg
        # Override the production config to use local database instead of placeholders
        self.prod_db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres',
            'database': 'prod_trading_db',
            'min_size': 1,
            'max_size': 10,
            'command_timeout': 60
        }
        self.prod_db_url = f"postgresql://{self.prod_db_config['user']}:{self.prod_db_config['password']}@{self.prod_db_config['host']}:{self.prod_db_config['port']}/{self.prod_db_config['database']}"
    
    def log(self, message: str, level: str = "INFO"):
        """Log migration progress."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        print(log_entry)
        self.migration_log.append(log_entry)
    
    async def create_prod_database(self):
        """Create the production database."""
        try:
            # Connect to postgres database to create the target database
            admin_url = f"postgresql://{self.prod_db_config['user']}:{self.prod_db_config['password']}@{self.prod_db_config['host']}:{self.prod_db_config['port']}/postgres"
            
            conn = await asyncpg.connect(admin_url)
            try:
                # Check if database exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1", self.prod_db_config['database']
                )
                
                if not exists:
                    await conn.execute(f'CREATE DATABASE "{self.prod_db_config["database"]}"')
                    self.log(f"Created database: {self.prod_db_config['database']}")
                else:
                    self.log(f"Database already exists: {self.prod_db_config['database']}")
                    
            finally:
                await conn.close()
            
            return True
        except Exception as e:
            self.log(f"Error creating production database: {e}", "ERROR")
            return False
    
    async def get_intg_tables(self) -> List[Dict[str, Any]]:
        """Get all tables from integration database."""
        try:
            conn = await asyncpg.connect(self.intg_db_url)
            tables = await conn.fetch("""
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'intg_%'
                ORDER BY table_name
            """)
            await conn.close()
            return [dict(row) for row in tables]
        except Exception as e:
            self.log(f"Error getting integration tables: {e}", "ERROR")
            return []
    
    async def get_table_structure(self, table_name: str, db_url: str) -> List[Dict[str, Any]]:
        """Get the structure of a table."""
        try:
            conn = await asyncpg.connect(db_url)
            columns = await conn.fetch("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
            """, table_name)
            await conn.close()
            return [dict(row) for row in columns]
        except Exception as e:
            self.log(f"Error getting structure for {table_name}: {e}", "ERROR")
            return []
    
    async def get_table_constraints(self, table_name: str, db_url: str) -> List[Dict[str, Any]]:
        """Get constraints for a table."""
        try:
            conn = await asyncpg.connect(db_url)
            constraints = await conn.fetch("""
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
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.table_name = $1 AND tc.table_schema = 'public'
                ORDER BY tc.constraint_name
            """, table_name)
            await conn.close()
            return [dict(row) for row in constraints]
        except Exception as e:
            self.log(f"Error getting constraints for {table_name}: {e}", "ERROR")
            return []
    
    def generate_create_table_sql(self, intg_table_name: str, prod_table_name: str, 
                                  columns: List[Dict], constraints: List[Dict]) -> str:
        """Generate CREATE TABLE SQL for production table."""
        col_definitions = []
        
        for col in columns:
            col_def = f"{col['column_name']} {col['data_type']}"
            
            # Add length/precision for specific types
            if col['character_maximum_length']:
                col_def += f"({col['character_maximum_length']})"
            elif col['numeric_precision'] and col['data_type'] in ['numeric', 'decimal']:
                if col['numeric_scale']:
                    col_def += f"({col['numeric_precision']},{col['numeric_scale']})"
                else:
                    col_def += f"({col['numeric_precision']})"
            
            # Add NOT NULL constraint
            if col['is_nullable'] == 'NO':
                col_def += " NOT NULL"
            
            # Add default value (but fix references to integration tables)
            if col['column_default']:
                default_val = col['column_default']
                # Replace any references to intg_ tables with prod_ tables in defaults
                if 'intg_' in default_val:
                    default_val = default_val.replace('intg_', 'prod_')
                col_def += f" DEFAULT {default_val}"
            
            col_definitions.append(col_def)
        
        # Add primary key constraints
        primary_keys = [c['column_name'] for c in constraints 
                       if c['constraint_type'] == 'PRIMARY KEY']
        if primary_keys:
            col_definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        
        # Add foreign key constraints (with table name updates)
        for constraint in constraints:
            if constraint['constraint_type'] == 'FOREIGN KEY':
                foreign_table = constraint['foreign_table_name']
                if foreign_table and foreign_table.startswith('intg_'):
                    prod_foreign_table = foreign_table.replace('intg_', 'prod_')
                    col_definitions.append(
                        f"FOREIGN KEY ({constraint['column_name']}) REFERENCES {prod_foreign_table}({constraint['foreign_column_name']})"
                    )
        
        sql = f"CREATE TABLE {prod_table_name} (\n"
        sql += ",\n".join(f"    {col_def}" for col_def in col_definitions)
        sql += "\n);"
        
        return sql
    
    async def create_prod_table(self, intg_table_name: str, prod_table_name: str) -> bool:
        """Create production table with same structure as integration table."""
        try:
            # Get integration table structure
            columns = await self.get_table_structure(intg_table_name, self.intg_db_url)
            constraints = await self.get_table_constraints(intg_table_name, self.intg_db_url)
            
            if not columns:
                self.log(f"No columns found for {intg_table_name}", "WARNING")
                return False
            
            # Generate CREATE TABLE SQL
            create_sql = self.generate_create_table_sql(intg_table_name, prod_table_name, 
                                                       columns, constraints)
            
            # Execute CREATE TABLE
            conn = await asyncpg.connect(self.prod_db_url)
            await conn.execute(create_sql)
            await conn.close()
            
            self.log(f"Created table {prod_table_name}")
            return True
            
        except Exception as e:
            self.log(f"Error creating table {prod_table_name}: {e}", "ERROR")
            return False
    
    async def copy_table_data(self, intg_table: str, prod_table: str) -> int:
        """Copy data from integration table to production table."""
        try:
            # Get integration data
            intg_conn = await asyncpg.connect(self.intg_db_url)
            rows = await intg_conn.fetch(f"SELECT * FROM {intg_table}")
            row_count = len(rows)
            
            if row_count == 0:
                self.log(f"No data to copy from {intg_table}")
                await intg_conn.close()
                return 0
            
            # Get column names
            columns = list(rows[0].keys())
            
            # Prepare production connection and insert data
            prod_conn = await asyncpg.connect(self.prod_db_url)
            
            # Create parameterized INSERT statement
            placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
            insert_sql = f"INSERT INTO {prod_table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Insert data in batches
            batch_size = 1000
            inserted = 0
            
            for i in range(0, row_count, batch_size):
                batch = rows[i:i + batch_size]
                batch_data = [tuple(row.values()) for row in batch]
                
                await prod_conn.executemany(insert_sql, batch_data)
                inserted += len(batch)
                
                if inserted % 10000 == 0 or inserted == row_count:
                    self.log(f"Copied {inserted}/{row_count} rows from {intg_table} to {prod_table}")
            
            await intg_conn.close()
            await prod_conn.close()
            
            self.log(f"Successfully copied {inserted} rows from {intg_table} to {prod_table}")
            return inserted
            
        except Exception as e:
            self.log(f"Error copying data from {intg_table} to {prod_table}: {e}", "ERROR")
            return 0
    
    async def create_prod_indexes(self):
        """Create indexes for production tables."""
        try:
            conn = await asyncpg.connect(self.prod_db_url)
            
            # Define indexes for production tables
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_prices_symbol ON prod_daily_prices (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_prices_date ON prod_daily_prices (date)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_prices_polygon_symbol ON prod_daily_prices_polygon (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_prices_polygon_date ON prod_daily_prices_polygon (date)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_prices_tiingo_symbol ON prod_daily_prices_tiingo (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_prices_tiingo_date ON prod_daily_prices_tiingo (date)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_adjusted_prices_symbol ON prod_daily_adjusted_prices (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_prod_daily_adjusted_prices_date ON prod_daily_adjusted_prices (date)",
                "CREATE INDEX IF NOT EXISTS idx_prod_instrument_polygon_symbol ON prod_instrument_polygon (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_prod_universe_membership_symbol ON prod_universe_membership (symbol)",
                "CREATE INDEX IF NOT EXISTS idx_prod_universe_membership_dates ON prod_universe_membership (start_at, end_at)",
                "CREATE INDEX IF NOT EXISTS idx_prod_universe_membership_universe_id ON prod_universe_membership (universe_id)",
                "CREATE INDEX IF NOT EXISTS idx_prod_spy_membership_change_date ON prod_spy_membership_change (change_date)",
                "CREATE INDEX IF NOT EXISTS idx_prod_events_date ON prod_events (date)",
                "CREATE INDEX IF NOT EXISTS idx_prod_events_symbol ON prod_events (symbol)",
            ]
            
            for index_sql in indexes:
                try:
                    await conn.execute(index_sql)
                    index_name = index_sql.split()[-1].split('(')[0]
                    self.log(f"Created index: {index_name}")
                except Exception as e:
                    self.log(f"Error creating index: {e}", "WARNING")
            
            await conn.close()
            
        except Exception as e:
            self.log(f"Error creating production indexes: {e}", "ERROR")
    
    async def migrate_intg_to_prod(self) -> Dict[str, Any]:
        """Complete migration from integration to production."""
        self.log("Starting migration from integration to production environment")
        
        results = {
            "success": True,
            "tables_migrated": 0,
            "total_rows_migrated": 0,
            "failed_tables": []
        }
        
        # Create production database
        if not await self.create_prod_database():
            results["success"] = False
            results["error"] = "Failed to create production database"
            return results
        
        # Get integration tables
        intg_tables = await self.get_intg_tables()
        if not intg_tables:
            results["success"] = False
            results["error"] = "No integration tables found"
            return results
        
        self.log(f"Found {len(intg_tables)} integration tables to migrate")
        
        # Create tables first (to handle foreign key dependencies)
        for table_info in intg_tables:
            intg_table = table_info['table_name']
            prod_table = intg_table.replace('intg_', 'prod_')
            
            self.log(f"Creating production table: {intg_table} -> {prod_table}")
            
            if not await self.create_prod_table(intg_table, prod_table):
                results["failed_tables"].append(intg_table)
                results["success"] = False
        
        # Copy data for all tables
        for table_info in intg_tables:
            intg_table = table_info['table_name']
            prod_table = intg_table.replace('intg_', 'prod_')
            
            self.log(f"Copying data: {intg_table} -> {prod_table}")
            
            rows_copied = await self.copy_table_data(intg_table, prod_table)
            if rows_copied >= 0:  # 0 is valid for empty tables
                results["tables_migrated"] += 1
                results["total_rows_migrated"] += rows_copied
            else:
                results["failed_tables"].append(intg_table)
                results["success"] = False
        
        # Create indexes
        self.log("Creating production indexes")
        await self.create_prod_indexes()
        
        self.log(f"Migration completed: {results['tables_migrated']} tables, {results['total_rows_migrated']} total rows")
        
        return results


async def main():
    """Main migration function."""
    print("=" * 80)
    print("INTEGRATION TO PRODUCTION MIGRATION")
    print("=" * 80)
    print("Source: intg_trading_db (integration environment)")
    print("Target: prod_trading_db (production environment)")
    print()
    
    # Confirm migration
    if len(sys.argv) > 1 and sys.argv[1] == "--confirm":
        proceed = True
    else:
        response = input("This will create production database and copy all integration data. Continue? (yes/no): ")
        proceed = response.lower() in ['yes', 'y']
    
    if not proceed:
        print("Migration cancelled.")
        return
    
    # Initialize migrator
    migrator = IntegrationToProductionMigrator()
    
    # Run migration
    try:
        results = await migrator.migrate_intg_to_prod()
        
        # Print results summary
        print("\n" + "=" * 80)
        print("MIGRATION RESULTS SUMMARY")
        print("=" * 80)
        
        status = "✅ SUCCESS" if results["success"] else "❌ FAILED"
        print(f"PRODUCTION MIGRATION: {status}")
        
        if results["success"]:
            print(f"  - Tables migrated: {results['tables_migrated']}")
            print(f"  - Rows migrated: {results['total_rows_migrated']}")
            if results.get('failed_tables'):
                print(f"  - Failed tables: {results['failed_tables']}")
        else:
            print(f"  - Error: {results.get('error', 'Unknown error')}")
            if results.get('failed_tables'):
                print(f"  - Failed tables: {results['failed_tables']}")
        
        # Save migration log
        log_file = f"prod_migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_file, 'w') as f:
            f.write("\n".join(migrator.migration_log))
        print(f"Migration log saved to: {log_file}")
        
    except Exception as e:
        print(f"Critical migration error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
