#!/usr/bin/env python3
"""
Data migration script to move existing data to environment-specific structure.
This script will:
1. Create new environment-specific databases (test_trading_db, intg_trading_db, prod_trading_db)
2. Create tables with environment-specific prefixes
3. Migrate all data from the original trading_db to the new structure
"""
import asyncio
import asyncpg
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.environment import EnvironmentType
from db.environment_migration import EnvironmentMigration


class DataMigrator:
    """Handles migration of data from original database to environment-specific structure."""
    
    def __init__(self, source_db_url: str):
        self.source_db_url = source_db_url
        self.migration_log = []
    
    def log(self, message: str, level: str = "INFO"):
        """Log migration progress."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        print(log_entry)
        self.migration_log.append(log_entry)
    
    async def get_source_tables(self) -> List[Dict[str, Any]]:
        """Get all tables from source database."""
        try:
            conn = await asyncpg.connect(self.source_db_url)
            tables = await conn.fetch("""
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            await conn.close()
            return [dict(row) for row in tables]
        except Exception as e:
            self.log(f"Error getting source tables: {e}", "ERROR")
            return []
    
    async def get_table_structure(self, table_name: str) -> List[Dict[str, Any]]:
        """Get the structure of a table."""
        try:
            conn = await asyncpg.connect(self.source_db_url)
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
    
    async def get_table_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """Get constraints for a table."""
        try:
            conn = await asyncpg.connect(self.source_db_url)
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
    
    def generate_create_table_sql(self, table_name: str, target_table_name: str, 
                                  columns: List[Dict], constraints: List[Dict]) -> str:
        """Generate CREATE TABLE SQL for target table."""
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
            
            # Add default value
            if col['column_default']:
                col_def += f" DEFAULT {col['column_default']}"
            
            col_definitions.append(col_def)
        
        # Add primary key constraints
        primary_keys = [c['column_name'] for c in constraints 
                       if c['constraint_type'] == 'PRIMARY KEY']
        if primary_keys:
            col_definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        
        sql = f"CREATE TABLE {target_table_name} (\n"
        sql += ",\n".join(f"    {col_def}" for col_def in col_definitions)
        sql += "\n);"
        
        return sql
    
    async def create_target_table(self, target_db_url: str, table_name: str, 
                                  target_table_name: str) -> bool:
        """Create target table with same structure as source."""
        try:
            # Get source table structure
            columns = await self.get_table_structure(table_name)
            constraints = await self.get_table_constraints(table_name)
            
            if not columns:
                self.log(f"No columns found for {table_name}", "WARNING")
                return False
            
            # Generate CREATE TABLE SQL
            create_sql = self.generate_create_table_sql(table_name, target_table_name, 
                                                       columns, constraints)
            
            # Execute CREATE TABLE
            conn = await asyncpg.connect(target_db_url)
            await conn.execute(create_sql)
            await conn.close()
            
            self.log(f"Created table {target_table_name}")
            return True
            
        except Exception as e:
            self.log(f"Error creating table {target_table_name}: {e}", "ERROR")
            return False
    
    async def migrate_table_data(self, source_table: str, target_db_url: str, 
                                 target_table: str) -> int:
        """Migrate data from source table to target table."""
        try:
            # Get source data
            source_conn = await asyncpg.connect(self.source_db_url)
            rows = await source_conn.fetch(f"SELECT * FROM {source_table}")
            row_count = len(rows)
            
            if row_count == 0:
                self.log(f"No data to migrate from {source_table}")
                await source_conn.close()
                return 0
            
            # Get column names
            columns = list(rows[0].keys())
            
            # Prepare target connection and insert data
            target_conn = await asyncpg.connect(target_db_url)
            
            # Create parameterized INSERT statement
            placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
            insert_sql = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Insert data in batches
            batch_size = 1000
            inserted = 0
            
            for i in range(0, row_count, batch_size):
                batch = rows[i:i + batch_size]
                batch_data = [tuple(row.values()) for row in batch]
                
                await target_conn.executemany(insert_sql, batch_data)
                inserted += len(batch)
                
                if inserted % 10000 == 0 or inserted == row_count:
                    self.log(f"Migrated {inserted}/{row_count} rows from {source_table} to {target_table}")
            
            await source_conn.close()
            await target_conn.close()
            
            self.log(f"Successfully migrated {inserted} rows from {source_table} to {target_table}")
            return inserted
            
        except Exception as e:
            self.log(f"Error migrating data from {source_table} to {target_table}: {e}", "ERROR")
            return 0
    
    async def migrate_to_environment(self, env_type: EnvironmentType) -> Dict[str, Any]:
        """Migrate all data to a specific environment."""
        self.log(f"Starting migration to {env_type.value} environment")
        
        # Initialize environment migration
        env_migration = EnvironmentMigration(env_type)
        
        # Setup environment (create database, tables, indexes)
        self.log(f"Setting up {env_type.value} environment")
        setup_success = await env_migration.setup_environment()
        
        if not setup_success:
            self.log(f"Failed to setup {env_type.value} environment", "ERROR")
            return {"success": False, "error": "Environment setup failed"}
        
        # Get source tables
        source_tables = await self.get_source_tables()
        if not source_tables:
            self.log("No source tables found", "ERROR")
            return {"success": False, "error": "No source tables found"}
        
        # Get target database URL
        target_db_url = env_migration.env.get_database_url()
        
        migration_results = {
            "environment": env_type.value,
            "tables_migrated": 0,
            "total_rows_migrated": 0,
            "failed_tables": [],
            "success": True
        }
        
        # Migrate each table
        for table_info in source_tables:
            source_table = table_info['table_name']
            target_table = env_migration.env.get_table_name(source_table)
            
            self.log(f"Migrating {source_table} -> {target_table}")
            
            try:
                # Migrate data (table should already exist from setup)
                rows_migrated = await self.migrate_table_data(source_table, target_db_url, target_table)
                
                if rows_migrated >= 0:  # 0 is valid for empty tables
                    migration_results["tables_migrated"] += 1
                    migration_results["total_rows_migrated"] += rows_migrated
                else:
                    migration_results["failed_tables"].append(source_table)
                    
            except Exception as e:
                self.log(f"Failed to migrate {source_table}: {e}", "ERROR")
                migration_results["failed_tables"].append(source_table)
        
        if migration_results["failed_tables"]:
            migration_results["success"] = False
        
        self.log(f"Migration to {env_type.value} completed: "
                f"{migration_results['tables_migrated']} tables, "
                f"{migration_results['total_rows_migrated']} total rows")
        
        return migration_results
    
    async def migrate_all_environments(self) -> Dict[str, Any]:
        """Migrate data to all environments."""
        self.log("Starting full migration to all environments")
        
        results = {
            "start_time": datetime.now(),
            "environments": {},
            "overall_success": True
        }
        
        # Migrate to each environment
        for env_type in [EnvironmentType.TEST, EnvironmentType.INTEGRATION, EnvironmentType.PRODUCTION]:
            try:
                env_result = await self.migrate_to_environment(env_type)
                results["environments"][env_type.value] = env_result
                
                if not env_result["success"]:
                    results["overall_success"] = False
                    
            except Exception as e:
                self.log(f"Critical error migrating to {env_type.value}: {e}", "ERROR")
                results["environments"][env_type.value] = {
                    "success": False,
                    "error": str(e)
                }
                results["overall_success"] = False
        
        results["end_time"] = datetime.now()
        results["duration"] = results["end_time"] - results["start_time"]
        
        self.log(f"Full migration completed in {results['duration']}")
        return results


async def main():
    """Main migration function."""
    # Source database URL from environment
    source_db_url = os.getenv('TSDB_URL', 'postgresql://postgres:postgres@localhost:5432/trading_db')
    
    print("=" * 80)
    print("DATA MIGRATION TO ENVIRONMENT-SPECIFIC STRUCTURE")
    print("=" * 80)
    print(f"Source Database: {source_db_url}")
    print(f"Target Environments: test_trading_db, intg_trading_db, prod_trading_db")
    print()
    
    # Confirm migration
    if len(sys.argv) > 1 and sys.argv[1] == "--confirm":
        proceed = True
    else:
        response = input("This will create new databases and migrate all data. Continue? (yes/no): ")
        proceed = response.lower() in ['yes', 'y']
    
    if not proceed:
        print("Migration cancelled.")
        return
    
    # Initialize migrator
    migrator = DataMigrator(source_db_url)
    
    # Run migration
    try:
        results = await migrator.migrate_all_environments()
        
        # Print results summary
        print("\n" + "=" * 80)
        print("MIGRATION RESULTS SUMMARY")
        print("=" * 80)
        
        for env_name, env_result in results["environments"].items():
            status = "✅ SUCCESS" if env_result.get("success", False) else "❌ FAILED"
            print(f"{env_name.upper()}: {status}")
            
            if env_result.get("success", False):
                print(f"  - Tables migrated: {env_result.get('tables_migrated', 0)}")
                print(f"  - Rows migrated: {env_result.get('total_rows_migrated', 0)}")
                if env_result.get('failed_tables'):
                    print(f"  - Failed tables: {env_result['failed_tables']}")
            else:
                print(f"  - Error: {env_result.get('error', 'Unknown error')}")
            print()
        
        overall_status = "✅ SUCCESS" if results["overall_success"] else "❌ PARTIAL FAILURE"
        print(f"OVERALL STATUS: {overall_status}")
        print(f"Duration: {results['duration']}")
        
        # Save migration log
        log_file = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_file, 'w') as f:
            f.write("\n".join(migrator.migration_log))
        print(f"Migration log saved to: {log_file}")
        
    except Exception as e:
        print(f"Critical migration error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
