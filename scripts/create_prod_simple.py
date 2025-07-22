#!/usr/bin/env python3
"""
Simplified production migration script that uses the existing environment migration utility
and then copies data from integration environment.
"""
import asyncio
import asyncpg
import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.environment import EnvironmentType, set_environment, get_environment
from db.environment_migration import EnvironmentMigration


class SimpleProductionMigrator:
    """Simple production migration using existing utilities."""
    
    def __init__(self):
        self.migration_log = []
        
        # Set up integration environment (source)
        set_environment(EnvironmentType.INTEGRATION)
        self.intg_env = get_environment()
        self.intg_db_url = self.intg_env.get_database_url()
        
        # Set up production environment (target) - override config for local setup
        self.prod_db_config = {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'postgres',
            'database': 'prod_trading_db'
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
            admin_url = f"postgresql://{self.prod_db_config['user']}:{self.prod_db_config['password']}@{self.prod_db_config['host']}:{self.prod_db_config['port']}/postgres"
            
            conn = await asyncpg.connect(admin_url)
            try:
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
    
    async def setup_prod_tables(self):
        """Set up production tables using the environment migration utility."""
        try:
            # Override production config temporarily for local setup
            import tempfile
            import configparser
            
            # Create temporary production config
            temp_config = configparser.ConfigParser()
            temp_config.add_section('database')
            temp_config.set('database', 'host', 'localhost')
            temp_config.set('database', 'port', '5432')
            temp_config.set('database', 'user', 'postgres')
            temp_config.set('database', 'password', 'postgres')
            temp_config.set('database', 'database', 'prod_trading_db')
            temp_config.set('database', 'prefix', 'prod_')
            
            # Write temporary config
            with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
                temp_config.write(f)
                temp_config_path = f.name
            
            # Temporarily override the production config file
            prod_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'prod.conf')
            backup_config_path = prod_config_path + '.backup'
            
            # Backup original config
            if os.path.exists(prod_config_path):
                os.rename(prod_config_path, backup_config_path)
            
            # Copy temp config to prod config
            import shutil
            shutil.copy2(temp_config_path, prod_config_path)
            
            try:
                # Now use the environment migration utility
                set_environment(EnvironmentType.PRODUCTION)
                prod_migration = EnvironmentMigration(EnvironmentType.PRODUCTION)
                
                self.log("Setting up production environment using EnvironmentMigration")
                success = await prod_migration.setup_environment()
                
                if success:
                    self.log("Production environment setup completed successfully")
                else:
                    self.log("Production environment setup failed", "ERROR")
                
                return success
                
            finally:
                # Restore original config
                os.remove(prod_config_path)
                os.remove(temp_config_path)
                if os.path.exists(backup_config_path):
                    os.rename(backup_config_path, prod_config_path)
                
        except Exception as e:
            self.log(f"Error setting up production tables: {e}", "ERROR")
            return False
    
    async def get_intg_tables_with_data(self):
        """Get integration tables that have data."""
        try:
            conn = await asyncpg.connect(self.intg_db_url)
            
            # Get all intg tables
            tables = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'intg_%'
                ORDER BY table_name
            """)
            
            tables_with_data = []
            for table in tables:
                table_name = table['table_name']
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                if count > 0:
                    tables_with_data.append({
                        'table_name': table_name,
                        'row_count': count
                    })
            
            await conn.close()
            return tables_with_data
            
        except Exception as e:
            self.log(f"Error getting integration tables: {e}", "ERROR")
            return []
    
    async def copy_table_data(self, intg_table: str, row_count: int) -> int:
        """Copy data from integration table to production table."""
        prod_table = intg_table.replace('intg_', 'prod_')
        
        try:
            # Get integration data
            intg_conn = await asyncpg.connect(self.intg_db_url)
            rows = await intg_conn.fetch(f"SELECT * FROM {intg_table}")
            
            if not rows:
                self.log(f"No data in {intg_table}")
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
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                batch_data = [tuple(row.values()) for row in batch]
                
                await prod_conn.executemany(insert_sql, batch_data)
                inserted += len(batch)
                
                if inserted % 10000 == 0 or inserted == len(rows):
                    self.log(f"Copied {inserted}/{len(rows)} rows from {intg_table} to {prod_table}")
            
            await intg_conn.close()
            await prod_conn.close()
            
            self.log(f"Successfully copied {inserted} rows from {intg_table} to {prod_table}")
            return inserted
            
        except Exception as e:
            self.log(f"Error copying data from {intg_table} to {prod_table}: {e}", "ERROR")
            return -1
    
    async def migrate(self):
        """Complete production migration."""
        self.log("Starting simplified production migration")
        
        results = {
            "success": True,
            "tables_migrated": 0,
            "total_rows_migrated": 0,
            "failed_tables": []
        }
        
        # Step 1: Create production database
        self.log("Step 1: Creating production database")
        if not await self.create_prod_database():
            results["success"] = False
            results["error"] = "Failed to create production database"
            return results
        
        # Step 2: Set up production tables using environment migration
        self.log("Step 2: Setting up production tables")
        if not await self.setup_prod_tables():
            results["success"] = False
            results["error"] = "Failed to set up production tables"
            return results
        
        # Step 3: Get integration tables with data
        self.log("Step 3: Finding integration tables with data")
        intg_tables = await self.get_intg_tables_with_data()
        if not intg_tables:
            self.log("No integration tables with data found", "WARNING")
            return results
        
        self.log(f"Found {len(intg_tables)} integration tables with data")
        
        # Step 4: Copy data from integration to production
        self.log("Step 4: Copying data from integration to production")
        for table_info in intg_tables:
            intg_table = table_info['table_name']
            row_count = table_info['row_count']
            
            self.log(f"Copying {intg_table} ({row_count} rows)")
            
            rows_copied = await self.copy_table_data(intg_table, row_count)
            if rows_copied >= 0:
                results["tables_migrated"] += 1
                results["total_rows_migrated"] += rows_copied
            else:
                results["failed_tables"].append(intg_table)
                results["success"] = False
        
        self.log(f"Migration completed: {results['tables_migrated']} tables, {results['total_rows_migrated']} total rows")
        
        if results["failed_tables"]:
            results["success"] = False
        
        return results


async def main():
    """Main migration function."""
    print("=" * 80)
    print("SIMPLIFIED PRODUCTION MIGRATION")
    print("=" * 80)
    print("Source: intg_trading_db (integration environment)")
    print("Target: prod_trading_db (production environment)")
    print("Method: Use EnvironmentMigration utility + data copy")
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
    migrator = SimpleProductionMigrator()
    
    # Run migration
    try:
        results = await migrator.migrate()
        
        # Print results summary
        print("\n" + "=" * 80)
        print("MIGRATION RESULTS SUMMARY")
        print("=" * 80)
        
        status = "✅ SUCCESS" if results["success"] else "❌ FAILED"
        print(f"PRODUCTION MIGRATION: {status}")
        
        if results["success"]:
            print(f"  - Tables migrated: {results['tables_migrated']}")
            print(f"  - Rows migrated: {results['total_rows_migrated']}")
        else:
            print(f"  - Error: {results.get('error', 'Unknown error')}")
            if results.get('failed_tables'):
                print(f"  - Failed tables: {results['failed_tables']}")
        
        # Save migration log
        log_file = f"simple_prod_migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(log_file, 'w') as f:
            f.write("\n".join(migrator.migration_log))
        print(f"Migration log saved to: {log_file}")
        
    except Exception as e:
        print(f"Critical migration error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
