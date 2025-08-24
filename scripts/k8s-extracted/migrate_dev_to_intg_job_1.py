#!/usr/bin/env python3
import asyncio

# Install required packages
      pip install asyncpg
      
      # Create migration script
      cat > /app/migrate.py << 'MIGRATION_SCRIPT_EOF'
      #!/usr/bin/env python3
      """
      Database Migration Script: dev_db to intg_db
      Copies all tables from dev_ prefix to intg_ prefix and synchronizes sequences
      """
      
      import asyncio
      import asyncpg
      import sys
      import logging
      from typing import List, Tuple, Dict
      
      # Configure logging
      logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
      logger = logging.getLogger(__name__)
      
      # Database connection configurations
      DEV_DB_CONFIG = {
          'host': 'postgres.ats-dev.svc.cluster.local',
          'port': 5432,
          'user': 'postgres',
          'password': 'postgres',
          'database': 'ats_dev'
      }
      
      INTG_DB_CONFIG = {
          'host': 'postgres.ats-intg.svc.cluster.local',
          'port': 5432,
          'user': 'postgres',
          'password': 'postgres',
          'database': 'ats_intg'
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
      
          async def get_table_columns(self, table_name: str) -> List[str]:
              """Get column names for a table"""
              query = """
              SELECT column_name
              FROM information_schema.columns 
              WHERE table_name = $1 
              AND table_schema = 'public'
              ORDER BY ordinal_position
              """
              rows = await self.dev_conn.fetch(query, table_name)
              return [row['column_name'] for row in rows]
      
          async def get_create_table_sql(self, table_name: str) -> str:
              """Get CREATE TABLE statement for recreating table structure"""
              intg_table = table_name.replace('dev_', 'intg_', 1)
              
              # Get column definitions
              query = """
              SELECT 
                  column_name,
                  data_type,
                  character_maximum_length,
                  numeric_precision,
                  numeric_scale,
                  is_nullable,
                  column_default
              FROM information_schema.columns 
              WHERE table_name = $1 
              AND table_schema = 'public'
              ORDER BY ordinal_position
              """
              columns = await self.dev_conn.fetch(query, table_name)
              
              # Build column definitions
              col_defs = []
              for col in columns:
                  col_def = f"{col['column_name']} {col['data_type']}"
                  
                  # Add length/precision
                  if col['character_maximum_length']:
                      col_def += f"({col['character_maximum_length']})"
                  elif col['numeric_precision'] and col['data_type'] in ('numeric', 'decimal'):
                      if col['numeric_scale']:
                          col_def += f"({col['numeric_precision']},{col['numeric_scale']})"
                      else:
                          col_def += f"({col['numeric_precision']})"
                  
                  # Add NOT NULL
                  if col['is_nullable'] == 'NO':
                      col_def += " NOT NULL"
                  
                  # Add default (handle sequence references)
                  if col['column_default']:
                      default_val = col['column_default']
                      if 'dev_' in default_val:
                          default_val = default_val.replace('dev_', 'intg_')
                      col_def += f" DEFAULT {default_val}"
                  
                  col_defs.append(col_def)
              
              return f"CREATE TABLE {intg_table} (\n  " + ",\n  ".join(col_defs) + "\n)"
      
          async def get_sequences_for_table(self, table_name: str) -> List[Tuple[str, str, int]]:
              """Get sequences associated with table and their current values"""
              # Find sequences owned by table columns
              query = """
              SELECT 
                  s.sequence_name,
                  c.column_name,
                  s.last_value::bigint
              FROM information_schema.sequences s
              JOIN pg_class seq_class ON seq_class.relname = s.sequence_name
              JOIN pg_depend d ON d.objid = seq_class.oid AND d.deptype = 'a'
              JOIN pg_class table_class ON d.refobjid = table_class.oid
              JOIN pg_attribute a ON a.attrelid = table_class.oid AND a.attnum = d.refobjsubid
              JOIN information_schema.columns c ON c.table_name = table_class.relname AND c.column_name = a.attname
              WHERE table_class.relname = $1
              """
              rows = await self.dev_conn.fetch(query, table_name)
              result = []
              
              for row in rows:
                  # Get actual current value from sequence
                  seq_name = row['sequence_name']
                  current_val = await self.dev_conn.fetchval(f"SELECT last_value FROM {seq_name}")
                  result.append((seq_name, row['column_name'], current_val))
              
              return result
      
          async def create_sequences(self, dev_table: str, intg_table: str, sequences: List[Tuple[str, str, int]]):
              """Create sequences in intg database"""
              for dev_seq, column, current_val in sequences:
                  intg_seq = dev_seq.replace('dev_', 'intg_', 1)
                  
                  # Drop if exists
                  await self.intg_conn.execute(f"DROP SEQUENCE IF EXISTS {intg_seq} CASCADE")
                  
                  # Create sequence with current value
                  await self.intg_conn.execute(f"CREATE SEQUENCE {intg_seq} START WITH {current_val}")
                  
                  # Update table default to use new sequence
                  await self.intg_conn.execute(f"ALTER TABLE {intg_table} ALTER COLUMN {column} SET DEFAULT nextval('{intg_seq}')")
                  
                  logger.info(f"✅ Created sequence {intg_seq} with value {current_val}")
      
          async def copy_table_data(self, dev_table: str, intg_table: str):
              """Copy all data from dev table to intg table"""
              # Get row count
              count = await self.dev_conn.fetchval(f"SELECT COUNT(*) FROM {dev_table}")
              if count == 0:
                  logger.info(f"⚠️  Table {dev_table} is empty")
                  return 0
              
              logger.info(f"📊 Copying {count} rows from {dev_table} to {intg_table}")
              
              # Get column names
              columns = await self.get_table_columns(dev_table)
              
              # Copy data in batches
              batch_size = 1000
              total_copied = 0
              
              for offset in range(0, count, batch_size):
                  # Fetch batch from dev
                  select_sql = f"SELECT {', '.join(columns)} FROM {dev_table} ORDER BY 1 LIMIT {batch_size} OFFSET {offset}"
                  rows = await self.dev_conn.fetch(select_sql)
                  
                  if not rows:
                      break
                  
                  # Prepare insert
                  placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
                  insert_sql = f"INSERT INTO {intg_table} ({', '.join(columns)}) VALUES ({placeholders})"
                  
                  # Insert batch
                  batch_values = [list(row.values()) for row in rows]
                  await self.intg_conn.executemany(insert_sql, batch_values)
                  
                  total_copied += len(rows)
                  logger.info(f"  📝 Copied {total_copied}/{count} rows")
              
              return total_copied
      
          async def migrate_table(self, dev_table: str):
              """Migrate a single table from dev to intg"""
              intg_table = dev_table.replace('dev_', 'intg_', 1)
              
              logger.info(f"🔄 Migrating {dev_table} → {intg_table}")
              
              try:
                  # 1. Get sequences before creating table
                  sequences = await self.get_sequences_for_table(dev_table)
                  
                  # 2. Drop existing intg table
                  await self.intg_conn.execute(f"DROP TABLE IF EXISTS {intg_table} CASCADE")
                  
                  # 3. Create table structure
                  create_sql = await self.get_create_table_sql(dev_table)
                  await self.intg_conn.execute(create_sql)
                  logger.info(f"✅ Created table {intg_table}")
                  
                  # 4. Copy data
                  copied_count = await self.copy_table_data(dev_table, intg_table)
                  
                  # 5. Create and sync sequences after data copy
                  if sequences:
                      await self.create_sequences(dev_table, intg_table, sequences)
                      
                      # Update sequences to max values from copied data
                      for dev_seq, column, _ in sequences:
                          intg_seq = dev_seq.replace('dev_', 'intg_', 1)
                          max_val = await self.intg_conn.fetchval(f"SELECT COALESCE(MAX({column}), 0) FROM {intg_table}")
                          if max_val > 0:
                              await self.intg_conn.execute(f"SELECT setval('{intg_seq}', {max_val})")
                              logger.info(f"🔢 Updated {intg_seq} to {max_val}")
                  
                  logger.info(f"✅ Successfully migrated {dev_table} ({copied_count} rows)")
                  return True
                  
              except Exception as e:
                  logger.error(f"❌ Failed to migrate {dev_table}: {e}")
                  return False
      
          async def verify_migration(self):
              """Verify migration results"""
              logger.info("🔍 Verifying migration...")
              
              dev_tables = await self.get_dev_tables()
              results = []
              
              for dev_table in dev_tables:
                  intg_table = dev_table.replace('dev_', 'intg_', 1)
                  
                  try:
                      # Check if table exists
                      intg_exists = await self.intg_conn.fetchval(
                          "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
                          intg_table
                      )
                      
                      if not intg_exists:
                          results.append(f"❌ {intg_table} does not exist")
                          continue
                      
                      # Compare counts
                      dev_count = await self.dev_conn.fetchval(f"SELECT COUNT(*) FROM {dev_table}")
                      intg_count = await self.intg_conn.fetchval(f"SELECT COUNT(*) FROM {intg_table}")
                      
                      if dev_count == intg_count:
                          results.append(f"✅ {dev_table} → {intg_table}: {dev_count} rows")
                      else:
                          results.append(f"❌ {dev_table} → {intg_table}: {dev_count} vs {intg_count} rows")
                          
                  except Exception as e:
                      results.append(f"❌ {dev_table} verification failed: {e}")
              
              logger.info("📊 Migration Verification:")
              for result in results:
                  logger.info(f"  {result}")
              
              return all("✅" in r for r in results)
      
          async def run_migration(self):
              """Run the complete migration"""
              await self.connect()
              
              try:
                  dev_tables = await self.get_dev_tables()
                  
                  if not dev_tables:
                      logger.warning("⚠️  No dev_ tables found")
                      return False
                  
                  logger.info(f"🚀 Starting migration of {len(dev_tables)} tables")
                  
                  success_count = 0
                  for table in dev_tables:
                      if await self.migrate_table(table):
                          success_count += 1
                  
                  logger.info(f"📊 Migration completed: {success_count}/{len(dev_tables)} tables successful")
                  
                  # Verify results
                  if await self.verify_migration():
                      logger.info("🎉 Migration completed successfully!")
                      return True
                  else:
                      logger.error("❌ Migration completed with verification errors")
                      return False
                      
              except Exception as e:
                  logger.error(f"💥 Migration failed: {e}")
                  return False
              finally:
                  await self.disconnect()
      
      async def main():
          migrator = DatabaseMigrator(DEV_DB_CONFIG, INTG_DB_CONFIG)
          success = await migrator.run_migration()
          sys.exit(0 if success else 1)
      
      if __name__ == "__main__":
          asyncio.run(main())
      MIGRATION_SCRIPT_EOF
      
      # Run migration
      cd /app && python migrate.py
    workingDir: /app
    resources:
      requests:
        memory: "128Mi"
        cpu: "100m"
      limits:
        memory: "256Mi"
        cpu: "200m"
  restartPolicy: Never
backoffLimit: 2
