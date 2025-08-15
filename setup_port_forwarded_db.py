#!/usr/bin/env python3
"""
Script to set up the database schema for the port-forwarded ats-dev database.
This script runs the database migrations to create the necessary tables.
"""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from db.migration_manager import MigrationManager

async def setup_database_schema():
    """Run database migrations to set up the schema."""
    # Set environment variables for port-forwarded connection
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5433"
    os.environ["DB_USER"] = "postgres"
    os.environ["DB_PASSWORD"] = "password"
    os.environ["DB_NAME"] = "dev_db"
    
    # Create the database URL
    db_url = f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    
    print(f"Using database URL: {db_url}")
    
    # Create the migration manager
    migration_manager = MigrationManager(db_url=db_url)
    
    # Get the current database version
    current_version = await migration_manager.get_current_version()
    print(f"Current database version: {current_version}")
    
    # Get all migration files
    migration_files = migration_manager._get_migration_files()
    print(f"Found {len(migration_files)} migration files")
    
    # Apply migrations
    for version, description, file_path in migration_files:
        if version > current_version:
            print(f"Applying migration {version:03d}: {description}")
            success = await migration_manager.apply_migration(version, description, file_path)
            if success:
                print(f"Migration {version:03d} applied successfully")
            else:
                print(f"Failed to apply migration {version:03d}")
                break
    
    # Get the new database version
    new_version = await migration_manager.get_current_version()
    print(f"New database version: {new_version}")
    
    # Check if any tables were created
    print("\nChecking for instrument-related tables...")
    import asyncpg
    conn = await asyncpg.connect(db_url)
    try:
        # Get the table prefix from the migration manager
        table_prefix = migration_manager.table_prefix
        print(f"Using table prefix: '{table_prefix}'")
        
        # List all tables
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        
        print("\n=== Tables in database ===")
        for table in tables:
            print(f"- {table['table_name']}")
        
        # Check for instrument tables
        instrument_tables = [t['table_name'] for t in tables 
                           if 'instrument' in t['table_name'].lower()]
        
        if instrument_tables:
            print("\n=== Instrument-related tables ===")
            for table in instrument_tables:
                print(f"- {table}")
                
                # Count rows in each instrument table
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                print(f"  Count: {count} rows")
        else:
            print("\nNo instrument-related tables found.")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(setup_database_schema())
