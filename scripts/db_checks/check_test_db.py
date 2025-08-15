#!/usr/bin/env python3
"""Script to check the state of the test database and migrations."""

import asyncio
import asyncpg
import os
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config.environment import Environment, EnvironmentType
from src.db.migrations.migrate import run_migrations

async def check_database():
    # Connect to the default postgres database
    conn = await asyncpg.connect('postgresql://postgres:password@localhost:5432/postgres')
    
    try:
        # List all databases
        dbs = await conn.fetch('SELECT datname FROM pg_database WHERE datistemplate = false')
        print("\n=== Available Databases ===")
        for db in dbs:
            print(f"- {db['datname']}")
        
        # Check for test databases
        test_dbs = [db['datname'] for db in dbs if db['datname'].startswith('test_')]
        print("\n=== Test Databases ===")
        for db in test_dbs:
            print(f"- {db}")
        
        # If we have test databases, check their state
        if test_dbs:
            test_db = test_dbs[0]  # Check the first test database
            print(f"\n=== Checking test database: {test_db} ===")
            
            # Connect to the test database
            test_conn = await asyncpg.connect(f'postgresql://postgres:password@localhost:5432/{test_db}')
            try:
                # Check if the migrations table exists
                has_migrations = await test_conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'db_version')"
                )
                
                if has_migrations:
                    # Get current migration version
                    version = await test_conn.fetchval('SELECT version FROM db_version')
                    print(f"Current migration version: {version}")
                else:
                    print("No migrations table found")
                
                # List all tables in the test database
                tables = await test_conn.fetch(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                )
                print("\n=== Tables in test database ===")
                for table in tables:
                    print(f"- {table['table_name']}")
                
            finally:
                await test_conn.close()
        
    finally:
        await conn.close()

async def run_checks():
    # First check the current state
    print("Checking database state...")
    await check_database()
    
    # Then try to run migrations on a test environment
    print("\n=== Running Migrations ===")
    env = Environment(EnvironmentType.TEST)
    await run_migrations(env)
    
    # Check state after migrations
    print("\n=== State after migrations ===")
    await check_database()

if __name__ == "__main__":
    asyncio.run(run_checks())
