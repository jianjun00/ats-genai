#!/usr/bin/env python3
"""Script to check database connection using credentials from .env.dev."""

import os
import asyncio
import asyncpg
from dotenv import load_dotenv

async def check_db_connection():
    """Check database connection using credentials from .env.dev."""
    # Load environment variables from .env.dev
    load_dotenv('.env.dev')
    
    # Get database connection details
    db_url = os.getenv('TSDB_URL')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    print(f"Database URL from .env.dev: {db_url}")
    print(f"Database User: {db_user}")
    print(f"Database Password: {'*' * len(db_password) if db_password else 'Not set'}")
    
    try:
        # Try to connect to the database
        print("\nAttempting to connect to the database...")
        conn = await asyncpg.connect(db_url)
        
        print("Connection successful!")
        
        # List all databases
        print("\n=== Available Databases ===")
        databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
        for db in databases:
            print(f"- {db['datname']}")
        
        # List all tables in the current database
        print(f"\n=== Tables in current database ===")
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
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
                
                # Show sample data if table has rows
                if count > 0:
                    sample = await conn.fetch(f"SELECT * FROM {table} LIMIT 3")
                    print(f"  Sample data:")
                    for row in sample:
                        print(f"    {dict(row)}")
        else:
            print("\nNo instrument-related tables found.")
        
        await conn.close()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        
        # Try to connect to postgres database directly
        try:
            print("\nAttempting to connect to postgres database directly...")
            postgres_url = db_url.rsplit('/', 1)[0] + '/postgres'
            conn = await asyncpg.connect(postgres_url)
            print("Connection to postgres database successful!")
            
            # List all databases
            print("\n=== Available Databases ===")
            databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
            for db in databases:
                print(f"- {db['datname']}")
                
            await conn.close()
        except Exception as e2:
            print(f"Error connecting to postgres database: {e2}")

if __name__ == "__main__":
    asyncio.run(check_db_connection())
