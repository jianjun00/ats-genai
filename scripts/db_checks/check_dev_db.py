#!/usr/bin/env python3
"""Script to check instrument data in the dev_db database."""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

async def check_dev_db_data():
    """Check the dev_db database for instrument data."""
    # Load environment variables from .env.dev
    load_dotenv('.env.dev')
    
    # Get database connection details
    db_url = os.getenv('TSDB_URL', 'postgresql://postgres:password@localhost:5432/trading_db_dev')
    
    # Modify the URL to use dev_db
    db_url = db_url.replace('trading_db_dev', 'dev_db')
    
    print(f"Connecting to database: {db_url}")
    
    try:
        # Connect to the database
        conn = await asyncpg.connect(db_url)
        try:
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
                    
                    # Show sample data if table has rows
                    if count > 0:
                        sample = await conn.fetch(f"SELECT * FROM {table} LIMIT 3")
                        print(f"  Sample data:")
                        for row in sample:
                            print(f"    {dict(row)}")
            else:
                print("\nNo instrument-related tables found.")
                
            # Check for instrument_xrefs table specifically
            if any(t['table_name'] == 'instrument_xrefs' for t in tables):
                print("\n=== Cross-references by provider ===")
                try:
                    xrefs_by_provider = await conn.fetch(
                        "SELECT provider_id, COUNT(*) FROM instrument_xrefs GROUP BY provider_id"
                    )
                    for row in xrefs_by_provider:
                        print(f"- Provider {row['provider_id']}: {row['count']} instruments")
                except Exception as e:
                    print(f"Error querying instrument_xrefs: {e}")
            
            # Check for dev_instrument_xrefs table (dev environment)
            if any(t['table_name'] == 'dev_instrument_xrefs' for t in tables):
                print("\n=== Dev cross-references by provider ===")
                try:
                    xrefs_by_provider = await conn.fetch(
                        "SELECT provider_id, COUNT(*) FROM dev_instrument_xrefs GROUP BY provider_id"
                    )
                    for row in xrefs_by_provider:
                        print(f"- Provider {row['provider_id']}: {row['count']} instruments")
                except Exception as e:
                    print(f"Error querying dev_instrument_xrefs: {e}")
                    
        finally:
            await conn.close()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        
        # Try to connect to postgres database to check if dev_db exists
        try:
            print("\nAttempting to connect to postgres database to check if dev_db exists...")
            postgres_url = db_url.rsplit('/', 1)[0] + '/postgres'
            conn = await asyncpg.connect(postgres_url)
            
            # Check if dev_db exists
            databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
            db_names = [db['datname'] for db in databases]
            
            if 'dev_db' in db_names:
                print("dev_db database exists but connection failed. Check permissions.")
            else:
                print("dev_db database does not exist. Available databases:")
                for db_name in db_names:
                    print(f"- {db_name}")
                
            await conn.close()
        except Exception as e2:
            print(f"Error connecting to postgres database: {e2}")

if __name__ == "__main__":
    asyncio.run(check_dev_db_data())
