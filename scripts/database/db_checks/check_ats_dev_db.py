#!/usr/bin/env python3
"""
Script to check if the ats_dev database exists and is accessible.
"""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

async def check_ats_dev_db():
    """Check if the ats_dev database exists and is accessible."""
    # Load environment variables from .env.dev
    load_dotenv('.env.dev')
    
    # Get database connection details
    db_url = os.getenv('TSDB_URL', 'postgresql://postgres:password@localhost:5432/trading_db_dev')
    
    # Extract components from the URL
    db_parts = db_url.split('/')
    db_conn_string = '/'.join(db_parts[:-1])
    
    # Try to connect to postgres database first
    import asyncpg
    
    # Connect to postgres database
    postgres_url = f"{db_conn_string}/postgres"
    print(f"Connecting to postgres database: {postgres_url}", flush=True)
    
    try:
        conn = await asyncpg.connect(postgres_url)
        
        # Check if ats_dev database exists
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = 'ats_dev'")
        if exists:
            print("ats_dev database exists")
        else:
            print("ats_dev database does not exist")
            
            # List all databases
            print("\nAvailable databases:")
            databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            for db in databases:
                print(f"- {db['datname']}")
        
        await conn.close()
        
        # If ats_dev exists, try to connect to it
        if exists:
            ats_dev_url = f"{db_conn_string}/ats_dev"
            print(f"\nConnecting to ats_dev database: {ats_dev_url}")
            
            try:
                conn = await asyncpg.connect(ats_dev_url)
                
                # List all tables
                tables = await conn.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' ORDER BY table_name"
                )
                
                print("\n=== Tables in ats_dev database ===")
                if tables:
                    for table in tables:
                        print(f"- {table['table_name']}")
                else:
                    print("No tables found in ats_dev database")
                
                # Check for instrument tables with dev_ prefix
                instrument_tables = [t['table_name'] for t in tables 
                                   if 'instrument' in t['table_name'].lower() and t['table_name'].startswith('dev_')]
                
                if instrument_tables:
                    print("\n=== Instrument-related tables with dev_ prefix ===")
                    for table in instrument_tables:
                        print(f"- {table}")
                        
                        # Count rows in each instrument table
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                        print(f"  Count: {count} rows")
                else:
                    print("\nNo instrument-related tables with dev_ prefix found.")
                
                await conn.close()
            except Exception as e:
                print(f"Error connecting to ats_dev database: {e}")
    except Exception as e:
        print(f"Error connecting to postgres database: {e}")
        
        # Try to connect directly to ats_dev as a fallback
        try:
            ats_dev_url = f"{db_conn_string}/ats_dev"
            print(f"\nTrying direct connection to ats_dev database: {ats_dev_url}")
            
            conn = await asyncpg.connect(ats_dev_url)
            print("Successfully connected to ats_dev database")
            await conn.close()
        except Exception as e2:
            print(f"Error connecting directly to ats_dev database: {e2}")

if __name__ == "__main__":
    print("Starting database check script...", flush=True)
    asyncio.run(check_ats_dev_db())
    print("Database check script completed.", flush=True)
