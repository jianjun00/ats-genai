#!/usr/bin/env python3
"""Script to check instrument data in the trading_db database."""

import asyncio
import asyncpg
import os
import sys
from dotenv import load_dotenv

async def check_trading_db_data():
    """Check the trading_db database for instrument data."""
    # Load environment variables from .env.dev
    load_dotenv('.env.dev')
    
    # Get database connection details
    db_url = os.getenv('TSDB_URL', 'postgresql://postgres:password@localhost:5432/trading_db_dev')
    
    # Modify the URL to use trading_db instead of trading_db_dev
    db_url = db_url.replace('trading_db_dev', 'trading_db')
    
    print(f"Connecting to database: {db_url}")
    sys.stdout.flush()  # Force output to be displayed immediately
    
    try:
        # Connect to the database
        print("Attempting to create connection...")
        sys.stdout.flush()
        conn = await asyncpg.connect(db_url)
        print("Connection established successfully!")
        sys.stdout.flush()
        
        try:
            # List all tables
            print("Fetching tables...")
            sys.stdout.flush()
            tables = await conn.fetch(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
            print(f"Found {len(tables)} tables")
            sys.stdout.flush()
            
            print("\n=== Tables in database ===")
            for table in tables:
                print(f"- {table['table_name']}")
            sys.stdout.flush()
            
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
                    sys.stdout.flush()
            else:
                print("\nNo instrument-related tables found.")
                sys.stdout.flush()
                
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
                sys.stdout.flush()
            
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
                sys.stdout.flush()
                    
        finally:
            print("Closing connection...")
            sys.stdout.flush()
            await conn.close()
            print("Connection closed")
            sys.stdout.flush()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.stdout.flush()

if __name__ == "__main__":
    print("Starting script...")
    sys.stdout.flush()
    asyncio.run(check_trading_db_data())
    print("Script completed")
    sys.stdout.flush()
