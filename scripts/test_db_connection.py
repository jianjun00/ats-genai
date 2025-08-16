#!/usr/bin/env python3
"""
Simple script to test database connection and verify tables.
"""

import asyncio
import asyncpg
import sys

async def test_database_connection():
    """Test connection to the database and verify tables."""
    print("Testing database connection...")
    
    # Connection parameters
    db_host = "localhost"
    db_port = 5440  # Port-forwarded port
    db_user = "postgres"
    db_pass = "postgres"
    db_name = "dev_db"
    
    try:
        # Connect to the database
        print(f"Connecting to {db_host}:{db_port}/{db_name} as {db_user}...")
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name
        )
        
        print("Connection successful!")
        
        # Check tables
        print("\nChecking database tables...")
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
        )
        
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                table_name = table['table_name']
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                print(f"- {table_name}: {count} rows")
        else:
            print("No tables found in the database.")
        
        # Check if specific tables exist
        dev_tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'dev\\_%' ORDER BY table_name"
        )
        
        if dev_tables:
            print(f"\nFound {len(dev_tables)} dev_ tables:")
            for table in dev_tables:
                print(f"- {table['table_name']}")
        else:
            print("\nNo dev_ tables found. This might indicate an issue with database initialization.")
        
        # Close the connection
        await conn.close()
        print("\nDatabase connection closed.")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_database_connection())
    sys.exit(0 if success else 1)
