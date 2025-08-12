#!/usr/bin/env python3
"""
Test script to verify PostgreSQL connection with test_user credentials.
"""
import asyncio
import asyncpg

async def test_connection():
    # Use test_user credentials
    db_host = "localhost"
    db_port = 5432
    db_user = "test_user"
    db_password = "test_password"
    
    print(f"Attempting to connect to PostgreSQL: host={db_host}, port={db_port}, user={db_user}")
    
    try:
        # Connect to the default 'postgres' database
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database="postgres"
        )
        print("Connection successful!")
        await conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(test_connection())
