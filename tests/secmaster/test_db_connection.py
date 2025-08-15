#!/usr/bin/env python3
import os
import sys
import asyncio
import asyncpg

async def test_connection():
    """Test database connection with different parameters."""
    print("Testing database connection...")
    
    # Set environment variables
    os.environ["ENVIRONMENT"] = "dev"
    
    # Database connection parameters
    host = "timescaledb"
    port = 5432
    user = "postgres"
    password = "postgres"
    database = "trading_db"
    
    # Try different connection strings
    connection_strings = [
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=disable",
        f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=allow",
        f"postgresql://{user}:{password}@{host}.ats-dev.svc.cluster.local:{port}/{database}?sslmode=disable",
    ]
    
    for i, conn_str in enumerate(connection_strings):
        print(f"\nAttempt {i+1}: Trying connection with: {conn_str}")
        try:
            conn = await asyncpg.connect(conn_str)
            version = await conn.fetchval("SELECT version();")
            print(f"✅ Connection successful!")
            print(f"PostgreSQL version: {version}")
            await conn.close()
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
    
    return False

if __name__ == "__main__":
    success = asyncio.run(test_connection())
    sys.exit(0 if success else 1)
