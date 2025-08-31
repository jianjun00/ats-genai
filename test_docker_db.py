#!/usr/bin/env python3
"""
Test database connection from Docker container
"""
import sys
import os
import json
import asyncio
import asyncpg
sys.path.append('src')

async def test_docker_db_connection():
    """Test database connection from Docker container"""
    print("🔗 Testing Docker container to database connection...")
    
    # Try different connection patterns
    connection_attempts = [
        {'host': 'ats-dev-postgres', 'port': 5432, 'user': 'postgres', 'password': 'dev_password', 'database': 'dev_db'},
        {'host': 'localhost', 'port': 3432, 'user': 'postgres', 'password': 'dev_password', 'database': 'dev_db'},
        {'host': 'host.docker.internal', 'port': 3432, 'user': 'postgres', 'password': 'dev_password', 'database': 'dev_db'},
    ]
    
    for i, config in enumerate(connection_attempts):
        try:
            print(f"🔧 Attempt {i+1}: Connecting to {config['host']}:{config['port']}")
            conn = await asyncpg.connect(**config)
            
            # Test a simple query
            result = await conn.fetchval("SELECT current_database()")
            print(f"✅ Connection successful! Database: {result}")
            
            # Test our table
            count = await conn.fetchval("SELECT COUNT(*) FROM dev_training_datasets")
            print(f"✅ Training datasets table accessible, {count} records found")
            
            await conn.close()
            return config
            
        except Exception as e:
            print(f"❌ Connection attempt {i+1} failed: {e}")
    
    print("❌ All connection attempts failed")
    return None

if __name__ == "__main__":
    result = asyncio.run(test_docker_db_connection())
    if result:
        print(f"🎯 Successful connection config: {result}")
    else:
        print("🎯 No successful database connection found")
        sys.exit(1)