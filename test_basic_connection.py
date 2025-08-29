#!/usr/bin/env python3
"""
Basic connection test script to validate database connectivity
This should be run in the CI environment to test the setup
"""
import asyncio
import os
import sys

async def test_basic_connection():
    """Test basic database connection with the CI setup"""
    print("🧪 Testing basic database connection...")
    
    # Check environment variables
    db_url = os.environ.get('DATABASE_URL', 'postgresql://testuser:testpass@localhost:5432/testdb')
    print(f"Database URL: {db_url}")
    
    try:
        import asyncpg
        print("✅ asyncpg imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import asyncpg: {e}")
        return False
    
    try:
        print(f"Attempting to connect to: {db_url}")
        conn = await asyncpg.connect(db_url)
        result = await conn.fetchval('SELECT 1 as test')
        print(f"✅ Database connection successful, result: {result}")
        await conn.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Try to provide more helpful error information
        if "Connect call failed" in str(e):
            print("💡 Hint: PostgreSQL service may not be running or ready")
        elif "authentication failed" in str(e).lower():
            print("💡 Hint: Check username/password credentials")
        elif "database" in str(e).lower() and "does not exist" in str(e).lower():
            print("💡 Hint: Target database may not exist")
        
        return False

if __name__ == "__main__":
    success = asyncio.run(test_basic_connection())
    sys.exit(0 if success else 1)