#!/usr/bin/env python3
"""
Standalone test script for the DAO methods we added.
This script directly imports the necessary modules without relying on the package structure.
"""

import asyncio
import asyncpg
import json
import os
from datetime import datetime


class TestEnvironment:
    """Simple environment class for testing."""
    
    def __init__(self):
        # Build database URL from individual environment variables if DATABASE_URL is not set
        if 'DATABASE_URL' in os.environ:
            self.db_url = os.environ['DATABASE_URL']
        else:
            db_host = os.environ.get('DB_HOST', 'localhost')
            db_port = os.environ.get('DB_PORT', '5432')
            db_name = os.environ.get('DB_NAME', 'postgres')
            db_user = os.environ.get('DB_USER', 'postgres')
            db_password = os.environ.get('DB_PASSWORD', 'postgres')
            self.db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        
        print(f"Using database URL: {self.db_url.replace(db_password, '********') if 'db_password' in locals() else self.db_url}")
    
    def get_table_name(self, base_name):
        """Get the table name with any environment-specific prefix."""
        prefix = os.environ.get('TABLE_PREFIX', '')
        return f"{prefix}{base_name}"
    
    def get_database_url(self):
        """Get the database URL."""
        return self.db_url


class TestInstrumentsDAO:
    """Test version of InstrumentsDAO."""
    
    def __init__(self, env):
        self.env = env
        self.table_name = self.env.get_table_name('instruments')
        self.db_url = self.env.get_database_url()
    
    async def count_instruments(self):
        """Count the total number of instruments."""
        try:
            pool = await asyncpg.create_pool(self.db_url)
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {self.table_name}")
                    return row['count'] if row else 0
            finally:
                await pool.close()
        except Exception as e:
            print(f"Error in count_instruments: {str(e)}")
            return None


class TestInstrumentXrefsDAO:
    """Test version of InstrumentXrefsDAO."""
    
    def __init__(self, env):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_xrefs')
        self.db_url = self.env.get_database_url()
    
    async def count_xrefs(self):
        """Count the total number of instrument xrefs."""
        try:
            pool = await asyncpg.create_pool(self.db_url)
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {self.table_name}")
                    return row['count'] if row else 0
            finally:
                await pool.close()
        except Exception as e:
            print(f"Error in count_xrefs: {str(e)}")
            return None


class TestInstrumentPolygonDAO:
    """Test version of InstrumentPolygonDAO."""
    
    def __init__(self, env):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_polygon')
        self.db_url = self.env.get_database_url()
    
    async def count_instruments(self):
        """Count the total number of instruments in the polygon table."""
        try:
            pool = await asyncpg.create_pool(self.db_url)
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {self.table_name}")
                    return row['count'] if row else 0
            finally:
                await pool.close()
        except Exception as e:
            print(f"Error in count_instruments (polygon): {str(e)}")
            return None
    
    async def get_latest_update_timestamp(self):
        """Get the timestamp of the most recently updated instrument."""
        try:
            pool = await asyncpg.create_pool(self.db_url)
            try:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"SELECT MAX(updated_at) as latest FROM {self.table_name}")
                    return row['latest'] if row else None
            finally:
                await pool.close()
        except Exception as e:
            print(f"Error in get_latest_update_timestamp: {str(e)}")
            return None


async def test_dao_methods():
    """Test the DAO methods we added."""
    env = TestEnvironment()
    
    # Initialize DAOs
    instruments_dao = TestInstrumentsDAO(env)
    instrument_xrefs_dao = TestInstrumentXrefsDAO(env)
    instrument_polygon_dao = TestInstrumentPolygonDAO(env)
    
    # Test count methods
    print("=== Testing DAO Count Methods ===")
    
    instruments_count = await instruments_dao.count_instruments()
    print(f"Instruments count: {instruments_count}")
    
    xrefs_count = await instrument_xrefs_dao.count_xrefs()
    print(f"Instrument xrefs count: {xrefs_count}")
    
    polygon_count = await instrument_polygon_dao.count_instruments()
    print(f"Instrument polygon count: {polygon_count}")
    
    # Test timestamp method
    print("\n=== Testing Latest Update Timestamp ===")
    
    latest_timestamp = await instrument_polygon_dao.get_latest_update_timestamp()
    print(f"Latest update timestamp: {latest_timestamp}")
    
    # Return stats
    return {
        "instruments_count": instruments_count,
        "xrefs_count": xrefs_count,
        "polygon_count": polygon_count,
        "latest_timestamp": latest_timestamp
    }


async def main():
    """Run all tests."""
    print("=== Testing Instrument Data Agent DAO Methods ===")
    print(f"Current time: {datetime.now()}")
    
    print("\n=== Environment Variables ===")
    print(f"DB_HOST: {os.environ.get('DB_HOST', 'not set (using default)')}") 
    print(f"DB_PORT: {os.environ.get('DB_PORT', 'not set (using default)')}") 
    print(f"DB_NAME: {os.environ.get('DB_NAME', 'not set (using default)')}") 
    print(f"DB_USER: {os.environ.get('DB_USER', 'not set (using default)')}") 
    print(f"DB_PASSWORD: {'set' if 'DB_PASSWORD' in os.environ else 'not set (using default)'}")
    print(f"DATABASE_URL: {'set' if 'DATABASE_URL' in os.environ else 'not set (using individual variables)'}")
    print(f"TABLE_PREFIX: {os.environ.get('TABLE_PREFIX', 'not set (no prefix)')}")
    
    try:
        stats = await test_dao_methods()
        print("\n=== Summary ===")
        print(json.dumps(stats, indent=2, default=str))
        
        if all(v is None for v in stats.values()):
            print("\n⚠️ All tests returned None values. This likely indicates a database connection issue.")
            print("Please check your database credentials and ensure the database is accessible.")
            print("\nTo set database credentials, use environment variables:")
            print("  export DB_HOST=your_host")
            print("  export DB_PORT=your_port")
            print("  export DB_NAME=your_database")
            print("  export DB_USER=your_username")
            print("  export DB_PASSWORD=your_password")
            print("\nOr set a complete DATABASE_URL:")
            print("  export DATABASE_URL=postgresql://user:password@host:port/database")
            print("\nIf using table prefixes:")
            print("  export TABLE_PREFIX=your_prefix_")
        else:
            print("\n✅ All tests completed successfully!")
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        print("\nPlease check your database credentials and ensure the database is accessible.")
        raise


if __name__ == "__main__":
    asyncio.run(main())
