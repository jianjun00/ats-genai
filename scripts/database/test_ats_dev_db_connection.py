#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_ats_dev_db_connection")

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

async def test_db_connection():
    """Test database connection to ats-dev environment."""
    # Set environment variables for port-forwarded ats-dev database
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5433"  # Port-forwarded port
    os.environ["DB_USER"] = "postgres"
    
    # Get password from user input or use default
    db_password = input("Enter the ats-dev database password (or press Enter to use default 'password'): ") or "password"
    os.environ["DB_PASSWORD"] = db_password
    os.environ["DB_NAME"] = "dev_db"  # Correct database name
    
    # Import after setting environment variables
    from config.database import Database
    
    try:
        # Create a connection pool using the centralized logic
        logger.info("Creating database connection pool for ats-dev")
        logger.info(f"Using host: {os.environ['DB_HOST']}, database: {os.environ['DB_NAME']}")
        
        # Create Database instance directly to check configuration
        db = Database()
        logger.info(f"Database configuration: host={db.host}, database={db.database}, user={db.user}")
        
        # Try to create a connection pool
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)
        logger.info("Successfully connected to the database!")
        
        # Test a simple query
        async with pool.acquire() as conn:
            logger.info("Testing a simple query...")
            result = await conn.fetchval("SELECT current_database()")
            logger.info(f"Connected to database: {result}")
            
            # Check if instrument_polygon table exists
            logger.info("Checking if instrument_polygon table exists...")
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'instrument_polygon'
                )
            """)
            
            if table_exists:
                logger.info("instrument_polygon table exists")
                # Count records
                count = await conn.fetchval("SELECT COUNT(*) FROM instrument_polygon")
                logger.info(f"Table has {count} records")
            else:
                logger.info("instrument_polygon table does not exist")
                
        await pool.close()
        return True
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_db_connection())
    sys.exit(0 if success else 1)
