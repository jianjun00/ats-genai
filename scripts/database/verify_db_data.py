#!/usr/bin/env python3
"""
Script to verify data in the dev_instrument_polygon table.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("verify_db_data")

# Add console handler to ensure output is visible
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

async def verify_db_data():
    """Verify data in the dev_instrument_polygon table."""
    # Set environment variables for port-forwarded connection
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5433"
    os.environ["DB_USER"] = "postgres"
    os.environ["DB_PASSWORD"] = "password"
    os.environ["DB_NAME"] = "dev_db"
    
    # Import the required modules
    from config.database import Database
    from config.environment import Environment, EnvironmentType
    
    # Create environment instance
    env = Environment()
    env.environment_type = EnvironmentType.DEV
    
    try:
        # Create database connection pool
        logger.info("Creating database connection pool")
        pool = await Database.create_connection_pool()
        
        # Query the dev_instrument_polygon table
        async with pool.acquire() as conn:
            # Get table name using environment
            table_name = env.get_table_name("instrument_polygon")
            logger.info(f"Using table name: {table_name}")
            
            # First, get the column names from the table
            columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name.replace('dev_', '')}'"
            columns = await conn.fetch(columns_query)
            logger.info(f"Available columns in {table_name}: {[col['column_name'] for col in columns]}")
            
            # Query for AAPL data with all columns
            query = f"SELECT * FROM {table_name} WHERE symbol = 'AAPL'"
            result = await conn.fetch(query)
            
            logger.info(f"Found {len(result)} rows for AAPL:")
            for row in result:
                logger.info(f"Row: {row}")
            
            # Get total count of records
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count = await conn.fetchval(count_query)
            logger.info(f"Total records in {table_name}: {count}")
            
            # Get all symbols in the table
            symbols_query = f"SELECT symbol FROM {table_name} ORDER BY symbol"
            symbols = await conn.fetch(symbols_query)
            logger.info(f"All symbols in {table_name}: {[row['symbol'] for row in symbols]}")
        
        # Close the connection pool
        await pool.close()
        return True
    except Exception as e:
        logger.error(f"Error verifying database data: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(verify_db_data())
    sys.exit(0 if success else 1)
