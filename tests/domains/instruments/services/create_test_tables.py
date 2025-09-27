#!/usr/bin/env python3
import os
import sys
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("create_test_tables")

async def create_instrument_polygon_table():
    """Create the instrument_polygon table in the test database."""
    logger.info("Creating instrument_polygon table in test database...")

    # Set database credentials from environment variables
    os.environ["DB_USER"] = "test_user"
    os.environ["DB_PASSWORD"] = "test_password"
    os.environ["DB_PORT"] = "5432"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_NAME"] = "test_db"
    os.environ["ENVIRONMENT"] = "test"

    # Import after setting environment variables to ensure they're picked up
    from core.shared.utils.database import Database

    # Create a connection pool using the centralized logic
    logger.info("Creating database connection pool")
    pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)

    # Create the instrument_polygon table
    async with pool.acquire() as conn:
        logger.info("Creating test_instrument_polygon table")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_instrument_polygon (
                symbol VARCHAR(20) PRIMARY KEY,
                name TEXT,
                exchange VARCHAR(20),
                type VARCHAR(10),
                currency VARCHAR(10),
                figi VARCHAR(50),
                isin VARCHAR(20),
                cusip VARCHAR(20),
                composite_figi VARCHAR(50),
                active BOOLEAN,
                list_date DATE,
                delist_date DATE,
                raw JSONB,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        logger.info("Table created successfully")

    await pool.close()
    return True
if __name__ == "__main__":
    success = asyncio.run(create_instrument_polygon_table())
    sys.exit(0 if success else 1)
