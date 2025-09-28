#!/usr/bin/env python
"""Test script to verify database connection logic locally."""
import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.shared.utils.database import Database

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parse command line arguments
parser = argparse.ArgumentParser(description='Test database connection with retry logic.')
parser.add_argument('--db-user', dest='db_user', default=os.getenv('DB_USER', 'postgres'),
                    help='Database user (default: from DB_USER env var or "postgres")')
parser.add_argument('--db-password', dest='db_password', default=os.getenv('DB_PASSWORD', 'postgres'),
                    help='Database password (default: from DB_PASSWORD env var or "postgres")')
parser.add_argument('--db-host', dest='db_host', default=os.getenv('DB_HOST', 'localhost'),
                    help='Database host (default: from DB_HOST env var or "localhost")')
parser.add_argument('--db-port', dest='db_port', default=os.getenv('DB_PORT', '5432'),
                    help='Database port (default: from DB_PORT env var or "5432")')
parser.add_argument('--environments', dest='environments', default='dev,intg,prod,local',
                    help='Comma-separated list of environments to test (default: "dev,intg,prod,local")')

@pytest.mark.asyncio

async def test_database_connection(args):
    """Test database connection with retry logic."""
    # Parse environments list
    environments = args.environments.split(',')

    # Set database credentials from arguments
    os.environ["DB_USER"] = args.db_user
    os.environ["DB_PASSWORD"] = args.db_password
    os.environ["DB_HOST"] = args.db_host
    os.environ["DB_PORT"] = args.db_port

    logger.info(f"Using database credentials:")
    logger.info(f"  User: {args.db_user}")
    logger.info(f"  Password: {'*' * len(args.db_password)}")
    logger.info(f"  Host: {args.db_host}")
    logger.info(f"  Port: {args.db_port}")

    for env in environments:
        logger.info(f"\n\n=== Testing {env} environment ===")

        # Set environment variables for testing
        os.environ["ENVIRONMENT"] = env

        if env == "dev":
            os.environ["DB_NAME"] = "dev_db"
        elif env == "intg":
            os.environ["DB_NAME"] = "intg_db"
        else:
            os.environ["DB_NAME"] = "trading_db"

        # Create database instance
        db = Database()

        # Print connection details
        logger.info(f"Database host: {db.host}")
        logger.info(f"Database port: {db.port}")
        logger.info(f"Database name: {db.database}")
        logger.info(f"Database URL: {db.get_database_url().replace(db.password, '******')}")

        # Test connection with retry logic
        # Import asyncpg here to avoid import errors if not installed
        import asyncpg

        logger.info("Attempting to connect to database...")
        pool = await db.create_pool_with_retry(asyncpg, max_retries=2, initial_delay=1.0)
        logger.info("Successfully connected to database!")

        # Test query
        async with pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            logger.info(f"PostgreSQL version: {version}")

        # Close pool
        await pool.close()

if __name__ == "__main__":
    args = parser.parse_args()
    asyncio.run(test_database_connection(args))
