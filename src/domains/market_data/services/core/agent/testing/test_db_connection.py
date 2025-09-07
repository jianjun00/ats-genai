#!/usr/bin/env python3
"""
Pre-deployment test script to verify database connectivity.
This script tests connection to the database using environment variables.
"""

import asyncio
import asyncpg
import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConnectionTester:
    """Tests database connectivity using environment variables."""

    def __init__(self):
        """Initialize the tester with environment variables."""
        # Get database connection info from environment variables
        if 'DATABASE_URL' in os.environ:
            self.db_url = os.environ['DATABASE_URL']
            logger.info("Using DATABASE_URL from environment")
        else:
            self.db_host = os.environ.get('DB_HOST')
            self.db_port = os.environ.get('DB_PORT', '5432')
            self.db_name = os.environ.get('DB_NAME')
            self.db_user = os.environ.get('DB_USER')
            self.db_password = os.environ.get('DB_PASSWORD')

            # Check if all required variables are set
            missing_vars = []
            for var_name, var_value in [
                ('DB_HOST', self.db_host),
                ('DB_NAME', self.db_name),
                ('DB_USER', self.db_user),
                ('DB_PASSWORD', self.db_password)
            ]:
                if not var_value:
                    missing_vars.append(var_name)

            if missing_vars:
                logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
                logger.error("Please set these variables or provide a complete DATABASE_URL")
                sys.exit(1)

            # Build connection string
            self.db_url = f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'

        # Get table prefix if any
        self.table_prefix = os.environ.get('TABLE_PREFIX', '')

    async def test_connection(self):
        """Test connection to the database."""
        try:
            # Mask password in logs
            if 'db_password' in vars(self):
                masked_url = self.db_url.replace(self.db_password, '********')
            else:
                # Try to mask password in DATABASE_URL
                masked_url = self.db_url
                if '@' in masked_url and '://' in masked_url:
                    start = masked_url.find('://') + 3
                    end = masked_url.find('@', start)
                    if ':' in masked_url[start:end]:
                        user_end = masked_url.find(':', start)
                        masked_url = masked_url[:user_end+1] + '********' + masked_url[end:]

            logger.info(f"Testing connection to {masked_url}")

            # Connect to database
            conn = await asyncpg.connect(self.db_url)

            # Test basic query
            logger.info("Testing basic query...")
            await conn.execute('SELECT 1')

            # Test table existence
            tables_to_check = [
                f"{self.table_prefix}instruments",
                f"{self.table_prefix}instrument_xrefs",
                f"{self.table_prefix}instrument_polygon"
            ]

            logger.info(f"Testing table existence for: {', '.join(tables_to_check)}")

            for table in tables_to_check:
                exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
                    table
                )
                if exists:
                    logger.info(f"✅ Table {table} exists")

                    # Get row count
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    logger.info(f"   - Row count: {count}")
                else:
                    logger.warning(f"⚠️ Table {table} does not exist")

            # Close connection
            await conn.close()
            logger.info("✅ Database connection test completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            return False

async def main():
    """Run the database connection test."""
    print(f"=== Database Connection Test ===")
    print(f"Current time: {datetime.now()}")
    print(f"Environment: {os.environ.get('ENVIRONMENT', 'not set')}")

    tester = DatabaseConnectionTester()
    success = await tester.test_connection()

    if success:
        print("\n✅ All database tests passed!")
        return 0
    else:
        print("\n❌ Database tests failed!")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
