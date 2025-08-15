#!/usr/bin/env python3
import os
import sys
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_db_connection")

async def test_centralized_connection():
    """Test database connection using the centralized Database class."""
    logger.info("Testing database connection using centralized Database class...")
    
    # Set database credentials from environment variables
    # For local testing
    os.environ["DB_USER"] = "postgres"
    os.environ["DB_PASSWORD"] = "password"  # Using password from .env file
    os.environ["DB_PORT"] = "5432"
    
    # Test with localhost first
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_NAME"] = "trading_db"
    
    # Import after setting environment variables to ensure they're picked up
    from config.database import Database
    from config.environment import Environment, EnvironmentType
    
    # Test only the test environment
    environments = ["test"]
    success = False
    
    for env_type in environments:
        logger.info(f"\nTesting with environment: {env_type}")
        os.environ["ENVIRONMENT"] = env_type
        
        try:
            # Create a connection pool using the centralized logic
            logger.info(f"Creating connection pool for {env_type} environment")
            pool = await Database.create_connection_pool(max_retries=2, initial_delay=1.0, timeout=5.0)
            
            # Test the connection by executing a simple query
            async with pool.acquire() as conn:
                version = await conn.fetchval("SELECT version();")
                logger.info(f"✅ Connection successful for {env_type}!")
                logger.info(f"PostgreSQL version: {version}")
                
                # Get database name
                db_name = await conn.fetchval("SELECT current_database();")
                logger.info(f"Connected to database: {db_name}")
                
                # Get connection info
                conn_info = await conn.fetchrow("SELECT inet_server_addr() as host, inet_server_port() as port;")
                logger.info(f"Connected to server: {conn_info['host']}:{conn_info['port']}")
            
            await pool.close()
            success = True
            break
        except Exception as e:
            logger.error(f"❌ Connection failed for {env_type}: {str(e)}")
    
    return success

if __name__ == "__main__":
    success = asyncio.run(test_centralized_connection())
    sys.exit(0 if success else 1)
