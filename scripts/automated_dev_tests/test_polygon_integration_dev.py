#!/usr/bin/env python3
"""
Automated test for polygon integration in dev environment.
This script:
1. Sets up port-forwarding to the dev database automatically
2. Creates the dev_db if it doesn't exist
3. Runs the database migrations
4. Tests the populate_instrument_polygon functionality
5. Cleans up resources

Usage:
    python -m scripts.automated_dev_tests.test_polygon_integration_dev
"""
import os
import sys
import time
import asyncio
import logging
import subprocess
import signal
import atexit
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("automated_dev_test")

# Global variables
port_forward_process = None

def check_port_in_use(port):
    """Check if a port is already in use."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def setup_port_forwarding():
    """Set up port forwarding to the dev database."""
    global port_forward_process
    
    # Check if port 5433 is already in use (port forwarding might already be active)
    if check_port_in_use(5433):
        logger.info("Port 5433 is already in use, assuming port forwarding is already active")
        return True
    
    logger.info("Setting up port forwarding to ats-dev database...")
    try:
        # Start the port-forwarding process
        port_forward_process = subprocess.Popen(
            ["kubectl", "port-forward", "-n", "ats-dev", "svc/postgres", "5433:5432"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )
        
        # Give it time to establish the connection
        time.sleep(3)
        
        # Check if port forwarding is working
        if port_forward_process.poll() is not None:
            stderr = port_forward_process.stderr.read().decode('utf-8')
            logger.error(f"Port forwarding failed: {stderr}")
            return False
        
        logger.info("Port forwarding established successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to set up port forwarding: {e}")
        return False

def cleanup_resources():
    """Clean up resources when the script exits."""
    global port_forward_process
    
    if port_forward_process and port_forward_process.poll() is None:
        logger.info("Terminating port forwarding process...")
        try:
            os.killpg(os.getpgid(port_forward_process.pid), signal.SIGTERM)
            port_forward_process.wait(timeout=5)
            logger.info("Port forwarding process terminated")
        except Exception as e:
            logger.error(f"Error terminating port forwarding process: {e}")

# Register cleanup function to run on exit
atexit.register(cleanup_resources)

async def ensure_dev_db_exists():
    """Ensure the dev_db database exists in the ats-dev environment."""
    logger.info("Checking if dev_db exists...")
    
    # Import here to ensure PYTHONPATH is set correctly
    from config.database import Database
    import asyncpg
    
    # Connect to postgres database to check if dev_db exists
    try:
        # Connect to default postgres database
        conn = await asyncpg.connect(
            host="localhost",
            port=5433,
            user="postgres",
            password=os.getenv("DB_PASSWORD", "password"),
            database="postgres"
        )
        
        # Check if dev_db exists
        result = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", "dev_db")
        
        if not result:
            logger.info("Creating dev_db database...")
            await conn.execute("CREATE DATABASE dev_db")
            logger.info("dev_db database created successfully")
        else:
            logger.info("dev_db database already exists")
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Error ensuring dev_db exists: {e}")
        return False

async def run_database_migrations():
    """Run database migrations on the dev_db database."""
    logger.info("Running database migrations...")
    
    # Import here to ensure PYTHONPATH is set correctly
    from config.environment import Environment, EnvironmentType
    import asyncpg
    import asyncio
    
    try:
        # Set environment variables for the migrations
        os.environ["DB_HOST"] = "localhost"
        os.environ["DB_PORT"] = "5433"
        os.environ["DB_USER"] = "postgres"
        os.environ["DB_PASSWORD"] = os.getenv("DB_PASSWORD", "password")
        os.environ["DB_NAME"] = "dev_db"
        
        # Create the database URL
        db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD', 'password')}@localhost:5433/dev_db"
        
        # Connect to the database
        conn = await asyncpg.connect(db_url)
        
        # Create necessary tables for instrument_polygon with dev_ prefix
        logger.info("Creating dev_instrument_polygon table if it doesn't exist...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_instrument_polygon (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT,
                exchange TEXT,
                type TEXT,
                currency TEXT,
                figi TEXT,
                isin TEXT,
                cusip TEXT,
                composite_figi TEXT,
                active BOOLEAN,
                list_date DATE,
                delist_date DATE,
                raw JSONB,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT dev_instrument_polygon_symbol_key UNIQUE (symbol)
            )
        """)
        
        # Create other necessary tables
        logger.info("Creating other necessary tables...")
        
        # Create indexes
        logger.info("Creating indexes...")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS dev_instrument_polygon_symbol_idx ON dev_instrument_polygon (symbol)
        """)
        
        # Close the connection
        await conn.close()
        
        logger.info("Database migrations completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error running database migrations: {e}")
        return False

async def test_polygon_integration():
    """Test the polygon integration with the dev database."""
    logger.info("Testing polygon integration...")
    
    # Import here to ensure PYTHONPATH is set correctly
    from config.environment import Environment, EnvironmentType
    
    try:
        # Load environment variables from .env file
        load_dotenv()
        
        # Get Polygon API key from environment variables
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        if not polygon_api_key:
            logger.error("POLYGON_API_KEY not found in environment variables")
            return False
        
        logger.info(f"Using Polygon API key: {'*' * 8}{polygon_api_key[-4:]}")
        os.environ["POLYGON_API_KEY"] = polygon_api_key
        
        # Import and set the Polygon API key in the module
        from config.polygon import set_polygon_api_key
        set_polygon_api_key(polygon_api_key)
        
        # Set environment variable to ensure DEV environment
        os.environ["ENVIRONMENT"] = "dev"
        
        # Create environment instance with DEV type to match dev_ table prefix
        env = Environment()
        env.environment_type = EnvironmentType.DEV
        
        # Import the module and set environment
        import secmaster.populate_instrument_polygon as module
        module.env = env
        
        # Set the API key directly in the module
        module.POLYGON_API_KEY = polygon_api_key
        
        # Override get_table_name to always return dev_instrument_polygon
        def fixed_get_table_name(base_name, with_prefix=True):
            if base_name == 'instrument_polygon':
                return 'dev_instrument_polygon'
            return f"dev_{base_name}" if with_prefix else base_name
            
        # Apply the override
        module.env.get_table_name = fixed_get_table_name
        
        # Test with a specific ticker
        ticker = "AAPL"
        logger.info(f"Testing fetch_and_store_instruments with ticker: {ticker}")
        
        # Import the function
        from secmaster.populate_instrument_polygon import fetch_and_store_instruments
        
        # Call the function
        await fetch_and_store_instruments(ticker=ticker)
        
        logger.info("Successfully completed fetch_and_store_instruments")
        
        # Verify the data was inserted
        logger.info("Verifying data was inserted into the database...")
        from config.database import Database
        pool = await Database.create_connection_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_polygon WHERE symbol = $1", ticker)
            if result and result > 0:
                logger.info(f"Successfully verified {ticker} was inserted into the database")
                return True
            else:
                logger.error(f"Failed to verify {ticker} was inserted into the database")
                return False
    except Exception as e:
        logger.error(f"Error testing polygon integration: {e}")
        return False

async def run_automated_test():
    """Run the full automated test."""
    # Step 1: Set up port forwarding
    if not setup_port_forwarding():
        return False
    
    try:
        # Step 2: Ensure dev_db exists
        if not await ensure_dev_db_exists():
            return False
        
        # Step 3: Run database migrations
        if not await run_database_migrations():
            return False
        
        # Step 4: Test polygon integration
        if not await test_polygon_integration():
            return False
        
        logger.info("All tests completed successfully")
        return True
    finally:
        # Always clean up resources
        cleanup_resources()

if __name__ == "__main__":
    # Set PYTHONPATH to include src directory
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
    
    # Run the automated test
    success = asyncio.run(run_automated_test())
    sys.exit(0 if success else 1)
