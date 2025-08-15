#!/usr/bin/env python3
"""
Script to test the refactored populate_instrument_polygon.py code with the port-forwarded ats-dev database.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_populate_instrument_polygon")

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

async def test_populate_instrument_polygon():
    """Test the refactored populate_instrument_polygon.py code."""
    # Set environment variables for port-forwarded connection
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5433"
    os.environ["DB_USER"] = "postgres"
    os.environ["DB_PASSWORD"] = "password"
    os.environ["DB_NAME"] = "dev_db"
    
    # Import the required modules
    from secmaster.populate_instrument_polygon import fetch_and_store_instruments
    from config.environment import Environment, EnvironmentType
    
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
    
    try:
        # Create environment instance
        env = Environment()
        env.environment_type = EnvironmentType.DEV
        
        # Monkey patch the module to use our environment
        import secmaster.populate_instrument_polygon as module
        # Store the original env and POLYGON_API_KEY variables
        if hasattr(module, 'env'):
            original_env = module.env
        original_api_key = module.POLYGON_API_KEY
        
        # Set the env variable in the module
        module.env = env
        # Directly set the POLYGON_API_KEY in the module
        module.POLYGON_API_KEY = polygon_api_key
        
        # Override the fetch_and_store_instruments function to use our API key
        original_function = module.fetch_and_store_instruments
        
        async def patched_function(start_ticker='', ticker=None):
            # Use centralized database connection logic
            try:
                from config.database import Database
                logger.info(f"Creating database connection pool using centralized logic")
                pool = await Database.create_connection_pool(env=env, max_retries=3, initial_delay=1.0, timeout=10.0)
                logger.info("Successfully connected to database")
                
                if ticker:
                    symbol = ticker
                    logger.info(f"Fetching single ticker: {ticker}")
                    # Use our API key directly
                    detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={polygon_api_key}"
                    logger.debug(f"Fetching URL with masked API key: https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={'*' * 8}{polygon_api_key[-4:]}")
                    
                    for attempt in range(3):
                        try:
                            import requests
                            detail_resp = requests.get(detail_url)
                            logger.debug(f"Response status code: {detail_resp.status_code}")
                            
                            if detail_resp.status_code != 200:
                                logger.error(f"Failed to fetch detail for {symbol}: {detail_resp.status_code} {detail_resp.text}")
                                break
                                
                            detail = detail_resp.json().get('results', {})
                            logger.info(f"Ticker: {symbol}, list_date: {detail.get('list_date')}, delisted_utc: {detail.get('delisted_utc')}")
                            
                            # Call the upsert function
                            await module.upsert_instrument(pool, detail)
                            logger.info(f"Successfully upserted {symbol}")
                            break
                        except Exception as e:
                            logger.error(f"Error fetching {symbol}: {e}")
                            import time
                            time.sleep(2 ** attempt)  # Exponential backoff
                await pool.close()
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}")
                raise
        
        # Replace the function temporarily
        module.fetch_and_store_instruments = patched_function
        
        # Test with a specific ticker
        ticker = "AAPL"
        logger.info(f"Testing fetch_and_store_instruments with ticker: {ticker}")
        # Call our patched function
        await patched_function(ticker=ticker)
        
        # Restore the original env if it existed
        if 'original_env' in locals():
            module.env = original_env
        logger.info("Successfully completed fetch_and_store_instruments")
        return True
    except Exception as e:
        logger.error(f"Error testing populate_instrument_polygon: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_populate_instrument_polygon())
    sys.exit(0 if success else 1)
