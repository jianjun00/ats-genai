#!/usr/bin/env python3
"""
Test script to diagnose database connection issues with connect_timeout parameter.
This script attempts connections with different parameters to isolate the issue.
"""

import asyncio
import asyncpg
import os
import sys
import logging
import time
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db_connection_test")

async def test_connection(connection_string, description):
    """Test a specific connection string and log the results."""
    logger.info(f"Testing connection: {description}")
    # Mask password in logs
    masked_conn_string = connection_string
    if ":" in masked_conn_string and "@" in masked_conn_string:
        parts = masked_conn_string.split("@")
        auth_parts = parts[0].split(":")
        if len(auth_parts) > 1:
            masked_conn_string = f"{auth_parts[0]}:****@{parts[1]}"
    
    logger.info(f"Connection string: {masked_conn_string}")
    
    try:
        start_time = time.time()
        conn = await asyncpg.connect(dsn=connection_string)
        elapsed = time.time() - start_time
        
        logger.info(f"✅ Connection successful! ({elapsed:.2f}s)")
        
        # Test a simple query
        result = await conn.fetchval("SELECT 1")
        logger.info(f"Query result: {result}")
        
        # Get server version
        version = await conn.fetchval("SELECT version()")
        logger.info(f"Server version: {version}")
        
        await conn.close()
        return True
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"❌ Connection failed after {elapsed:.2f}s: {type(e).__name__}: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def run_tests():
    """Run a series of connection tests with different parameters."""
    # Get connection parameters from environment or use defaults
    host = os.environ.get("DB_HOST", "timescaledb.ats-dev.svc.cluster.local")
    port = os.environ.get("DB_PORT", "5432")
    user = os.environ.get("DB_USER", "postgres")
    password = os.environ.get("DB_PASSWORD", "postgres")
    database = os.environ.get("DB_NAME", "dev_db")
    
    # Log environment variables (masking sensitive data)
    logger.info("Environment variables:")
    for k, v in os.environ.items():
        if k.startswith("DB_") or k.startswith("POSTGRES"):
            if "PASSWORD" in k or "PASS" in k:
                logger.info(f"  {k}=****")
            else:
                logger.info(f"  {k}={v}")
    
    # Test different connection strings
    test_cases = [
        # Test 1: Basic connection string without parameters
        {
            "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}",
            "description": "Basic connection without parameters"
        },
        
        # Test 2: With sslmode=disable
        {
            "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=disable",
            "description": "With sslmode=disable"
        },
        
        # Test 3: With connect_timeout=10
        {
            "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?connect_timeout=10",
            "description": "With connect_timeout=10"
        },
        
        # Test 4: With both connect_timeout and sslmode
        {
            "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?connect_timeout=10&sslmode=disable",
            "description": "With connect_timeout=10 and sslmode=disable"
        },
        
        # Test 5: With different connect_timeout value
        {
            "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?connect_timeout=5&sslmode=disable",
            "description": "With connect_timeout=5 and sslmode=disable"
        },
        
        # Test 6: Using direct parameters instead of connection string
        {
            "conn_string": None,
            "description": "Using direct parameters instead of connection string",
            "params": {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": database
            }
        }
    ]
    
    results = []
    
    for i, test in enumerate(test_cases, 1):
        logger.info(f"\n--- Test {i}: {test['description']} ---")
        
        if test.get("conn_string") is not None:
            success = await test_connection(test["conn_string"], test["description"])
        else:
            # Handle direct parameters case
            params = test.get("params", {})
            logger.info(f"Testing connection with direct parameters: {test['description']}")
            masked_params = params.copy()
            if "password" in masked_params:
                masked_params["password"] = "****"
            logger.info(f"Parameters: {masked_params}")
            
            try:
                start_time = time.time()
                conn = await asyncpg.connect(**params)
                elapsed = time.time() - start_time
                
                logger.info(f"✅ Connection successful! ({elapsed:.2f}s)")
                
                # Test a simple query
                result = await conn.fetchval("SELECT 1")
                logger.info(f"Query result: {result}")
                
                await conn.close()
                success = True
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"❌ Connection failed after {elapsed:.2f}s: {type(e).__name__}: {str(e)}")
                logger.error(traceback.format_exc())
                success = False
        
        results.append({
            "test": i,
            "description": test["description"],
            "success": success
        })
    
    # Print summary
    logger.info("\n=== Test Results Summary ===")
    for result in results:
        status = "✅ PASSED" if result["success"] else "❌ FAILED"
        logger.info(f"Test {result['test']} ({result['description']}): {status}")
    
    # Return True if any test succeeded
    return any(r["success"] for r in results)

if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
