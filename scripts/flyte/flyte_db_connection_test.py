#!/usr/bin/env python
"""
Flyte Workflow for Database Connection Testing

This script defines a Flyte workflow for testing database connections with various parameters.
It helps diagnose connection issues and verify that the database can be accessed properly.
"""

import os
import asyncio
import logging
import time
import sys
from typing import Dict, List, Optional, Any, Tuple

import flytekit
from flytekit import task, workflow, dynamic
from flytekit.types.file import FlyteFile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db_connection_test')

@task
def test_db_connection(
    host: str = "timescaledb.ats-dev.svc.cluster.local",
    port: str = "5432",
    user: str = "",  # Will be populated from secrets
    password: str = "",  # Will be populated from secrets
    database: str = "",  # Will be populated from secrets
    connection_params: str = "sslmode=disable"
) -> Dict[str, Any]:
    """
    Test database connection with various connection string formats.
    
    Args:
        host: Database host
        port: Database port
        user: Database user
        password: Database password
        database: Database name
        connection_params: Connection parameters
        
    Returns:
        Dictionary with test results
    """
    import asyncpg
    
    async def test_connection(conn_string: str, description: str) -> Dict[str, Any]:
        """Test a specific connection string."""
        start_time = time.time()
        result = {
            "conn_string": conn_string.replace(password, "****"),
            "description": description,
            "success": False,
            "error": None,
            "duration_ms": 0,
            "pg_version": None
        }
        
        try:
            logger.info(f"Testing connection: {description}")
            conn = await asyncpg.connect(conn_string, timeout=5.0)
            version = await conn.fetchval('SELECT version()')
            await conn.close()
            
            result["success"] = True
            result["pg_version"] = version
            logger.info(f"Connection successful: {description}")
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Connection failed: {description} - Error: {str(e)}")
        
        result["duration_ms"] = round((time.time() - start_time) * 1000, 2)
        return result
    
    async def run_tests() -> List[Dict[str, Any]]:
        """Run all connection tests."""
        test_cases = [
            # Test 1: Basic connection string without parameters
            {
                "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}",
                "description": "Basic connection without parameters"
            },
            # Test 2: Connection string with connect_timeout parameter
            {
                "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?connect_timeout=10",
                "description": "With connect_timeout parameter"
            },
            # Test 3: Connection string with sslmode=disable
            {
                "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=disable",
                "description": "With sslmode=disable"
            },
            # Test 4: Connection string with custom parameters
            {
                "conn_string": f"postgresql://{user}:{password}@{host}:{port}/{database}?{connection_params}",
                "description": f"With custom parameters: {connection_params}"
            }
        ]
        
        results = []
        for test_case in test_cases:
            result = await test_connection(test_case["conn_string"], test_case["description"])
            results.append(result)
        
        return results
    
    # Log environment variables (masking sensitive data)
    logger.info("Environment variables:")
    for k, v in os.environ.items():
        if k.startswith("DB_") or k.startswith("POSTGRES"):
            if "PASSWORD" in k or "PASS" in k:
                logger.info(f"  {k}=****")
            else:
                logger.info(f"  {k}={v}")
    
    # Run the tests
    results = asyncio.run(run_tests())
    
    # Summarize results
    success_count = sum(1 for r in results if r["success"])
    logger.info(f"Test summary: {success_count}/{len(results)} connections successful")
    
    return {
        "results": results,
        "summary": {
            "total_tests": len(results),
            "successful_tests": success_count,
            "host": host,
            "port": port,
            "user": user,
            "database": database
        }
    }

@workflow
def db_connection_test_workflow(
    host: str = "timescaledb.ats-dev.svc.cluster.local",
    port: str = "5432",
    connection_params: str = "sslmode=disable"
) -> Dict[str, Any]:
    """
    Workflow for testing database connections.
    
    Args:
        host: Database host
        port: Database port
        connection_params: Connection parameters
        
    Returns:
        Dictionary with test results
    """
    # User, password, and database will be populated from secrets in the Flyte environment
    return test_db_connection(
        host=host,
        port=port,
        connection_params=connection_params
    )

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test database connections")
    parser.add_argument('--host', type=str, default="timescaledb.ats-dev.svc.cluster.local",
                        help='Database host')
    parser.add_argument('--port', type=str, default="5432",
                        help='Database port')
    parser.add_argument('--user', type=str, default="",
                        help='Database user')
    parser.add_argument('--password', type=str, default="",
                        help='Database password')
    parser.add_argument('--database', type=str, default="",
                        help='Database name')
    parser.add_argument('--connection-params', type=str, default="sslmode=disable",
                        help='Connection parameters')
    
    args = parser.parse_args()
    
    # For local testing, we can pass credentials directly
    # In Flyte, these will come from secrets
    if args.user and args.password and args.database:
        result = test_db_connection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            connection_params=args.connection_params
        )
    else:
        # Use environment variables if available
        result = test_db_connection(
            host=args.host,
            port=args.port,
            user=os.environ.get("DB_USER", ""),
            password=os.environ.get("DB_PASSWORD", ""),
            database=os.environ.get("DB_NAME", ""),
            connection_params=args.connection_params
        )
    
    # Print results in a readable format
    print("\nDB Connection Test Results:")
    print("==========================")
    for i, test in enumerate(result["results"]):
        print(f"\nTest {i+1}: {test['description']}")
        print(f"Connection string: {test['conn_string']}")
        print(f"Success: {test['success']}")
        if test['success']:
            print(f"PostgreSQL version: {test['pg_version']}")
        else:
            print(f"Error: {test['error']}")
        print(f"Duration: {test['duration_ms']} ms")
    
    print("\nSummary:")
    print(f"Total tests: {result['summary']['total_tests']}")
    print(f"Successful tests: {result['summary']['successful_tests']}")
