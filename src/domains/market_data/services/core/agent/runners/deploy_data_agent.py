#!/usr/bin/env python
"""
Deployment script for the data agent.
This script sets up the data agent in a development environment.
"""

import os
import sys
import asyncio
import logging
import argparse
import json

import asyncpg
from core.platform.config.environment import Environment

from market_data.agent.mcp_integration import MCPToolRegistry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def setup_database():
    """Set up database connection pool"""
    env = Environment()
    db_host = env.get_db_host()
    db_port = env.get_db_port()
    db_name = env.get_db_name()
    db_user = env.get_db_user()
    db_password = env.get_db_password()

    # Create connection pool
    pool = await asyncpg.create_pool(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    logger.info(f"Connected to database {db_name} at {db_host}:{db_port}")

    return pool

async def setup_mcp_tools(pool, config):
    """Set up MCP tools for the data agent"""
    # Initialize registry
    registry = MCPToolRegistry()

    # Initialize data agent
    await registry.initialize_data_agent(pool, config)

    # Return registry for further use
    return registry

async def validate_deployment(registry):
    """Validate deployment by running a simple test"""
    # Get missing data points
    try:
        result = await registry.execute_tool("get_missing_data_points", {"limit": 5})
        logger.info(f"Successfully retrieved missing data points: {result}")
    except Exception as e:
        logger.error(f"Failed to retrieve missing data points: {e}")
        return False

    # Try to process a data point if any missing points found
    if result.get("missing_points"):
        point = result["missing_points"][0]
        try:
            process_result = await registry.execute_tool(
                "process_data_point",
                {
                    "symbol": point["symbol"],
                    "date_str": point["date"]
                }
            )
            logger.info(f"Successfully processed data point: {process_result}")
        except Exception as e:
            logger.error(f"Failed to process data point: {e}")
            return False

    return True

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Data Agent Deployment")
    parser.add_argument("--vendor-priority", type=str, default="tiingo,polygon",
                      help="Comma-separated list of vendor priority (default: tiingo,polygon)")
    parser.add_argument("--lookback-years", type=int, default=5,
                      help="Number of years to look back for historical data (default: 5)")
    parser.add_argument("--validate", action="store_true",
                      help="Validate deployment after setup")
    parser.add_argument("--config-file", type=str,
                      help="Path to JSON configuration file")

    args = parser.parse_args()

    # Load configuration from file if provided
    config = {}
    if args.config_file and os.path.exists(args.config_file):
        with open(args.config_file, 'r') as f:
            config = json.load(f)

    # Override config with command line arguments
    if args.vendor_priority:
        config["vendor_priority"] = args.vendor_priority.split(",")

    if args.lookback_years:
        config["lookback_years"] = args.lookback_years

    # Set up database
    pool = await setup_database()

    try:
        # Set up MCP tools
        registry = await setup_mcp_tools(pool, config)

        # Validate deployment if requested
        if args.validate:
            success = await validate_deployment(registry)
            if success:
                logger.info("Deployment validation successful")
            else:
                logger.error("Deployment validation failed")
                sys.exit(1)

        logger.info("Data agent deployment completed successfully")

        # Print available tools
        logger.info(f"Available tools: {list(registry.tools.keys())}")

    finally:
        # Close the connection pool
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
