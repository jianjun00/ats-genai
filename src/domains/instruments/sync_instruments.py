#!/usr/bin/env python3
"""
Sync instruments from instrument_polygon table to core instruments and instrument_xrefs tables.
This script creates the linkage needed for daily price backfill.
"""

import os
import asyncio
import argparse
import gin
import sys
import logging
from core.platform.config_env.environment import Environment, EnvironmentType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("sync_instruments")

async def sync_instruments(pool, env, limit=None):
    """
    Sync instruments from instrument_polygon to instruments and instrument_xrefs tables.

    Args:
        pool: Database connection pool
        env: Environment instance
        limit: Optional limit on number of instruments to sync (for testing)
    """
    polygon_table = env.get_table_name('instrument_polygon')
    instruments_table = env.get_table_name('instruments')
    xrefs_table = env.get_table_name('instrument_xrefs')

    logger.info(f"Syncing from {polygon_table} to {instruments_table} and {xrefs_table}")

    async with pool.acquire() as conn:
        # Get polygon instruments that need syncing
        limit_clause = f"LIMIT {limit}" if limit else ""
        polygon_instruments = await conn.fetch(f"""
            SELECT symbol, name, exchange, type, currency, active, list_date, delist_date
            FROM {polygon_table}
            WHERE active = true AND list_date IS NOT NULL
            ORDER BY symbol
            {limit_clause}
        """)

        logger.info(f"Found {len(polygon_instruments)} active instruments to sync")

        if not polygon_instruments:
            logger.warning("No instruments to sync")
            return 0

        # Get Polygon vendor ID
        vendor_result = await conn.fetchrow("SELECT id FROM dev_vendors WHERE name = 'polygon'")
        if not vendor_result:
            logger.error("Polygon vendor not found in vendors table")
            return 0

        polygon_vendor_id = vendor_result['id']
        logger.info(f"Using Polygon vendor_id: {polygon_vendor_id}")

        synced_count = 0

        for inst in polygon_instruments:
            symbol = inst['symbol']

            try:
                # Check if instrument already exists
                existing_instrument = await conn.fetchrow(f"""
                    SELECT i.id FROM {instruments_table} i
                    JOIN {xrefs_table} x ON i.id = x.instrument_id
                    WHERE x.vendor_id = $1 AND x.vendor_symbol = $2
                """, polygon_vendor_id, symbol)

                if existing_instrument:
                    logger.debug(f"Instrument {symbol} already synced (instrument_id: {existing_instrument['id']})")
                    continue

                # Insert into instruments table
                instrument_id = await conn.fetchval(f"""
                    INSERT INTO {instruments_table}
                    (name, symbol, exchange, is_active, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    RETURNING id
                """,
                inst['name'],
                symbol,
                inst['exchange'],
                inst['active']
                )

                # Insert into instrument_xrefs table
                await conn.execute(f"""
                    INSERT INTO {xrefs_table}
                    (instrument_id, vendor_id, vendor_symbol, created_at, updated_at)
                    VALUES ($1, $2, $3, NOW(), NOW())
                """, instrument_id, polygon_vendor_id, symbol)

                logger.info(f"Synced {symbol} -> instrument_id: {instrument_id}")
                synced_count += 1

            except Exception as e:
                logger.error(f"Failed to sync instrument {symbol}: {e}")
                continue

        logger.info(f"Successfully synced {synced_count} instruments")
        return synced_count

async def main():
    parser = argparse.ArgumentParser(description="Sync instruments from Polygon to core tables")
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of instruments to sync (for testing)')
    parser.add_argument('--db_host', type=str, default=None, help='Database host override')
    parser.add_argument('--db_port', type=str, default=None, help='Database port override')
    parser.add_argument('--db_user', type=str, default=None, help='Database user override')
    parser.add_argument('--db_password', type=str, default=None, help='Database password override')
    parser.add_argument('--db_name', type=str, default=None, help='Database name override')

    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    # Set database environment variables if provided
    if args.db_host:
        os.environ["DB_HOST"] = args.db_host
    if args.db_port:
        os.environ["DB_PORT"] = args.db_port
    if args.db_user:
        os.environ["DB_USER"] = args.db_user
    if args.db_password:
        os.environ["DB_PASSWORD"] = args.db_password
    if args.db_name:
        os.environ["DB_NAME"] = args.db_name

    # Determine Gin config file
    if args.gin_config:
        gin_config_path = args.gin_config
    else:
        gin_config_map = {
            'test': 'config/app_test.gin',
            'intg': 'config/app_intg.gin',
            'prod': 'config/app_prod.gin',
            'dev': 'config/app_dev.gin',
        }
        gin_config_path = gin_config_map.get(args.environment)

    logger.info(f"Using Gin config: {gin_config_path}")

    if not os.path.exists(gin_config_path):
        logger.error(f"Gin config file not found: {gin_config_path}")
        sys.exit(1)

    try:
        # Import Database before parsing Gin config
        from core.shared.database import Database

        gin.parse_config_file(gin_config_path)
        logger.info(f"Successfully parsed Gin config: {gin_config_path}")
    except Exception as e:
        logger.error(f"Failed to parse Gin config: {e}")
        sys.exit(1)

    try:
        # Set environment
        env_type = EnvironmentType(args.environment)
        env = Environment(gin_config_path=gin_config_path, env_type=env_type)
        logger.info(f"Using environment: {env_type}")

        # Create database connection pool
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)
        logger.info("Connected to database")

        # Sync instruments
        synced_count = await sync_instruments(pool, env, limit=args.limit)

        await pool.close()
        logger.info(f"Sync complete. Total instruments synced: {synced_count}")

    except Exception as e:
        logger.error(f"Failed to sync instruments: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())