#!/usr/bin/env python3
"""
Database Connection Utilities

Provides standardized database connection patterns with fallbacks for standalone scripts.
Handles complex import dependencies gracefully.

USAGE:
======

from src.core.shared.utils.database_connections import get_database_pool, get_table_name

# Get database connection with automatic fallbacks
pool = await get_database_pool(environment='dev')

# Get environment-specific table names
table_name = get_table_name('news', environment='dev')  # Returns 'dev_news'
"""

import os
import asyncpg
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

async def get_database_pool(environment: str = 'dev', max_retries: int = 3, timeout: float = 10.0) -> asyncpg.Pool:
    """
    Get database connection pool with automatic fallbacks.

    Attempts to use advanced Database class from the codebase, then falls back
    to simple asyncpg connection if complex imports fail.

    Args:
        environment: Environment name (dev, test, intg, prod)
        max_retries: Maximum connection retries
        timeout: Connection timeout in seconds

    Returns:
        asyncpg.Pool connection pool

    Example:
        >>> pool = await get_database_pool('dev')
        >>> async with pool.acquire() as conn:
        ...     result = await conn.fetchrow('SELECT version()')
    """
    # Set environment for Database class
    os.environ["ENVIRONMENT"] = environment

    # Method 1: Try advanced Database class from codebase
    try:
        from src.core.shared.data_handling.utils.database import Database
        from src.core.shared.data_handling.utils.environment import Environment, EnvironmentType

        env = Environment(env_type=EnvironmentType(environment))
        pool = await Database.create_connection_pool(env=env, max_retries=max_retries, timeout=timeout)
        logger.info("Using advanced database connection system")
        return pool

    except (ImportError, Exception) as e:
        logger.info(f"Advanced database system not available ({e}), using simple connection")

    # Method 2: Fallback to simple asyncpg connection
    db_config = get_simple_db_config(environment)

    try:
        pool = await asyncpg.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password'],
            min_size=1,
            max_size=5,
            command_timeout=timeout
        )
        logger.info(f"Connected to database using simple connection: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        return pool

    except Exception as e:
        logger.error(f"Failed to create database connection pool: {e}")
        raise

def get_simple_db_config(environment: str = 'dev') -> Dict[str, Any]:
    """
    Get simple database configuration based on environment.

    Args:
        environment: Environment name

    Returns:
        Dictionary with database connection parameters
    """
    # Environment-specific database configurations
    env_configs = {
        'dev': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '3432')),
            'database': os.getenv('DB_NAME', 'dev_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'dev_password')
        },
        'test': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5433')),
            'database': os.getenv('DB_NAME', 'test_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'test_password')
        },
        'intg': {
            'host': os.getenv('DB_HOST', 'ats-intg-postgres'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'intg_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'intg_password')
        },
        'prod': {
            'host': os.getenv('DB_HOST', 'ats-prod-postgres'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'prod_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'prod_password')
        }
    }

    return env_configs.get(environment, env_configs['dev'])

def get_table_name(base_name: str, environment: str = 'dev') -> str:
    """
    Get environment-specific table name.

    Args:
        base_name: Base table name (e.g., 'news', 'prices', 'instruments')
        environment: Environment name

    Returns:
        Environment-prefixed table name (e.g., 'dev_news', 'prod_prices')

    Example:
        >>> table_name = get_table_name('news', 'dev')  # Returns 'dev_news'
        >>> table_name = get_table_name('prices', 'prod')  # Returns 'prod_prices'
    """
    # Try to use Environment class first
    try:
        from src.core.shared.data_handling.utils.environment import Environment, EnvironmentType
        env = Environment(env_type=EnvironmentType(environment))
        return env.get_table_name(base_name)
    except (ImportError, Exception):
        # Fallback to simple naming convention
        return f"{environment}_{base_name}"

async def test_database_connection(environment: str = 'dev') -> bool:
    """
    Test database connection for an environment.

    Args:
        environment: Environment to test

    Returns:
        True if connection successful, False otherwise
    """
    try:
        pool = await get_database_pool(environment)
        async with pool.acquire() as conn:
            result = await conn.fetchrow('SELECT version()')
            logger.info(f"Database connection test successful: {result[0][:50]}...")
        await pool.close()
        return True
    except Exception as e:
        logger.error(f"Database connection test failed for {environment}: {e}")
        return False

class DatabaseConnectionManager:
    """
    Context manager for database connections with automatic cleanup.

    Example:
        >>> async with DatabaseConnectionManager('dev') as pool:
        ...     async with pool.acquire() as conn:
        ...         result = await conn.fetchrow('SELECT COUNT(*) FROM dev_news')
    """

    def __init__(self, environment: str = 'dev', max_retries: int = 3, timeout: float = 10.0):
        self.environment = environment
        self.max_retries = max_retries
        self.timeout = timeout
        self.pool = None

    async def __aenter__(self) -> asyncpg.Pool:
        self.pool = await get_database_pool(
            environment=self.environment,
            max_retries=self.max_retries,
            timeout=self.timeout
        )
        return self.pool

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()
            self.pool = None