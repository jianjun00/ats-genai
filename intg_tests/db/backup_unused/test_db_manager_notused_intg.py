"""
Test Database Management for Unit and Integration Tests.

This module provides:
- Isolated test database setup/teardown for unit tests
- Shared integration test database management
- Test data fixtures and cleanup utilities
- Environment-aware test table management
"""

import asyncio
import asyncpg
import pytest
import pytest_asyncio
import uuid
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
from config.environment import get_environment
from db.migration_manager import MigrationManager
import logging
logger = logging.getLogger(__name__)


class TestDatabaseManager:
    """Manages test databases for unit and integration tests."""

    async def backup_tables(self, tables: list):
        """
        For each table in tables, create a temp backup table with all data.
        Example: intg_vendors -> intg_vendors__bak
        """
        import asyncpg
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                for table in tables:
                    full_table = f"{self.table_prefix}{table}"
                    backup_table = f"{full_table}__bak"
                    # Drop backup if exists
                    await conn.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE")
                    # Create backup as copy
                    await conn.execute(f"CREATE TABLE {backup_table} AS TABLE {full_table}")
        finally:
            await pool.close()

    async def restore_tables(self, tables: list):
        """
        For each table in tables, restore data from temp backup table and drop the backup.
        """
        import asyncpg
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                for table in tables:
                    full_table = f"{self.table_prefix}{table}"
                    backup_table = f"{full_table}__bak"
                    # Truncate table
                    await conn.execute(f"TRUNCATE {full_table} RESTART IDENTITY CASCADE")
                    # Restore data
                    await conn.execute(f"INSERT INTO {full_table} SELECT * FROM {backup_table}")
                    # Drop backup
                    await conn.execute(f"DROP TABLE IF EXISTS {backup_table} CASCADE")
        finally:
            await pool.close()


    async def setup_isolated_test_tables(self, base_tables, testname):
        """
        For each table in base_tables, create a new table named intg_<table>_<testname> by copying schema and data from intg_<table>.
        Drops the test table if it exists.
        """
        import asyncpg
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                for base in base_tables:
                    base_table = f"{self.table_prefix}{base}"
                    test_table = f"{self.table_prefix}{base}_{testname}"
                    # Drop if exists
                    await conn.execute(f"DROP TABLE IF EXISTS {test_table} CASCADE")
                    # Recreate with same schema
                    await conn.execute(f"CREATE TABLE {test_table} (LIKE {base_table} INCLUDING ALL)")
                    # Copy data
                    await conn.execute(f"INSERT INTO {test_table} SELECT * FROM {base_table}")
        finally:
            await pool.close()
    
    def __init__(self, test_type: str = "unit"):
        """
        Initialize test database manager.
        
        Args:
            test_type: "unit" or "integration"
        """
        # Use the appropriate environment based on test type
        if test_type == "integration":
            from config.environment import Environment, EnvironmentType
            self.env = Environment(EnvironmentType.INTEGRATION)
        else:
            self.env = get_environment()
        
        self.test_type = test_type
        self.db_url = self.env.get_database_url()
        # Extract table prefix from environment
        sample_table = self.env.get_table_name("sample")
        self.table_prefix = sample_table.replace("sample", "")
        
        # For unit tests, create unique database per test
        if test_type == "unit":
            self.test_db_suffix = f"_{uuid.uuid4().hex[:8]}"
        else:
            self.test_db_suffix = ""
    
    async def setup_test_database(self) -> str:
        """
        Set up test database with latest schema.
        
        Returns:
            Database URL for the test database
        """
        if self.test_type == "unit":
            # Create isolated database for unit test
            db_config = self.env.get_database_config()
            base_db_name = db_config['database']
            test_db_name = f"{base_db_name}{self.test_db_suffix}"
            await self._create_test_database(test_db_name)
            test_db_url = self.db_url.replace(base_db_name, test_db_name)
        else:
            # Use shared integration database
            test_db_url = self.db_url
        
        # For integration tests, apply migrations to ensure latest schema
        # Unit tests will manage their own migration state
        if self.test_type == "integration":
            migration_manager = MigrationManager(test_db_url)
            await migration_manager.migrate_to_latest()
        
        return test_db_url
    
    async def teardown_test_database(self):
        """Clean up test database after test completion."""
        if self.test_type == "unit":
            # Drop the entire test database
            db_config = self.env.get_database_config()
            base_db_name = db_config['database']
            test_db_name = f"{base_db_name}{self.test_db_suffix}"
            await self._drop_test_database(test_db_name)
        else:
            # For integration tests, clean up test data but keep schema
            await self._cleanup_test_data()
    
    async def _create_test_database(self, db_name: str):
        """Create a new test database."""
        set_environment(EnvironmentType.INTEGRATION)
        env = get_environment()
        TSDB_URL = env.get_database_url()
        # Connect to postgres database to create new database
        db_config = self.env.get_database_config()
        base_db_name = db_config['database']
        postgres_url = self.db_url.replace(f"/{base_db_name}", "/postgres")
        pool = await asyncpg.create_pool(postgres_url)
        try:
            async with pool.acquire() as conn:
                # Check if database exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1", db_name
                )
                if not exists:
                    await conn.execute(f'CREATE DATABASE "{db_name}"')
        finally:
            await pool.close()
    
    async def _drop_test_database(self, db_name: str):
        """Drop a test database."""
        db_config = self.env.get_database_config()
        base_db_name = db_config['database']
        postgres_url = self.db_url.replace(f"/{base_db_name}", "/postgres")
        pool = await asyncpg.create_pool(postgres_url)
        try:
            async with pool.acquire() as conn:
                # Terminate active connections to the database
                await conn.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = $1 AND pid <> pg_backend_pid()
                """, db_name)
                
                # Drop the database
                await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        finally:
            await pool.close()
    
    async def _cleanup_test_data(self):
        """Clean up test data from integration test database."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Define cleanup order (respecting foreign key constraints)
                    cleanup_tables = [
                        'universe_membership',
                        'instrument_aliases',
                        'instrument_metadata',
                        'daily_prices',
                        'daily_prices_tiingo',
                        'daily_prices_polygon',
                        'daily_market_cap',
                        'fundamentals',
                        'events',
                        'instruments',
                        'instrument_polygon',
                        'universe',
                        'vendors'
                    ]
                    
                    for table in cleanup_tables:
                        # Only clean up test data, not reference data
                        if table in ['status_code']:  # Skip reference tables
                            continue
                        
                        full_table_name = f"{self.table_prefix}{table}"
                        await conn.execute(f"DELETE FROM {full_table_name}")
                        
                        # Reset sequences for tables with serial primary keys
                        if table in ['vendors', 'instruments', 'universe', 'events']:
                            await conn.execute(f"""
                                SELECT setval(pg_get_serial_sequence('{full_table_name}', 'id'), 1, false)
                            """)
        finally:
            await pool.close()

    async def load_test_fixtures(self, fixtures: Dict[str, List[Dict[str, Any]]]):
        logger.info("[DEBUG] load_test_fixtures called!")
        logger.info("[DEBUG] fixtures type: %r", type(fixtures))
        logger.info("[DEBUG] fixtures: %r", fixtures)

        """
        Load test data fixtures into the database.
        
        Args:
            fixtures: Dictionary mapping table names to lists of row data
        """
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for table_name, rows in fixtures.items():
                        print(f"[DEBUG] Table: {table_name} - rows: {len(rows)}")
                        if rows:
                            print(f"[DEBUG] First row: {rows[0]}")
                        if not rows:
                            continue
                        
                        full_table_name = f"{self.table_prefix}{table_name}"
                        
                        # Build insert statement
                        columns = list(rows[0].keys())
                        placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
                        column_names = ', '.join(columns)
                        
                        insert_sql = f"""
                            INSERT INTO {full_table_name} ({column_names})
                            VALUES ({placeholders})
                        """
                        
                        # Insert all rows
                        print(f"[DEBUG] Inserting rows into table: {full_table_name}")
                        from datetime import datetime, date
                        import re
                        for row in rows:
                            values = []
                            for col in columns:
                                val = row[col]
                                print(f"[DEBUG] Pre-insert: column={col}, type={type(val)}, value={val}")
                                # Convert date/datetime objects to YYYY-MM-DD string
                                if hasattr(val, 'isoformat') and (isinstance(val, datetime) or hasattr(val, 'year')):
                                    print(f"[CONVERT] Converting date object to string: column={col}, value={val}, type={type(val)}")
                                    val = val.isoformat()
                                values.append(val)
                            print(f"[DEBUG] Row values (post-conversion): {values}")
                            print(f"[DEBUG] Row types: {[type(v) for v in values]}")
                            for col, val in zip(columns, values):
                                print(f"[DEBUG] Inserting into {full_table_name}: column={col}, type={type(val)}, value={val}")
                                if isinstance(val, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                                    print(f"[WARNING] String date detected: column={col}, value={val}")
                            print(f"[DEBUG] SQL: {insert_sql.strip()} VALUES: {tuple(values)}")
                            import sys; sys.stdout.flush()
                            try:
                                await conn.execute(insert_sql, *values)
                            except Exception as e:
                                print("[ERROR] Exception during DB insert!")
                                print(f"  Table: {full_table_name}")
                                print(f"  Insert SQL: {insert_sql.strip()}")
                                print(f"  Values: {values}")
                                print(f"  Types: {[type(v) for v in values]}")
                                import traceback
                                traceback.print_exc()
                                import sys; sys.stdout.flush()
                                raise
        finally:
            await pool.close()

class IntegrationTestSession:
    """Manages shared state for integration test sessions."""
    
    _instance = None
    _db_manager = None
    _test_db_url = None
    _session_id = None
    
    @classmethod
    async def get_instance(cls):
        """Get or create the singleton integration test session."""
        if cls._instance is None:
            cls._instance = cls()
            cls._session_id = uuid.uuid4().hex[:8]
            cls._db_manager = TestDatabaseManager("integration")
            cls._test_db_url = await cls._db_manager.setup_test_database()
        return cls._instance
    
    @classmethod
    async def cleanup(cls):
        """Clean up the integration test session."""
        if cls._db_manager:
            await cls._db_manager.teardown_test_database()
            cls._instance = None
            cls._db_manager = None
            cls._test_db_url = None
            cls._session_id = None
    
    @property
    def db_url(self) -> str:
        """Get the test database URL."""
        return self._test_db_url
    
    @property
    def session_id(self) -> str:
        """Get the session ID for this test run."""
        return self._session_id

# Pytest fixtures for test database management

import pytest_asyncio

@pytest_asyncio.fixture
async def backup_and_restore_tables():
    """
    Async fixture for strict test isolation: backs up and restores tables touched by a test.
    Usage: add as a fixture to integration tests.
    """
    db_manager = TestDatabaseManager("integration")
    tables = [
        'vendors', 'instruments', 'universe', 'events',
        'universe_membership', 'instrument_aliases', 'instrument_metadata',
        'daily_prices', 'daily_prices_tiingo', 'daily_prices_polygon',
        'daily_market_cap', 'fundamentals', 'instrument_polygon'
    ]
    # Clean up tables BEFORE backing up
    await db_manager.backup_tables(tables)
    print("[DEBUG] backup_and_restore_tables: tables backed up")
    # await db_manager._cleanup_test_data()
    try:
        yield
    finally:
        await db_manager.restore_tables(tables)

@pytest_asyncio.fixture
async def unit_test_db():
    """Fixture for unit tests - provides isolated database per test."""
    db_manager = TestDatabaseManager("unit")
    test_db_url = await db_manager.setup_test_database()
    
    yield test_db_url
    
    await db_manager.teardown_test_database()

@pytest_asyncio.fixture(scope="session")
async def integration_test_db():
    """Fixture for integration tests - provides shared database for session."""
    session = await IntegrationTestSession.get_instance()
    
    yield session.db_url
    
    # Cleanup happens at session end

import pytest_asyncio

@pytest_asyncio.fixture
async def clean_integration_db(integration_test_db):
    """Async fixture that cleans integration database before each test. Compatible with pytest-asyncio."""
    session = await IntegrationTestSession.get_instance()
    await session._db_manager._cleanup_test_data()
    yield integration_test_db

@asynccontextmanager
async def test_database_context(test_type: str = "unit"):
    """
    Context manager for test database setup/teardown.
    
    Usage:
        async with test_database_context("unit") as db_url:
            # Use db_url for testing
    """
    db_manager = TestDatabaseManager(test_type)
    test_db_url = await db_manager.setup_test_database()
    
    try:
        yield test_db_url
    finally:
        await db_manager.teardown_test_database()

# Example test fixtures for common test data

import random
from datetime import date, timedelta

# Generate ~500 SPY tickers
SPY_TICKERS = [f"SPY{str(i).zfill(3)}" for i in range(1, 501)]
TEST_DATE = date(2023, 1, 3)

SAMPLE_FIXTURES = {
    "vendors": [
        {
            "name": "test_vendor",
            "description": "Test data vendor",
            "website": "https://test.com",
            "api_key_env_var": "TEST_API_KEY"
        }
    ],
    "instruments": [
        {"symbol": ticker, "name": f"{ticker} Stock", "exchange": "NYSE", "type": "stock", "currency": "USD", "active": True}
        for ticker in SPY_TICKERS
    ],
    "spy_membership_change": [
        {"change_date": TEST_DATE, "added": ticker, "removed": None}
        for ticker in SPY_TICKERS
    ],
    "daily_prices": [
        {
            "date": TEST_DATE,
            "symbol": ticker,
            "open": float(100 + i % 10),
            "high": float(105 + i % 5),
            "low": float(95 + i % 5),
            "close": float(100 + i % 10),
            "volume": 1000000 + i * 10
        }
        for i, ticker in enumerate(SPY_TICKERS)
    ],
    "splits": [
        {
            "symbol": SPY_TICKERS[0],
            "ex_date": (TEST_DATE - timedelta(days=30)),
            "split_ratio": 2.0/1.0,
            "split_from": 1,
            "split_to": 2,
            "source": "test"
        },
        {
            "symbol": SPY_TICKERS[1],
            "ex_date": (TEST_DATE - timedelta(days=60)),
            "split_ratio": 3.0/2.0,
            "split_from": 2,
            "split_to": 3,
            "source": "test"
        }
    ],
    "dividends": [
        {
            "ex_date": (TEST_DATE - timedelta(days=15)),
            "symbol": SPY_TICKERS[0],
            "amount": 1.23
        },
        {
            "ex_date": (TEST_DATE - timedelta(days=45)),
            "symbol": SPY_TICKERS[1],
            "amount": 0.56
        }
    ]
}

if __name__ == "__main__":
    import sys
    import argparse
    parser = argparse.ArgumentParser(description="Test DB Manager CLI")
    parser.add_argument('--action', type=str, default=None)
    args = parser.parse_args()
    if args.action == "load_fixtures":
        print("Loading integration test fixtures...")
        asyncio.run(TestDatabaseManager("integration").load_test_fixtures(SAMPLE_FIXTURES))
        print("Fixtures loaded.")
