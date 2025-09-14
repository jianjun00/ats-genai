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
import os
import pytest
import pytest_asyncio
import uuid
from typing import Dict, List, Any
from contextlib import asynccontextmanager
from shared.data_handling.utils.environment import Environment, EnvironmentType
from infrastructure.database.migration_manager import MigrationManager
import logging
logger = logging.getLogger(__name__)
from dateutil import parser as date_parser
import gin
from shared.data_handling.utils.database import Database

@gin.configurable
class DatabaseTestManager:
    """Manages test databases for unit and integration tests."""


    def __init__(self, test_type: str = "unit", run_migrations: bool = True, database_obj: Database = None):
        print(f"[GIN DEBUG] DatabaseTestManager __init__: test_type={test_type}, database_obj={database_obj}")
        """
        Initialize test database manager.

        Args:
            test_type: "unit" or "integration"
            run_migrations: If False, do not apply migrations after DB creation (default: True)
            database_obj: Optional, a Database object to use as the test database (used for DB name override)
        """
        # Use the appropriate environment based on test type
        logger.debug(f"TestDatabaseManager: test_type={test_type}, run_migrations={run_migrations}, database_obj={database_obj}")
        self.test_type = test_type
        if database_obj:
            # Override DB name in URL
            url = database_obj.get_database_url()
            import re
            # Replace last path segment with database_name
            self.db_url = re.sub(r"(?<=/)[^/]+$", database_obj.database, url)
            print(f"[GIN DEBUG] TestDatabaseManager using database_name override: {database_obj.database} → db_url={self.db_url}")
        else:
            self.db_url = None  # Will be set below
        if test_type == "integration":
            if self.db_url is None:
                # Default integration DB URL
                from intg_tests.db.test_intg_db_base_intg import get_test_db_url
                self.db_url = get_test_db_url()
            self.env = Environment(EnvironmentType.INTEGRATION, db_url=self.db_url)
        else:
            if self.db_url is None:
                raise ValueError("TestDatabaseManager requires database_obj for unit tests to supply db_url.")
            self.env = Environment(env_type=EnvironmentType.TEST, db_url=self.db_url)
        # Extract table prefix from environment
        sample_table = self.env.get_table_name("sample")
        self.table_prefix = sample_table.replace("sample", "")
        self.run_migrations = run_migrations
        # test_db_suffix logic moved to fixture


    async def setup_test_database(self) -> str:
        """
        Set up test database with latest schema (unless run_migrations is False).

        Returns:
            Database URL for the test database
        """
        if self.test_type == "unit":
            # Create isolated database for unit test
            db_config = self.env.get_database_config()
            test_db_name = db_config['database']
            print(f"[DEBUG] Creating test DB: {test_db_name}")
            await self._create_test_database(test_db_name)
            test_db_url = self.db_url
            print(f"[DEBUG] Using test DB URL: {test_db_url}, test_db_name: {test_db_name}")
            # Run migrations after DB creation
            if self.run_migrations:
                print(f"[DEBUG] Running migrations on test DB: {test_db_name}")
                # Create migration manager with explicit table prefix
                migration_manager = MigrationManager(self.db_url)
                # Ensure we're using the correct table prefix for test environment
                migration_manager.table_prefix = self.table_prefix
                print(f"[DEBUG] Running migrations with table prefix: '{self.table_prefix}'")
                success = await migration_manager.migrate_to_latest()
                if not success:
                    raise RuntimeError("Failed to apply migrations to test database")
        else:
            # For integration tests, we use a shared database
            test_db_url = self.db_url
            print(f"[DEBUG] Using shared integration DB URL: {test_db_url}")
        return test_db_url


    async def teardown_test_database(self):
        """Clean up test database after test completion."""
        if self.test_type == "unit":
            # Drop isolated unit test database
            db_config = self.env.get_database_config()
            test_db_name = db_config['database']
            print(f"[DEBUG] Dropping test DB: {test_db_name}")
            await self._drop_test_database(test_db_name)
        else:
            # For integration tests, we clean up test data but keep the database
            await self.cleanup_test_data()


    async def _create_test_database(self, db_name: str):
        """Create a new test database."""
        # Connect to default postgres database to create test database
        try:
            # Extract connection parameters from the database URL
            from urllib.parse import urlparse
            url_parts = urlparse(self.db_url)
            host = url_parts.hostname or "localhost"
            port = url_parts.port or 5432
            user = url_parts.username or "postgres"
            password = url_parts.password or "password"

            # Connect to postgres database
            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database="postgres"
            )
            # Check if database already exists
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                db_name
            )
            if exists:
                # Drop existing database
                await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            # Create new database
            await conn.execute(f'CREATE DATABASE "{db_name}"')
            await conn.close()
        except Exception as e:
            print(f"[ERROR] Failed to create test database: {e}")
            raise


    async def _drop_test_database(self, db_name: str):
        """Drop a test database."""
        try:
            # Extract connection parameters from the database URL
            from urllib.parse import urlparse
            url_parts = urlparse(self.db_url)
            host = url_parts.hostname or "localhost"
            port = url_parts.port or 5432
            user = url_parts.username or "postgres"
            password = url_parts.password or "password"

            # Connect to postgres database
            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database="postgres"
            )
            # Drop database
            await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            await conn.close()
        except Exception as e:
            print(f"[ERROR] Failed to drop test database: {e}")
            # Don't raise, as this is cleanup code


    async def cleanup_test_data(self):
        """Clean up test data from integration test database."""
        try:
            # Connect to the integration test database
            conn = await asyncpg.connect(self.db_url)

            # Get all tables in the database
            tables = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
            """)

            # Truncate all tables (except migration tables)
            for table in tables:
                table_name = table['table_name']
                if not table_name.startswith('_'):  # Skip migration tables
                    try:
                        await conn.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
                    except Exception as e:
                        print(f"[WARNING] Failed to truncate {table_name}: {e}")

            await conn.close()
        except Exception as e:
            print(f"[ERROR] Failed to clean up test data: {e}")
            # Don't raise, as this is cleanup code


    async def backup_tables(self, tables: List[str]):
        """Backup tables by copying their contents to *_backup tables."""
        conn = await asyncpg.connect(self.db_url)
        for table in tables:
            backup_table = f"{table}_backup"
            try:
                # Drop backup table if it exists
                await conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')
                # Create backup table with same structure
                await conn.execute(f'CREATE TABLE "{backup_table}" AS SELECT * FROM "{table}"')
            except Exception as e:
                print(f"[ERROR] Failed to backup table {table}: {e}")
        await conn.close()


    async def restore_tables(self, tables: List[str]):
        """Restore tables from their *_backup tables and drop the backups."""
        conn = await asyncpg.connect(self.db_url)
        for table in tables:
            backup_table = f"{table}_backup"
            try:
                # Truncate original table
                await conn.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                # Copy data from backup
                await conn.execute(f'INSERT INTO "{table}" SELECT * FROM "{backup_table}"')
                # Drop backup table
                await conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')
            except Exception as e:
                print(f"[ERROR] Failed to restore table {table}: {e}")
        await conn.close()


    async def load_test_fixtures(self, fixtures: Dict[str, List[Dict[str, Any]]]):
        """
        Load test data fixtures into the database.
        Converts string date/datetime fields to Python objects for asyncpg compatibility.
        Args:
            fixtures: Dictionary mapping table names to lists of row data
        """
        conn = await asyncpg.connect(self.db_url)

        for table, rows in fixtures.items():
            if not rows:
                continue

            # Process each row to convert string dates to Python objects
            processed_rows = []
            for row in rows:
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, str) and (
                        key.endswith('_at') or
                        key.endswith('_date') or
                        key == 'date' or
                        key == 'datetime'
                    ):
                        try:
                            # Try to parse as date/datetime
                            dt = date_parser.parse(value)
                            if any(x in key for x in ['date', 'day']):
                                # Use date object for date fields
                                processed_row[key] = dt.date()
                            else:
                                # Use datetime for timestamp fields
                                processed_row[key] = dt
                        except Exception:
                            # If parsing fails, keep original value
                            processed_row[key] = value
                    else:
                        processed_row[key] = value
                processed_rows.append(processed_row)

            # Insert rows into table
            columns = list(processed_rows[0].keys())
            values = [tuple(row[col] for col in columns) for row in processed_rows]

            # Build INSERT statement
            placeholders = [f'${i+1}' for i in range(len(columns))]
            columns_str = ', '.join(f'"{col}"' for col in columns)
            placeholders_str = ', '.join(placeholders)

            # Execute INSERT for each row
            for row_values in values:
                try:
                    await conn.execute(
                        f'INSERT INTO "{table}" ({columns_str}) VALUES ({placeholders_str})',
                        *row_values
                    )
                except Exception as e:
                    print(f"[ERROR] Failed to insert into {table}: {e}")

        await conn.close()


class IntegrationTestSession:
    """Manages shared state for integration test sessions."""

    _instance = None
    _db_manager = None
    _test_db_url = None
    _session_id = None

    @classmethod
    def get_instance(cls):
        """Get or create the singleton integration test session."""
        if cls._instance is None:
            cls._instance = IntegrationTestSession()
            cls._db_manager = TestDatabaseManager("integration")
            cls._test_db_url = asyncio.run(cls._db_manager.setup_test_database())
            cls._session_id = uuid.uuid4().hex
        return cls._instance

    @classmethod
    def cleanup(cls):
        """Clean up the integration test session."""
        if cls._instance is not None:
            if cls._db_manager is not None:
                asyncio.run(cls._db_manager.cleanup_test_data())
            cls._instance = None
            cls._db_manager = None
            cls._test_db_url = None

    def db_url(self):
        """Get the test database URL."""
        return self._test_db_url

    def session_id(self):
        """Get the session ID for this test run."""
        return self._session_id


@pytest.fixture(scope="session")
def integration_test_db():
    """Fixture for integration tests - provides shared database for session."""
    session = IntegrationTestSession.get_instance()
    yield session.db_url()
    # Cleanup happens at end of session
    IntegrationTestSession.cleanup()


@pytest.fixture
def clean_integration_db(integration_test_db):
    """Fixture that cleans integration database before each test."""
    IntegrationTestSession.get_instance()
    db_manager = TestDatabaseManager("integration")
    asyncio.run(db_manager.cleanup_test_data())
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
    db_url = await db_manager.setup_test_database()
    try:
        yield db_url
    finally:
        await db_manager.teardown_test_database()


# Example test fixtures for common test data

import pytest_asyncio
import pytest
import asyncpg
from pathlib import Path
from infrastructure.database.migration_manager import MigrationManager
from shared.data_handling.utils.environment import Environment, EnvironmentType

@pytest_asyncio.fixture
async def unit_test_db(request):
    """
    Fixture for a completely empty unit test database (no tables, no migrations applied).
    Ensures environment is TEST. Used for DAO and non-migration tests.
    Generates a unique test DB name per test with timestamp for proper isolation.
    """
    import gin
    import uuid
    from shared.data_handling.utils.database import Database
    test_file = str(request.fspath) if hasattr(request, 'fspath') else "nofile"
    # Take only first 8 chars of file name to keep DB name short
    test_file_base = os.path.splitext(os.path.basename(test_file))[0]
    test_file_base = ''.join(c for c in test_file_base if c.isalnum())[:8]
    test_name = request.node.name if hasattr(request, 'node') else None

    # Generate timestamp-based unique suffix for proper test isolation
    import time
    timestamp_suffix = str(int(time.time() * 1000))[-10:]  # Last 10 digits of millisecond timestamp

    if test_name:
        # Use first 8 chars of test name for readability
        test_name_short = ''.join(c for c in test_name if c.isalnum())[:8]
        db_name = f"test_{test_file_base}_{test_name_short}_{timestamp_suffix}"
    else:
        db_name = f"test_{test_file_base}_{uuid.uuid4().hex[:8]}_{timestamp_suffix}"

    # Ensure DB name doesn't exceed PostgreSQL's 63-char limit
    if len(db_name) > 63:
        db_name = f"test_{timestamp_suffix}_{uuid.uuid4().hex[:8]}"

    # Construct Database object for this test
    # Use test_user credentials explicitly for test database - TimescaleDB instance
    db_host = "localhost"
    db_port = 5433  # Changed to use our TimescaleDB instance with TimescaleDB extension
    db_user = "test_user"
    db_password = "test_password"
    db_pool_min = 1
    db_pool_max = 10
    db_cmd_timeout = 60

    print(f"[ENV DEBUG] Using database credentials: host={db_host}, port={db_port}, user={db_user}, database={db_name}")

    db_obj = Database(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
        base_database=db_name,
        pool_min_size=db_pool_min,
        pool_max_size=db_pool_max,
        command_timeout=db_cmd_timeout
    )
    db_manager = gin.get_configurable(DatabaseTestManager)("unit", database_obj=db_obj)
    test_db_url = await db_manager.setup_test_database()

    # Get the table prefix from the db_manager's env
    table_prefix = db_manager.table_prefix
    print(f"[DEBUG] Using table prefix from db_manager: '{table_prefix}'")

    # Create migration manager with the correct table prefix
    migration_manager = MigrationManager(test_db_url)
    migration_manager.table_prefix = table_prefix  # Ensure we use the same prefix
    print(f"[DEBUG] Applying migrations to {test_db_url} with table prefix: '{table_prefix}'")

    # Run migrations with the correct prefix
    success = await migration_manager.migrate_to_latest()
    if not success:
        raise RuntimeError("Failed to apply migrations to test database")

    # Verify tables were created with the correct prefix
    print(f"[DEBUG] Verifying tables with prefix: '{table_prefix}'")
    conn = await asyncpg.connect(test_db_url)
    try:
        tables = await conn.fetch("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        print(f"[DEBUG] Tables in database after migration: {[t['tablename'] for t in tables]}")
    finally:
        await conn.close()

    yield test_db_url
    await db_manager.teardown_test_database()
    # Clean up backup/dump files created for this test DB
    backup_dir = Path(__file__).parent / "../db/migrations/backups"
    if backup_dir.exists():
        pattern = f"{db_name}_*.dump"
        for dump_file in backup_dir.glob(pattern):
            try:
                dump_file.unlink()
            except Exception:
                pass

@pytest_asyncio.fixture
async def unit_test_db_clean(request):
    """
    Fixture for a completely empty unit test database (no tables).
    Ensures environment is TEST. Used for DB migration and isolation tests.
    Generates a unique test DB name per test with timestamp for proper isolation.
    """
    import gin
    import uuid
    from shared.data_handling.utils.database import Database
    test_file = str(request.fspath) if hasattr(request, 'fspath') else "nofile"
    # Take only first 8 chars of file name to keep DB name short
    test_file_base = os.path.splitext(os.path.basename(test_file))[0]
    test_file_base = ''.join(c for c in test_file_base if c.isalnum())[:8]
    test_name = request.node.name if hasattr(request, 'node') else None

    # Generate timestamp-based unique suffix for proper test isolation
    import time
    timestamp_suffix = str(int(time.time() * 1000))[-10:]  # Last 10 digits of millisecond timestamp

    if test_name:
        # Use first 8 chars of test name for readability
        test_name_short = ''.join(c for c in test_name if c.isalnum())[:8]
        db_name = f"test_{test_file_base}_{test_name_short}_{timestamp_suffix}"
    else:
        db_name = f"test_{test_file_base}_{uuid.uuid4().hex[:8]}_{timestamp_suffix}"

    # Ensure DB name doesn't exceed PostgreSQL's 63-char limit
    if len(db_name) > 63:
        db_name = f"test_{timestamp_suffix}_{uuid.uuid4().hex[:8]}"
    # Construct Database object for this test
    # Use test_user credentials explicitly for test database
    db_host = "localhost"
    db_port = 5432
    db_user = "test_user"
    db_password = "test_password"
    db_pool_min = 1
    db_pool_max = 10
    db_cmd_timeout = 60

    print(f"[ENV DEBUG] Using database credentials: host={db_host}, port={db_port}, user={db_user}, database={db_name}")

    db_obj = Database(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
        base_database=db_name,
        pool_min_size=db_pool_min,
        pool_max_size=db_pool_max,
        command_timeout=db_cmd_timeout
    )
    db_manager = gin.get_configurable(DatabaseTestManager)("unit", database_obj=db_obj)
    test_db_url = await db_manager.setup_test_database()

    # Drop all tables for a clean DB
    import asyncpg
    conn = await asyncpg.connect(test_db_url)
    tables = await conn.fetch("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE';
    """)
    for row in tables:
        await conn.execute(f'DROP TABLE IF EXISTS "{row["table_name"]}" CASCADE;')
    await conn.close()

    yield test_db_url
    await db_manager.teardown_test_database()
    # Clean up backup/dump files created for this test DB
    backup_dir = Path(__file__).parent / "../db/migrations/backups"
    if backup_dir.exists():
        pattern = f"{db_name}_*.dump"
        for dump_file in backup_dir.glob(pattern):
            try:
                dump_file.unlink()
            except Exception:
                pass

@pytest.fixture(scope="session")
def test_env():
    """Fixture to provide a test Environment instance with universe_id set."""
    env = Environment(EnvironmentType.TEST)
    return env

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
        {
            "symbol": "TEST",
            "name": "Test Stock",
            "exchange": "NYSE",
            "type": "stock",
            "currency": "USD",
            "active": True
        }
    ],
    "daily_prices": [
        {
            "date": "2024-01-01",
            "symbol": "TEST",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 103.0,
            "volume": 1000000
        }
    ]
}
