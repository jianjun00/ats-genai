"""
Shared Test Database Utilities.

Provides standardized, reusable test database setup patterns following CLAUDE.md principles:
- Real objects only (no mocks)
- Fail-fast validation
- Enhanced existing infrastructure
- Consolidated test patterns

Usage:
    @pytest.fixture
    async def test_db(isolated_test_db):
        yield isolated_test_db
        
    @pytest.fixture  
    async def shared_test_db(shared_integration_db):
        yield shared_integration_db
"""

import asyncio
import asyncpg
import gin
import os
import pytest
import pytest_asyncio
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse

from core.platform.config_env.environment import Environment, EnvironmentType
from core.shared.database import Database
from src.infrastructure.database.migration_manager import MigrationManager

import logging
logger = logging.getLogger(__name__)


class TestDatabaseError(Exception):
    """Raised when test database operations fail."""
    pass


@gin.configurable
class TestDatabaseManager:
    """
    Consolidated test database management following CLAUDE.md real objects principles.
    
    Provides two database types:
    1. Isolated: Fresh PostgreSQL instance per test (unit tests)
    2. Shared: Single database with cleanup between tests (integration tests)
    """
    
    def __init__(self, 
                 isolation_level: str = "isolated",
                 run_migrations: bool = True,
                 cleanup_strategy: str = "truncate"):
        """
        Initialize test database manager.
        
        Args:
            isolation_level: "isolated" (new DB per test) or "shared" (cleanup between tests)
            run_migrations: Whether to run database migrations after setup
            cleanup_strategy: "truncate" (fast) or "drop_recreate" (thorough)
        """
        self.isolation_level = isolation_level
        self.run_migrations = run_migrations
        self.cleanup_strategy = cleanup_strategy
        self._temp_dirs = []  # Track temporary directories for cleanup
        
    async def setup_database(self, test_name: Optional[str] = None) -> str:
        """
        Set up test database based on isolation level.
        
        Args:
            test_name: Optional test name for database naming
            
        Returns:
            Database URL for the test database
        """
        if self.isolation_level == "isolated":
            return await self._setup_isolated_database(test_name)
        elif self.isolation_level == "shared":
            return await self._setup_shared_database()
        else:
            raise TestDatabaseError(f"Unknown isolation level: {self.isolation_level}")
    
    async def cleanup_database(self, db_url: str):
        """Clean up database based on strategy."""
        if self.isolation_level == "isolated":
            await self._cleanup_isolated_database()
        elif self.isolation_level == "shared":
            await self._cleanup_shared_database(db_url)
    
    async def _setup_isolated_database(self, test_name: Optional[str] = None) -> str:
        """Create isolated PostgreSQL instance for single test."""
        # Create temporary directory for PostgreSQL instance
        temp_dir = tempfile.mkdtemp(prefix="postgres_test_")
        self._temp_dirs.append(temp_dir)
        logger.debug(f"Created temporary PostgreSQL directory: {temp_dir}")
        
        # Find available port
        db_port = self._find_free_port()
        db_name = "test_db"
        db_user = "test_user"
        
        logger.debug(f"Starting isolated PostgreSQL on port {db_port}")
        
        try:
            # PostgreSQL binary location
            pg_bin_dir = "/usr/lib/postgresql/16/bin"
            
            # Initialize database cluster
            init_cmd = [
                f"{pg_bin_dir}/initdb",
                "-D", temp_dir,
                "--auth-local=trust",
                "--auth-host=trust",
                "-U", db_user
            ]
            subprocess.run(init_cmd, check=True, capture_output=True, text=True)
            
            # Start PostgreSQL server
            pg_ctl_cmd = [
                f"{pg_bin_dir}/pg_ctl",
                "-D", temp_dir,
                "-o", f"-p {db_port} -k {temp_dir}",
                "-l", f"{temp_dir}/postgres.log",
                "start"
            ]
            subprocess.run(pg_ctl_cmd, check=True, capture_output=True, text=True)
            
            # Wait for server to be ready
            time.sleep(1)
            
            # Create test database
            postgres_url = f"postgresql://{db_user}:dummy@localhost:{db_port}/postgres"
            conn = await asyncpg.connect(postgres_url)
            await conn.execute(f'CREATE DATABASE "{db_name}"')
            await conn.close()
            
            # Construct final database URL
            test_db_url = f"postgresql://{db_user}:dummy@localhost:{db_port}/{db_name}"
            
            # Run migrations if requested
            if self.run_migrations:
                await self._run_migrations(test_db_url)
            
            logger.debug(f"Isolated PostgreSQL ready at: {test_db_url}")
            return test_db_url
            
        except Exception as e:
            # Cleanup on failure
            await self._cleanup_isolated_database()
            raise TestDatabaseError(f"Failed to setup isolated database: {e}")
    
    async def _setup_shared_database(self) -> str:
        """Set up shared integration test database."""
        # Use integration database configuration
        db_url = os.getenv("TEST_DATABASE_URL", "postgresql://test_user:test_password@localhost:5432/test_db")
        
        # Ensure database exists and is migrated
        if self.run_migrations:
            await self._run_migrations(db_url)
        
        logger.debug(f"Using shared database: {db_url}")
        return db_url
    
    async def _cleanup_isolated_database(self):
        """Clean up isolated PostgreSQL instances."""
        for temp_dir in self._temp_dirs:
            try:
                # Stop PostgreSQL server
                pg_bin_dir = "/usr/lib/postgresql/16/bin"
                stop_cmd = [f"{pg_bin_dir}/pg_ctl", "-D", temp_dir, "stop", "-m", "immediate"]
                subprocess.run(stop_cmd, capture_output=True, text=True)
                
                # Remove temporary directory
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up PostgreSQL instance: {temp_dir}")
            except Exception as e:
                logger.warning(f"Error cleaning up PostgreSQL instance {temp_dir}: {e}")
        
        self._temp_dirs.clear()
    
    async def _cleanup_shared_database(self, db_url: str):
        """Clean up shared database using strategy."""
        try:
            if self.cleanup_strategy == "truncate":
                await self._truncate_tables(db_url)
            elif self.cleanup_strategy == "drop_recreate":
                await self._recreate_database(db_url)
        except Exception as e:
            logger.warning(f"Error cleaning up shared database: {e}")
    
    async def _truncate_tables(self, db_url: str):
        """Truncate all tables in database."""
        conn = await asyncpg.connect(db_url)
        
        # Get all user tables
        tables = await conn.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE '_%'
        """)
        
        # Truncate all tables
        for table in tables:
            table_name = table['table_name']
            await conn.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')
        
        await conn.close()
    
    async def _recreate_database(self, db_url: str):
        """Drop and recreate database."""
        url_parts = urlparse(db_url)
        db_name = url_parts.path[1:]  # Remove leading slash
        
        # Connect to postgres database to recreate test database
        postgres_url = db_url.replace(f"/{db_name}", "/postgres")
        conn = await asyncpg.connect(postgres_url)
        
        await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        await conn.execute(f'CREATE DATABASE "{db_name}"')
        await conn.close()
        
        # Run migrations on new database
        if self.run_migrations:
            await self._run_migrations(db_url)
    
    async def _run_migrations(self, db_url: str):
        """Run database migrations."""
        migration_manager = MigrationManager(db_url)
        success = await migration_manager.migrate_to_latest()
        if not success:
            raise TestDatabaseError("Failed to run database migrations")
    
    def _find_free_port(self) -> int:
        """Find an available port for PostgreSQL."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port


# Pytest fixtures using the consolidated manager

@pytest_asyncio.fixture
async def isolated_test_db(request):
    """
    Provides isolated PostgreSQL instance per test.
    Best for unit tests requiring complete database isolation.
    """
    test_name = request.node.name if hasattr(request, 'node') else None
    manager = TestDatabaseManager(isolation_level="isolated")
    
    db_url = await manager.setup_database(test_name)
    yield db_url
    await manager.cleanup_database(db_url)


@pytest_asyncio.fixture(scope="session")
async def shared_integration_db():
    """
    Provides shared database for integration tests.
    Database persists for session but is cleaned between tests.
    """
    manager = TestDatabaseManager(isolation_level="shared")
    db_url = await manager.setup_database()
    yield db_url
    await manager.cleanup_database(db_url)


@pytest_asyncio.fixture  
async def clean_shared_db(shared_integration_db):
    """
    Provides shared database with cleanup before each test.
    Use this for integration tests that need fresh state.
    """
    manager = TestDatabaseManager(isolation_level="shared", cleanup_strategy="truncate")
    await manager._cleanup_shared_database(shared_integration_db)
    yield shared_integration_db


@asynccontextmanager
async def test_database_context(isolation_level: str = "isolated", run_migrations: bool = True):
    """
    Context manager for test database setup/teardown.
    
    Usage:
        async with test_database_context("isolated") as db_url:
            # Use db_url for testing
    """
    manager = TestDatabaseManager(isolation_level=isolation_level, run_migrations=run_migrations)
    db_url = await manager.setup_database()
    try:
        yield db_url
    finally:
        await manager.cleanup_database(db_url)


async def load_test_fixtures(db_url: str, fixtures: Dict[str, List[Dict[str, Any]]]):
    """
    Load test data fixtures into database.
    
    Args:
        db_url: Database connection URL
        fixtures: Dict mapping table names to lists of row data
    """
    from dateutil import parser as date_parser
    
    conn = await asyncpg.connect(db_url)
    
    for table, rows in fixtures.items():
        if not rows:
            continue
        
        # Process rows to convert string dates to Python objects
        processed_rows = []
        for row in rows:
            processed_row = {}
            for key, value in row.items():
                # Convert string dates/times to proper Python objects
                if isinstance(value, str) and any(suffix in key for suffix in ['_at', '_date', '_time']):
                    try:
                        dt = date_parser.parse(value)
                        processed_row[key] = dt.date() if 'date' in key else dt
                    except:
                        processed_row[key] = value
                else:
                    processed_row[key] = value
            processed_rows.append(processed_row)
        
        # Insert rows
        if processed_rows:
            columns = list(processed_rows[0].keys())
            for row in processed_rows:
                placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
                columns_str = ', '.join(f'"{col}"' for col in columns)
                values = [row[col] for col in columns]
                
                # Use ON CONFLICT DO NOTHING to avoid duplicate key errors
                await conn.execute(
                    f'INSERT INTO "{table}" ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                    *values
                )
    
    await conn.close()


# Common test fixtures
STANDARD_TEST_FIXTURES = {
    "test_instrument": [
        {
            "id": 363996,
            "symbol": "AAPL", 
            "name": "Apple Inc",
            "exchange": "NASDAQ",
            "active": True
        },
        {
            "id": 465844,
            "symbol": "TSLA",
            "name": "Tesla Inc", 
            "exchange": "NASDAQ",
            "active": True
        },
        {
            "id": 123456,
            "symbol": "MSFT",
            "name": "Microsoft Corp", 
            "exchange": "NASDAQ",
            "active": True
        }
    ],
    "test_universe": [
        {
            "id": 1,
            "name": "Test Universe",
            "description": "Universe for testing"
        }
    ],
    "test_instrument_xrefs": [
        {
            "id": 1,
            "instrument_id": 363996,
            "vendor_id": 4,
            "symbol": "AAPL",
            "type": "ticker",
            "active": True
        },
        {
            "id": 2, 
            "instrument_id": 465844,
            "vendor_id": 4,
            "symbol": "TSLA", 
            "type": "ticker",
            "active": True
        },
        {
            "id": 3,
            "instrument_id": 123456,
            "vendor_id": 4,
            "symbol": "MSFT",
            "type": "ticker", 
            "active": True
        }
    ]
}