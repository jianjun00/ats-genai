"""
Example test showing how to use the shared test database utilities.

This demonstrates the consolidated test database patterns following CLAUDE.md principles.
"""

import pytest
import asyncpg
from core.shared.utils_core.test_database import (
    isolated_test_db,
    shared_integration_db,
    clean_shared_db,
    test_database_context,
    load_test_fixtures,
    STANDARD_TEST_FIXTURES
)


class TestSharedDatabaseExample:
    """Examples of using consolidated test database utilities."""
    
    @pytest.mark.asyncio
    async def test_isolated_database_example(self, isolated_test_db):
        """
        Example: Isolated PostgreSQL instance per test.
        Best for unit tests requiring complete database isolation.
        """
        # Each test gets its own PostgreSQL instance
        conn = await asyncpg.connect(isolated_test_db)
        
        # Verify database is empty and ready
        tables = await conn.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        
        # Should have migrated tables available
        table_names = [t['table_name'] for t in tables]
        assert 'test_instrument' in table_names  # Singular form from migrations
        assert 'test_universe' in table_names   # Singular form from migrations
        
        await conn.close()
    
    @pytest.mark.asyncio
    async def test_shared_database_example(self, clean_shared_db):
        """
        Example: Shared database with cleanup between tests.
        Best for integration tests that don't need full isolation.
        """
        # Database is cleaned before each test but shared between tests
        conn = await asyncpg.connect(clean_shared_db)
        
        # Load some test data
        await load_test_fixtures(clean_shared_db, STANDARD_TEST_FIXTURES)
        
        # Verify data was loaded
        instruments = await conn.fetch("SELECT * FROM test_instrument")
        assert len(instruments) == 2
        assert instruments[0]['symbol'] == 'AAPL'
        assert instruments[1]['symbol'] == 'TSLA'
        
        await conn.close()
    
    @pytest.mark.asyncio
    async def test_context_manager_example(self):
        """
        Example: Using context manager for temporary database.
        Best for tests that need custom database configuration.
        """
        async with test_database_context("isolated", run_migrations=True) as db_url:
            conn = await asyncpg.connect(db_url)
            
            # Custom test logic with temporary database
            result = await conn.fetchval("SELECT 1")
            assert result == 1
            
            await conn.close()
        # Database is automatically cleaned up here
    
    @pytest.mark.asyncio  
    async def test_fixture_loading_example(self, isolated_test_db):
        """
        Example: Loading custom test fixtures.
        Shows how to populate database with test data.
        """
        # Define custom test fixtures
        custom_fixtures = {
            "test_instrument": [
                {
                    "instrument_id": 999,
                    "symbol": "TEST",
                    "name": "Test Stock",
                    "exchange": "NASDAQ",
                    "active": True
                }
            ],
            "test_universe": [
                {
                    "universe_id": 99,
                    "name": "Test Universe",
                    "description": "Custom test universe"
                }
            ]
        }
        
        # Load fixtures into database
        await load_test_fixtures(isolated_test_db, custom_fixtures)
        
        # Verify fixtures were loaded
        conn = await asyncpg.connect(isolated_test_db)
        
        instrument = await conn.fetchrow(
            "SELECT * FROM test_instrument WHERE symbol = $1", "TEST"
        )
        assert instrument['instrument_id'] == 999
        assert instrument['name'] == "Test Stock"
        
        universe = await conn.fetchrow(
            "SELECT * FROM test_universe WHERE universe_id = $1", 99  
        )
        assert universe['name'] == "Test Universe"
        
        await conn.close()