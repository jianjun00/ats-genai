"""
Integration tests for database operations.
These tests share a common database for the entire test session.
"""

import pytest
import asyncio
from src.db.test_db_manager import integration_test_db, clean_integration_db, SAMPLE_FIXTURES
from src.db.test_db_manager import TestDatabaseManager


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_shared_database_setup(integration_test_db):
    """Test that integration database is properly set up and shared."""
    import asyncpg
    
    pool = await asyncpg.create_pool(integration_test_db)
    try:
        async with pool.acquire() as conn:
            # Verify that tables exist with correct prefix
            result = await conn.fetchval("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name LIKE 'intg_%'
            """)
            assert result > 0
            
            # Verify db_version table exists and has records
            version = await conn.fetchval("""
                SELECT MAX(version) FROM intg_db_version
            """)
            assert version is not None
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_test_fixtures_loading(clean_integration_db):
    """Test loading test fixtures into integration database."""
    db_manager = TestDatabaseManager("integration")
    
    # Load sample fixtures
    await db_manager.load_test_fixtures(SAMPLE_FIXTURES)
    
    # Verify data was loaded
    import asyncpg
    pool = await asyncpg.create_pool(clean_integration_db)
    try:
        async with pool.acquire() as conn:
            # Check vendors
            vendor_count = await conn.fetchval("SELECT COUNT(*) FROM intg_vendors")
            assert vendor_count == 1
            
            # Check instruments
            instrument_count = await conn.fetchval("SELECT COUNT(*) FROM intg_instruments")
            assert instrument_count == 1
            
            # Check daily prices
            price_count = await conn.fetchval("SELECT COUNT(*) FROM intg_daily_prices")
            assert price_count == 1
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_data_isolation_between_tests(clean_integration_db):
    """Test that data is cleaned between integration tests."""
    import asyncpg
    
    pool = await asyncpg.create_pool(clean_integration_db)
    try:
        async with pool.acquire() as conn:
            # Insert test data
            await conn.execute("""
                INSERT INTO intg_vendors (name, description) 
                VALUES ('test_isolation', 'Test isolation vendor')
            """)
            
            # Verify data exists
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_vendors WHERE name = 'test_isolation'
            """)
            assert count == 1
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_data_cleaned_from_previous_test(clean_integration_db):
    """Test that data from previous test is cleaned up."""
    import asyncpg
    
    pool = await asyncpg.create_pool(clean_integration_db)
    try:
        async with pool.acquire() as conn:
            # Check that test data from previous test is gone
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_vendors WHERE name = 'test_isolation'
            """)
            assert count == 0
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.slow
@pytest.mark.asyncio
async def test_cross_table_relationships(clean_integration_db):
    """Test complex operations across multiple tables."""
    db_manager = TestDatabaseManager("integration")
    
    # Load fixtures with relationships
    fixtures = {
        "vendors": [
            {"name": "test_vendor", "description": "Test vendor"}
        ],
        "instruments": [
            {"symbol": "TEST1", "name": "Test Stock 1", "exchange": "NYSE", "type": "stock", "currency": "USD", "active": True},
            {"symbol": "TEST2", "name": "Test Stock 2", "exchange": "NASDAQ", "type": "stock", "currency": "USD", "active": True}
        ],
        "universe": [
            {"name": "test_universe", "description": "Test universe"}
        ]
    }
    
    await db_manager.load_test_fixtures(fixtures)
    
    import asyncpg
    pool = await asyncpg.create_pool(clean_integration_db)
    try:
        async with pool.acquire() as conn:
            # Get universe ID
            universe_id = await conn.fetchval("""
                SELECT id FROM intg_universe WHERE name = 'test_universe'
            """)
            assert universe_id is not None
            
            # Add universe membership
            await conn.execute("""
                INSERT INTO intg_universe_membership (universe_id, symbol, start_at)
                VALUES ($1, 'TEST1', '2024-01-01'), ($1, 'TEST2', '2024-01-01')
            """, universe_id)
            
            # Verify relationships
            member_count = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_universe_membership WHERE universe_id = $1
            """, universe_id)
            assert member_count == 2
    finally:
        await pool.close()
