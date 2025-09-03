import os
import pytest
import asyncpg
from dotenv import load_dotenv
from shared.utils.environment import Environment, EnvironmentType

load_dotenv()

# Create a test environment to get the correct table names
env = Environment(EnvironmentType.TEST)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_instruments_table_schema(unit_test_db):
    # Use the test database URL from the fixture
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        # Get the environment-aware table name
        table_name = env.get_table_name('instruments')
        
        # Check columns
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = $1
        """, table_name)
        
        col_names = {row['column_name'] for row in cols}
        print(f"[DEBUG] Columns in {table_name}: {col_names}")
        
        # Check for 'id' instead of 'instrument_id' as that's what the schema uses
        assert 'id' in col_names, f"'id' column not found in {table_name}"
        assert 'symbol' in col_names, f"'symbol' column not found in {table_name}"
        assert 'exchange' in col_names, f"'exchange' column not found in {table_name}"
        assert 'figi' in col_names, f"'figi' column not found in {table_name}"
        assert 'active' in col_names, f"'active' column not found in {table_name}"
        assert 'created_at' in col_names, f"'created_at' column not found in {table_name}"
        # Check unique constraint
        res = await conn.fetch("""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_name = $1 AND constraint_type = 'UNIQUE'
        """, table_name)
        unique_constraint_count = res[0]['count']
        print(f"[DEBUG] Found {unique_constraint_count} unique constraints on {table_name}")
        assert unique_constraint_count >= 1, f"Expected at least 1 unique constraint on {table_name}, found {unique_constraint_count}"
    await pool.close()

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_insert_and_query_instrument(unit_test_db):
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        table_name = env.get_table_name('instruments')
        
        # Clean up any existing test data
        await conn.execute(f"DELETE FROM {table_name} WHERE symbol = 'TESTXYZ'")
        
        # Insert a test instrument
        await conn.execute(f"""
            INSERT INTO {table_name} (symbol, exchange, name, type, currency, figi, isin, cusip, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, 'TESTXYZ', 'NASDAQ', 'Test Instrument', 'Equity', 'USD', 'BBG000TEST', 'US000000TEST', '000000TEST', True)
        
        # Query the inserted instrument
        row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE symbol = 'TESTXYZ'")
        assert row is not None, "Failed to insert test instrument"
        assert row['symbol'] == 'TESTXYZ', f"Expected symbol 'TESTXYZ', got {row['symbol']}"
        assert row['exchange'] == 'NASDAQ', f"Expected exchange 'NASDAQ', got {row['exchange']}"
        assert row['active'] is True, f"Expected active=True, got {row['active']}"
        
        # Clean up
        await conn.execute(f"DELETE FROM {table_name} WHERE symbol = 'TESTXYZ'")
    await pool.close()

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_instrument_aliases_and_metadata(unit_test_db):
    """Test instrument insertion and updates with the current schema.
    
    Note: The instrument_aliases and instrument_metadata tables were removed in migration 016.
    This test now focuses on testing the core instrument functionality.
    """
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        table_name = env.get_table_name('instruments')
        
        # Clean up any existing test data
        await conn.execute(f"DELETE FROM {table_name} WHERE symbol = 'TEST123'")
        
        # Test instrument insertion
        await conn.execute(f"""
            INSERT INTO {table_name} (symbol, exchange, name, type, currency, figi, isin, cusip, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, 'TEST123', 'NYSE', 'Test Instrument', 'Equity', 'USD', 'BBG001', 'US000001', '000001', True)
        
        # Test metadata updates
        await conn.execute(f"""
            UPDATE {table_name} 
            SET name = 'Updated Name', active = FALSE 
            WHERE symbol = 'TEST123'
        """)
        
        # Verify updates
        row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE symbol = 'TEST123'")
        assert row is not None, "Failed to insert test instrument for metadata test"
        assert row['name'] == 'Updated Name', f"Expected name 'Updated Name', got {row['name']}"
        assert row['active'] is False, f"Expected active=False, got {row['active']}"
        
        # Clean up
        await conn.execute(f"DELETE FROM {table_name} WHERE symbol = 'TEST123'")
    await pool.close()
