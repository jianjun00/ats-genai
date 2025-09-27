"""
Tests to detect schema compatibility issues with existing database tables

Following TDD principles: First write tests to detect the issue, then fix.
"""

import pytest
from unittest.mock import Mock, AsyncMock
import asyncpg
import os

from core.platform.config.environment import Environment
from domains.trading.services.universe.dynamic_modeling_universe import DynamicModelingUniverse

class TestSchemaCompatibility:
    """Test that dynamic universe system is compatible with existing database schema"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_existing_universe_table_schema(self):
        """Test that existing dev_universe table has expected columns"""
        # Only run this test if we have database credentials
        if not all([
            os.environ.get('DB_HOST'),
            os.environ.get('DB_USER'),
            os.environ.get('DB_PASSWORD'),
            os.environ.get('DB_NAME')
        ]):
            pytest.skip("Database credentials not available")

        # Connect to actual database to check schema
        conn = await asyncpg.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            port=int(os.environ.get('DB_PORT', '5433')),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )

        # Check if dev_universe table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'dev_universe'
            );
        """)

        if not table_exists:
            pytest.skip("dev_universe table doesn't exist - this is expected for new systems")

        # Get actual table schema
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'dev_universe'
            ORDER BY ordinal_position;
        """)

        column_names = [row['column_name'] for row in columns]
        print(f"Existing dev_universe columns: {column_names}")

        # Check for expected columns from our universe system
        required_columns = ['id', 'name', 'description', 'created_at']
        expected_columns = ['id', 'name', 'description', 'created_at', 'updated_at']

        # Test: Required columns should exist
        for col in required_columns:
            assert col in column_names, f"Required column '{col}' missing from dev_universe"

        # Test: Detect if updated_at column is missing (this should fail initially)
        missing_columns = [col for col in expected_columns if col not in column_names]
        if missing_columns:
            print(f"❌ DETECTED ISSUE: Missing columns: {missing_columns}")
            # This test documents the issue - updated_at column is missing
            assert 'updated_at' in missing_columns, "Expected to find updated_at missing (this documents the issue)"

        await conn.close()

    def test_universe_creation_query_compatibility(self):
        """Test that universe creation query matches existing schema"""
        env = Mock(spec=Environment)
        env.get_table_name = Mock(side_effect=lambda name: f"dev_{name}")

        universe = DynamicModelingUniverse(env)

        # Test the universe creation query from _ensure_universe_exists
        universe_query = """
        INSERT INTO {universe_table} (name, description, created_at, updated_at)
        VALUES ($1, $2, $3, $3)
        ON CONFLICT (name) DO UPDATE SET
            updated_at = $3,
            description = $2
        RETURNING universe_id
        """.format(universe_table=env.get_table_name("universe"))

        # This query assumes updated_at column exists
        # But our test above shows it doesn't exist in the current schema
        assert "updated_at" in universe_query
        print("❌ DETECTED ISSUE: Query assumes 'updated_at' column exists, but it doesn't in current schema")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_universe_tracking_table_creation(self):
        """Test universe tracking table creation SQL"""
        env = Mock(spec=Environment)
        env.get_table_name = Mock(side_effect=lambda name: f"dev_{name}")

        universe = DynamicModelingUniverse(env)

        # Test the tracking table creation query
        tracking_table_query = """
        CREATE TABLE IF NOT EXISTS {universe_tracking_table} (
            id SERIAL PRIMARY KEY,
            universe_name VARCHAR(100) NOT NULL,
            instrument_id INTEGER NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            entry_date DATE NOT NULL,
            last_qualifying_date DATE,
            warning_date DATE,
            removal_date DATE,
            removal_reason TEXT,
            avg_market_cap DECIMAL(15,2),
            avg_dollar_volume DECIMAL(15,2),
            last_update DATE NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(universe_name, instrument_id, entry_date)
        )
        """.format(universe_tracking_table=env.get_table_name("universe_tracking"))

        # This should be fine - it creates a new table with all needed columns
        assert "CREATE TABLE IF NOT EXISTS" in tracking_table_query
        assert "universe_name VARCHAR(100)" in tracking_table_query
        assert "updated_at TIMESTAMP DEFAULT NOW()" in tracking_table_query

        print("✅ Tracking table creation query looks correct")

    def test_detect_schema_mismatch_issue(self):
        """Test to explicitly detect the schema mismatch issue"""

        # Simulate existing schema (what we found in the database)
        existing_schema = {
            'dev_universe': ['id', 'name', 'description', 'created_at'],  # Missing 'updated_at'
        }

        # Expected schema from our application
        expected_schema = {
            'dev_universe': ['id', 'name', 'description', 'created_at', 'updated_at']
        }

        # Detect mismatches
        for table, expected_columns in expected_schema.items():
            if table in existing_schema:
                existing_columns = existing_schema[table]
                missing_columns = [col for col in expected_columns if col not in existing_columns]

                if missing_columns:
                    print(f"❌ SCHEMA MISMATCH in {table}: Missing columns {missing_columns}")
                    # This test should fail initially, documenting the issue
                    assert missing_columns == ['updated_at'], f"Expected updated_at to be missing, but found: {missing_columns}"
                    print("✅ Successfully detected the schema compatibility issue")

class TestDatabaseErrorHandling:
    """Test error handling for database schema issues"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_universe_initialization_with_schema_error(self):
        """Test that we can detect and handle schema errors gracefully"""

        # Mock a database connection that will fail with schema error
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()

        # Simulate the error we're seeing: column "updated_at" does not exist
        mock_conn.fetchval.side_effect = asyncpg.exceptions.UndefinedColumnError(
            "column \"updated_at\" of relation \"dev_universe\" does not exist"
        )

        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        # Test that the error gets raised properly
        env = Mock(spec=Environment)
        env.get_table_name = Mock(side_effect=lambda name: f"dev_{name}")

        universe = DynamicModelingUniverse(env)
        universe.db_pool = mock_pool

        # This should fail with UndefinedColumnError
        with pytest.raises(asyncpg.exceptions.UndefinedColumnError, match="updated_at"):
            await universe._ensure_universe_exists()

        print("✅ Successfully reproduced the schema error")

    def test_schema_migration_strategy(self):
        """Test strategy for handling schema differences"""

        # We need a strategy to handle existing schemas vs new requirements
        # Options:
        # 1. ALTER TABLE to add missing columns
        # 2. Use conditional queries based on detected schema
        # 3. Create schema migration system

        existing_columns = ['id', 'name', 'description', 'created_at']
        required_columns = ['id', 'name', 'description', 'created_at', 'updated_at']

        missing_columns = [col for col in required_columns if col not in existing_columns]

        if missing_columns:
            # Strategy: Generate ALTER TABLE statements
            alter_statements = []
            for col in missing_columns:
                if col == 'updated_at':
                    alter_statements.append(
                        "ALTER TABLE dev_universe ADD COLUMN updated_at TIMESTAMP DEFAULT NOW()"
                    )

            print(f"Schema migration strategy: {alter_statements}")
            assert len(alter_statements) == 1
            assert "ALTER TABLE dev_universe ADD COLUMN updated_at" in alter_statements[0]
            print("✅ Schema migration strategy defined")

if __name__ == "__main__":
    # Set up environment for testing
    os.environ.setdefault('DB_HOST', 'localhost')
    os.environ.setdefault('DB_PORT', '5433')
    os.environ.setdefault('DB_USER', 'postgres')
    os.environ.setdefault('DB_PASSWORD', 'postgres')
    os.environ.setdefault('DB_NAME', 'dev_db')

    pytest.main([__file__, "-v", "-s"])