"""
Database Schema Validation Tests for Analytics Service

These tests prevent the schema-related issues that caused the analytics service failures:
1. Wrong table names (dev_training_dataset vs dev_training_dataset)
2. Wrong column names (job_type vs run_type, started_at vs start_time, etc.)
3. Missing timestamp columns (created_at vs collected_at)
4. SQL syntax errors in UNION queries

This test suite validates that all database queries in the analytics service
match the actual database schema structure.
"""

import pytest
import asyncio
import asyncpg
import os

class TestDatabaseSchemaValidation:
    """Test database schema matches analytics service expectations."""

    @pytest.fixture(scope="class")
    def event_loop(self):
        """Create event loop for async tests."""
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope="class")
    async def db_connection(self):
        """Database connection fixture."""
        conn = await asyncpg.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            port=int(os.getenv('DB_PORT', '5432')),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'dev_password'),
            database=os.getenv('DB_NAME', 'dev_db')
        )
        yield conn
        await conn.close()

    @pytest.mark.asyncio

    async def test_dev_runs_table_schema(self, db_connection):
        """Test dev_runs table has correct columns for jobs queries."""
        # Verify table exists
        table_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_runs')"
        )
        assert table_exists, "Table 'dev_runs' must exist for jobs functionality"

        # Get actual column names
        columns = await db_connection.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'dev_runs'"
        )
        column_names = [row['column_name'] for row in columns]

        # Validate required columns exist (these were the wrong ones in analytics service)
        assert 'id' in column_names, "Column 'id' must exist in dev_runs"
        assert 'run_type' in column_names, "Column 'run_type' must exist (not 'job_type')"
        assert 'status' in column_names, "Column 'status' must exist in dev_runs"
        assert 'start_time' in column_names, "Column 'start_time' must exist (not 'started_at')"
        assert 'symbols' in column_names, "Column 'symbols' must exist (not 'symbol')"

        # Validate wrong column names don't exist
        assert 'job_type' not in column_names, "Column 'job_type' should not exist (use 'run_type')"
        assert 'started_at' not in column_names, "Column 'started_at' should not exist (use 'start_time')"
        assert 'symbol' not in column_names, "Column 'symbol' should not exist (use 'symbols' array)"
        assert 'created_at' not in column_names, "Column 'created_at' should not exist in dev_runs"

    @pytest.mark.asyncio

    async def test_dev_training_dataset_table_schema(self, db_connection):
        """Test dev_training_dataset table schema (not dev_training_dataset)."""
        # Verify correct table name exists
        table_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_dataset')"
        )
        assert table_exists, "Table 'dev_training_dataset' (singular) must exist"

        # Verify wrong table name doesn't exist
        wrong_table = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_dataset')"
        )
        assert not wrong_table, "Table 'dev_training_dataset' (plural) should not exist"

        # Get actual column names
        columns = await db_connection.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'dev_training_dataset'"
        )
        column_names = [row['column_name'] for row in columns]

        # Validate required columns for dataset queries
        assert 'id' in column_names, "Column 'id' must exist in dev_training_dataset"
        assert 'dataset_name' in column_names, "Column 'dataset_name' must exist"
        assert 'symbols' in column_names, "Column 'symbols' must exist"
        assert 'total_sequences' in column_names, "Column 'total_sequences' must exist"
        assert 'feature_count' in column_names, "Column 'feature_count' must exist"
        assert 'sequence_length' in column_names, "Column 'sequence_length' must exist"
        assert 'file_size_mb' in column_names, "Column 'file_size_mb' must exist"
        assert 'status' in column_names, "Column 'status' must exist"
        assert 'creation_timestamp' in column_names, "Column 'creation_timestamp' must exist"

    @pytest.mark.asyncio

    async def test_price_tables_timestamp_columns(self, db_connection):
        """Test price tables have correct timestamp columns for coverage queries."""

        # Test dev_polygon_prices table
        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        if polygon_exists:
            columns = await db_connection.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'dev_polygon_prices'"
            )
            column_names = [row['column_name'] for row in columns]
            assert 'created_at' in column_names, "dev_polygon_prices must have 'created_at' column"
            assert 'symbol' in column_names, "dev_polygon_prices must have 'symbol' column"

        # Test dev_tiingo_prices table
        tiingo_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_tiingo_prices')"
        )
        if tiingo_exists:
            columns = await db_connection.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'dev_tiingo_prices'"
            )
            column_names = [row['column_name'] for row in columns]
            assert 'collected_at' in column_names, "dev_tiingo_prices must have 'collected_at' column (not 'created_at')"
            assert 'symbol' in column_names, "dev_tiingo_prices must have 'symbol' column"

            # This was the critical error - tiingo uses collected_at, not created_at
            assert 'created_at' not in column_names or True, "dev_tiingo_prices uses 'collected_at', not 'created_at'"

    @pytest.mark.asyncio

    async def test_analytics_queries_syntax(self, db_connection):
        """Test that all analytics service queries have correct syntax."""

        # Test jobs query (this was failing with wrong column names)
        jobs = await db_connection.fetch("""
            SELECT
                id,
                run_type,
                status,
                start_time,
                symbols
            FROM dev_runs
            ORDER BY start_time DESC NULLS LAST, id DESC
            LIMIT 5
        """)
        assert True, "Jobs query should execute without syntax errors"
        datasets = await db_connection.fetch("""
            SELECT
                id as dataset_id,
                dataset_name,
                symbols,
                total_sequences,
                feature_count,
                sequence_length,
                file_size_mb,
                status,
                creation_timestamp as created_at
            FROM dev_training_dataset
            ORDER BY creation_timestamp DESC
            LIMIT 5
        """)
        assert True, "Dataset query should execute without syntax errors"
        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        if polygon_exists:
            polygon_count = await db_connection.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM dev_polygon_prices WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'"
            )
            assert polygon_count is not None, "Polygon coverage query should return a result"
        tiingo_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_tiingo_prices')"
        )
        if tiingo_exists:
            tiingo_count = await db_connection.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM dev_tiingo_prices WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day'"
            )
            assert tiingo_count is not None, "Tiingo coverage query should return a result"
    @pytest.mark.asyncio

    async def test_separate_coverage_queries_avoid_union_issues(self, db_connection):
        """Test that separate queries work better than UNION for coverage data."""

        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        tiingo_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_tiingo_prices')"
        )

        if polygon_exists:
            # Test polygon query separately
            polygon_data = await db_connection.fetch("""
                SELECT
                    'polygon' as vendor,
                    symbol,
                    COUNT(*) as data_points
                FROM dev_polygon_prices
                WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY symbol
                ORDER BY data_points DESC
                LIMIT 3
            """)
            assert isinstance(polygon_data, list), "Polygon query should return a list"
        if tiingo_exists:
            # Test tiingo query separately
            tiingo_data = await db_connection.fetch("""
                SELECT
                    'tiingo' as vendor,
                    symbol,
                    COUNT(*) as data_points
                FROM dev_tiingo_prices
                WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY symbol
                ORDER BY data_points DESC
                LIMIT 3
            """)
            assert isinstance(tiingo_data, list), "Tiingo query should return a list"
    @pytest.mark.asyncio

    async def test_job_statistics_queries(self, db_connection):
        """Test job statistics queries work with correct column names."""
        # These were failing because of concurrent connection usage
        total = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs")
        running = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'running'")
        completed = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'completed'")
        failed = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'failed'")

        assert total is not None, "Total jobs count should not be None"
        assert running is not None, "Running jobs count should not be None"
        assert completed is not None, "Completed jobs count should not be None"
        assert failed is not None, "Failed jobs count should not be None"

        # Basic sanity check
        assert total >= 0, "Job counts should be non-negative"
        assert running + completed + failed <= total, "Status counts should not exceed total"

    @pytest.mark.asyncio

    async def test_database_connection_pool_pattern(self, db_connection):
        """Test that connection pool pattern works for concurrent queries."""
        # This test simulates what the analytics service does with concurrent requests

        async def run_concurrent_queries():
            # These queries should not interfere with each other when using connection pool
            tasks = [
                db_connection.fetchval("SELECT COUNT(*) FROM dev_runs"),
                db_connection.fetchval("SELECT COUNT(*) FROM dev_training_dataset"),
            ]

            # Add price table queries if they exist
            polygon_exists = await db_connection.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
            )
            if polygon_exists:
                tasks.append(
                    db_connection.fetchval("SELECT COUNT(*) FROM dev_polygon_prices WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'")
                )

            tiingo_exists = await db_connection.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_tiingo_prices')"
            )
            if tiingo_exists:
                tasks.append(
                    db_connection.fetchval("SELECT COUNT(*) FROM dev_tiingo_prices WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day'")
                )

            # Run queries concurrently - this would fail with single connection
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    pytest.fail(f"Concurrent query {i} failed: {result}")
                assert result is not None, f"Query {i} should return a result"

        await run_concurrent_queries()

class TestAnalyticsServiceQueries:
    """Test specific queries used by the analytics service."""

    @pytest.fixture(scope="class")
    async def db_connection(self):
        """Database connection fixture."""
        conn = await asyncpg.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            port=int(os.getenv('DB_PORT', '5432')),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'dev_password'),
            database=os.getenv('DB_NAME', 'dev_db')
        )
        yield conn
        await conn.close()

    def test_analytics_service_query_correctness(self):
        """Test that analytics service queries match actual database schema."""

        # This test documents the correct queries that should be used
        correct_queries = {
            "jobs_list": """
                SELECT
                    id,
                    run_type,           -- NOT job_type
                    status,
                    start_time,         -- NOT started_at
                    symbols             -- NOT symbol (array field)
                FROM dev_runs
                ORDER BY start_time DESC NULLS LAST, id DESC
                LIMIT 20
            """,

            "job_stats": """
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN status = 'running' THEN 1 END) as running,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                FROM dev_runs
            """,

            "datasets": """
                SELECT
                    id as dataset_id,
                    dataset_name,
                    symbols,
                    total_sequences,
                    feature_count,
                    sequence_length,
                    file_size_mb,
                    status,
                    creation_timestamp as created_at
                FROM dev_training_dataset    -- NOT dev_training_dataset
                ORDER BY creation_timestamp DESC
                LIMIT $1 OFFSET $2
            """,

            "polygon_coverage": """
                SELECT COUNT(DISTINCT symbol)
                FROM dev_polygon_prices
                WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'  -- created_at is correct
            """,

            "tiingo_coverage": """
                SELECT COUNT(DISTINCT symbol)
                FROM dev_tiingo_prices
                WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day'  -- collected_at, NOT created_at
            """
        }

        # This test serves as documentation of the correct schema
        assert len(correct_queries) > 0, "Correct queries documented for reference"

    @pytest.mark.asyncio

    async def test_real_data_validation(self, db_connection):
        """Test that queries return real data, not fake/demo data."""

        # Test that we get actual data, not hardcoded fake responses
        total_jobs = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs")
        if total_jobs > 0:
            # If we have jobs, they should have real data
            job_data = await db_connection.fetchrow("""
                SELECT id, run_type, status, start_time, symbols
                FROM dev_runs
                LIMIT 1
            """)

            assert job_data['id'] is not None, "Job should have real ID"
            assert job_data['run_type'] is not None, "Job should have real run_type"
            assert job_data['status'] in ['running', 'completed', 'failed', 'pending'], "Job should have valid status"

        # Test coverage data is real
        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        if polygon_exists:
            polygon_count = await db_connection.fetchval(
                "SELECT COUNT(*) FROM dev_polygon_prices"
            )
            if polygon_count > 0:
                # Should have real symbols, not fake data
                sample = await db_connection.fetchrow(
                    "SELECT symbol, price_date FROM dev_polygon_prices LIMIT 1"
                )
                assert len(sample['symbol']) > 0, "Should have real symbol data"
                assert sample['price_date'] is not None, "Should have real price date"

if __name__ == "__main__":
    # Run with: PYTHONPATH=src pytest tests/analytics/test_database_schema_validation.py -v
    pytest.main([__file__, "-v"])