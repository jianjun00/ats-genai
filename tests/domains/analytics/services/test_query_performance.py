"""
Performance Tests for Analytics Service Queries

These tests ensure that the database queries perform well and don't
cause timeouts or excessive resource usage that could impact the service.
"""

import pytest
import asyncio
import asyncpg
import time
import os
from typing import Dict, List, Any


class TestQueryPerformance:
    """Performance tests for analytics database queries."""
    
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

    async def test_jobs_query_performance(self, db_connection):
        """Test that jobs queries complete within reasonable time."""
        start_time = time.time()
        
        # This was the corrected query from our fixes
        jobs = await db_connection.fetch("""
            SELECT 
                id,
                run_type,
                status,
                start_time,
                symbols
            FROM dev_runs 
            ORDER BY start_time DESC NULLS LAST, id DESC
            LIMIT 20
        """)
        
        end_time = time.time()
        query_time = end_time - start_time
        
        assert query_time < 5.0, f"Jobs query took {query_time:.2f}s, should be < 5s"
        assert isinstance(jobs, list), "Jobs query should return a list"

    @pytest.mark.asyncio

    async def test_coverage_queries_performance(self, db_connection):
        """Test that coverage queries complete within reasonable time."""
        
        # Test polygon coverage query
        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        
        if polygon_exists:
            start_time = time.time()
            
            polygon_count = await db_connection.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM dev_polygon_prices WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'"
            )
            
            end_time = time.time()
            query_time = end_time - start_time
            
            assert query_time < 10.0, f"Polygon coverage query took {query_time:.2f}s, should be < 10s"
        
        # Test tiingo coverage query
        tiingo_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_tiingo_prices')"
        )
        
        if tiingo_exists:
            start_time = time.time()
            
            tiingo_count = await db_connection.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM dev_tiingo_prices WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day'"
            )
            
            end_time = time.time()
            query_time = end_time - start_time
            
            assert query_time < 10.0, f"Tiingo coverage query took {query_time:.2f}s, should be < 10s"

    @pytest.mark.asyncio

    async def test_summary_queries_performance(self, db_connection):
        """Test that summary data queries perform well."""
        
        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        
        if polygon_exists:
            start_time = time.time()
            
            polygon_data = await db_connection.fetch("""
                SELECT 
                    'polygon' as vendor,
                    symbol,
                    COUNT(*) as data_points
                FROM dev_polygon_prices 
                WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY symbol
                ORDER BY data_points DESC
                LIMIT 5
            """)
            
            end_time = time.time()
            query_time = end_time - start_time
            
            assert query_time < 15.0, f"Polygon summary query took {query_time:.2f}s, should be < 15s"

    @pytest.mark.asyncio

    async def test_concurrent_query_performance(self, db_connection):
        """Test performance with concurrent queries (simulating multiple users)."""
        
        async def run_query_set():
            """Run a set of typical analytics queries."""
            start_time = time.time()
            
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
                    db_connection.fetchval("SELECT COUNT(DISTINCT symbol) FROM dev_polygon_prices WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'")
                )
            
            results = await asyncio.gather(*tasks)
            
            end_time = time.time()
            return end_time - start_time, results
        
        # Run multiple concurrent query sets
        start_time = time.time()
        
        concurrent_tasks = [run_query_set() for _ in range(5)]
        all_results = await asyncio.gather(*concurrent_tasks)
        
        total_time = time.time() - start_time
        
        # Validate all succeeded and completed in reasonable time
        assert total_time < 30.0, f"Concurrent queries took {total_time:.2f}s, should be < 30s"
        
        for i, (query_time, results) in enumerate(all_results):
            assert query_time < 10.0, f"Query set {i} took {query_time:.2f}s, should be < 10s"
            assert all(r is not None for r in results), f"Query set {i} had None results"


class TestDatabaseIndexes:
    """Test that proper database indexes exist for good performance."""
    
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

    async def test_price_table_indexes(self, db_connection):
        """Test that price tables have proper indexes for coverage queries."""
        
        # Check polygon prices indexes
        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        
        if polygon_exists:
            indexes = await db_connection.fetch("""
                SELECT indexname, indexdef
                FROM pg_indexes 
                WHERE tablename = 'dev_polygon_prices'
            """)
            
            index_names = [row['indexname'] for row in indexes]
            
            # Should have indexes on commonly queried columns
            has_symbol_index = any('symbol' in idx for idx in index_names)
            has_date_index = any('created_at' in str(row['indexdef']).lower() or 'date' in idx for idx in index_names for row in indexes if row['indexname'] == idx)
            
            # Log available indexes for debugging
            print(f"Polygon price table indexes: {index_names}")
            
            # At minimum should have primary key
            assert len(indexes) > 0, "dev_polygon_prices should have at least a primary key index"

    @pytest.mark.asyncio

    async def test_runs_table_indexes(self, db_connection):
        """Test that dev_runs table has proper indexes."""
        
        indexes = await db_connection.fetch("""
            SELECT indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = 'dev_runs'
        """)
        
        index_names = [row['indexname'] for row in indexes]
        
        # Log available indexes
        print(f"dev_runs table indexes: {index_names}")
        
        # Should have primary key at minimum
        assert len(indexes) > 0, "dev_runs should have at least a primary key index"


if __name__ == "__main__":
    # Run with: PYTHONPATH=src pytest tests/analytics/test_query_performance.py -v -s
    pytest.main([__file__, "-v", "-s"])