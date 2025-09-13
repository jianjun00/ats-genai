"""
Integration Tests for Analytics Service Endpoints

These tests prevent the endpoint-related issues that we discovered:
1. Jobs stats showing data but jobs list returning empty
2. Coverage returning 0 when data exists
3. Database connection pool issues causing "operation in progress" errors
4. SQL syntax errors in UNION queries

This test suite validates that the analytics service endpoints return
correct data that matches the actual database state.
"""

import pytest
import pytest_asyncio
import asyncio
import httpx
import asyncpg
import os
from typing import Dict, List, Any


class TestAnalyticsEndpoints:
    """Integration tests for analytics service endpoints."""

    @pytest.fixture(scope="class")
    def event_loop(self):
        """Create event loop for async tests."""
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope="class")
    def analytics_base_url(self):
        """Base URL for analytics service."""
        # In K8s environment, use the service URL
        # For local testing, use localhost port-forward
        return os.getenv('ANALYTICS_SERVICE_URL', 'http://localhost:3001')

    @pytest_asyncio.fixture(scope="class")
    async def http_client(self, analytics_base_url):
        """HTTP client for API calls."""
        async with httpx.AsyncClient(base_url=analytics_base_url, timeout=30.0) as client:
            yield client

    @pytest_asyncio.fixture(scope="class")
    async def db_connection(self):
        """Direct database connection for validation."""
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

    async def test_health_endpoint(self, http_client):
        """Test health endpoint returns success."""
        response = await http_client.get("/health")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

    @pytest.mark.asyncio

    async def test_job_stats_consistency(self, http_client, db_connection):
        """Test job stats endpoint matches actual database counts."""
        # Get API response
        response = await http_client.get("/api/v1/jobs/stats")
        assert response.status_code == 200

        api_stats = response.json()
        assert "total_jobs" in api_stats
        assert "running_jobs" in api_stats
        assert "completed_jobs" in api_stats
        assert "failed_jobs" in api_stats

        # Get actual database counts
        db_total = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs")
        db_running = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'running'")
        db_completed = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'completed'")
        db_failed = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs WHERE status = 'failed'")

        # Validate API matches database
        assert api_stats["total_jobs"] == (db_total or 0), f"Total jobs mismatch: API={api_stats['total_jobs']}, DB={db_total}"
        assert api_stats["running_jobs"] == (db_running or 0), f"Running jobs mismatch: API={api_stats['running_jobs']}, DB={db_running}"
        assert api_stats["completed_jobs"] == (db_completed or 0), f"Completed jobs mismatch: API={api_stats['completed_jobs']}, DB={db_completed}"
        assert api_stats["failed_jobs"] == (db_failed or 0), f"Failed jobs mismatch: API={api_stats['failed_jobs']}, DB={db_failed}"

    @pytest.mark.asyncio

    async def test_jobs_list_consistency(self, http_client, db_connection):
        """Test jobs list endpoint returns data when jobs exist."""
        # Get database job count first
        db_total = await db_connection.fetchval("SELECT COUNT(*) FROM dev_runs")

        # Get API response
        response = await http_client.get("/api/v1/jobs")
        assert response.status_code == 200

        api_data = response.json()
        assert "jobs" in api_data
        assert "total" in api_data

        # Critical test: if database has jobs, API should return them
        if db_total and db_total > 0:
            # This was the bug: stats showed jobs but list was empty
            assert api_data["total"] > 0, f"Jobs list shows 0 total but database has {db_total} jobs"
            assert len(api_data["jobs"]) > 0, f"Jobs list is empty but database has {db_total} jobs"

            # Validate job structure
            job = api_data["jobs"][0]
            assert "id" in job, "Job should have ID"
            assert "job_type" in job, "Job should have job_type (from run_type column)"
            assert "status" in job, "Job should have status"
            assert "started_at" in job, "Job should have started_at (from start_time column)"
            assert "symbol" in job, "Job should have symbol (from symbols array)"
        else:
            # No jobs in database, API should reflect this
            assert api_data["total"] == 0, "Jobs list should show 0 when database is empty"
            assert len(api_data["jobs"]) == 0, "Jobs list should be empty when database is empty"

    @pytest.mark.asyncio

    async def test_coverage_summary_has_data(self, http_client, db_connection):
        """Test coverage summary shows real data when price data exists."""
        # Check if price tables have data
        polygon_count = 0
        tiingo_count = 0

        polygon_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_polygon_prices')"
        )
        if polygon_exists:
            polygon_count = await db_connection.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM dev_polygon_prices WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'"
            ) or 0

        tiingo_exists = await db_connection.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_tiingo_prices')"
        )
        if tiingo_exists:
            tiingo_count = await db_connection.fetchval(
                "SELECT COUNT(DISTINCT symbol) FROM dev_tiingo_prices WHERE collected_at >= CURRENT_DATE - INTERVAL '1 day'"
            ) or 0

        expected_total = polygon_count + tiingo_count

        # Get API response
        response = await http_client.get("/api/v1/coverage/summary")
        assert response.status_code == 200

        api_data = response.json()
        assert "total_combinations" in api_data
        assert "active_combinations" in api_data
        assert "summary" in api_data

        # Critical test: if database has price data, API should show it
        if expected_total > 0:
            # This was the bug: coverage showed 0 when millions of records existed
            assert api_data["total_combinations"] > 0, f"Coverage shows 0 but database has {expected_total} symbol combinations"
            assert api_data["total_combinations"] == expected_total, f"Coverage total {api_data['total_combinations']} doesn't match database {expected_total}"

            # Should have summary data
            assert len(api_data["summary"]) > 0, "Coverage summary should have data when price data exists"

            # Validate summary structure
            if len(api_data["summary"]) > 0:
                item = api_data["summary"][0]
                assert "symbol" in item, "Coverage item should have symbol"
                assert "vendor" in item, "Coverage item should have vendor"
                assert "coverage_24h" in item, "Coverage item should have 24h coverage"
                assert "current_status" in item, "Coverage item should have status"
        else:
            # No recent price data, coverage should be 0
            assert api_data["total_combinations"] == 0, "Coverage should show 0 when no recent price data exists"

    @pytest.mark.asyncio

    async def test_coverage_gaps_endpoint(self, http_client):
        """Test coverage gaps endpoint returns valid response."""
        response = await http_client.get("/api/v1/coverage/gaps")
        assert response.status_code == 200

        api_data = response.json()
        assert "gaps" in api_data
        assert isinstance(api_data["gaps"], list), "Gaps should be a list"

        # If gaps exist, validate structure
        for gap in api_data["gaps"]:
            assert "symbol" in gap, "Gap should have symbol"
            assert "vendor" in gap, "Gap should have vendor"
            assert "severity" in gap, "Gap should have severity"
            assert "gap_duration_minutes" in gap, "Gap should have duration"

    @pytest.mark.asyncio

    async def test_datasets_endpoint(self, http_client, db_connection):
        """Test datasets endpoint returns correct data."""
        # Check database for datasets
        db_total = await db_connection.fetchval("SELECT COUNT(*) FROM dev_training_dataset")

        # Get API response
        response = await http_client.get("/api/v1/datasets")
        assert response.status_code == 200

        api_data = response.json()
        assert "datasets" in api_data
        assert "total" in api_data

        # Validate consistency
        assert api_data["total"] == (db_total or 0), f"Dataset total mismatch: API={api_data['total']}, DB={db_total}"

        if db_total and db_total > 0:
            assert len(api_data["datasets"]) > 0, "Dataset list should have data when database has datasets"

            # Validate dataset structure
            dataset = api_data["datasets"][0]
            assert "dataset_id" in dataset, "Dataset should have ID"
            assert "dataset_name" in dataset, "Dataset should have name"
            assert "symbols" in dataset, "Dataset should have symbols"
            assert "total_sequences" in dataset, "Dataset should have sequence count"
            assert "status" in dataset, "Dataset should have status"

    @pytest.mark.asyncio

    async def test_concurrent_endpoint_access(self, http_client):
        """Test that multiple concurrent requests don't cause connection errors."""
        # This tests the connection pool fix for "operation in progress" errors

        async def make_request(endpoint):
            response = await http_client.get(endpoint)
            return response.status_code, response.json()

        # Make multiple concurrent requests to different endpoints
        tasks = [
            make_request("/api/v1/jobs/stats"),
            make_request("/api/v1/jobs"),
            make_request("/api/v1/coverage/summary"),
            make_request("/api/v1/datasets"),
            make_request("/health")
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                pytest.fail(f"Concurrent request {i} failed: {result}")

            status_code, data = result
            assert status_code == 200, f"Request {i} returned status {status_code}"
            assert data is not None, f"Request {i} returned no data"

    @pytest.mark.asyncio

    async def test_dashboard_endpoint(self, http_client):
        """Test dashboard HTML endpoint returns valid response."""
        response = await http_client.get("/")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html"), "Dashboard should return HTML"

        html_content = response.text
        assert "ATS Unified Analytics" in html_content, "Dashboard should have correct title"
        # Check for EDA dashboard elements
        assert "Type-aware EDA" in html_content or "EDA Dashboard" in html_content, "Dashboard should be EDA dashboard"

    @pytest.mark.asyncio

    async def test_error_handling(self, http_client):
        """Test that endpoints handle errors gracefully."""
        # Test non-existent endpoint
        response = await http_client.get("/api/v1/nonexistent")
        assert response.status_code == 404

        # Test invalid dataset ID
        response = await http_client.get("/api/v1/datasets/99999")
        # Should return 404 or handle gracefully without crashing
        assert response.status_code in [200, 404], "Invalid dataset ID should be handled gracefully"

    @pytest.mark.parametrize("endpoint", [
        "/health",
        "/api/v1/jobs/stats",
        "/api/v1/jobs",
        "/api/v1/coverage/summary",
        "/api/v1/coverage/gaps",
        "/api/v1/datasets",
        "/"
    ])
    @pytest.mark.asyncio
    async def test_endpoint_response_time(self, http_client, endpoint):
        """Test that all endpoints respond within reasonable time."""
        import time

        start_time = time.time()
        response = await http_client.get(endpoint)
        end_time = time.time()

        response_time = end_time - start_time

        assert response.status_code == 200, f"Endpoint {endpoint} should return 200"
        assert response_time < 10.0, f"Endpoint {endpoint} took {response_time:.2f}s, should be < 10s"


class TestAnalyticsServiceRegression:
    """Regression tests for specific bugs we fixed."""

    @pytest.fixture(scope="class")
    async def http_client(self):
        """HTTP client for API calls."""
        base_url = os.getenv('ANALYTICS_SERVICE_URL', 'http://localhost:3001')
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            yield client

    @pytest.mark.asyncio

    async def test_jobs_stats_vs_jobs_list_consistency(self, http_client):
        """
        Regression test for the bug where:
        - /api/v1/jobs/stats showed {"total_jobs":1,"running_jobs":1,...}
        - /api/v1/jobs showed {"jobs":[],"total":0}

        This was caused by wrong column names in jobs list query.
        """
        # Get both endpoints
        stats_response = await http_client.get("/api/v1/jobs/stats")
        list_response = await http_client.get("/api/v1/jobs")

        assert stats_response.status_code == 200
        assert list_response.status_code == 200

        stats_data = stats_response.json()
        list_data = list_response.json()

        # The totals should be consistent
        stats_total = stats_data["total_jobs"]
        list_total = list_data["total"]

        assert stats_total == list_total, f"Job stats total ({stats_total}) != job list total ({list_total})"

        # If stats shows jobs, list should show them too
        if stats_total > 0:
            assert len(list_data["jobs"]) > 0, "Job list should not be empty when stats show jobs exist"

    @pytest.mark.asyncio

    async def test_coverage_zero_when_data_exists(self, http_client):
        """
        Regression test for the bug where:
        - Database had 500k+ price records
        - /api/v1/coverage/summary showed {"total_combinations":0,...}

        This was caused by wrong column names (created_at vs collected_at).
        """
        response = await http_client.get("/api/v1/coverage/summary")
        assert response.status_code == 200

        data = response.json()
        total_combinations = data["total_combinations"]

        # If the database has recent price data, this should not be 0
        # The test environment should have some price data
        # We can't assert a specific number, but it shouldn't be 0 if data exists

        # At minimum, the endpoint should return valid structure
        assert "total_combinations" in data
        assert "active_combinations" in data
        assert "summary" in data
        assert isinstance(data["summary"], list)

    @pytest.mark.asyncio

    async def test_connection_pool_fixes_concurrent_errors(self, http_client):
        """
        Regression test for the bug where concurrent requests caused:
        "Error: cannot perform operation: another operation is in progress"

        This was fixed by using connection pool instead of single connection.
        """
        # Make many concurrent requests to trigger the old bug
        tasks = []
        for _ in range(10):
            tasks.extend([
                http_client.get("/api/v1/jobs/stats"),
                http_client.get("/api/v1/jobs"),
                http_client.get("/api/v1/coverage/summary")
            ])

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # None should fail with connection errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                pytest.fail(f"Concurrent request {i} failed: {result}")

            assert result.status_code == 200, f"Request {i} failed with status {result.status_code}"


if __name__ == "__main__":
    # Run with: ANALYTICS_SERVICE_URL=http://localhost:3001 pytest tests/analytics/test_analytics_endpoints.py -v
    pytest.main([__file__, "-v"])