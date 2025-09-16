"""
Tests for Data Quality Agent Endpoints

Tests the new agent endpoints added to the analytics service:
- /agent/status
- /agent/start
- /agent/stop
- /agent/health
"""

import pytest
import pytest_asyncio
import httpx
import os

class TestAgentEndpoints:
    """Test data quality agent endpoints in analytics service."""

    @pytest.fixture(scope="class")
    def analytics_base_url(self):
        """Base URL for analytics service."""
        return os.getenv('ANALYTICS_SERVICE_URL', 'http://localhost:4000')

    @pytest_asyncio.fixture(scope="class")
    async def http_client(self, analytics_base_url):
        """HTTP client for API calls."""
        async with httpx.AsyncClient(base_url=analytics_base_url, timeout=30.0) as client:
            yield client

    @pytest.mark.asyncio
    async def test_agent_status_endpoint(self, http_client):
        """Test agent status endpoint returns proper structure."""
        response = await http_client.get("/agent/status")
        assert response.status_code == 200

        data = response.json()
        assert "agent_id" in data
        assert "status" in data
        assert "tools_available" in data
        assert "tools" in data
        assert "timestamp" in data
        assert isinstance(data["tools"], list)
        assert data["tools_available"] >= 0

    @pytest.mark.asyncio
    async def test_agent_health_endpoint(self, http_client):
        """Test agent health endpoint returns health status."""
        response = await http_client.get("/agent/health")
        assert response.status_code == 200

        data = response.json()
        assert "healthy" in data
        assert "status" in data
        assert "last_health_check" in data
        assert isinstance(data["healthy"], bool)

    @pytest.mark.asyncio
    async def test_agent_start_endpoint(self, http_client):
        """Test agent start endpoint."""
        response = await http_client.get("/agent/start")
        assert response.status_code == 200

        data = response.json()
        assert "message" in data

    @pytest.mark.asyncio
    async def test_agent_stop_endpoint(self, http_client):
        """Test agent stop endpoint."""
        response = await http_client.get("/agent/stop")
        assert response.status_code == 200

        data = response.json()
        assert "message" in data

    @pytest.mark.asyncio
    async def test_agent_endpoints_included_in_404_response(self, http_client):
        """Test that agent endpoints are listed in 404 response."""
        response = await http_client.get("/nonexistent")
        assert response.status_code == 404

        data = response.json()
        assert "available_endpoints" in data
        endpoints = data["available_endpoints"]

        # Check that agent endpoints are listed
        assert "/agent/status" in endpoints
        assert "/agent/start" in endpoints
        assert "/agent/stop" in endpoints
        assert "/agent/health" in endpoints

    @pytest.mark.asyncio
    async def test_data_quality_dashboard_accessible(self, http_client):
        """Test that data quality dashboard is accessible."""
        response = await http_client.get("/data-quality/dashboard")
        assert response.status_code == 200
        assert response.headers.get('content-type', '').startswith('text/html')

    @pytest.mark.asyncio
    async def test_data_quality_issues_api(self, http_client):
        """Test that data quality issues API returns proper structure."""
        response = await http_client.get("/data-quality/api/issues")
        assert response.status_code == 200

        data = response.json()
        assert "issues" in data
        assert isinstance(data["issues"], list)

        # If there are issues, check structure
        if data["issues"]:
            issue = data["issues"][0]
            assert "id" in issue
            assert "severity" in issue
            assert "issue_type" in issue
            assert "status" in issue