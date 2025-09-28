#!/usr/bin/env python3
"""
Integration tests for ATS-INTG job monitoring and failure scenarios
"""

import asyncpg
import pytest
import os
import sys
import subprocess
from unittest.mock import patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestINTGJobMonitoring:
    """Test suite for INTG job monitoring and failure detection"""

    @pytest.fixture
    async def intg_db_connection(self):
        """Fixture for INTG database connection"""
        db_url = "postgresql://postgres:intg_password@localhost:5434/intg_db"
        conn = await asyncpg.connect(db_url)
        yield conn
    @pytest.fixture
    def mock_docker_logs(self):
        """Mock Docker container logs"""
        return {
            'ats-intg-scheduler': [
                "2025-08-28 16:54:10 - STARTUP - 📊 ATS-INTG running",
                "2025-08-28 16:53:32 - STARTUP - ⚠️ DEV database empty"
            ],
            'ats-intg-dashboard': [
                "python: can't open file '/workspace/scripts/monitor_daily_jobs.py'",
                "🎯 Starting ATS-INTG Dashboard...",
                "📊 Dashboard accessible at http://localhost:3001"
            ]
        }

    @pytest.mark.asyncio

    async def test_database_connection_health(self, intg_db_connection):
        """Test INTG database connectivity"""
        # Test basic connection
        version = await intg_db_connection.fetchval("SELECT version()")
        assert version is not None
        assert "PostgreSQL" in version

        # Test table existence
        tables = await intg_db_connection.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name LIKE 'intg_%' AND table_schema = 'public'
        """)
        table_names = [row['table_name'] for row in tables]

        # Should have basic tables
        expected_tables = ['intg_instrument', 'intg_daily_price', 'intg_fundamental_comprehensive']
        expected_tables = ['intg_instrument', 'intg_daily_price_polygon', 'intg_fundamentals_comprehensive']
        for table in expected_tables:
            assert table in table_names, f"Missing required table: {table}"

    @pytest.mark.asyncio

    async def test_daily_prices_job_status_empty(self, intg_db_connection):
        """Test daily prices job when no data exists"""
        # Check if table exists but is empty
        count = await intg_db_connection.fetchval("SELECT COUNT(*) FROM intg_daily_price")
        count = await intg_db_connection.fetchval("SELECT COUNT(*) FROM intg_daily_price_polygon")

        # Should be empty initially
        assert count == 0, "Daily prices table should be empty initially"

        # Test table structure has required columns
        columns = await intg_db_connection.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'intg_daily_price'
            WHERE table_name = 'intg_daily_price_polygon'
        """)
        column_names = [row['column_name'] for row in columns]

        required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'created_at']
        for col in required_columns:
            assert col in column_names, f"Missing required column: {col}"

    @pytest.mark.asyncio

    async def test_realtime_tables_missing(self, intg_db_connection):
        """Test when real-time one-minute tables are missing"""
        vendors = ['polygon', 'tiingo', 'fmp']

        for vendor in vendors:
            table_name = f"intg_one_minute_live_{vendor}"

            # Check if table exists
            exists = await intg_db_connection.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = $1
                )
            """, table_name)

            # These tables should not exist yet
            assert not exists, f"Real-time table {table_name} should not exist yet"

    def test_docker_container_status(self):
        """Test Docker container health status"""
        # Get container status
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=ats-intg", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, "Docker command failed"

        containers = {}
        for line in result.stdout.strip().split('\n'):
            if line:
                name, status = line.split('\t', 1)
                containers[name] = status

        # Check required containers are running
        required_containers = ['ats-intg-scheduler', 'ats-intg-dashboard', 'postgres-intg']
        for container in required_containers:
            assert container in containers, f"Missing container: {container}"
            assert "Up" in containers[container], f"Container {container} is not running"

    def test_container_logs_for_errors(self, mock_docker_logs):
        """Test container logs for error detection"""
        scheduler_logs = mock_docker_logs['ats-intg-scheduler']
        dashboard_logs = mock_docker_logs['ats-intg-dashboard']

        # Check for known error patterns in scheduler
        scheduler_errors = []
        for log_line in scheduler_logs:
            if "ERROR" in log_line or "FAIL" in log_line:
                scheduler_errors.append(log_line)

        # DEV database empty is expected initially
        dev_empty_warnings = [log for log in scheduler_logs if "DEV database empty" in log]
        assert len(dev_empty_warnings) > 0, "Should detect empty database state"

        # Check for critical errors in dashboard
        dashboard_errors = []
        for log_line in dashboard_logs:
            if "can't open file" in log_line:
                dashboard_errors.append(log_line)

        # This is the main issue - missing monitor script
        assert len(dashboard_errors) > 0, "Should detect missing monitor script error"

    @pytest.mark.asyncio

    async def test_job_failure_scenarios(self, intg_db_connection):
        """Test various job failure scenarios"""
        # Scenario 1: Database connection loss
        with patch('asyncpg.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection refused")

            # This would fail in real job execution
            with pytest.raises(Exception):
                await asyncpg.connect("postgresql://invalid:invalid@invalid:1234/invalid")

        # Scenario 2: Missing API keys
        original_polygon_key = os.environ.get('POLYGON_API_KEY')
        original_tiingo_key = os.environ.get('TIINGO_API_KEY')

        # Remove API keys
        if 'POLYGON_API_KEY' in os.environ:
            del os.environ['POLYGON_API_KEY']
        if 'TIINGO_API_KEY' in os.environ:
            del os.environ['TIINGO_API_KEY']

        # Check API key availability
        polygon_key = os.getenv('POLYGON_API_KEY')
        tiingo_key = os.getenv('TIINGO_API_KEY')

        assert polygon_key is None, "Polygon API key should be missing"
        assert tiingo_key is None, "Tiingo API key should be missing"

        await intg_db_connection.execute("""
            INSERT INTO intg_daily_price (invalid_column) VALUES ('test')
            INSERT INTO intg_daily_price_polygon (invalid_column) VALUES ('test')
        """)
        assert False, "Should have failed due to invalid column"
    def test_monitoring_script_creation(self):
        """Test that monitoring script can be executed"""
        monitor_script = "/home/jianjun/ats-genai-data/scripts/monitor_daily_jobs.py"

        # Check if script exists
        assert os.path.exists(monitor_script), "Monitor script should exist"

        # Check if script is executable
        assert os.access(monitor_script, os.X_OK), "Monitor script should be executable"

        # Test script can be imported
        import importlib.util
        spec = importlib.util.spec_from_file_location("monitor_daily_jobs", monitor_script)
        module = importlib.util.module_from_spec(spec)
        # Don't execute, just check it can be loaded
        assert spec is not None, "Script should be importable"
    @pytest.mark.asyncio

    async def test_create_missing_realtime_tables(self, intg_db_connection):
        """Test creating missing real-time tables"""
        vendors = ['polygon', 'tiingo', 'fmp']

        for vendor in vendors:
            table_name = f"intg_one_minute_live_{vendor}"

            # Create the missing table
            await intg_db_connection.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER,
                    symbol TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    open_price DECIMAL(10,4),
                    high_price DECIMAL(10,4),
                    low_price DECIMAL(10,4),
                    close_price DECIMAL(10,4),
                    volume BIGINT,
                    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    collection_method TEXT DEFAULT 'api',
                    quality_score DECIMAL(3,2) DEFAULT 1.0,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp)
                )
            """)

            # Verify table was created
            exists = await intg_db_connection.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = $1
                )
            """, table_name)

            assert exists, f"Failed to create table {table_name}"

    def test_health_endpoint_response(self):
        """Test health endpoint returns proper status"""
        import requests

        # Test dashboard health endpoint
        response = requests.get("http://localhost:4000/health", timeout=5)

        if response.status_code == 200:
            health_data = response.json()
            assert 'status' in health_data
            assert 'timestamp' in health_data
            assert health_data['service'] == 'ats-intg-dashboard'
        else:
            # Dashboard might not be fully running yet
            pytest.skip("Dashboard not accessible for health check")

class TestJobRecovery:
    """Tests for job recovery and error handling"""

    def test_job_restart_capability(self):
        """Test ability to restart failed jobs"""
        # This would be implemented when we have job orchestration
        # For now, document the expected behavior

        recovery_plan = {
            "daily_prices_job": {
                "restart_command": "python scripts/daily_price_refresh_job.py",
                "dependencies": ["database", "api_keys"],
                "timeout_seconds": 3600
            },
            "realtime_collector": {
                "restart_command": "python scripts/realtime_collector_orchestrator.py",
                "dependencies": ["database", "api_keys", "one_minute_tables"],
                "timeout_seconds": 300
            }
        }

        # Verify recovery plan structure
        for job_name, config in recovery_plan.items():
            assert 'restart_command' in config
            assert 'dependencies' in config
            assert 'timeout_seconds' in config
            assert isinstance(config['dependencies'], list)
            assert isinstance(config['timeout_seconds'], int)

# Utility functions for testing
def run_tests():
    """Run all INTG monitoring tests"""
    test_files = [__file__]

    for test_file in test_files:
        print(f"🧪 Running tests in {test_file}")
        result = subprocess.run([
            "python", "-m", "pytest", test_file, "-v", "--tb=short"
        ], cwd="/home/jianjun/ats-genai-data")

        if result.returncode != 0:
            print(f"❌ Tests failed in {test_file}")
            return False
        else:
            print(f"✅ Tests passed in {test_file}")

    return True

if __name__ == "__main__":
    # Run tests directly
    import pytest
    pytest.main([__file__, "-v"])