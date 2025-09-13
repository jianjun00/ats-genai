#!/usr/bin/env python3
"""
Test cases to catch environment-aware API configuration errors

This test suite ensures APIs work correctly across DEV and INTG environments
and prevents hardcoded database connections from breaking cross-environment functionality.
"""

import pytest
import asyncio
import os
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
import asyncpg

# Test cases that would have caught the training datasets API error


class TestEnvironmentAwareAPIs:
    """Test APIs work correctly in both DEV and INTG environments"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_training_datasets_api_uses_environment_config(self):
        """Test that training datasets API uses environment-specific database config"""
        from src.ml.training_data.apis.training_dataset_simple_api import get_db_connection, list_training_datasets
        from shared.utils.environment import Environment

        # Test DEV environment
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'dev',
            'DB_HOST': 'ats-dev-postgres',
            'DB_PORT': '5432',
            'DB_NAME': 'dev_db',
            'DB_PASSWORD': 'dev_password'
        }):
            # Mock asyncpg.connect to verify correct connection string
            with patch('asyncpg.connect') as mock_connect:
                mock_conn = MagicMock()
                mock_connect.return_value = mock_conn

                await get_db_connection()

                # Verify dev database URL was used
                args, kwargs = mock_connect.call_args
                assert 'dev_db' in args[0]
                assert 'dev_password' in args[0]

        # Test INTG environment
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'intg',
            'DB_HOST': '172.17.0.1',
            'DB_PORT': '5433',
            'DB_NAME': 'intg_db',
            'DB_PASSWORD': 'intg_password'
        }):
            with patch('asyncpg.connect') as mock_connect:
                mock_conn = MagicMock()
                mock_connect.return_value = mock_conn

                await get_db_connection()

                # Verify intg database URL was used
                args, kwargs = mock_connect.call_args
                assert 'intg_db' in args[0]
                assert 'intg_password' in args[0]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_training_datasets_api_uses_correct_table_names(self):
        """Test that API queries use environment-specific table names"""
        from interfaces.rest_api.training_dataset_simple_api import list_training_datasets

        # Mock database connection and query execution
        with patch('src.api.training_dataset_simple_api.get_db_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            mock_conn.fetch.return_value = []

            # Test DEV environment
            with patch('src.api.training_dataset_simple_api.Environment') as mock_env_class:
                mock_env = MagicMock()
                mock_env.get_table_name.return_value = 'dev_training_dataset'
                mock_env_class.return_value = mock_env

                await list_training_datasets()

                # Verify correct table name was used in query
                query_call = mock_conn.fetch.call_args[0][0]
                assert 'dev_training_dataset' in query_call
                assert 'intg_training_dataset' not in query_call

            # Test INTG environment
            with patch('src.api.training_dataset_simple_api.Environment') as mock_env_class:
                mock_env = MagicMock()
                mock_env.get_table_name.return_value = 'intg_training_dataset'
                mock_env_class.return_value = mock_env

                await list_training_datasets()

                # Verify correct table name was used in query
                query_call = mock_conn.fetch.call_args[0][0]
                assert 'intg_training_dataset' in query_call
                assert 'dev_training_dataset' not in query_call

    def test_training_datasets_api_endpoint_responds_in_both_environments(self):
        """Integration test: API endpoint works in both environments"""
        from src.services.analytics.unified_analytics_app import app

        client = TestClient(app)

        # Test DEV environment
        with patch.dict(os.environ, {'ENVIRONMENT': 'dev'}):
            with patch('src.api.training_dataset_simple_api.get_db_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__aenter__.return_value = mock_conn
                mock_conn.fetch.return_value = []

                response = client.get("/api/v1/training-datasets/")
                assert response.status_code == 200
                data = response.json()
                assert 'datasets' in data
                assert 'total_count' in data

        # Test INTG environment
        with patch.dict(os.environ, {'ENVIRONMENT': 'intg'}):
            with patch('src.api.training_dataset_simple_api.get_db_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__aenter__.return_value = mock_conn
                mock_conn.fetch.return_value = []

                response = client.get("/api/v1/training-datasets/")
                assert response.status_code == 200
                data = response.json()
                assert 'datasets' in data
                assert 'total_count' in data

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_no_hardcoded_database_connections_in_apis(self):
        """Test that APIs don't contain hardcoded database connection strings"""
        import ast
        import os

        # Check all API files for hardcoded connection strings
        api_files = []
        for root, dirs, files in os.walk('src/api'):
            for file in files:
                if file.endswith('.py'):
                    api_files.append(os.path.join(root, file))

        hardcoded_patterns = [
            'ats-dev-postgres',
            'dev_password',
            'localhost:3432',
            'localhost:4432',
            'dev_training_dataset',  # Should use env.get_table_name()
            'intg_training_dataset'  # Should use env.get_table_name()
        ]

        for api_file in api_files:
            with open(api_file, 'r') as f:
                content = f.read()

                for pattern in hardcoded_patterns:
                    if pattern in content and 'test_' not in api_file:  # Exclude test files
                        pytest.fail(
                            f"❌ HARDCODED DATABASE CONFIG DETECTED: '{pattern}' found in {api_file}. "
                            f"Use Environment() class and env.get_table_name() instead."
                        )

class TestEnvironmentConfigurationValidation:
    """Test environment configuration edge cases"""

    def test_environment_detection_accuracy(self):
        """Test that environment is correctly detected from different sources"""
        from shared.utils.environment import Environment

        # Test explicit environment variable
        with patch.dict(os.environ, {'ENVIRONMENT': 'intg'}, clear=False):
            env = Environment()
            # Check that it uses INTG-specific configuration
            assert 'intg_' in env.get_table_name('training_datasets')

        with patch.dict(os.environ, {'ENVIRONMENT': 'dev'}, clear=False):
            env = Environment()
            # Check that it uses DEV-specific configuration
            assert 'dev_' in env.get_table_name('training_datasets')

    def test_missing_environment_config_handling(self):
        """Test graceful handling when environment config is missing"""
        from shared.utils.environment import Environment

        # Test with missing environment variables
        with patch.dict(os.environ, {}, clear=True):
            # Should not crash, should provide sensible defaults or clear error
            try:
                env = Environment()
                # Should be able to determine some environment
                table_name = env.get_table_name('training_datasets')
                assert table_name is not None
                assert len(table_name) > 0
            except Exception as e:
                # If it raises an exception, it should be clear and actionable
                assert 'environment' in str(e).lower() or 'config' in str(e).lower()


# Test cases for CI/CD pipeline
class TestCrosEnvironmentIntegration:
    """Integration tests that validate cross-environment functionality"""

    @pytest.mark.integration
    @pytest.mark.parametrize("environment", ["dev", "intg"])
    def test_training_datasets_api_works_in_environment(self, environment):
        """Test API actually works in specified environment"""
        # This would be run in CI/CD with actual database connections
        # Skip in unit tests, run only in integration test environments
        pytest.skip("Integration test - requires actual database")

        # In real CI/CD, this would:
        # 1. Set up environment-specific database
        # 2. Create test data in correct tables (dev_* vs intg_*)
        # 3. Call API and verify correct data is returned
        # 4. Clean up test data

    @pytest.mark.smoke
    def test_analytics_service_starts_in_both_environments(self):
        """Smoke test: Analytics service starts without crashes in both environments"""
        # This would test service startup in different environments
        pytest.skip("Smoke test - requires service deployment")


if __name__ == "__main__":
    # Run specific test categories
    pytest.main([
        __file__,
        "-v",
        "-k", "not integration and not smoke"  # Run only unit tests by default
    ])