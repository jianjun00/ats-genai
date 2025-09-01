"""
Enhanced pytest configuration with common fixtures and utilities.

This module provides centralized test configuration and reusable fixtures
to eliminate duplication across the 300+ test files in the ATS platform.
"""

import pytest
import asyncio
import tempfile
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, patch

# Import common test utilities
from tests.fixtures.test_data_factory import TestDataFactory, get_sample_price_data


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir():
    """Provide a temporary directory for test operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_database_connection():
    """Mock database connection for tests that don't need real database."""
    with patch('src.core.database.connection_manager.get_raw_connection') as mock_conn:
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        mock_cursor.description = []
        
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__exit__ = Mock(return_value=None)
        mock_conn.return_value.__exit__ = Mock(return_value=None)
        
        yield mock_conn


@pytest.fixture
def mock_settings():
    """Mock settings for tests."""
    mock_settings = Mock()
    mock_settings.get_table_name.return_value = "test_table"
    mock_settings.database = Mock()
    mock_settings.database.host = "localhost"
    mock_settings.database.port = 5432
    mock_settings.database.database = "test_db"
    mock_settings.database.user = "test_user"
    mock_settings.database.password = "test_password"
    
    with patch('src.core.config.settings.get_settings') as mock_get_settings:
        mock_get_settings.return_value = mock_settings
        yield mock_settings


@pytest.fixture
def sample_instrument_data():
    """Provide sample instrument data for testing."""
    return [
        {
            'id': 1,
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'active': True,
            'start_date': datetime(2020, 1, 1).date(),
            'end_date': None
        },
        {
            'id': 2,
            'symbol': 'TSLA',
            'name': 'Tesla Inc.',
            'exchange': 'NASDAQ',
            'active': True,
            'start_date': datetime(2019, 1, 1).date(),
            'end_date': None
        },
        {
            'id': 3,
            'symbol': 'MSFT',
            'name': 'Microsoft Corporation',
            'exchange': 'NASDAQ',
            'active': True,
            'start_date': datetime(2018, 1, 1).date(),
            'end_date': None
        }
    ]


@pytest.fixture
def sample_price_data():
    """Provide sample price data using the test data factory."""
    return get_sample_price_data("AAPL", days=30)


@pytest.fixture
def test_data_factory():
    """Provide TestDataFactory instance."""
    return TestDataFactory(seed=42)  # Fixed seed for reproducible tests


@pytest.fixture
def mock_api_responses():
    """Mock API responses for different vendors."""
    factory = TestDataFactory(seed=42)
    return {
        'polygon': factory.generate_polygon_response("AAPL", "2024-01-01", "2024-01-02"),
        'tiingo': factory.generate_tiingo_response("AAPL", "2024-01-01", "2024-01-02"),
    }


@pytest.fixture
def common_test_symbols():
    """Provide common test symbols used across tests."""
    return ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN']


@pytest.fixture
def test_date_ranges():
    """Provide common test date ranges."""
    return {
        'recent': {
            'start': datetime(2024, 1, 1),
            'end': datetime(2024, 1, 31)
        },
        'historical': {
            'start': datetime(2020, 1, 1),
            'end': datetime(2020, 12, 31)
        },
        'long_term': {
            'start': datetime(2010, 1, 1),
            'end': datetime(2024, 1, 1)
        }
    }


@pytest.fixture
def mock_environment_variables():
    """Mock environment variables for tests."""
    test_env = {
        'ENVIRONMENT': 'test',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'DB_NAME': 'test_db',
        'POLYGON_API_KEY': 'test_polygon_key',
        'TIINGO_API_KEY': 'test_tiingo_key'
    }
    
    with patch.dict(os.environ, test_env):
        yield test_env


@pytest.fixture
def mock_gin_config():
    """Mock gin configuration for tests."""
    with patch('gin.clear_config'), \
         patch('gin.parse_config_file'), \
         patch('gin.get_configurable') as mock_get_configurable:
        
        # Default gin config values
        mock_get_configurable.side_effect = lambda key: {
            'env_type': 'test',
            'database.host': 'localhost',
            'database.port': 5432,
            'database.database': 'test_db',
            'enable_caching': False,
            'use_test_data': True
        }.get(key)
        
        yield mock_get_configurable


# Custom pytest markers for test organization
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "slow: marks tests as slow running")
    config.addinivalue_line("markers", "database: marks tests that require database")
    config.addinivalue_line("markers", "api: marks tests that call external APIs")
    config.addinivalue_line("markers", "regression: marks regression tests")


# Test collection configuration
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on path."""
    for item in items:
        # Add markers based on test file path
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "regression" in str(item.fspath):
            item.add_marker(pytest.mark.regression)
            
        # Add database marker for tests that use database
        if "database" in str(item.fspath) or "dao" in str(item.fspath):
            item.add_marker(pytest.mark.database)
            
        # Add API marker for tests that call external APIs
        if "api" in str(item.fspath) or "agent" in str(item.fspath):
            item.add_marker(pytest.mark.api)


# Performance tracking
@pytest.fixture(autouse=True)
def track_test_performance(request):
    """Track test performance for optimization."""
    start_time = datetime.now()
    yield
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Log slow tests
    if duration > 5.0:  # Tests taking more than 5 seconds
        print(f"\n⚠️  Slow test detected: {request.node.nodeid} took {duration:.2f}s")