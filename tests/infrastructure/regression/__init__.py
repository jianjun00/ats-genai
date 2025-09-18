"""
Regression Test Suite for Critical Issues

This package contains comprehensive regression tests to prevent the recurrence
of major issues that were identified and fixed during development.

Test Categories:

1. test_tiingo_end_date_interpretation.py
   - Prevents Tiingo active stocks from being marked as delisted
   - Tests the fix for 9,834 incorrectly classified instruments
   - Validates major stocks (AAPL, MSFT, etc.) are active

2. test_hardcoded_api_keys_security.py
   - Prevents hardcoded API keys from being committed to codebase
   - Tests environment variable usage patterns
   - Validates security documentation and practices

3. test_database_schema_compatibility.py
   - Prevents database schema compatibility issues
   - Tests table structure matches script expectations
   - Validates column names and data types

Usage:
    # Run all regression tests
    pytest tests/regression/ -v

    # Run specific test category
    pytest tests/regression/test_tiingo_end_date_interpretation.py -v

    # Run with integration tests
    pytest tests/regression/ -v -m integration

These tests should be run:
- Before every deployment
- After any changes to instrument population logic
- After any database schema changes
- After any API key management changes
"""

# Test configuration and shared fixtures can go here
import pytest
import os


def pytest_configure(config):
    """Configure pytest for regression tests"""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may be slower)"
    )


@pytest.fixture(scope="session")
def regression_test_config():
    """Configuration for regression tests"""
    return {
        "db_host": os.getenv('DB_HOST', 'postgres'),
        "db_port": os.getenv('DB_PORT', '5432'),
        "db_user": os.getenv('DB_USER', 'postgres'),
        "db_password": os.getenv('DB_PASSWORD', 'dev_password'),
        "db_name": os.getenv('DB_NAME', 'dev_db'),
        "project_root": "/workspace"
    }