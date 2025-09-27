"""
Conftest for unit tests to provide access to test database fixtures.
"""

# Import the unit_test_db fixture so it's available to all unit tests
from infrastructure.database.test_db_manager import unit_test_db

# The fixture is now available to all tests in this directory