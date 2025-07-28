import pytest
from config.environment import Environment, EnvironmentType

@pytest.fixture
def unit_test_db():
    # Dummy DB URL for tests that only need the value (not a real DB)
    return "postgresql://postgres:password@localhost:5432/test_db"

@pytest.fixture(scope="session")
def test_env():
    """Fixture to provide a test Environment instance with universe_id set."""
    env = Environment(EnvironmentType.TEST)
    # Ensure universe_id is set and matches config, or default to 1
    return env
