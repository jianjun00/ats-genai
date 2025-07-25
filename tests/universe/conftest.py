import pytest
from src.config.environment import Environment, EnvironmentType

@pytest.fixture(scope="session")
def test_env():
    """Fixture to provide a test Environment instance with universe_id set."""
    env = Environment(EnvironmentType.TEST)
    # Ensure universe_id is set and matches config, or default to 1
    return env
