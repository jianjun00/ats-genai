import gin
import pytest

from config.database import Database  # Import first, so Gin registers it
from config.logging_config import LoggingConfig  # Register logging config

@pytest.fixture(autouse=True)
def gin_clear():
    gin.clear_config()
    yield
    gin.clear_config()

def test_database_configurable_instantiation():
    gin.parse_config_file('config/app.gin')
    db = Database(database='test_db_patch')
    assert db.host == 'localhost'
    assert db.port == 5432
    assert db.user == 'postgres'
    assert db.password == 'password'
    assert db.database == 'test_db_patch'
    assert db.pool_min_size == 1
    assert db.pool_max_size == 10
    assert db.command_timeout == 60
    # DSN string check
    assert db.get_database_url() == 'postgresql://postgres:password@localhost:5432/test_db_patch'
