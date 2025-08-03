from db.test_db_manager import unit_test_db_clean
import pytest
import gin
from config.logging_config import LoggingConfig

@pytest.fixture(autouse=True)
def gin_test_setup(request):
    gin.clear_config()
    # Register configurables for Gin (fixes 'No configurable matching' error)
    from config.database import Database
    from config.logging_config import LoggingConfig
    gin.external_configurable(Database, module='config.database')
    gin.external_configurable(LoggingConfig, module='config.logging_config')
    # Default bindings for all required Database parameters
    gin.bind_parameter('Database.host', 'localhost')
    gin.bind_parameter('Database.port', 5432)
    gin.bind_parameter('Database.user', 'postgres')
    gin.bind_parameter('Database.password', 'password')
    gin.bind_parameter('Database.pool_min_size', 1)
    gin.bind_parameter('Database.pool_max_size', 10)
    gin.bind_parameter('Database.command_timeout', 60)
    gin.bind_parameter('LoggingConfig.level', 'INFO')
    gin.bind_parameter('LoggingConfig.format', '%(asctime)s - %(levelname)s - %(message)s')
    # Dynamically bind Database.database to the correct test DB name if unit_test_db is used
    if 'unit_test_db' in getattr(request, 'fixturenames', []):
        # Get the actual DB URL from the fixture
        db_url = request.getfixturevalue('unit_test_db')
        if db_url:
            db_name = db_url.split('/')[-1]
            gin.bind_parameter('Database.database', db_name)
            gin.bind_parameter('Database.base_database', db_name)
    yield
    gin.clear_config()
