from db.test_db_manager import unit_test_db
import pytest
import os
import gin
from config.logging_config import LoggingConfig

@pytest.fixture(autouse=True)
def gin_test_setup(request):
    gin_cfg = os.getenv("GIN_CONFIG", "config/app.gin")
    gin.parse_config_file(gin_cfg)
    # Default bindings for all required Database parameters
    print("GIN REGISTERED1:", gin.config._CONFIG.keys())
