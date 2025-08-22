from src.db.test_db_manager import unit_test_db, unit_test_db_clean
import pytest
import os
import gin
from config.logging_config import LoggingConfig

@pytest.fixture(autouse=True)
def gin_test_setup(request):
    gin_cfg = os.getenv("GIN_CONFIG", "config/app_test.gin")
    # Ensure we're in the right directory or provide absolute path
    if not os.path.exists(gin_cfg):
        gin_cfg = os.path.join(os.path.dirname(__file__), "..", gin_cfg)
        if not os.path.exists(gin_cfg):
            gin_cfg = "config/app_test.gin"  # fallback
    
    try:
        gin.clear_config()  # Clear any previous config
        gin.parse_config_file(gin_cfg)
    except (FileNotFoundError, IOError):
        # If all else fails, skip gin config for tests
        pass
    # Default bindings for all required Database parameters
    print("GIN REGISTERED1:", gin.config._CONFIG.keys())
