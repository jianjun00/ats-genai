import pytest
import os
import gin

# Defensive import handling for LoggingConfig
LoggingConfig = None
from core.config.logging_config import LoggingConfig
@pytest.fixture(autouse=True, scope="function")
def gin_test_setup(request):
    """Setup gin configuration for tests with proper isolation."""
    # Clear any existing configuration first
    gin.clear_config()

    # Load test configuration
    gin_cfg = os.getenv("GIN_CONFIG", "config/app_test.gin")
    if not os.path.exists(gin_cfg):
        gin_cfg = os.path.join(os.path.dirname(__file__), "..", gin_cfg)
        if not os.path.exists(gin_cfg):
            gin_cfg = "config/app_test.gin"  # fallback

    gin.parse_config_file(gin_cfg)
    yield

    # Clean up after test
    gin.clear_config()
