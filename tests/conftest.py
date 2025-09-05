from infrastructure.database.test_db_manager import unit_test_db, unit_test_db_clean
import pytest
import os
import gin

# Defensive import handling for LoggingConfig
try:
    from config.logging_config import LoggingConfig
except ImportError:
    try:
        from core.logging.logger_config import LoggingConfig
    except ImportError:
        # Emergency: Create a minimal LoggingConfig class for tests
        from dataclasses import dataclass
        
        @dataclass
        class LoggingConfig:
            """Emergency logging configuration for tests"""
            log_level: str = "INFO"
            log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

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
    
    try:
        gin.parse_config_file(gin_cfg)
    except (FileNotFoundError, IOError) as e:
        # If gin config fails, provide minimal configuration
        print(f"Warning: Could not load gin config {gin_cfg}: {e}")
        # Set up minimal required configuration
        gin.bind_parameter('config.environment.Environment.env_type', 'TEST')
        gin.bind_parameter('config.environment.Environment.db_url', 'postgresql://test_user:test_password@localhost:5432/test_db')
    
    yield
    
    # Clean up after test
    gin.clear_config()
