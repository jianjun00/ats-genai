from shared.utils.logging_config import LoggingConfig
import gin

def test_logging_config_defaults():
    gin.clear_config()  # Clear any existing gin configuration
    cfg = LoggingConfig()
    assert cfg.log_level == "INFO"
    assert cfg.log_format.startswith("%(")

def test_logging_config_gin_override():
    gin.clear_config()
    gin.parse_config("""
    config.logging_config.LoggingConfig.log_level = "DEBUG"
    config.logging_config.LoggingConfig.log_format = "%(levelname)s - %(message)s"
    """)
    cfg = LoggingConfig()
    assert cfg.log_level == "DEBUG"
    assert cfg.log_format == "%(levelname)s - %(message)s"


