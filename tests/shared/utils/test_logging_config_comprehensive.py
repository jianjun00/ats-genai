import gin
from shared.utils.logging_config import LoggingConfig

class TestLoggingConfigComprehensive:
    """Comprehensive test coverage for LoggingConfig."""

    def setup_method(self):
        """Clear gin configuration before each test."""
        gin.clear_config()

    def teardown_method(self):
        """Clean up gin configuration after each test."""
        gin.clear_config()

    def test_default_initialization(self):
        """Test LoggingConfig with default values."""
        config = LoggingConfig()

        assert config.log_level == "INFO"
        assert config.log_format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def test_explicit_initialization(self):
        """Test LoggingConfig with explicit values."""
        config = LoggingConfig(
            log_level="DEBUG",
            log_format="%(levelname)s: %(message)s"
        )

        assert config.log_level == "DEBUG"
        assert config.log_format == "%(levelname)s: %(message)s"

    def test_gin_configuration_override(self):
        """Test LoggingConfig with gin configuration override."""
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_level = "ERROR"
        config.logging_config.LoggingConfig.log_format = "%(asctime)s [%(levelname)s] %(message)s"
        """)

        config = LoggingConfig()

        assert config.log_level == "ERROR"
        assert config.log_format == "%(asctime)s [%(levelname)s] %(message)s"

    def test_gin_partial_override(self):
        """Test gin configuration with partial override."""
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_level = "WARNING"
        """)

        config = LoggingConfig()

        assert config.log_level == "WARNING"
        # Should use default format when not overridden
        assert config.log_format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def test_all_standard_log_levels(self):
        """Test LoggingConfig with all standard Python log levels."""
        standard_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in standard_levels:
            gin.clear_config()
            gin.parse_config(f"""
            config.logging_config.LoggingConfig.log_level = "{level}"
            """)

            config = LoggingConfig()
            assert config.log_level == level

    def test_custom_log_levels(self):
        """Test LoggingConfig with custom log level strings."""
        custom_levels = ["TRACE", "VERBOSE", "NOTICE", "FATAL"]

        for level in custom_levels:
            gin.clear_config()
            gin.parse_config(f"""
            config.logging_config.LoggingConfig.log_level = "{level}"
            """)

            config = LoggingConfig()
            assert config.log_level == level

    def test_various_log_formats(self):
        """Test LoggingConfig with various log format patterns."""
        test_formats = [
            "%(message)s",  # Minimal format
            "%(asctime)s - %(message)s",  # Time and message
            "%(name)s:%(lineno)d - %(message)s",  # Name and line number
            "[%(thread)d] %(asctime)s %(levelname)s %(name)s: %(message)s",  # Full format
            "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s"  # Detailed
        ]

        for fmt in test_formats:
            gin.clear_config()
            gin.parse_config(f'''
            config.logging_config.LoggingConfig.log_format = "{fmt}"
            ''')

            config = LoggingConfig()
            assert config.log_format == fmt

    def test_empty_log_format(self):
        """Test LoggingConfig with empty log format."""
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_format = ""
        """)

        config = LoggingConfig()
        assert config.log_format == ""

    def test_special_characters_in_format(self):
        """Test LoggingConfig with special characters in format string."""
        special_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s | 🔍"
        gin.parse_config(f'''
        config.logging_config.LoggingConfig.log_format = "{special_format}"
        ''')

        config = LoggingConfig()
        assert config.log_format == special_format

    def test_multiline_configuration(self):
        """Test gin configuration with multiple parameters."""
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_level = "DEBUG"
        config.logging_config.LoggingConfig.log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        """)

        config = LoggingConfig()
        assert config.log_level == "DEBUG"
        assert config.log_format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def test_gin_configuration_persistence(self):
        """Test that gin configuration persists across multiple instantiations."""
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_level = "ERROR"
        config.logging_config.LoggingConfig.log_format = "PERSISTENT: %(message)s"
        """)

        # Create multiple instances
        config1 = LoggingConfig()
        config2 = LoggingConfig()
        config3 = LoggingConfig()

        # All should have the same gin-configured values
        assert config1.log_level == config2.log_level == config3.log_level == "ERROR"
        assert config1.log_format == config2.log_format == config3.log_format == "PERSISTENT: %(message)s"

    def test_gin_clear_configuration(self):
        """Test that gin.clear_config() resets to defaults."""
        # Set custom configuration
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_level = "CUSTOM"
        config.logging_config.LoggingConfig.log_format = "CUSTOM FORMAT"
        """)

        config_before = LoggingConfig()
        assert config_before.log_level == "CUSTOM"
        assert config_before.log_format == "CUSTOM FORMAT"

        # Clear configuration
        gin.clear_config()

        config_after = LoggingConfig()
        assert config_after.log_level == "INFO"  # Back to default
        assert config_after.log_format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # Back to default

    def test_dataclass_properties(self):
        """Test that LoggingConfig behaves as expected dataclass."""
        config = LoggingConfig()

        # Should be a dataclass instance
        assert hasattr(config, '__dataclass_fields__')

        # Should have the expected fields
        fields = config.__dataclass_fields__
        assert 'log_level' in fields
        assert 'log_format' in fields
        assert len(fields) == 2

    def test_dataclass_equality(self):
        """Test dataclass equality comparison."""
        config1 = LoggingConfig(log_level="INFO", log_format="%(message)s")
        config2 = LoggingConfig(log_level="INFO", log_format="%(message)s")
        config3 = LoggingConfig(log_level="DEBUG", log_format="%(message)s")

        # Same values should be equal
        assert config1 == config2

        # Different values should not be equal
        assert config1 != config3

    def test_dataclass_representation(self):
        """Test dataclass string representation."""
        config = LoggingConfig(log_level="DEBUG", log_format="%(message)s")

        repr_str = repr(config)
        assert "LoggingConfig" in repr_str
        assert "log_level='DEBUG'" in repr_str
        assert "log_format='%(message)s'" in repr_str

    def test_gin_configurable_decorator(self):
        """Test that @gin.configurable decorator is working."""
        # The fact that gin configuration works proves the decorator is applied
        gin.parse_config("""
        config.logging_config.LoggingConfig.log_level = "DECORATOR_TEST"
        """)

        config = LoggingConfig()
        assert config.log_level == "DECORATOR_TEST"

    def test_case_sensitivity(self):
        """Test case sensitivity of log level configuration."""
        test_cases = [
            ("info", "info"),
            ("INFO", "INFO"),
            ("Debug", "Debug"),
            ("ERROR", "ERROR")
        ]

        for input_level, expected_level in test_cases:
            gin.clear_config()
            gin.parse_config(f"""
            config.logging_config.LoggingConfig.log_level = "{input_level}"
            """)

            config = LoggingConfig()
            assert config.log_level == expected_level