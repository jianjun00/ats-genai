"""
Tests for Environment configuration management.
"""

import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from config.environment import Environment, EnvironmentType, get_environment, set_environment


class TestEnvironment:
    """Test cases for Environment class."""
    
    def test_environment_type_enum(self):
        """Test EnvironmentType enum values."""
        assert EnvironmentType.TEST.value == "test"
        assert EnvironmentType.INTEGRATION.value == "intg"
        assert EnvironmentType.PRODUCTION.value == "prod"
    
    @patch.dict(os.environ, {"ENVIRONMENT": "test"})
    def test_detect_environment_from_env_var(self):
        """Test environment detection from ENVIRONMENT variable."""
        env = Environment()
        assert env.env_type == EnvironmentType.TEST
    
    @patch.dict(os.environ, {"ENVIRONMENT": "intg"})
    def test_detect_integration_environment(self):
        """Test integration environment detection."""
        env = Environment()
        assert env.env_type == EnvironmentType.INTEGRATION
    
    @patch.dict(os.environ, {"ENVIRONMENT": "prod"})
    def test_detect_production_environment(self):
        """Test production environment detection."""
        env = Environment()
        assert env.env_type == EnvironmentType.PRODUCTION
    
    @patch.dict(os.environ, {"ENVIRONMENT": "invalid"})
    def test_detect_invalid_environment_defaults_to_test(self):
        """Test that invalid environment defaults to test."""
        env = Environment()
        assert env.env_type == EnvironmentType.TEST
    
    def test_explicit_environment_type(self):
        """Test explicit environment type setting."""
        env = Environment(EnvironmentType.PRODUCTION)
        assert env.env_type == EnvironmentType.PRODUCTION
    
    def test_get_database_url_test_environment(self):
        """Test database URL generation for test environment."""
        env = Environment(EnvironmentType.TEST)
        url = env.get_database_url()
        assert "test_trading_db" in url
        assert "postgresql://" in url
    
    def test_get_database_url_integration_environment(self):
        """Test database URL generation for integration environment."""
        env = Environment(EnvironmentType.INTEGRATION)
        url = env.get_database_url()
        assert "intg_trading_db" in url
    
    def test_get_database_url_production_environment(self):
        """Test database URL generation for production environment."""
        env = Environment(EnvironmentType.PRODUCTION)
        url = env.get_database_url()
        assert "prod_trading_db" in url
    
    def test_get_table_name_with_prefix(self):
        """Test table name prefixing."""
        env = Environment(EnvironmentType.TEST)
        table_name = env.get_table_name("daily_prices")
        assert table_name == "test_daily_prices"
        
        env = Environment(EnvironmentType.INTEGRATION)
        table_name = env.get_table_name("universe_membership")
        assert table_name == "intg_universe_membership"
        
        env = Environment(EnvironmentType.PRODUCTION)
        table_name = env.get_table_name("splits")
        assert table_name == "prod_splits"
    
    def test_get_api_key(self):
        """Test API key retrieval."""
        env = Environment(EnvironmentType.TEST)
        # Test environment should have test API keys
        polygon_key = env.get_api_key("polygon")
        assert polygon_key == "test_polygon_key"
    
    @patch.dict(os.environ, {"POLYGON_API_KEY": "real_polygon_key"})
    def test_get_api_key_with_env_substitution(self):
        """Test API key retrieval with environment variable substitution."""
        env = Environment(EnvironmentType.INTEGRATION)
        polygon_key = env.get_api_key("polygon")
        assert polygon_key == "real_polygon_key"
    
    def test_is_feature_enabled(self):
        """Test feature flag checking."""
        env = Environment(EnvironmentType.TEST)
        # Test environment has strict_validation=true
        assert env.is_feature_enabled("strict_validation") is True
        assert env.is_feature_enabled("enable_caching") is False
    
    def test_get_database_config(self):
        """Test database configuration dictionary."""
        env = Environment(EnvironmentType.TEST)
        config = env.get_database_config()
        
        assert "host" in config
        assert "port" in config
        assert "user" in config
        assert "password" in config
        assert "database" in config
        assert config["database"] == "test_trading_db"
        assert isinstance(config["port"], int)
        assert isinstance(config["min_size"], int)
        assert isinstance(config["max_size"], int)
    
    def test_get_config_value_with_default(self):
        """Test getting configuration value with default."""
        env = Environment(EnvironmentType.TEST)
        value = env.get("nonexistent", "key", "default_value")
        assert value == "default_value"
    
    def test_string_representations(self):
        """Test string representations of Environment."""
        env = Environment(EnvironmentType.TEST)
        assert str(env) == "Environment(test)"
        assert "Environment(type=test" in repr(env)


class TestGlobalEnvironment:
    """Test cases for global environment functions."""
    
    def test_get_environment_singleton(self):
        """Test that get_environment returns singleton instance."""
        env1 = get_environment()
        env2 = get_environment()
        assert env1 is env2
    
    def test_set_environment(self):
        """Test setting global environment type."""
        set_environment(EnvironmentType.PRODUCTION)
        env = get_environment()
        assert env.env_type == EnvironmentType.PRODUCTION
        
        # Reset to test for other tests
        set_environment(EnvironmentType.TEST)


class TestEnvironmentVariableSubstitution:
    """Test environment variable substitution in config values."""
    
    @patch.dict(os.environ, {"TEST_VAR": "test_value"})
    def test_environment_variable_expansion(self):
        """Test that environment variables are expanded in config values."""
        # Create a temporary config file with environment variable
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write("""
[test_section]
test_key=${TEST_VAR}
""")
            temp_config = f.name
        
        try:
            env = Environment(EnvironmentType.TEST)
            # Mock the config loading to use our temp file
            env.config.read(temp_config)
            
            value = env.get("test_section", "test_key")
            assert value == "test_value"
        finally:
            os.unlink(temp_config)


class TestEnvironmentConfiguration:
    """Test environment-specific configuration loading."""
    
    def test_configuration_sections_loaded(self):
        """Test that configuration sections are properly loaded."""
        env = Environment(EnvironmentType.TEST)
        
        # Should have sections from both shared and test configs
        sections = env.config.sections()
        expected_sections = ["database", "api_keys", "logging", "features", "application", "trading"]
        
        for section in expected_sections:
            assert section in sections, f"Section '{section}' not found in config"
    
    def test_shared_config_values(self):
        """Test that shared configuration values are accessible."""
        env = Environment(EnvironmentType.TEST)
        
        app_name = env.get("application", "name")
        assert app_name == "market-forecast-app"
        
        version = env.get("application", "version")
        assert version == "1.0.0"
    
    def test_environment_specific_overrides(self):
        """Test that environment-specific configs override shared ones."""
        test_env = Environment(EnvironmentType.TEST)
        intg_env = Environment(EnvironmentType.INTEGRATION)
        
        # Both should have different database names
        test_db = test_env.get("database", "database")
        intg_db = intg_env.get("database", "database")
        
        assert test_db == "test_trading_db"
        assert intg_db == "intg_trading_db"
        assert test_db != intg_db
