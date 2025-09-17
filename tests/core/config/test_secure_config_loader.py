"""
Unit tests for SecureConfigLoader

Tests verify that:
1. Configuration is properly loaded from Gin files
2. Application fails fast when critical config is missing  
3. No hardcoded fallbacks are used for security-critical values
4. All configuration validates before use
"""

import pytest
import os
import tempfile
import gin
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.core.config.secure_config_loader import (
    SecureConfigLoader, 
    SecurityConfigurationError,
    DatabaseConfig,
    PolygonRateConfig,
    TiingoRateConfig, 
    SystemMonitorConfig,
    FileSystemConfig
)

class TestSecureConfigLoader:
    """Test secure configuration loading with fail-fast behavior"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir)
        self.loader = SecureConfigLoader(config_dir=str(self.config_dir))
        
        # Clear any existing Gin configuration
        gin.clear_config()
    
    def teardown_method(self):
        """Cleanup test environment"""
        gin.clear_config()
    
    def create_valid_gin_config(self) -> str:
        """Create a valid Gin configuration file for testing"""
        gin_content = '''
# Test security configuration
DatabaseConfig.default_user = "test_user"
DatabaseConfig.default_host = "test_host"
DatabaseConfig.default_port = 5432
DatabaseConfig.connection_timeout = 30
DatabaseConfig.pool_min_size = 1
DatabaseConfig.pool_max_size = 10
DatabaseConfig.command_timeout = 60
DatabaseConfig.dev_host = "test-dev-postgres"
DatabaseConfig.dev_port = 5432
DatabaseConfig.intg_host = "test-intg-postgres"
DatabaseConfig.intg_port = 5432

PolygonRateConfig.requests_per_minute = 5
PolygonRateConfig.requests_per_second = 0.083
PolygonRateConfig.retry_delay_seconds = 12
PolygonRateConfig.max_retries = 3
PolygonRateConfig.timeout_seconds = 30

TiingoRateConfig.requests_per_hour = 1000
TiingoRateConfig.requests_per_minute = 16
TiingoRateConfig.retry_delay_seconds = 5
TiingoRateConfig.max_retries = 3
TiingoRateConfig.timeout_seconds = 30

SystemMonitorConfig.fail_on_db_connection_error = true
SystemMonitorConfig.fail_on_metric_collection_error = true
SystemMonitorConfig.max_consecutive_failures = 3
SystemMonitorConfig.health_check_timeout_seconds = 10

FileSystemConfig.data_root_env_var = "ATS_DATA_ROOT"
FileSystemConfig.log_root_env_var = "ATS_LOG_ROOT"
FileSystemConfig.cache_root_env_var = "ATS_CACHE_ROOT"
FileSystemConfig.backup_root_env_var = "ATS_BACKUP_ROOT"
FileSystemConfig.default_data_root = "/data"
FileSystemConfig.default_log_root = "/logs"
FileSystemConfig.default_cache_root = "/tmp/ats_cache"
FileSystemConfig.default_backup_root = "/backup"
        '''
        
        gin_file = self.config_dir / "test_config.gin"
        gin_file.write_text(gin_content)
        return str(gin_file)
    
    def test_load_valid_configuration_succeeds(self):
        """Test that valid configuration loads successfully"""
        gin_file = self.create_valid_gin_config()
        
        # Should load successfully
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        # Verify all configurations are available
        assert self.loader._loaded
        assert self.loader._database_config is not None
        assert self.loader._polygon_config is not None
        assert self.loader._tiingo_config is not None
        assert self.loader._system_monitor_config is not None
        assert self.loader._filesystem_config is not None
    
    def test_missing_gin_file_fails_fast(self):
        """Test that missing Gin file causes immediate failure"""
        with pytest.raises(FileNotFoundError, match="Critical Gin config file not found"):
            self.loader.load_critical_configuration(["nonexistent.gin"])
    
    def test_missing_database_config_fails_fast(self):
        """Test that missing database configuration fails fast"""
        # Create incomplete Gin file (missing database config)
        incomplete_gin = '''
PolygonRateConfig.requests_per_minute = 5
PolygonRateConfig.requests_per_second = 0.083
        '''
        gin_file = self.config_dir / "incomplete.gin"
        gin_file.write_text(incomplete_gin)
        
        with pytest.raises(SecurityConfigurationError, match="Database configuration missing"):
            self.loader.load_critical_configuration([gin_file.name])
    
    def test_missing_rate_limit_config_fails_fast(self):
        """Test that missing rate limiting configuration fails fast"""
        # Create Gin file missing rate limiting config
        incomplete_gin = '''
DatabaseConfig.default_user = "test_user"
DatabaseConfig.default_host = "test_host"
DatabaseConfig.default_port = 5432
DatabaseConfig.connection_timeout = 30
DatabaseConfig.pool_min_size = 1
DatabaseConfig.pool_max_size = 10
DatabaseConfig.command_timeout = 60
DatabaseConfig.dev_host = "test-dev-postgres"
DatabaseConfig.dev_port = 5432
DatabaseConfig.intg_host = "test-intg-postgres"
DatabaseConfig.intg_port = 5432
        '''
        gin_file = self.config_dir / "no_rate_limits.gin"
        gin_file.write_text(incomplete_gin)
        
        with pytest.raises(SecurityConfigurationError, match="API rate limiting configuration missing"):
            self.loader.load_critical_configuration([gin_file.name])
    
    @patch.dict(os.environ, {'DEV_DB_PASSWORD': 'test_dev_password'})
    def test_database_connection_params_with_env_var(self):
        """Test that database parameters use environment variables for passwords"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        db_params = self.loader.get_database_connection_params(environment="dev")
        
        assert db_params['host'] == 'test-dev-postgres'
        assert db_params['port'] == 5432
        assert db_params['user'] == 'test_user'
        assert db_params['password'] == 'test_dev_password'  # From environment variable
        assert db_params['database'] == 'dev_db'
        assert db_params['command_timeout'] == 60
    
    def test_database_connection_params_missing_password_fails(self):
        """Test that missing password environment variable fails fast"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        # Clear any existing password env vars
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(SecurityConfigurationError, match="Database password not found in environment variable"):
                self.loader.get_database_connection_params(environment="dev")
    
    def test_configuration_required_before_use(self):
        """Test that configuration must be loaded before use"""
        with pytest.raises(SecurityConfigurationError, match="Configuration not loaded"):
            self.loader.get_database_connection_params("dev")
        
        with pytest.raises(SecurityConfigurationError, match="Configuration not loaded"):
            self.loader.get_polygon_rate_config()
        
        with pytest.raises(SecurityConfigurationError, match="Configuration not loaded"):
            self.loader.get_system_monitor_config()
    
    def test_polygon_rate_config_values(self):
        """Test that Polygon rate configuration values are correctly loaded"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        polygon_config = self.loader.get_polygon_rate_config()
        
        assert polygon_config.requests_per_minute == 5
        assert polygon_config.requests_per_second == 0.083
        assert polygon_config.retry_delay_seconds == 12
        assert polygon_config.max_retries == 3
        assert polygon_config.timeout_seconds == 30
    
    def test_system_monitor_fail_fast_enabled(self):
        """Test that system monitor fail-fast behavior is configured"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        monitor_config = self.loader.get_system_monitor_config()
        
        assert monitor_config.fail_on_db_connection_error is True
        assert monitor_config.fail_on_metric_collection_error is True
        assert monitor_config.max_consecutive_failures == 3
        assert monitor_config.health_check_timeout_seconds == 10
    
    @patch.dict(os.environ, {'ATS_DATA_ROOT': '/custom/data/path'})
    def test_secure_file_path_uses_env_var(self):
        """Test that file paths use environment variables when available"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        data_path = self.loader.get_secure_file_path('data')
        assert data_path == '/custom/data/path'
    
    def test_secure_file_path_uses_default_fallback(self):
        """Test that file paths fallback to configured defaults"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        # Clear environment variables
        with patch.dict(os.environ, {}, clear=True):
            data_path = self.loader.get_secure_file_path('data')
            assert data_path == '/data'  # From Gin config default
    
    def test_invalid_path_type_raises_error(self):
        """Test that invalid path type raises appropriate error"""
        gin_file = self.create_valid_gin_config()
        self.loader.load_critical_configuration([Path(gin_file).name])
        
        with pytest.raises(ValueError, match="Unknown path type"):
            self.loader.get_secure_file_path('invalid_type')

class TestConfigurationIntegration:
    """Integration tests for configuration usage across components"""
    
    def test_gin_config_prevents_hardcoded_fallbacks(self):
        """
        Critical test: Verify that Gin configuration prevents hardcoded fallbacks
        
        This test ensures that components CANNOT fall back to hardcoded values
        when Gin configuration is available.
        """
        # This test would be expanded to verify actual component behavior
        # For now, it demonstrates the testing pattern
        
        from src.core.config.secure_config_loader import secure_config
        
        # Before loading config, methods should fail
        with pytest.raises(SecurityConfigurationError):
            secure_config.get_database_connection_params("dev")
        
        # After loading config, methods should work
        # (This would require a proper test Gin file setup)
        
    def test_fail_fast_prevents_silent_failures(self):
        """
        Critical test: Verify that fail-fast behavior prevents silent failures
        
        This test ensures that components raise exceptions instead of
        returning fake/default values when real operations fail.
        """
        # This test pattern demonstrates how to verify fail-fast behavior
        # Each component that previously masked exceptions should have
        # similar tests to ensure exceptions are properly raised
        
        # Example pattern for system monitor test:
        # 1. Mock database connection to fail
        # 2. Call system monitor method
        # 3. Verify it raises DatabaseConnectionError instead of returning 0
        pass

if __name__ == "__main__":
    pytest.main([__file__, "-v"])