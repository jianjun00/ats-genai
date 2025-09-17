"""
Secure Configuration Loader - CRITICAL SECURITY COMPONENT

This module ensures that all security-critical constants are loaded from Gin config
and NEVER fall back to hardcoded defaults. It implements fail-fast principles
from CLAUDE.md.

SECURITY PRINCIPLES:
- Fail fast when configuration is missing
- Never use hardcoded passwords or credentials
- Validate all configuration before use
- Log security-relevant configuration loads
"""

import os
import gin
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@gin.configurable
@dataclass
class DatabaseConfig:
    """Database configuration - sourced from Gin config only"""
    default_user: str = gin.REQUIRED
    default_host: str = gin.REQUIRED
    default_port: int = gin.REQUIRED
    connection_timeout: int = gin.REQUIRED
    pool_min_size: int = gin.REQUIRED
    pool_max_size: int = gin.REQUIRED
    command_timeout: int = gin.REQUIRED
    dev_host: str = gin.REQUIRED
    dev_port: int = gin.REQUIRED
    intg_host: str = gin.REQUIRED  
    intg_port: int = gin.REQUIRED

@gin.configurable
@dataclass
class PolygonRateConfig:
    """Polygon API rate limiting - sourced from Gin config only"""
    requests_per_minute: int = gin.REQUIRED
    requests_per_second: float = gin.REQUIRED
    retry_delay_seconds: int = gin.REQUIRED
    max_retries: int = gin.REQUIRED
    timeout_seconds: int = gin.REQUIRED

@gin.configurable
@dataclass
class TiingoRateConfig:
    """Tiingo API rate limiting - sourced from Gin config only"""
    requests_per_hour: int = gin.REQUIRED
    requests_per_minute: int = gin.REQUIRED
    retry_delay_seconds: int = gin.REQUIRED
    max_retries: int = gin.REQUIRED
    timeout_seconds: int = gin.REQUIRED

@gin.configurable
@dataclass
class SystemMonitorConfig:
    """System monitoring - fail-fast configuration"""
    fail_on_db_connection_error: bool = gin.REQUIRED
    fail_on_metric_collection_error: bool = gin.REQUIRED
    max_consecutive_failures: int = gin.REQUIRED
    health_check_timeout_seconds: int = gin.REQUIRED

@gin.configurable
@dataclass
class FileSystemConfig:
    """File system paths - deployment safe"""
    data_root_env_var: str = gin.REQUIRED
    log_root_env_var: str = gin.REQUIRED
    cache_root_env_var: str = gin.REQUIRED
    backup_root_env_var: str = gin.REQUIRED
    default_data_root: str = gin.REQUIRED
    default_log_root: str = gin.REQUIRED
    default_cache_root: str = gin.REQUIRED
    default_backup_root: str = gin.REQUIRED

class SecurityConfigurationError(Exception):
    """Raised when security-critical configuration is missing or invalid"""
    pass

class SecureConfigLoader:
    """
    Secure configuration loader that enforces fail-fast principles
    
    This class ensures that:
    1. All security-critical config is loaded from Gin files
    2. No hardcoded defaults are used for credentials/secrets
    3. Configuration is validated before use
    4. Missing configuration causes immediate failure
    """
    
    def __init__(self, config_dir: str = "/home/jianjun/ats-genai-model/config"):
        self.config_dir = Path(config_dir)
        self._loaded = False
        self._database_config: Optional[DatabaseConfig] = None
        self._polygon_config: Optional[PolygonRateConfig] = None
        self._tiingo_config: Optional[TiingoRateConfig] = None
        self._system_monitor_config: Optional[SystemMonitorConfig] = None
        self._filesystem_config: Optional[FileSystemConfig] = None
        
    def load_critical_configuration(self, gin_files: List[str] = None) -> None:
        """
        Load all critical configuration from Gin files
        
        Args:
            gin_files: List of Gin files to load. If None, loads critical defaults.
            
        Raises:
            SecurityConfigurationError: If any critical config is missing
            FileNotFoundError: If Gin files don't exist
        """
        if gin_files is None:
            gin_files = [
                'security_critical_constants.gin',
                'hardcoded_values.gin'
            ]
            
        # Ensure all files exist
        for gin_file in gin_files:
            gin_path = self.config_dir / gin_file
            if not gin_path.exists():
                raise FileNotFoundError(f"Critical Gin config file not found: {gin_path}")
                
        # Load Gin configuration
        try:
            gin.clear_config()
            for gin_file in gin_files:
                gin_path = str(self.config_dir / gin_file)
                logger.info(f"Loading critical security config from: {gin_path}")
                gin.parse_config_file(gin_path)
                
            # Configure and validate all critical components
            self._configure_database()
            self._configure_rate_limiting() 
            self._configure_system_monitoring()
            self._configure_filesystem()
            
            self._loaded = True
            logger.info("✅ All critical security configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to load security configuration: {e}")
            raise SecurityConfigurationError(f"Failed to load critical configuration: {e}")
    
    def _configure_database(self) -> None:
        """Configure database settings - NO HARDCODED CREDENTIALS"""
        try:
            self._database_config = DatabaseConfig()
            logger.info("✅ Database configuration loaded from Gin config")
        except Exception as e:
            raise SecurityConfigurationError(
                f"CRITICAL: Database configuration missing from Gin files. "
                f"This prevents hardcoded password defaults. Error: {e}"
            )
    
    def _configure_rate_limiting(self) -> None:
        """Configure API rate limiting - NO HARDCODED DEFAULTS"""
        try:
            self._polygon_config = PolygonRateConfig()
            self._tiingo_config = TiingoRateConfig() 
            logger.info("✅ API rate limiting configuration loaded from Gin config")
        except Exception as e:
            raise SecurityConfigurationError(
                f"CRITICAL: API rate limiting configuration missing from Gin files. "
                f"This could lead to vendor API abuse. Error: {e}"
            )
    
    def _configure_system_monitoring(self) -> None:
        """Configure system monitoring - FAIL FAST ENABLED"""
        try:
            self._system_monitor_config = SystemMonitorConfig()
            if not self._system_monitor_config.fail_on_db_connection_error:
                logger.warning("⚠️ Database connection error masking is enabled")
            logger.info("✅ System monitoring configuration loaded from Gin config")
        except Exception as e:
            raise SecurityConfigurationError(
                f"CRITICAL: System monitoring configuration missing from Gin files. "
                f"This could mask critical failures. Error: {e}"
            )
    
    def _configure_filesystem(self) -> None:
        """Configure filesystem paths - DEPLOYMENT SAFE"""
        try:
            self._filesystem_config = FileSystemConfig()
            logger.info("✅ Filesystem configuration loaded from Gin config")
        except Exception as e:
            raise SecurityConfigurationError(
                f"CRITICAL: Filesystem configuration missing from Gin files. "
                f"This could cause deployment failures. Error: {e}"
            )
    
    def get_database_connection_params(self, environment: str = "dev") -> Dict[str, Any]:
        """
        Get database connection parameters for specified environment
        
        Args:
            environment: Environment name (dev, intg, prod)
            
        Returns:
            Database connection parameters
            
        Raises:
            SecurityConfigurationError: If config not loaded or invalid
        """
        if not self._loaded or not self._database_config:
            raise SecurityConfigurationError("Configuration not loaded. Call load_critical_configuration() first.")
            
        # Get password from environment variable - NEVER hardcoded
        password_env_var = f"{environment.upper()}_DB_PASSWORD"
        password = os.getenv(password_env_var)
        if not password:
            raise SecurityConfigurationError(
                f"Database password not found in environment variable: {password_env_var}. "
                f"Set this environment variable to prevent hardcoded password defaults."
            )
        
        # Select host based on environment
        if environment == "dev":
            host = self._database_config.dev_host
            port = self._database_config.dev_port
        elif environment == "intg":
            host = self._database_config.intg_host
            port = self._database_config.intg_port
        else:
            host = self._database_config.default_host
            port = self._database_config.default_port
            
        return {
            'host': host,
            'port': port,
            'user': self._database_config.default_user,
            'password': password,
            'database': f"{environment}_db",
            'command_timeout': self._database_config.command_timeout,
            'min_size': self._database_config.pool_min_size,
            'max_size': self._database_config.pool_max_size
        }
    
    def get_polygon_rate_config(self) -> PolygonRateConfig:
        """Get Polygon API rate limiting configuration"""
        if not self._loaded or not self._polygon_config:
            raise SecurityConfigurationError("Configuration not loaded. Call load_critical_configuration() first.")
        return self._polygon_config
    
    def get_tiingo_rate_config(self) -> TiingoRateConfig:
        """Get Tiingo API rate limiting configuration"""
        if not self._loaded or not self._tiingo_config:
            raise SecurityConfigurationError("Configuration not loaded. Call load_critical_configuration() first.")
        return self._tiingo_config
    
    def get_system_monitor_config(self) -> SystemMonitorConfig:
        """Get system monitoring configuration"""
        if not self._loaded or not self._system_monitor_config:
            raise SecurityConfigurationError("Configuration not loaded. Call load_critical_configuration() first.")
        return self._system_monitor_config
    
    def get_secure_file_path(self, path_type: str) -> str:
        """
        Get secure file path from environment variable with safe default
        
        Args:
            path_type: Type of path (data, log, cache, backup)
            
        Returns:
            Secure file path
        """
        if not self._loaded or not self._filesystem_config:
            raise SecurityConfigurationError("Configuration not loaded. Call load_critical_configuration() first.")
            
        env_var_map = {
            'data': self._filesystem_config.data_root_env_var,
            'log': self._filesystem_config.log_root_env_var, 
            'cache': self._filesystem_config.cache_root_env_var,
            'backup': self._filesystem_config.backup_root_env_var
        }
        
        default_map = {
            'data': self._filesystem_config.default_data_root,
            'log': self._filesystem_config.default_log_root,
            'cache': self._filesystem_config.default_cache_root,
            'backup': self._filesystem_config.default_backup_root
        }
        
        if path_type not in env_var_map:
            raise ValueError(f"Unknown path type: {path_type}")
            
        # Try environment variable first, use configured default if not set
        path = os.getenv(env_var_map[path_type], default_map[path_type])
        logger.debug(f"Resolved {path_type} path to: {path}")
        
        return path

# Global instance for application use
secure_config = SecureConfigLoader()