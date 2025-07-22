"""
Environment configuration management for market-forecast-app.

This module provides environment-specific configuration management with support for
test, integration, and production environments. Each environment has its own database
prefixes and configuration settings.
"""

import os
# Automatically load environment variables from .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    import logging
    logging.warning("python-dotenv not installed; .env file will not be loaded automatically.")
import configparser
from pathlib import Path
from typing import Dict, Any, Optional
from enum import Enum
import logging


class EnvironmentType(Enum):
    """Supported environment types."""
    TEST = "test"
    INTEGRATION = "intg"
    PRODUCTION = "prod"


class Environment:
    """
    Environment configuration manager.
    
    Manages environment-specific configurations including database connections,
    API keys, and application settings. Supports test, integration, and production
    environments with proper database prefixing.
    """
    
    def __init__(self, env_type: Optional[EnvironmentType] = None):
        """
        Initialize environment configuration.
        
        Args:
            env_type: Environment type. If None, will be determined from ENVIRONMENT env var.
        """
        self.env_type = env_type or self._detect_environment()
        self.config = configparser.ConfigParser()
        self._load_configurations()
        self.logger = self._setup_logging()
    
    def _detect_environment(self) -> EnvironmentType:
        """Detect environment from ENVIRONMENT environment variable."""
        env_str = os.getenv("ENVIRONMENT", "test").lower()
        try:
            return EnvironmentType(env_str)
        except ValueError:
            logging.warning(f"Unknown environment '{env_str}', defaulting to test")
            return EnvironmentType.TEST
    
    def _load_configurations(self):
        """Load shared and environment-specific configurations."""
        config_dir = Path(__file__).parent.parent.parent / "config"
        
        # Load shared configuration first
        shared_config = config_dir / "shared.conf"
        if shared_config.exists():
            self.config.read(shared_config)
        
        # Load environment-specific configuration
        env_config = config_dir / f"{self.env_type.value}.conf"
        if env_config.exists():
            self.config.read(env_config)
        else:
            raise FileNotFoundError(f"Configuration file not found: {env_config}")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging based on environment configuration."""
        logger = logging.getLogger("market-forecast-app")
        
        level_str = self.get("logging", "level", "INFO")
        level = getattr(logging, level_str.upper(), logging.INFO)
        logger.setLevel(level)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            format_str = self.get("logging", "format", "%(asctime)s - %(levelname)s - %(message)s")
            formatter = logging.Formatter(format_str)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get configuration value with environment variable substitution.
        
        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if key not found
            
        Returns:
            Configuration value with environment variables expanded
        """
        try:
            value = self.config.get(section, key)
            # Expand environment variables
            return os.path.expandvars(value)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default
    
    def get_database_url(self) -> str:
        """
        Get database URL for current environment.
        
        Returns:
            PostgreSQL connection URL with environment-specific database name
        """
        host = self.get("database", "host", "localhost")
        port = self.get("database", "port", "5432")
        user = self.get("database", "user", "postgres")
        password = self.get("database", "password", "password")
        database = self.get("database", "database", f"{self.env_type.value}_trading_db")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    def get_table_name(self, base_table_name: str) -> str:
        """
        Get prefixed table name for current environment.
        
        Args:
            base_table_name: Base table name without prefix
            
        Returns:
            Table name with environment prefix (e.g., test_daily_prices)
        """
        prefix = self.get("database", "prefix", f"{self.env_type.value}_")
        return f"{prefix}{base_table_name}"
    
    def get_api_key(self, service: str) -> Optional[str]:
        """
        Get API key for specified service.
        
        Args:
            service: Service name (e.g., 'polygon', 'tiingo')
            
        Returns:
            API key or None if not found
        """
        key_name = f"{service}_api_key"
        return self.get("api_keys", key_name)
    
    def is_feature_enabled(self, feature: str) -> bool:
        """
        Check if a feature is enabled in current environment.
        
        Args:
            feature: Feature name
            
        Returns:
            True if feature is enabled, False otherwise
        """
        value = self.get("features", feature, "false")
        return value.lower() in ("true", "1", "yes", "on")
    
    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration dictionary.
        
        Returns:
            Dictionary with database connection parameters
        """
        return {
            "host": self.get("database", "host", "localhost"),
            "port": int(self.get("database", "port", "5432")),
            "user": self.get("database", "user", "postgres"),
            "password": self.get("database", "password", "password"),
            "database": self.get("database", "database", f"{self.env_type.value}_trading_db"),
            "min_size": int(self.get("database", "pool_min_size", "1")),
            "max_size": int(self.get("database", "pool_max_size", "10")),
            "command_timeout": int(self.get("database", "command_timeout", "60")),
        }
    
    def __str__(self) -> str:
        """String representation of environment."""
        return f"Environment({self.env_type.value})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"Environment(type={self.env_type.value}, config_sections={list(self.config.sections())})"


# Global environment instance
_env_instance: Optional[Environment] = None


def get_environment() -> Environment:
    """
    Get global environment instance (singleton pattern).
    
    Returns:
        Global Environment instance
    """
    global _env_instance
    if _env_instance is None:
        _env_instance = Environment()
    return _env_instance


def set_environment(env_type: EnvironmentType):
    """
    Set global environment type and reinitialize.
    
    Args:
        env_type: Environment type to set
    """
    global _env_instance
    _env_instance = Environment(env_type)
