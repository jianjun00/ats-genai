"""
Centralized configuration management for ATS-GenAI.

This module provides a unified way to manage all application configuration
including database connections, API keys, and environment-specific settings.
"""

import os
from typing import Optional, List, Dict, Any
from enum import Enum


class Environment(str, Enum):
    """Supported environments."""
    DEV = "dev"
    TEST = "test"
    INTG = "intg"
    PROD = "prod"


class LogLevel(str, Enum):
    """Supported log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings:
    """
    Centralized application settings.
    
    All configuration is loaded from environment variables with
    validation and type conversion.
    """
    
    def __init__(self):
        """Initialize settings from environment variables."""
        # Environment settings
        self.environment = self._get_environment()
        self.debug = self._get_bool("DEBUG", True)
        
        # Application settings
        self.app_name = os.getenv("APP_NAME", "ATS-GenAI")
        self.app_version = os.getenv("APP_VERSION", "1.0.0")
        self.api_host = os.getenv("API_HOST", "0.0.0.0")
        self.api_port = int(os.getenv("API_PORT", "8000"))
        
        # Database settings
        self.database_host = os.getenv("DB_HOST", "localhost")
        self.database_port = int(os.getenv("DB_PORT", "5432"))
        self.database_name = os.getenv("DB_NAME", "ats_dev")
        self.database_user = os.getenv("DB_USER", "postgres")
        self.database_password = os.getenv("DB_PASSWORD", "postgres")
        self.database_pool_size = int(os.getenv("DB_POOL_SIZE", "20"))
        self.database_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "30"))
        self.database_pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))
        self.database_pool_recycle = int(os.getenv("DB_POOL_RECYCLE", "3600"))
        self.database_echo = self._get_bool("DB_ECHO", False)
        
        # API Keys
        self.polygon_api_key = os.getenv("POLYGON_API_KEY")
        self.tiingo_api_key = os.getenv("TIINGO_API_KEY")
        self.alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.finnhub_api_key = os.getenv("FINNHUB_API_KEY")
        self.fmp_api_key = os.getenv("FMP_API_KEY")
        self.iex_api_key = os.getenv("IEX_API_KEY")
        
        # Logging settings
        self.log_level = LogLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        self.log_format = os.getenv("LOG_FORMAT", "structured")
        self.log_file = os.getenv("LOG_FILE")
        self.log_rotation = self._get_bool("LOG_ROTATION", True)
        self.log_retention_days = int(os.getenv("LOG_RETENTION_DAYS", "30"))
        
        # Market data settings
        self.market_data_batch_size = int(os.getenv("MARKET_DATA_BATCH_SIZE", "1000"))
        self.market_data_retry_attempts = int(os.getenv("MARKET_DATA_RETRY_ATTEMPTS", "3"))
        self.market_data_timeout = int(os.getenv("MARKET_DATA_TIMEOUT", "30"))
        
        # Real-time settings
        self.real_time_enabled = self._get_bool("ENABLE_REAL_TIME", True)
        self.real_time_buffer_size = int(os.getenv("REAL_TIME_BUFFER_SIZE", "10000"))
        self.real_time_flush_interval = int(os.getenv("REAL_TIME_FLUSH_INTERVAL_MS", "100"))
        
        # Cache settings
        self.cache_enabled = self._get_bool("ENABLE_CACHING", True)
        self.cache_ttl = int(os.getenv("CACHE_TTL", "300"))
        self.redis_host = os.getenv("REDIS_HOST")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_password = os.getenv("REDIS_PASSWORD")
        
        # Feature flags
        self.enable_metrics = self._get_bool("ENABLE_METRICS", True)
        self.enable_monitoring = self._get_bool("ENABLE_MONITORING", True)
        self.enable_tracing = self._get_bool("ENABLE_TRACING", False)
        
        # Portfolio settings
        self.portfolio_default_value = float(os.getenv("PORTFOLIO_DEFAULT_VALUE", "200000.0"))
        self.portfolio_max_position_weight = float(os.getenv("PORTFOLIO_MAX_POSITION_WEIGHT", "0.06"))
        self.portfolio_max_leverage = float(os.getenv("PORTFOLIO_MAX_LEVERAGE", "1.8"))
        self.portfolio_transaction_cost_bps = float(os.getenv("PORTFOLIO_TRANSACTION_COST_BPS", "4.0"))
        
        # Risk management settings
        self.risk_max_market_beta = float(os.getenv("RISK_MAX_MARKET_BETA", "0.03"))
        self.risk_max_sector_beta = float(os.getenv("RISK_MAX_SECTOR_BETA", "0.08"))
        self.risk_target_dollar_neutral = self._get_bool("RISK_TARGET_DOLLAR_NEUTRAL", True)
    
    def _get_environment(self) -> Environment:
        """Detect and validate environment."""
        env_name = os.getenv("ENVIRONMENT", "dev").lower()
        
        if env_name in ["development", "dev"]:
            return Environment.DEV
        elif env_name in ["testing", "test"]:
            return Environment.TEST
        elif env_name in ["integration", "intg", "staging"]:
            return Environment.INTG
        elif env_name in ["production", "prod"]:
            return Environment.PROD
        else:
            return Environment.DEV
    
    def _get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean value from environment."""
        value = os.getenv(key, str(default)).lower()
        return value in ("true", "1", "yes", "on")
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEV
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PROD
    
    @property
    def is_testing(self) -> bool:
        """Check if running in test environment."""
        return self.environment == Environment.TEST
    
    @property
    def table_prefix(self) -> str:
        """Get database table prefix for current environment."""
        return f"{self.environment.value}_"
    
    @property
    def database_url(self) -> str:
        """Get complete database URL."""
        return (
            f"postgresql://{self.database_user}:{self.database_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )
    
    @property
    def async_database_url(self) -> str:
        """Get async database URL."""
        return (
            f"postgresql+asyncpg://{self.database_user}:{self.database_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )
    
    def get_table_name(self, base_name: str) -> str:
        """Get environment-prefixed table name."""
        return f"{self.table_prefix}{base_name}"
    
    def get_api_key(self, vendor: str) -> Optional[str]:
        """Get API key for specific vendor."""
        key_mapping = {
            "polygon": self.polygon_api_key,
            "tiingo": self.tiingo_api_key,
            "alpha_vantage": self.alpha_vantage_api_key,
            "finnhub": self.finnhub_api_key,
            "fmp": self.fmp_api_key,
            "iex": self.iex_api_key,
        }
        return key_mapping.get(vendor.lower())
    
    def validate_required_settings(self) -> List[str]:
        """Validate that all required settings are present."""
        errors = []
        
        # Check required database settings
        if not self.database_host:
            errors.append("Missing required database setting: database_host")
        if not self.database_name:
            errors.append("Missing required database setting: database_name")
        if not self.database_user:
            errors.append("Missing required database setting: database_user")
        if not self.database_password:
            errors.append("Missing required database setting: database_password")
        
        # Check at least one API key is present for non-test environments
        if not self.is_testing:
            api_keys = [
                self.polygon_api_key, self.tiingo_api_key, 
                self.alpha_vantage_api_key, self.finnhub_api_key,
                self.fmp_api_key, self.iex_api_key
            ]
            if not any(api_keys):
                errors.append("At least one API key should be configured")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary (excluding sensitive data)."""
        sensitive_fields = {
            "database_password", "polygon_api_key", "tiingo_api_key",
            "alpha_vantage_api_key", "finnhub_api_key", "fmp_api_key",
            "iex_api_key", "redis_password"
        }
        
        result = {}
        for key, value in self.__dict__.items():
            if key not in sensitive_fields:
                if isinstance(value, Enum):
                    result[key] = value.value
                else:
                    result[key] = value
        
        return result


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get global settings instance (singleton pattern)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Reload settings from environment (useful for testing)."""
    global _settings
    _settings = Settings()
    return _settings