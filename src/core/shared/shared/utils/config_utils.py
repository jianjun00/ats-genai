#!/usr/bin/env python3
"""
Configuration Utils - Unified configuration management for ATS platform

Consolidates configuration patterns from vendor services, environment management,
and application settings. Provides standardized configuration loading, validation,
and environment-specific overrides.

USAGE:
======

from src.core.shared.utils.config_utils import (
    ConfigManager,
    load_vendor_config,
    get_api_key_with_fallback,
    validate_configuration
)

# Load vendor-specific configuration
config = load_vendor_config('polygon', environment='intg')

# Get API key with multiple fallback sources
api_key = get_api_key_with_fallback('POLYGON_API_KEY', config, env)

# Validate configuration completeness
is_valid = validate_configuration(config, required_keys=['api_key', 'base_url'])
"""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import gin

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION DATA CLASSES
# =============================================================================

@dataclass
class VendorConfig:
    """Configuration for vendor API integration."""
    name: str
    api_key: str
    base_url: str
    rate_limit: int = 60  # requests per minute
    timeout: int = 30     # seconds
    retry_attempts: int = 3
    retry_delay: int = 1  # seconds
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    
    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return bool(self.name and self.api_key and self.base_url)
    
    def get_auth_header(self) -> Dict[str, str]:
        """Get authentication header for API requests."""
        if self.name.lower() == 'polygon':
            return {'Authorization': f'Bearer {self.api_key}'}
        elif self.name.lower() in ['tiingo', 'eodhd']:
            return {'Authorization': f'Token {self.api_key}'}
        else:
            return {'X-API-Key': self.api_key}

@dataclass  
class DatabaseConfig:
    """Configuration for database connections."""
    host: str
    port: int
    user: str
    password: str
    database: str
    environment: str = 'dev'
    pool_size: int = 10
    max_overflow: int = 20
    connection_timeout: int = 30
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_async_connection_string(self) -> str:
        """Get async PostgreSQL connection string."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return all([self.host, self.port, self.user, self.password, self.database])

@dataclass
class AppConfig:
    """Main application configuration."""
    environment: str
    log_level: str = 'INFO'
    debug: bool = False
    data_path: str = '/data'
    temp_path: str = '/tmp'
    backup_path: str = '/backup'
    max_workers: int = 4
    batch_size: int = 1000
    
    vendors: Dict[str, VendorConfig] = field(default_factory=dict)
    database: Optional[DatabaseConfig] = None
    
    def add_vendor(self, vendor_config: VendorConfig):
        """Add vendor configuration."""
        self.vendors[vendor_config.name] = vendor_config
    
    def get_vendor(self, name: str) -> Optional[VendorConfig]:
        """Get vendor configuration by name."""
        return self.vendors.get(name.lower())
    
    def is_valid(self) -> bool:
        """Check if configuration is valid."""
        return bool(self.environment and (self.database is None or self.database.is_valid()))

# =============================================================================
# CONFIGURATION MANAGER
# =============================================================================

class ConfigManager:
    """Central configuration manager for ATS platform."""
    
    def __init__(self, config_dir: Optional[Union[str, Path]] = None):
        self.config_dir = Path(config_dir or 'config')
        self.app_config: Optional[AppConfig] = None
        self._config_cache: Dict[str, Any] = {}
        
    def load_config(self, environment: str = 'dev') -> AppConfig:
        """
        Load application configuration for environment.
        
        Args:
            environment: Environment name (dev, intg, prod)
            
        Returns:
            AppConfig instance
        """
        cache_key = f"app_config_{environment}"
        if cache_key in self._config_cache:
            return self._config_cache[cache_key]
            
        # Load from environment variables first
        app_config = self._load_from_environment(environment)
        
        # Override with config file if exists
        config_file = self.config_dir / f"app_{environment}.json"
        if config_file.exists():
            file_config = self._load_from_file(config_file)
            app_config = self._merge_configs(app_config, file_config)
            
        # Load Gin config if available
        gin_file = self.config_dir / f"app_{environment}.gin"
        if gin_file.exists():
            try:
                gin.parse_config_file(str(gin_file))
                logger.debug(f"Loaded Gin config: {gin_file}")
            except Exception as e:
                logger.warning(f"Failed to load Gin config {gin_file}: {e}")
        
        self.app_config = app_config
        self._config_cache[cache_key] = app_config
        
        logger.info(f"Loaded configuration for environment: {environment}")
        return app_config
    
    def _load_from_environment(self, environment: str) -> AppConfig:
        """Load configuration from environment variables."""
        # Database configuration
        db_config = None
        if all(key in os.environ for key in ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']):
            db_config = DatabaseConfig(
                host=os.environ['DB_HOST'],
                port=int(os.environ.get('DB_PORT', 5432)),
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD'],
                database=os.environ['DB_NAME'],
                environment=environment
            )
        
        # Application configuration
        app_config = AppConfig(
            environment=environment,
            log_level=os.environ.get('LOG_LEVEL', 'INFO'),
            debug=os.environ.get('DEBUG', 'false').lower() == 'true',
            data_path=os.environ.get('DATA_PATH', '/data'),
            temp_path=os.environ.get('TEMP_PATH', '/tmp'),
            backup_path=os.environ.get('BACKUP_PATH', '/backup'),
            max_workers=int(os.environ.get('MAX_WORKERS', 4)),
            batch_size=int(os.environ.get('BATCH_SIZE', 1000)),
            database=db_config
        )
        
        # Load vendor configurations
        self._load_vendor_configs(app_config)
        
        return app_config
    
    def _load_vendor_configs(self, app_config: AppConfig):
        """Load vendor configurations from environment."""
        vendors = ['polygon', 'tiingo', 'eodhd']
        
        for vendor in vendors:
            api_key = get_api_key_with_fallback(vendor)
            if api_key:
                vendor_config = VendorConfig(
                    name=vendor,
                    api_key=api_key,
                    base_url=self._get_vendor_base_url(vendor),
                    rate_limit=self._get_vendor_rate_limit(vendor),
                    timeout=int(os.environ.get(f'{vendor.upper()}_TIMEOUT', 30)),
                    retry_attempts=int(os.environ.get(f'{vendor.upper()}_RETRY_ATTEMPTS', 3))
                )
                app_config.add_vendor(vendor_config)
    
    def _get_vendor_base_url(self, vendor: str) -> str:
        """Get base URL for vendor API."""
        urls = {
            'polygon': 'https://api.polygon.io',
            'tiingo': 'https://api.tiingo.com',
            'eodhd': 'https://eodhd.com/api'
        }
        return os.environ.get(f'{vendor.upper()}_BASE_URL', urls.get(vendor, ''))
    
    def _get_vendor_rate_limit(self, vendor: str) -> int:
        """Get rate limit for vendor API."""
        default_limits = {
            'polygon': 5,    # Free tier is 5 per minute
            'tiingo': 500,   # More generous
            'eodhd': 20      # Moderate
        }
        env_key = f'{vendor.upper()}_RATE_LIMIT'
        return int(os.environ.get(env_key, default_limits.get(vendor, 60)))
    
    def _load_from_file(self, config_file: Path) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config file {config_file}: {e}")
            return {}
    
    def _merge_configs(self, base_config: AppConfig, file_config: Dict[str, Any]) -> AppConfig:
        """Merge file configuration with base configuration."""
        # This is a simplified merge - in practice you'd want deeper merging
        # For now, just update database config if present
        if 'database' in file_config:
            db_data = file_config['database']
            base_config.database = DatabaseConfig(**db_data)
            
        if 'vendors' in file_config:
            for vendor_name, vendor_data in file_config['vendors'].items():
                vendor_config = VendorConfig(name=vendor_name, **vendor_data)
                base_config.add_vendor(vendor_config)
                
        return base_config

# =============================================================================
# VENDOR CONFIGURATION UTILITIES
# =============================================================================

def load_vendor_config(vendor: str, environment: str = 'dev') -> Optional[VendorConfig]:
    """
    Load configuration for specific vendor.
    
    Consolidates vendor config loading from multiple services.
    
    Args:
        vendor: Vendor name (polygon, tiingo, eodhd)
        environment: Environment name
        
    Returns:
        VendorConfig instance or None
    """
    api_key = get_api_key_with_fallback(vendor)
    if not api_key:
        logger.warning(f"No API key found for vendor: {vendor}")
        return None
        
    config = VendorConfig(
        name=vendor,
        api_key=api_key,
        base_url=_get_vendor_base_url(vendor),
        rate_limit=_get_vendor_rate_limit(vendor),
        timeout=int(os.environ.get(f'{vendor.upper()}_TIMEOUT', 30))
    )
    
    return config if config.is_valid() else None

def get_api_key_with_fallback(
    vendor: str,
    config: Optional[Dict[str, Any]] = None,
    env_instance: Optional[Any] = None
) -> Optional[str]:
    """
    Get API key with multiple fallback sources.
    
    Consolidates API key resolution from vendor services.
    
    Args:
        vendor: Vendor name
        config: Configuration dictionary
        env_instance: Environment instance with get_api_key method
        
    Returns:
        API key string or None
    """
    vendor_upper = vendor.upper()
    
    # Try multiple environment variable patterns
    env_patterns = [
        f'{vendor_upper}_API_KEY',
        f'{vendor_upper}_KEY',
        f'API_KEY_{vendor_upper}',
        f'{vendor.lower()}_api_key'
    ]
    
    for pattern in env_patterns:
        key = os.environ.get(pattern)
        if key and key.strip() and key != 'your_api_key_here':
            return key.strip()
    
    # Try config dictionary
    if config:
        key = config.get('api_key') or config.get(f'{vendor}_api_key')
        if key and key.strip():
            return key.strip()
    
    # Try environment instance
    if env_instance and hasattr(env_instance, 'get_api_key'):
        try:
            key = env_instance.get_api_key(vendor)
            if key and key.strip():
                return key.strip()
        except Exception:
            pass
    
    return None

def _get_vendor_base_url(vendor: str) -> str:
    """Get base URL for vendor API."""
    urls = {
        'polygon': 'https://api.polygon.io',
        'tiingo': 'https://api.tiingo.com',
        'eodhd': 'https://eodhd.com/api'
    }
    return urls.get(vendor.lower(), '')

def _get_vendor_rate_limit(vendor: str) -> int:
    """Get rate limit for vendor API."""
    default_limits = {
        'polygon': 5,    # Free tier is strict
        'tiingo': 500,   # More generous  
        'eodhd': 20      # Moderate
    }
    return default_limits.get(vendor.lower(), 60)

# =============================================================================
# DATABASE CONFIGURATION UTILITIES
# =============================================================================

def load_database_config(environment: str = 'dev') -> Optional[DatabaseConfig]:
    """
    Load database configuration for environment.
    
    Consolidates database config loading from multiple services.
    
    Args:
        environment: Environment name
        
    Returns:
        DatabaseConfig instance or None
    """
    # Environment-specific defaults
    env_defaults = {
        'dev': {
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'dev_password',
            'database': 'dev_db'
        },
        'intg': {
            'host': 'ats-intg-postgres',
            'port': 5432,
            'user': 'postgres', 
            'password': 'intg_password',
            'database': 'intg_db'
        },
        'prod': {
            'host': 'ats-prod-postgres',
            'port': 5432,
            'user': 'postgres',
            'password': os.environ.get('PROD_DB_PASSWORD'),
            'database': 'prod_db'
        }
    }
    
    defaults = env_defaults.get(environment, env_defaults['dev'])
    
    config = DatabaseConfig(
        host=os.environ.get('DB_HOST', defaults['host']),
        port=int(os.environ.get('DB_PORT', defaults['port'])),
        user=os.environ.get('DB_USER', defaults['user']),
        password=os.environ.get('DB_PASSWORD', defaults['password']),
        database=os.environ.get('DB_NAME', defaults['database']),
        environment=environment
    )
    
    return config if config.is_valid() else None

def get_table_name(base_name: str, environment: str = 'dev') -> str:
    """
    Get environment-specific table name.
    
    Args:
        base_name: Base table name
        environment: Environment name
        
    Returns:
        Environment-prefixed table name
    """
    if environment == 'prod':
        return base_name  # Production uses clean names
    else:
        return f"{environment}_{base_name}"

# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================

def validate_configuration(
    config: Union[AppConfig, VendorConfig, DatabaseConfig],
    required_keys: Optional[List[str]] = None
) -> Tuple[bool, List[str]]:
    """
    Validate configuration completeness and correctness.
    
    Args:
        config: Configuration instance to validate
        required_keys: Additional required keys to check
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    if isinstance(config, AppConfig):
        if not config.environment:
            errors.append("Missing environment")
        if config.database and not config.database.is_valid():
            errors.append("Invalid database configuration")
        for vendor_name, vendor_config in config.vendors.items():
            if not vendor_config.is_valid():
                errors.append(f"Invalid vendor configuration: {vendor_name}")
                
    elif isinstance(config, VendorConfig):
        if not config.name:
            errors.append("Missing vendor name")
        if not config.api_key:
            errors.append("Missing API key")
        if not config.base_url:
            errors.append("Missing base URL")
            
    elif isinstance(config, DatabaseConfig):
        if not config.host:
            errors.append("Missing database host")
        if not config.user:
            errors.append("Missing database user")
        if not config.password:
            errors.append("Missing database password")
        if not config.database:
            errors.append("Missing database name")
    
    # Check additional required keys
    if required_keys:
        for key in required_keys:
            if not hasattr(config, key) or not getattr(config, key):
                errors.append(f"Missing required key: {key}")
    
    return len(errors) == 0, errors

def validate_environment_setup(environment: str) -> Tuple[bool, List[str]]:
    """
    Validate that environment is properly configured.
    
    Args:
        environment: Environment name to validate
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    warnings = []
    
    # Check required environment variables
    required_vars = ['PYTHONPATH']
    for var in required_vars:
        if var not in os.environ:
            errors.append(f"Missing environment variable: {var}")
    
    # Check database configuration
    db_config = load_database_config(environment)
    if not db_config:
        errors.append(f"Invalid database configuration for {environment}")
    
    # Check vendor API keys
    vendors = ['polygon', 'tiingo', 'eodhd']
    missing_keys = []
    for vendor in vendors:
        if not get_api_key_with_fallback(vendor):
            missing_keys.append(vendor)
    
    if missing_keys:
        warnings.append(f"Missing API keys for vendors: {missing_keys}")
    
    # Check data directories
    data_paths = ['/data', '/mnt/d/ats-data']
    missing_paths = []
    for path in data_paths:
        if not Path(path).exists():
            missing_paths.append(path)
    
    if missing_paths:
        warnings.append(f"Missing data directories: {missing_paths}")
    
    # Log warnings
    for warning in warnings:
        logger.warning(f"Environment setup warning: {warning}")
    
    return len(errors) == 0, errors

# =============================================================================
# CONFIGURATION CACHING AND REFRESH
# =============================================================================

class ConfigCache:
    """Cache for configuration objects with TTL."""
    
    def __init__(self, ttl_minutes: int = 60):
        self.ttl = timedelta(minutes=ttl_minutes)
        self._cache: Dict[str, Tuple[datetime, Any]] = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached configuration."""
        if key in self._cache:
            timestamp, value = self._cache[key]
            if datetime.now() - timestamp < self.ttl:
                return value
            else:
                del self._cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """Set cached configuration."""
        self._cache[key] = (datetime.now(), value)
    
    def clear(self):
        """Clear all cached configurations."""
        self._cache.clear()
    
    def refresh(self, key: str):
        """Remove specific key from cache to force refresh."""
        if key in self._cache:
            del self._cache[key]

# Global configuration cache
config_cache = ConfigCache()

def get_cached_config(key: str, loader_func: callable, *args, **kwargs) -> Any:
    """
    Get configuration with caching.
    
    Args:
        key: Cache key
        loader_func: Function to load configuration
        *args, **kwargs: Arguments for loader function
        
    Returns:
        Configuration object
    """
    config = config_cache.get(key)
    if config is None:
        config = loader_func(*args, **kwargs)
        if config:
            config_cache.set(key, config)
    return config