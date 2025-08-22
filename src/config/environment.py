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
import gin
from pathlib import Path
from typing import Dict, Any, Optional
from enum import Enum
import logging


class EnvironmentType(Enum):
    """Supported environment types."""
    TEST = "test"
    DEV = "dev"
    INTEGRATION = "intg"
    PRODUCTION = "prod"


from signals.indicator_config import IndicatorConfig
from config.logging_config import LoggingConfig

import gin

@gin.configurable
@gin.configurable
class Environment:
    def get(self, key, default=None):
        """
        Get a configuration value by key, supporting both 'section.key' and ('section', 'key') forms for backward compatibility.
        First attempts to retrieve from Gin, then falls back to configparser if available.
        """
        import gin
        # Support both Environment.get('section.key') and Environment.get(section, key)
        if isinstance(key, tuple) and len(key) == 2:
            section, option = key
            full_key = f"{section}.{option}"
        else:
            full_key = key
        try:
            value = gin.query_parameter(full_key)
            if value is not None:
                return value
        except Exception:
            pass
        # Fallback: configparser (if present)
        if hasattr(self, 'config'):
            section, sep, option = full_key.partition('.')
            if sep and self.config.has_section(section) and self.config.has_option(section, option):
                return self.config.get(section, option)
        return default


    """
    Gin-based Environment configuration manager.
    Loads config from Gin config file (e.g., config/app.gin).
    All config values are bound as module-level variables.
    """
    def __init__(self, gin_config_path: Optional[str] = None, env_type: Optional[object] = None, db_url: Optional[str] = None, logging_config: LoggingConfig = LoggingConfig()):
        print(f"[GIN DEBUG] Environment.__init__ received db_url: {db_url}")
        # Accept either a gin config path or an environment type (enum or str)
        config_path = None
        # If gin_config_path is actually an EnvironmentType or str, treat as env_type
        if gin_config_path is not None and not isinstance(gin_config_path, str):
            env_type = gin_config_path
            gin_config_path = None
        # Set self.env_type based on env_type or config
        if env_type is not None:
            if isinstance(env_type, EnvironmentType):
                self.env_type = env_type
            else:
                # Try to coerce to EnvironmentType
                try:
                    self.env_type = EnvironmentType(env_type)
                except Exception:
                    self.env_type = EnvironmentType.TEST
        else:
            # Try to infer from GIN_CONFIG or default
            gin_cfg = gin_config_path or os.getenv("GIN_CONFIG", "config/app.gin")
            if "intg" in gin_cfg:
                self.env_type = EnvironmentType.INTEGRATION
            elif "prod" in gin_cfg:
                self.env_type = EnvironmentType.PRODUCTION
            elif "dev" in gin_cfg:
                self.env_type = EnvironmentType.DEV
            else:
                # Check ENVIRONMENT environment variable
                env_str = os.getenv("ENVIRONMENT", "test").lower()
                try:
                    self.env_type = EnvironmentType(env_str)
                except Exception:
                    self.env_type = EnvironmentType.TEST

        # --- BEGIN PATCH: allow custom gin config path ---
        import gin
        if gin_config_path:
            config_path = gin_config_path
        elif env_type is not None:
            if isinstance(env_type, EnvironmentType):
                env_type_str = env_type.value
            else:
                env_type_str = str(env_type)
            if env_type_str in ("test", "TEST"):
                config_path = "config/app.gin"
            elif env_type_str in ("dev", "DEV"):
                config_path = "config/app_docker.gin"
            elif env_type_str in ("intg", "integration", "INTEGRATION"):
                config_path = "config/app_intg.gin"
            elif env_type_str in ("prod", "production", "PRODUCTION"):
                config_path = "config/app_prod.gin"
            else:
                config_path = "config/app.gin"
        else:
            config_path = os.getenv("GIN_CONFIG", "config/app.gin")
        import logging
        print(f"[GIN DEBUG] Using Gin config: {config_path}, env_type={getattr(self, 'env_type', None)}")
        self.gin_config_path = config_path
        # Import Database before parsing Gin config to register it as a configurable
        from config.database import Database
        from config.polygon import set_polygon_api_key, POLYGON_API_KEY

        import gin
        if config_path and os.environ.get('GIN_LOAD_DEFAULT_CONFIG', '1') == '1':
            if not (hasattr(gin.config, '_CONFIG') and gin.config._CONFIG.get('was_configured', False)):
                gin.parse_config_file(config_path)
        from config.logging_config import LoggingConfig
        self.logging_config = LoggingConfig()
        set_polygon_api_key()  # This will set POLYGON_API_KEY from Gin config
        print(f"[DEBUG] POLYGON_API_KEY after Gin load: {POLYGON_API_KEY}")
        # Make db_url optional: try argument, then env var, then None
        if db_url is None:
            db_url = os.getenv("DATABASE_URL")
            if db_url:
                print(f"[GIN DEBUG] Environment auto-discovered db_url from DATABASE_URL env: {db_url}")
            else:
                print(f"[GIN DEBUG] No db_url provided and DATABASE_URL env not set. Proceeding with empty Database config.")
                
        # Check for individual database credential environment variables
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_name = os.getenv("DB_NAME")
        
        # Set database host based on environment type
        if self.env_type == EnvironmentType.DEV:
            # In dev environment (Kubernetes), use timescaledb service
            if not self.db_host:
                self.db_host = 'timescaledb.ats-dev.svc.cluster.local'
                print(f"[ENV DEBUG] Setting database host to {self.db_host} for DEV environment")
        elif self.env_type == EnvironmentType.INTEGRATION:
            # In integration environment, use timescaledb service in intg namespace
            if not self.db_host:
                self.db_host = 'timescaledb.ats-intg.svc.cluster.local'
                print(f"[ENV DEBUG] Setting database host to {self.db_host} for INTEGRATION environment")
        
        print(f"[ENV DEBUG] Using database credentials from environment variables: host={self.db_host}, port={self.db_port}, user={self.db_user}, password={'*****' if self.db_password else None}, database={self.db_name}")
        self.db_url = db_url
        print(f"[GIN DEBUG] Environment.__init__ received db_url: {db_url}")
        if db_url:
            import re
            m = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.*)", db_url)
            if m:
                user = m.group(1)
                password = m.group(2)
                host = m.group(3)
                port = int(m.group(4))
                database = m.group(5)
                base_database = database
                # Use defaults for pool sizes and timeout, or get from Gin if desired
                pool_min_size = 1
                pool_max_size = 10
                command_timeout = 60
                print(f"[ENV DEBUG] Constructing Database: host={host}, port={port}, user={user}, password=***, database={database}, base_database={base_database}, pool_min_size={pool_min_size}, pool_max_size={pool_max_size}, command_timeout={command_timeout}")
                self.database = Database(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database,
                    base_database=base_database,
                    pool_min_size=pool_min_size,
                    pool_max_size=pool_max_size,
                    command_timeout=command_timeout
                )
            else:
                raise ValueError(f"Could not parse db_url: {db_url}")
        else:
            # Use environment variables if available, otherwise use Gin config
            host = os.getenv("DB_HOST") or None
            port = os.getenv("DB_PORT") or None
            user = os.getenv("DB_USER") or None
            password = os.getenv("DB_PASSWORD") or None
            database = os.getenv("DB_NAME") or None
            base_database = database
            
            # Log what we're using (mask password)
            print(f"[ENV DEBUG] Using database credentials from environment variables: host={host}, port={port}, user={user}, password={'*****' if password else None}, database={database}")
            
            self.database = Database(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                base_database=base_database
            )
        try:
            print(f"[GIN DEBUG] Loaded database config from Gin: host={self.database.host}, db={self.database.database}")
        except Exception as e:
            print(f"[GIN DEBUG] Could not instantiate Database from Gin: {e}")
        self._indicator_config = IndicatorConfig.default_config()
        self.logger = self._setup_logging()

    def get_database_url(self) -> str:
        # Prefer explicit db_url, else build from Database instance
        if self.db_url:
            return self.db_url
        elif hasattr(self, 'database') and self.database:
            return self.database.get_database_url()
        return None

    def get_polygon_api_key(self) -> str:
        from config.polygon import POLYGON_API_KEY
        return POLYGON_API_KEY

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("market-forecast-app")
        level_str = getattr(self.logging_config, "level", "INFO")
        level = getattr(logging, level_str.upper(), logging.INFO)
        logger.setLevel(level)
        if not logger.handlers:
            handler = logging.StreamHandler()
            format_str = getattr(self.logging_config, "format", "%(asctime)s - %(levelname)s - %(message)s")
            formatter = logging.Formatter(format_str)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def get_database_config(self) -> dict:
        return {
            "host": self.database.host,
            "port": self.database.port,
            "user": self.database.user,
            "password": self.database.password,
            "database": self.database.database,
            "min_size": self.database.pool_min_size,
            "max_size": self.database.pool_max_size,
            "command_timeout": self.database.command_timeout,
        }

    @property
    def indicator_config(self) -> IndicatorConfig:
        return self._indicator_config

    @indicator_config.setter
    def indicator_config(self, value: IndicatorConfig):
        self._indicator_config = value

    
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
    
    def get_table_name(self, base_table_name: str, with_prefix: bool = True):
        """
        Get prefixed table name for current environment.
        
        Args:
            base_table_name: The base name of the table
            with_prefix: Whether to include the environment prefix (default: True)
            
        Returns:
            str: The table name with appropriate prefix if with_prefix is True,
                 otherwise returns the base table name
        """
        # If no prefix is requested, return the base table name
        if not with_prefix:
            return base_table_name
            
        # Get the environment prefix
        env_type = getattr(self, "env_type", None)
        if env_type is None:
            prefix = "test"
        else:
            # Handle both enum and string environment types
            prefix = env_type.value if hasattr(env_type, "value") else str(env_type)
            
        # Special case: if we're in test environment, always use 'test_' prefix
        # regardless of the actual env_type value to ensure test isolation
        if os.getenv('PYTEST_CURRENT_TEST'):
            prefix = "test"
            
        # Remove any existing prefix to prevent double-prefixing
        if base_table_name.startswith(f"{prefix}_"):
            return base_table_name
            
        return f"{prefix}_{base_table_name}"

    def get_api_key(self, service: str) -> Optional[str]:
        """
        Get API key for specified service.
        
        Args:
            service: Service name (e.g., 'polygon', 'tiingo')
            
        Returns:
            API key or None if not found
        """
        key_name = f"{service}_api_key"
        return self.get(f"{key_name}")
    
    @property
    def indicator_config(self) -> IndicatorConfig:
        """
        Get or set the indicator configuration for this environment.
        Defaults to IndicatorConfig.default_config() if not explicitly set.
        """
        return self._indicator_config

    @indicator_config.setter
    def indicator_config(self, value: IndicatorConfig):
        self._indicator_config = value

    # --- Duration-related methods migrated from UniverseStateBuilder ---
    def get_base_duration(self) -> 'TimeDuration':
        # Default to 5 minutes if not set in config
        from calendars.time_duration import TimeDuration
        duration_str = self.get('universe.base_duration', '5m')
        return TimeDuration(duration_str)

    def get_target_durations(self) -> 'List[TimeDuration]':
        from calendars.time_duration import TimeDuration
        durations_str = self.get('universe.target_durations', '5m')
        durations = [d.strip() for d in durations_str.split(',')]
        return [TimeDuration(d) for d in durations]

    def get_universe_id(self) -> int:
        return int(self.get('universe.universe_id', '1'))

    def set_target_durations(self, durations: 'List[TimeDuration]'):
        durations_str = ','.join(str(d) for d in durations)
        self.set('universe', 'target_durations', durations_str)
        self._validate_duration_compatibility()

    def _validate_duration_compatibility(self):
        # Example compatibility check (could be extended)
        base = self.get_base_duration()
        targets = self.get_target_durations()
        if any(t < base for t in targets):
            raise ValueError('Target durations must be >= base duration')

    # --- End duration methods ---

    def is_feature_enabled(self, feature: str) -> bool:
        """
        Check if a feature is enabled in current environment.
        
        Args:
            feature: Feature name
            
        Returns:
            True if feature is enabled, False otherwise
        """
        value = self.get(f"features.{feature}", "false")
        return value.lower() in ("true", "1", "yes", "on")
    
    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration dictionary.
        
        Returns:
            Dictionary with database connection parameters
        """
        return {
            "host": self.database.host,
            "port": int(self.database.port),
            "user": self.database.user,
            "password": self.database.password,
            "base_database": self.database.base_database,
            "database": self.database.database,
            "min_size": int(self.database.pool_min_size),
            "max_size": int(self.database.pool_max_size),
            "command_timeout": int(self.database.command_timeout),

        }
    
    def __str__(self) -> str:
        """String representation of environment."""
        return f"Environment({self.env_type.value})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"Environment(type={self.env_type.value}, db_url={self.db_url})"


# Global environment instance for easy access
env = Environment()

