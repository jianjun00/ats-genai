#!/usr/bin/env python3
"""
Environment-Specific Configuration Management
Manages gin configuration loading based on environment detection
"""

import os
import gin
import logging
from enum import Enum
from typing import Optional, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class Environment(Enum):
    """Environment types for ATS platform"""
    DEVELOPMENT = "dev"
    INTEGRATION = "intg"
    PRODUCTION = "prod"
    TEST = "test"

class EnvironmentConfigLoader:
    """
    Centralized configuration loader for environment-specific gin configs
    """

    def __init__(self, config_root: Optional[str] = None):
        """
        Initialize environment config loader

        Args:
            config_root: Root directory for configuration files (defaults to project config/)
        """
        if config_root is None:
            # Auto-detect config directory relative to this file
            current_dir = Path(__file__).parent
            project_root = current_dir.parent.parent  # Go up from src/config/ to project root
            config_root = str(project_root / "config")

        self.config_root = Path(config_root)
        self.current_env = None
        self.loaded_configs = set()

        logger.info(f"Environment config loader initialized with root: {self.config_root}")

    def detect_environment(self) -> Environment:
        """
        Detect current environment based on various indicators

        Returns:
            Environment enum value
        """
        # Priority order for environment detection:

        # 1. Explicit environment variable
        env_var = os.getenv('ATS_ENVIRONMENT', '').lower()
        if env_var:
            env_mapping = {
                'dev': Environment.DEVELOPMENT,
                'development': Environment.DEVELOPMENT,
                'intg': Environment.INTEGRATION,
                'integration': Environment.INTEGRATION,
                'staging': Environment.INTEGRATION,
                'prod': Environment.PRODUCTION,
                'production': Environment.PRODUCTION,
                'test': Environment.TEST,
                'testing': Environment.TEST
            }
            if env_var in env_mapping:
                logger.info(f"Environment detected from ATS_ENVIRONMENT: {env_var}")
                return env_mapping[env_var]

        # 2. Database connection indicators
        db_host = os.getenv('DB_HOST', '').lower()
        db_port = os.getenv('DB_PORT', '')

        if db_port == '3432' or 'dev' in db_host:
            logger.info("Environment detected from database config: development")
            return Environment.DEVELOPMENT
        elif db_port == '4432' or 'intg' in db_host:
            logger.info("Environment detected from database config: integration")
            return Environment.INTEGRATION
        elif 'prod' in db_host or db_host.endswith('.internal'):
            logger.info("Environment detected from database config: production")
            return Environment.PRODUCTION

        # 3. Container/hostname indicators
        hostname = os.getenv('HOSTNAME', '').lower()
        if any(indicator in hostname for indicator in ['dev', 'development']):
            logger.info("Environment detected from hostname: development")
            return Environment.DEVELOPMENT
        elif any(indicator in hostname for indicator in ['intg', 'integration', 'staging']):
            logger.info("Environment detected from hostname: integration")
            return Environment.INTEGRATION
        elif any(indicator in hostname for indicator in ['prod', 'production']):
            logger.info("Environment detected from hostname: production")
            return Environment.PRODUCTION

        # 4. Testing framework indicators
        if any(test_var in os.environ for test_var in ['PYTEST_CURRENT_TEST', 'TEST_ENV', 'TESTING']):
            logger.info("Environment detected from test indicators: test")
            return Environment.TEST

        # 5. Default fallback
        logger.warning("No environment indicators found, defaulting to development")
        return Environment.DEVELOPMENT

    def get_config_path(self, env: Environment) -> Path:
        """
        Get the gin configuration file path for an environment

        Args:
            env: Environment enum

        Returns:
            Path to gin configuration file

        Raises:
            FileNotFoundError: If configuration file doesn't exist
        """
        config_filename = f"app_{env.value}.gin"
        config_path = self.config_root / config_filename

        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}\n"
                f"Available configs: {list(self.config_root.glob('*.gin'))}"
            )

        return config_path

    def load_environment_config(self, env: Optional[Environment] = None, force_reload: bool = False) -> Environment:
        """
        Load gin configuration for specified or detected environment

        Args:
            env: Environment to load (auto-detects if None)
            force_reload: Whether to clear and reload configuration

        Returns:
            Environment that was loaded

        Raises:
            FileNotFoundError: If configuration file doesn't exist
        """
        # Detect environment if not specified
        if env is None:
            env = self.detect_environment()

        # Clear gin config if force reload or switching environments
        if force_reload or (self.current_env and self.current_env != env):
            gin.clear_config()
            self.loaded_configs.clear()
            logger.info("Gin configuration cleared for environment change")

        # Load configuration if not already loaded
        config_key = env.value
        if config_key not in self.loaded_configs:
            config_path = self.get_config_path(env)

            try:
                gin.parse_config_file(str(config_path))
                self.loaded_configs.add(config_key)
                self.current_env = env

                logger.info(f"Loaded gin configuration for {env.value} environment from {config_path}")

            except Exception as e:
                logger.error(f"Failed to load gin configuration from {config_path}: {e}")
                raise
        else:
            logger.debug(f"Configuration for {env.value} already loaded")

        return env

    def get_current_environment(self) -> Optional[Environment]:
        """
        Get the currently loaded environment

        Returns:
            Current environment or None if not loaded
        """
        return self.current_env

    def get_environment_info(self) -> Dict[str, Any]:
        """
        Get information about the current environment configuration

        Returns:
            Dictionary with environment details
        """
        current_env = self.get_current_environment()

        info = {
            'current_environment': current_env.value if current_env else None,
            'config_root': str(self.config_root),
            'loaded_configs': list(self.loaded_configs),
            'detection_indicators': {
                'ATS_ENVIRONMENT': os.getenv('ATS_ENVIRONMENT'),
                'DB_HOST': os.getenv('DB_HOST'),
                'DB_PORT': os.getenv('DB_PORT'),
                'HOSTNAME': os.getenv('HOSTNAME')
            }
        }

        if current_env:
            try:
                config_path = self.get_config_path(current_env)
                info['config_file'] = str(config_path)
                info['config_exists'] = config_path.exists()
            except FileNotFoundError:
                info['config_file'] = 'NOT_FOUND'
                info['config_exists'] = False

        return info

# Global configuration loader instance
_config_loader = None

def get_config_loader() -> EnvironmentConfigLoader:
    """
    Get the global configuration loader instance

    Returns:
        EnvironmentConfigLoader singleton instance
    """
    global _config_loader
    if _config_loader is None:
        _config_loader = EnvironmentConfigLoader()
    return _config_loader

def load_gin_config(env: Optional[Environment] = None, force_reload: bool = False) -> Environment:
    """
    Convenience function to load gin configuration

    Args:
        env: Environment to load (auto-detects if None)
        force_reload: Whether to clear and reload configuration

    Returns:
        Environment that was loaded
    """
    loader = get_config_loader()
    return loader.load_environment_config(env, force_reload)

def get_current_env() -> Optional[Environment]:
    """
    Convenience function to get current environment

    Returns:
        Current environment or None if not loaded
    """
    loader = get_config_loader()
    return loader.get_current_environment()

def get_env_info() -> Dict[str, Any]:
    """
    Convenience function to get environment information

    Returns:
        Dictionary with environment details
    """
    loader = get_config_loader()
    return loader.get_environment_info()