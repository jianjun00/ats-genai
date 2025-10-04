"""
Gin Configuration Loader with Environment Support

This module provides a standardized way to load Gin configurations
with proper environment inheritance and validation.
"""

import os
import gin
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class GinConfigLoader:
    """
    Centralized Gin configuration loader with environment support.

    Automatically loads base configuration and applies environment-specific overrides.
    """

    def __init__(self, config_root: Optional[Union[str, Path]] = None):
        """
        Initialize the configuration loader.

        Args:
            config_root: Root directory containing config files (default: auto-detect)
        """
        if config_root is None:
            # Auto-detect config directory
            current_file = Path(__file__)
            config_root = current_file.parent.parent.parent.parent / "config"

        self.config_root = Path(config_root)
        self.environments_dir = self.config_root / "environments"
        self._loaded_configs: List[str] = []

        # Ensure config directory exists
        if not self.config_root.exists():
            raise FileNotFoundError(f"Config directory not found: {self.config_root}")

    def load_config(self, environment: Optional[str] = None,
                   additional_configs: Optional[List[str]] = None,
                   clear_existing: bool = True) -> None:
        """
        Load configuration for specified environment.

        Args:
            environment: Target environment (dev, intg, prod, test)
            additional_configs: Additional config files to load
            clear_existing: Whether to clear existing gin configuration
        """
        if clear_existing:
            gin.clear_config()
            self._loaded_configs = []

        # Detect environment if not specified
        if environment is None:
            environment = self._detect_environment()

        # Load base configuration first
        base_config = self.config_root / "base.gin"
        if base_config.exists():
            self._load_gin_file(base_config)
        else:
            logger.warning(f"Base configuration not found: {base_config}")

        # Load environment-specific configuration
        env_config = self.environments_dir / f"{environment}.gin"
        if env_config.exists():
            self._load_gin_file(env_config)
        else:
            # Fallback to legacy config files
            legacy_config = self.config_root / f"app_{environment}.gin"
            if legacy_config.exists():
                logger.warning(f"Using legacy config: {legacy_config}")
                self._load_gin_file(legacy_config)
            else:
                logger.error(f"Environment config not found: {environment}")
                raise FileNotFoundError(f"Config for environment '{environment}' not found")

        # Load additional configurations
        if additional_configs:
            for config_name in additional_configs:
                config_path = self._resolve_config_path(config_name)
                if config_path and config_path.exists():
                    self._load_gin_file(config_path)
                else:
                    logger.warning(f"Additional config not found: {config_name}")

        logger.info(f"Loaded configuration for environment: {environment}")
        logger.debug(f"Loaded configs: {self._loaded_configs}")

    def _load_gin_file(self, config_path: Path) -> None:
        """Load a single gin configuration file."""
        try:
            gin.parse_config_file(str(config_path))
            self._loaded_configs.append(str(config_path))
            logger.debug(f"Loaded gin config: {config_path}")
        except Exception as e:
            logger.error(f"Failed to load gin config {config_path}: {e}")
            raise

    def _resolve_config_path(self, config_name: str) -> Optional[Path]:
        """
        Resolve config name to full path, checking multiple locations.

        Args:
            config_name: Config filename or path

        Returns:
            Resolved Path or None if not found
        """
        # If it's already a path, use it
        if '/' in config_name or '\\' in config_name:
            path = Path(config_name)
            if path.is_absolute():
                return path
            else:
                return self.config_root / path

        # Check common locations
        candidates = [
            self.config_root / config_name,
            self.config_root / f"{config_name}.gin",
            self.environments_dir / config_name,
            self.environments_dir / f"{config_name}.gin",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        return None

    def _detect_environment(self) -> str:
        """
        Auto-detect environment from various sources.

        Returns:
            Detected environment name
        """
        # Check environment variable
        env_var = os.getenv('ENVIRONMENT') or os.getenv('ENV')
        if env_var:
            return env_var.lower()

        # Check if running in Docker
        if os.path.exists('/.dockerenv'):
            # Check for container-specific environment markers
            if os.getenv('ATS_ENV'):
                return os.getenv('ATS_ENV').lower()

            # Default for containers
            return 'intg' if os.getenv('INTG_MODE') else 'dev'

        # Check for test execution
        if 'pytest' in os.environ.get('_', ''):
            return 'test'

        # Default to dev
        return 'dev'

    def get_loaded_configs(self) -> List[str]:
        """Get list of loaded configuration files."""
        return self._loaded_configs.copy()

    def validate_config(self) -> Dict[str, bool]:
        """
        Validate that required configuration values are set.

        Returns:
            Dictionary of validation results
        """
        validation_results = {}

        # Check critical configuration values
        critical_configs = [
            'env_type',
            'database.host',
            'database.database',
        ]

        for config_key in critical_configs:
            try:
                value = gin.get_configurable(config_key)
                validation_results[config_key] = value is not None
            except (ValueError, KeyError):
                validation_results[config_key] = False

        return validation_results


# Global loader instance
_gin_loader: Optional[GinConfigLoader] = None


def get_gin_loader() -> GinConfigLoader:
    """Get global gin configuration loader (singleton)."""
    global _gin_loader
    if _gin_loader is None:
        _gin_loader = GinConfigLoader()
    return _gin_loader


def load_config(environment: Optional[str] = None,
               additional_configs: Optional[List[str]] = None) -> None:
    """
    Convenience function to load gin configuration.

    Args:
        environment: Target environment
        additional_configs: Additional config files to load
    """
    loader = get_gin_loader()
    loader.load_config(environment=environment, additional_configs=additional_configs)


def get_config_info() -> Dict[str, Union[str, List[str]]]:
    """Get information about loaded configuration."""
    loader = get_gin_loader()
    return {
        'loaded_configs': loader.get_loaded_configs(),
        'validation': loader.validate_config()
    }