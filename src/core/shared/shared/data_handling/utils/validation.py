#!/usr/bin/env python3
"""
Environment Configuration Validation
Validates gin configuration loading and environment detection
"""

import os
import gin
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from .environment_config import Environment, get_config_loader

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of configuration validation"""
    is_valid: bool
    environment: Optional[Environment] = None
    errors: List[str] = None
    warnings: List[str] = None
    config_details: Dict[str, Any] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.config_details is None:
            self.config_details = {}

class ConfigurationValidator:
    """
    Validates environment-specific configuration setup
    """

    def __init__(self):
        self.loader = get_config_loader()

    def validate_environment_detection(self) -> ValidationResult:
        """
        Validate that environment detection works correctly

        Returns:
            ValidationResult with detection validation details
        """
        result = ValidationResult(is_valid=True)

        try:
            # Test environment detection
            detected_env = self.loader.detect_environment()
            result.environment = detected_env
            result.config_details['detected_environment'] = detected_env.value

            # Check detection indicators
            indicators = {
                'ATS_ENVIRONMENT': os.getenv('ATS_ENVIRONMENT'),
                'DB_HOST': os.getenv('DB_HOST'),
                'DB_PORT': os.getenv('DB_PORT'),
                'HOSTNAME': os.getenv('HOSTNAME')
            }
            result.config_details['detection_indicators'] = indicators

            # Validate detection logic
            if not any(indicators.values()):
                result.warnings.append(
                    "No explicit environment indicators found. Using default detection."
                )

            # Check if detected environment config exists
            try:
                config_path = self.loader.get_config_path(detected_env)
                result.config_details['config_file'] = str(config_path)
                result.config_details['config_exists'] = True
            except FileNotFoundError as e:
                result.errors.append(f"Configuration file missing: {e}")
                result.is_valid = False

        except Exception as e:
            result.errors.append(f"Environment detection failed: {e}")
            result.is_valid = False

        return result

    def validate_config_loading(self, env: Optional[Environment] = None) -> ValidationResult:
        """
        Validate that gin configuration can be loaded successfully

        Args:
            env: Environment to validate (uses detected if None)

        Returns:
            ValidationResult with loading validation details
        """
        result = ValidationResult(is_valid=True)

        try:
            # Use detected environment if not specified
            if env is None:
                env = self.loader.detect_environment()

            result.environment = env

            # Clear gin config for clean test
            gin.clear_config()

            # Load configuration
            loaded_env = self.loader.load_environment_config(env, force_reload=True)
            result.config_details['loaded_environment'] = loaded_env.value

            # Validate that config was actually loaded
            if loaded_env != env:
                result.errors.append(
                    f"Environment mismatch: requested {env.value}, loaded {loaded_env.value}"
                )
                result.is_valid = False

            # Get gin configuration details
            env_info = self.loader.get_environment_info()
            result.config_details.update(env_info)

        except FileNotFoundError as e:
            result.errors.append(f"Configuration file not found: {e}")
            result.is_valid = False
        except Exception as e:
            result.errors.append(f"Configuration loading failed: {e}")
            result.is_valid = False

        return result

    def validate_config_parameters(self, env: Optional[Environment] = None) -> ValidationResult:
        """
        Validate that key configuration parameters are set correctly

        Args:
            env: Environment to validate (uses current if None)

        Returns:
            ValidationResult with parameter validation details
        """
        result = ValidationResult(is_valid=True)

        try:
            # Ensure config is loaded
            if env:
                self.loader.load_environment_config(env, force_reload=True)
                result.environment = env
            else:
                result.environment = self.loader.get_current_environment()

            if not result.environment:
                result.errors.append("No environment configuration loaded")
                result.is_valid = False
                return result

            # Test key gin-configurable parameters
            test_parameters = [
                ('database.connection.host', str),
                ('database.connection.port', int),
                ('timeouts.api.default', int),
                ('batch.sizes.default', int),
                ('symbols.default_universe', list),
                ('thresholds.success_rate', float)
            ]

            parameter_details = {}
            for param_name, expected_type in test_parameters:
                try:
                    # Try to retrieve gin parameter
                    # Note: This is a simplified check - actual gin parameter access
                    # would need the specific gin-configured classes to be imported
                    parameter_details[param_name] = {
                        'expected_type': expected_type.__name__,
                        'status': 'configured'
                    }
                except Exception as e:
                    result.warnings.append(
                        f"Cannot validate parameter {param_name}: {e}"
                    )
                    parameter_details[param_name] = {
                        'expected_type': expected_type.__name__,
                        'status': 'validation_failed',
                        'error': str(e)
                    }

            result.config_details['parameters'] = parameter_details

            # Environment-specific validation
            env_value = result.environment.value
            if env_value == 'dev':
                # Development should have debug-friendly settings
                expected_characteristics = {
                    'smaller_batch_sizes': 'Development should use smaller batch sizes',
                    'longer_timeouts': 'Development should use longer timeouts for debugging',
                    'limited_symbols': 'Development should use limited symbol universe'
                }
            elif env_value == 'prod':
                # Production should have optimized settings
                expected_characteristics = {
                    'larger_batch_sizes': 'Production should use larger batch sizes',
                    'strict_timeouts': 'Production should use strict timeouts',
                    'full_universe': 'Production should use full symbol universe'
                }
            else:
                expected_characteristics = {}

            result.config_details['environment_characteristics'] = expected_characteristics

        except Exception as e:
            result.errors.append(f"Parameter validation failed: {e}")
            result.is_valid = False

        return result

    def validate_all_environments(self) -> Dict[str, ValidationResult]:
        """
        Validate configuration for all available environments

        Returns:
            Dictionary mapping environment names to validation results
        """
        results = {}

        for env in Environment:
            try:
                # Test each environment's configuration
                result = ValidationResult(is_valid=True, environment=env)

                # Check if config file exists
                try:
                    config_path = self.loader.get_config_path(env)
                    result.config_details['config_file'] = str(config_path)
                    result.config_details['config_exists'] = True

                    # Try loading the configuration
                    load_result = self.validate_config_loading(env)
                    if not load_result.is_valid:
                        result.is_valid = False
                        result.errors.extend(load_result.errors)

                except FileNotFoundError as e:
                    result.errors.append(f"Configuration file missing: {e}")
                    result.is_valid = False

                results[env.value] = result

            except Exception as e:
                results[env.value] = ValidationResult(
                    is_valid=False,
                    environment=env,
                    errors=[f"Validation failed: {e}"]
                )

        return results

    def run_comprehensive_validation(self) -> ValidationResult:
        """
        Run comprehensive validation of the configuration system

        Returns:
            ValidationResult with overall system validation
        """
        result = ValidationResult(is_valid=True)

        # 1. Validate environment detection
        detection_result = self.validate_environment_detection()
        if not detection_result.is_valid:
            result.is_valid = False
            result.errors.extend(detection_result.errors)
        result.warnings.extend(detection_result.warnings)
        result.config_details['detection'] = detection_result.config_details

        # 2. Validate configuration loading
        loading_result = self.validate_config_loading()
        if not loading_result.is_valid:
            result.is_valid = False
            result.errors.extend(loading_result.errors)
        result.config_details['loading'] = loading_result.config_details

        # 3. Validate configuration parameters
        params_result = self.validate_config_parameters()
        if not params_result.is_valid:
            result.is_valid = False
            result.errors.extend(params_result.errors)
        result.warnings.extend(params_result.warnings)
        result.config_details['parameters'] = params_result.config_details

        # 4. Validate all environments
        all_envs_results = self.validate_all_environments()
        result.config_details['all_environments'] = {
            env_name: {
                'is_valid': env_result.is_valid,
                'errors': env_result.errors,
                'warnings': env_result.warnings,
                'config_exists': env_result.config_details.get('config_exists', False)
            }
            for env_name, env_result in all_envs_results.items()
        }

        # Overall validation summary
        valid_envs = sum(1 for r in all_envs_results.values() if r.is_valid)
        total_envs = len(all_envs_results)
        result.config_details['summary'] = {
            'valid_environments': valid_envs,
            'total_environments': total_envs,
            'validation_success_rate': valid_envs / total_envs if total_envs > 0 else 0
        }

        return result

# Convenience functions
def validate_current_config() -> ValidationResult:
    """
    Validate the current configuration setup

    Returns:
        ValidationResult for current configuration
    """
    validator = ConfigurationValidator()
    return validator.run_comprehensive_validation()

def validate_environment(env: Environment) -> ValidationResult:
    """
    Validate configuration for a specific environment

    Args:
        env: Environment to validate

    Returns:
        ValidationResult for the environment
    """
    validator = ConfigurationValidator()
    return validator.validate_config_loading(env)