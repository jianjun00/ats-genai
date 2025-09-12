"""
Environment Configuration Validator

This module prevents network misconfigurations by validating environment
setup before starting services.
"""

import os
import subprocess
import socket
import logging
from typing import Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class EnvironmentRequirements:
    """Environment-specific configuration requirements."""
    network: str
    postgres_host: str
    postgres_port: str
    db_name: str
    analytics_port: str
    min_tables: int
    expected_containers: list

# Environment requirements matrix
ENVIRONMENT_REQUIREMENTS = {
    'dev': EnvironmentRequirements(
        network='ats-dev-network',
        postgres_host='ats-dev-postgres',
        postgres_port='5432',
        db_name='dev_db',
        analytics_port='3000',
        min_tables=30,
        expected_containers=['ats-dev-postgres', 'ats-dev-analytics']
    ),
    'intg': EnvironmentRequirements(
        network='ats-intg-network',
        postgres_host='ats-intg-postgres',
        postgres_port='5432',
        db_name='intg_db',
        analytics_port='4000',
        min_tables=50,
        expected_containers=['ats-intg-postgres', 'ats-intg-analytics']
    ),
    'prod': EnvironmentRequirements(
        network='ats-prod-network',
        postgres_host='ats-prod-postgres',
        postgres_port='5432',
        db_name='prod_db',
        analytics_port='4000',
        min_tables=50,
        expected_containers=['ats-prod-postgres', 'ats-prod-analytics']
    )
}

class EnvironmentValidationError(Exception):
    """Raised when environment configuration is invalid."""
    pass

class NetworkValidationError(Exception):
    """Raised when network configuration is invalid."""
    pass

def validate_environment_config(environment: str) -> Dict[str, Any]:
    """
    Validate all environment configuration before starting services.

    Args:
        environment: Target environment (dev/intg/prod)

    Returns:
        Dict with validation results

    Raises:
        EnvironmentValidationError: If validation fails
    """
    logger.info(f"🔍 Validating environment configuration for: {environment}")

    requirements = ENVIRONMENT_REQUIREMENTS.get(environment)
    if not requirements:
        raise EnvironmentValidationError(f"Unknown environment: {environment}")

    validation_results = {
        'environment': environment,
        'network_validation': False,
        'container_validation': False,
        'dns_validation': False,
        'env_vars_validation': False,
        'issues': []
    }

    try:
        # 1. Validate Docker network exists
        _validate_docker_network(requirements.network, validation_results)

        # 2. Validate environment variables
        _validate_environment_variables(requirements, validation_results)

        # 3. Validate DNS resolution
        _validate_dns_resolution(requirements, validation_results)

        # 4. Validate container presence and network assignment
        _validate_container_networks(requirements, validation_results)

        # Overall validation
        all_validations = [
            validation_results['network_validation'],
            validation_results['container_validation'],
            validation_results['dns_validation'],
            validation_results['env_vars_validation']
        ]

        if all(all_validations):
            logger.info(f"✅ Environment validation successful for {environment}")
            validation_results['overall_success'] = True
        else:
            failed_checks = [k for k, v in validation_results.items() if k.endswith('_validation') and not v]
            raise EnvironmentValidationError(
                f"Environment validation failed for {environment}. "
                f"Failed checks: {failed_checks}. Issues: {validation_results['issues']}"
            )

    except subprocess.CalledProcessError as e:
        raise EnvironmentValidationError(f"Docker command failed: {e}")
    except Exception as e:
        raise EnvironmentValidationError(f"Validation error: {e}")

    return validation_results

def _validate_docker_network(network_name: str, results: Dict[str, Any]):
    """Validate Docker network exists."""
    try:
        result = subprocess.run(
            ['docker', 'network', 'inspect', network_name],
            check=True, capture_output=True, text=True
        )
        logger.debug(f"✅ Network {network_name} exists")
        results['network_validation'] = True
    except subprocess.CalledProcessError:
        error_msg = f"Network {network_name} does not exist"
        logger.error(f"❌ {error_msg}")
        results['issues'].append(error_msg)
        results['network_validation'] = False

def _validate_environment_variables(requirements: EnvironmentRequirements, results: Dict[str, Any]):
    """Validate environment variables match requirements."""
    env_checks = {
        'DB_HOST': requirements.postgres_host,
        'DB_PORT': requirements.postgres_port,
        'DB_NAME': requirements.db_name
    }

    issues = []
    for env_key, expected_value in env_checks.items():
        actual_value = os.getenv(env_key)
        if actual_value != expected_value:
            issue = f"{env_key}='{actual_value}' but expected '{expected_value}'"
            issues.append(issue)
            logger.warning(f"⚠️ {issue}")

    if issues:
        results['issues'].extend(issues)
        results['env_vars_validation'] = False
    else:
        logger.debug("✅ Environment variables validation passed")
        results['env_vars_validation'] = True

def _validate_dns_resolution(requirements: EnvironmentRequirements, results: Dict[str, Any]):
    """Validate DNS resolution for database host."""
    try:
        socket.gethostbyname(requirements.postgres_host)
        logger.debug(f"✅ DNS resolution successful for {requirements.postgres_host}")
        results['dns_validation'] = True
    except socket.gaierror as e:
        error_msg = f"Cannot resolve {requirements.postgres_host}: {e}"
        logger.error(f"❌ {error_msg}")
        results['issues'].append(error_msg)
        results['dns_validation'] = False

def _validate_container_networks(requirements: EnvironmentRequirements, results: Dict[str, Any]):
    """Validate containers are running and on correct network."""
    try:
        # Get running containers
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}'],
            check=True, capture_output=True, text=True
        )
        running_containers = result.stdout.strip().split('\n')

        container_issues = []

        for expected_container in requirements.expected_containers:
            if expected_container not in running_containers:
                container_issues.append(f"Container {expected_container} not running")
                continue

            # Check container network
            try:
                inspect_result = subprocess.run(
                    ['docker', 'inspect', expected_container, '--format', '{{json .NetworkSettings.Networks}}'],
                    check=True, capture_output=True, text=True
                )

                import json
                networks = json.loads(inspect_result.stdout.strip())
                container_networks = list(networks.keys())

                if requirements.network not in container_networks:
                    container_issues.append(
                        f"Container {expected_container} on networks {container_networks} "
                        f"but expected {requirements.network}"
                    )
                else:
                    logger.debug(f"✅ Container {expected_container} on correct network {requirements.network}")

            except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
                container_issues.append(f"Failed to inspect {expected_container}: {e}")

        if container_issues:
            results['issues'].extend(container_issues)
            results['container_validation'] = False
            for issue in container_issues:
                logger.error(f"❌ {issue}")
        else:
            logger.debug("✅ Container network validation passed")
            results['container_validation'] = True

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to check container status: {e}"
        logger.error(f"❌ {error_msg}")
        results['issues'].append(error_msg)
        results['container_validation'] = False

def validate_current_environment() -> Dict[str, Any]:
    """Validate the current environment based on ENVIRONMENT variable."""
    environment = os.getenv('ENVIRONMENT', 'dev')
    return validate_environment_config(environment)

if __name__ == '__main__':
    # CLI usage
    import sys

    if len(sys.argv) > 1:
        env = sys.argv[1]
    else:
        env = os.getenv('ENVIRONMENT', 'dev')

    try:
        results = validate_environment_config(env)
        print(f"✅ Environment {env} validation successful")
        exit(0)
    except EnvironmentValidationError as e:
        print(f"❌ Environment {env} validation failed: {e}")
        exit(1)