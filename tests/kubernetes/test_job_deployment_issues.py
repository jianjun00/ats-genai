#!/usr/bin/env python3
"""
Tests for Kubernetes job deployment issues discovered during data population.
"""

import pytest
import yaml
from unittest.mock import Mock, patch
import os

# Set up test environment
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))


class TestKubernetesJobDeploymentIssues:
    """Test suite for Kubernetes job deployment issues."""

    def test_job_yaml_structure_validation(self):
        """Test validation of Kubernetes job YAML structure."""

        # Valid job structure
        valid_job = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {
                'name': 'test-job',
                'namespace': 'ats-dev'
            },
            'spec': {
                'template': {
                    'spec': {
                        'containers': [{
                            'name': 'test-container',
                            'image': 'python:3.12-slim',
                            'env': [
                                {'name': 'DB_HOST', 'value': 'postgres'},
                                {'name': 'DB_PASSWORD', 'value': 'dev_password'}
                            ]
                        }],
                        'restartPolicy': 'OnFailure'
                    }
                }
            }
        }

        def validate_job_structure(job_spec):
            """Validate Kubernetes job structure."""
            required_fields = ['apiVersion', 'kind', 'metadata', 'spec']
            for field in required_fields:
                if field not in job_spec:
                    return False, f"Missing required field: {field}"

            if job_spec.get('kind') != 'Job':
                return False, "Kind must be 'Job'"

            if 'template' not in job_spec.get('spec', {}):
                return False, "Missing spec.template"

            return True, "Valid"

        is_valid, message = validate_job_structure(valid_job)
        assert is_valid
        assert message == "Valid"

        # Test invalid structure
        invalid_job = {'apiVersion': 'batch/v1'}
        is_valid, message = validate_job_structure(invalid_job)
        assert not is_valid
        assert "Missing required field" in message

    def test_environment_variable_validation(self):
        """Test validation of required environment variables in jobs."""

        def validate_env_vars(container_spec, required_vars):
            """Validate that required environment variables are present."""
            env_vars = container_spec.get('env', [])
            env_names = {var['name'] for var in env_vars}

            missing = set(required_vars) - env_names
            return len(missing) == 0, list(missing)

        # Test with all required vars
        container_with_all_vars = {
            'name': 'test',
            'env': [
                {'name': 'DB_HOST', 'value': 'postgres'},
                {'name': 'DB_PASSWORD', 'value': 'dev_password'},
                {'name': 'DB_NAME', 'value': 'dev_db'}
            ]
        }

        required = ['DB_HOST', 'DB_PASSWORD', 'DB_NAME']
        is_valid, missing = validate_env_vars(container_with_all_vars, required)
        assert is_valid
        assert len(missing) == 0

        # Test with missing vars
        container_missing_vars = {
            'name': 'test',
            'env': [{'name': 'DB_HOST', 'value': 'postgres'}]
        }

        is_valid, missing = validate_env_vars(container_missing_vars, required)
        assert not is_valid
        assert 'DB_PASSWORD' in missing
        assert 'DB_NAME' in missing

    def test_resource_limits_validation(self):
        """Test validation of resource limits for large-scale jobs."""

        def validate_resource_limits(container_spec, min_memory="1Gi", min_cpu="500m"):
            """Validate that resource limits are appropriate for data processing."""
            resources = container_spec.get('resources', {})
            limits = resources.get('limits', {})
            requests = resources.get('requests', {})

            issues = []

            # Check memory limits
            memory_limit = limits.get('memory', '0')
            if not memory_limit or memory_limit == '0':
                issues.append("No memory limit specified")
            elif memory_limit.endswith('Mi') and int(memory_limit[:-2]) < 1024:
                issues.append(f"Memory limit too low: {memory_limit}")

            # Check CPU limits
            cpu_limit = limits.get('cpu', '0')
            if not cpu_limit or cpu_limit == '0':
                issues.append("No CPU limit specified")

            return len(issues) == 0, issues

        # Test adequate resources
        adequate_container = {
            'name': 'test',
            'resources': {
                'requests': {'memory': '1Gi', 'cpu': '500m'},
                'limits': {'memory': '2Gi', 'cpu': '1000m'}
            }
        }

        is_valid, issues = validate_resource_limits(adequate_container)
        assert is_valid
        assert len(issues) == 0

        # Test inadequate resources
        inadequate_container = {
            'name': 'test',
            'resources': {
                'limits': {'memory': '128Mi', 'cpu': '100m'}
            }
        }

        is_valid, issues = validate_resource_limits(inadequate_container)
        assert not is_valid
        assert "Memory limit too low" in str(issues)

    def test_api_key_security_validation(self):
        """Test validation of API key security in job specs."""

        def validate_api_key_security(job_spec):
            """Validate that API keys are not hardcoded in job specs."""
            issues = []

            # Convert job spec to string for pattern matching
            job_str = str(job_spec)

            # Check for hardcoded API keys (common patterns)
            api_key_patterns = [
                'test_api_key_placeholder',  # Polygon key pattern from our jobs
                '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5',  # Tiingo key from our jobs
                'api_key = "',
                'API_KEY = "',
                'token = "'
            ]

            for pattern in api_key_patterns:
                if pattern in job_str:
                    issues.append(f"Potential hardcoded API key found: {pattern[:10]}...")

            return len(issues) == 0, issues

        # Test job with hardcoded API key (bad)
        bad_job = {
            'spec': {
                'template': {
                    'spec': {
                        'containers': [{
                            'command': ['python', '-c', 'API_KEY = "test_api_key_placeholder"']
                        }]
                    }
                }
            }
        }

        is_secure, issues = validate_api_key_security(bad_job)
        assert not is_secure
        assert len(issues) > 0

        # Test job with environment variables (good)
        good_job = {
            'spec': {
                'template': {
                    'spec': {
                        'containers': [{
                            'env': [{'name': 'API_KEY', 'valueFrom': {'secretKeyRef': {'name': 'api-secrets'}}}]
                        }]
                    }
                }
            }
        }

        is_secure, issues = validate_api_key_security(good_job)
        assert is_secure
        assert len(issues) == 0

    def test_job_timeout_configuration(self):
        """Test job timeout and TTL configuration."""

        def validate_job_timeouts(job_spec):
            """Validate job timeout configurations."""
            spec = job_spec.get('spec', {})
            issues = []

            # Check TTL after finished
            ttl = spec.get('ttlSecondsAfterFinished')
            if ttl is None:
                issues.append("No ttlSecondsAfterFinished specified")
            elif ttl < 3600:  # Less than 1 hour
                issues.append(f"TTL too short: {ttl} seconds")
            elif ttl > 604800:  # More than 1 week
                issues.append(f"TTL too long: {ttl} seconds")

            # Check active deadline
            deadline = spec.get('activeDeadlineSeconds')
            if deadline and deadline < 3600:  # Less than 1 hour for data processing
                issues.append(f"Active deadline too short for data processing: {deadline} seconds")

            return len(issues) == 0, issues

        # Test good configuration
        good_job = {
            'spec': {
                'ttlSecondsAfterFinished': 86400,  # 24 hours
                'activeDeadlineSeconds': 7200      # 2 hours
            }
        }

        is_valid, issues = validate_job_timeouts(good_job)
        assert is_valid
        assert len(issues) == 0

        # Test bad configuration
        bad_job = {
            'spec': {
                'ttlSecondsAfterFinished': 60,  # Too short
                'activeDeadlineSeconds': 300    # Too short for data processing
            }
        }

        is_valid, issues = validate_job_timeouts(bad_job)
        assert not is_valid
        assert "TTL too short" in str(issues)
        assert "Active deadline too short" in str(issues)

    def test_database_connectivity_validation(self):
        """Test database connectivity validation in job environment."""

        def validate_db_connectivity_config(env_vars):
            """Validate database connectivity configuration."""
            env_dict = {var['name']: var['value'] for var in env_vars}

            required_db_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
            missing = [var for var in required_db_vars if var not in env_dict]

            issues = []
            if missing:
                issues.append(f"Missing DB environment variables: {missing}")

            # Validate specific values
            if env_dict.get('DB_HOST') == 'localhost':
                issues.append("DB_HOST should not be 'localhost' in Kubernetes")

            if env_dict.get('DB_PORT') != '5432':
                issues.append(f"Unexpected DB_PORT: {env_dict.get('DB_PORT')}")

            return len(issues) == 0, issues

        # Test good configuration
        good_env = [
            {'name': 'DB_HOST', 'value': 'postgres'},
            {'name': 'DB_PORT', 'value': '5432'},
            {'name': 'DB_USER', 'value': 'postgres'},
            {'name': 'DB_PASSWORD', 'value': 'dev_password'},
            {'name': 'DB_NAME', 'value': 'dev_db'}
        ]

        is_valid, issues = validate_db_connectivity_config(good_env)
        assert is_valid
        assert len(issues) == 0

        # Test bad configuration
        bad_env = [
            {'name': 'DB_HOST', 'value': 'localhost'},  # Wrong for K8s
            {'name': 'DB_PORT', 'value': '5433'},       # Wrong port
            {'name': 'DB_USER', 'value': 'postgres'}    # Missing other vars
        ]

        is_valid, issues = validate_db_connectivity_config(bad_env)
        assert not is_valid
        assert "should not be 'localhost'" in str(issues)
        assert "Missing DB environment variables" in str(issues)

    def test_backup_job_validation(self):
        """Test backup job specific validations."""

        def validate_backup_job(job_spec):
            """Validate backup job configuration."""
            issues = []

            # Check if it's a CronJob for scheduled backups
            if job_spec.get('kind') == 'CronJob':
                schedule = job_spec.get('spec', {}).get('schedule')
                if not schedule:
                    issues.append("CronJob missing schedule")
                elif not schedule.startswith('0 '):  # Should run at minute 0
                    issues.append("Backup should run at minute 0 for consistency")

            # Check for volume mounts for backup storage
            if job_spec.get('kind') == 'CronJob':
                containers = job_spec.get('spec', {}).get('jobTemplate', {}).get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])
            else:
                containers = job_spec.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])

            if containers:
                volume_mounts = containers[0].get('volumeMounts', [])
                has_backup_mount = any(mount.get('mountPath') == '/backup' for mount in volume_mounts)
                if not has_backup_mount:
                    issues.append("Backup job should have /backup volume mount")

            return len(issues) == 0, issues

        # Test good backup job
        good_backup = {
            'kind': 'CronJob',
            'spec': {
                'schedule': '0 2 * * *',  # 2 AM daily
                'jobTemplate': {
                    'spec': {
                        'template': {
                            'spec': {
                                'containers': [{
                                    'volumeMounts': [{'mountPath': '/backup'}]
                                }]
                            }
                        }
                    }
                }
            }
        }

        is_valid, issues = validate_backup_job(good_backup)
        assert is_valid
        assert len(issues) == 0

        # Test bad backup job
        bad_backup = {
            'kind': 'CronJob',
            'spec': {
                'schedule': '30 2 * * *',  # Not at minute 0
                'jobTemplate': {
                    'spec': {
                        'template': {
                            'spec': {
                                'containers': [{}]  # No volume mounts
                            }
                        }
                    }
                }
            }
        }

        is_valid, issues = validate_backup_job(bad_backup)
        assert not is_valid
        assert "should run at minute 0" in str(issues)
        assert "should have /backup volume mount" in str(issues)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])