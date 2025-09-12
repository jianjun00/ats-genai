"""
Configuration Migration Manager for Service Architecture

Handles configuration file transformation and environment setup for service-based architecture.
Manages service discovery configuration, environment variables, and deployment configurations.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
import json
import yaml
import os
import shutil

logger = logging.getLogger(__name__)


@dataclass
class ConfigMigrationResult:
    """Result of configuration migration operation."""
    config_file: str
    migration_type: str
    status: str  # success, failed, skipped
    changes: List[str]
    backup_path: Optional[str]
    error_message: Optional[str]


@dataclass
class ServiceConfig:
    """Service configuration definition."""
    service_name: str
    service_type: str
    environment_variables: Dict[str, Any]
    dependencies: List[str]
    health_check_config: Dict[str, Any]
    cache_config: Dict[str, Any]
    api_config: Dict[str, Any]


class ConfigMigrator:
    """Manages configuration migration for service architecture transformation."""
    
    def __init__(
        self,
        source_config_dir: str = "config",
        target_config_dir: str = "config/services",
        backup_dir: str = "config/backup"
    ):
        self.source_config_dir = Path(source_config_dir)
        self.target_config_dir = Path(target_config_dir)
        self.backup_dir = Path(backup_dir)
        
        # Service configuration templates
        self.service_config_templates = self._initialize_service_templates()
        
        # Environment mappings
        self.environment_mappings = {
            'dev': 'development',
            'staging': 'staging', 
            'prod': 'production',
            'test': 'testing'
        }
    
    def migrate_all_configurations(
        self,
        target_services: Optional[List[str]] = None,
        create_backup: bool = True
    ) -> List[ConfigMigrationResult]:
        """Migrate all configuration files to service-based architecture."""
        logger.info("Starting comprehensive configuration migration")
        
        results = []
        
        # Create directories
        self._ensure_directories()
        
        # Create backup if requested
        if create_backup:
            self._create_configuration_backup()
        
        # Migrate each service configuration
        services_to_migrate = target_services or list(self.service_config_templates.keys())
        
        for service_name in services_to_migrate:
            logger.info(f"Migrating configuration for service: {service_name}")
            
            try:
                # Generate service configuration
                service_config = self._generate_service_config(service_name)
                
                # Create service config files
                service_results = self._create_service_config_files(service_name, service_config)
                results.extend(service_results)
                
                # Create environment-specific configs
                env_results = self._create_environment_configs(service_name, service_config)
                results.extend(env_results)
                
                # Create Docker compose configuration
                docker_result = self._create_docker_config(service_name, service_config)
                results.append(docker_result)
                
                # Create Kubernetes configuration  
                k8s_result = self._create_kubernetes_config(service_name, service_config)
                results.append(k8s_result)
                
                logger.info(f"Successfully migrated configuration for {service_name}")
                
            except Exception as e:
                logger.error(f"Failed to migrate configuration for {service_name}: {e}")
                results.append(ConfigMigrationResult(
                    config_file=f"{service_name}_config",
                    migration_type="service_config",
                    status="failed",
                    changes=[],
                    backup_path=None,
                    error_message=str(e)
                ))
        
        # Create global service discovery configuration
        discovery_result = self._create_service_discovery_config(services_to_migrate)
        results.append(discovery_result)
        
        # Create API gateway configuration
        gateway_result = self._create_api_gateway_config(services_to_migrate)
        results.append(gateway_result)
        
        # Create monitoring configuration
        monitoring_result = self._create_monitoring_config(services_to_migrate)
        results.append(monitoring_result)
        
        logger.info(f"Configuration migration completed. {len(results)} files processed")
        return results
    
    def migrate_environment_variables(
        self, 
        source_env_file: str = ".env",
        target_services: Optional[List[str]] = None
    ) -> List[ConfigMigrationResult]:
        """Migrate environment variables to service-specific configurations."""
        logger.info("Migrating environment variables to service configs")
        
        results = []
        
        # Load source environment variables
        env_vars = self._load_environment_file(source_env_file)
        if not env_vars:
            logger.warning(f"No environment variables found in {source_env_file}")
            return results
        
        # Categorize environment variables by service
        service_env_vars = self._categorize_environment_variables(env_vars)
        
        # Create service-specific environment files
        services_to_process = target_services or service_env_vars.keys()
        
        for service_name in services_to_process:
            service_vars = service_env_vars.get(service_name, {})
            
            if not service_vars:
                logger.info(f"No environment variables found for service: {service_name}")
                continue
            
            try:
                # Create service environment file
                env_file_path = self.target_config_dir / service_name / f"{service_name}.env"
                env_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(env_file_path, 'w') as f:
                    f.write(f"# Environment variables for {service_name} service\n")
                    f.write(f"# Generated on {datetime.now().isoformat()}\n\n")
                    
                    for key, value in service_vars.items():
                        f.write(f"{key}={value}\n")
                
                # Create environment-specific variants
                for env_name in self.environment_mappings.keys():
                    env_specific_path = self.target_config_dir / service_name / f"{service_name}.{env_name}.env"
                    
                    with open(env_specific_path, 'w') as f:
                        f.write(f"# {env_name.upper()} environment variables for {service_name} service\n")
                        f.write(f"# Generated on {datetime.now().isoformat()}\n\n")
                        
                        for key, value in service_vars.items():
                            # Modify values for specific environments if needed
                            env_value = self._adapt_value_for_environment(key, value, env_name)
                            f.write(f"{key}={env_value}\n")
                
                results.append(ConfigMigrationResult(
                    config_file=str(env_file_path),
                    migration_type="environment_variables",
                    status="success",
                    changes=[f"Created environment config with {len(service_vars)} variables"],
                    backup_path=None,
                    error_message=None
                ))
                
                logger.info(f"Created environment config for {service_name} with {len(service_vars)} variables")
                
            except Exception as e:
                logger.error(f"Failed to create environment config for {service_name}: {e}")
                results.append(ConfigMigrationResult(
                    config_file=f"{service_name}.env",
                    migration_type="environment_variables", 
                    status="failed",
                    changes=[],
                    backup_path=None,
                    error_message=str(e)
                ))
        
        return results
    
    def validate_migrated_configs(self) -> Dict[str, Any]:
        """Validate all migrated configuration files."""
        logger.info("Validating migrated configuration files")
        
        validation_results = {
            'service_configs': [],
            'environment_configs': [],
            'deployment_configs': [],
            'overall_status': 'unknown',
            'issues_found': []
        }
        
        # Validate service configurations
        for service_dir in self.target_config_dir.iterdir():
            if service_dir.is_dir():
                service_name = service_dir.name
                service_validation = self._validate_service_config(service_name)
                validation_results['service_configs'].append(service_validation)
                
                if service_validation['status'] != 'valid':
                    validation_results['issues_found'].extend(service_validation['issues'])
        
        # Validate environment configurations
        env_validation = self._validate_environment_configs()
        validation_results['environment_configs'] = env_validation
        
        # Validate deployment configurations
        deployment_validation = self._validate_deployment_configs()
        validation_results['deployment_configs'] = deployment_validation
        
        # Determine overall status
        if not validation_results['issues_found']:
            validation_results['overall_status'] = 'valid'
        elif len(validation_results['issues_found']) < 5:
            validation_results['overall_status'] = 'warning'
        else:
            validation_results['overall_status'] = 'invalid'
        
        logger.info(f"Configuration validation completed: {validation_results['overall_status']}")
        return validation_results
    
    def generate_migration_report(
        self, 
        migration_results: List[ConfigMigrationResult]
    ) -> Dict[str, Any]:
        """Generate comprehensive migration report."""
        successful_migrations = [r for r in migration_results if r.status == 'success']
        failed_migrations = [r for r in migration_results if r.status == 'failed']
        skipped_migrations = [r for r in migration_results if r.status == 'skipped']
        
        migration_by_type = {}
        for result in migration_results:
            migration_type = result.migration_type
            if migration_type not in migration_by_type:
                migration_by_type[migration_type] = {'success': 0, 'failed': 0, 'skipped': 0}
            migration_by_type[migration_type][result.status] += 1
        
        report = {
            'summary': {
                'total_migrations': len(migration_results),
                'successful': len(successful_migrations),
                'failed': len(failed_migrations),
                'skipped': len(skipped_migrations),
                'success_rate': len(successful_migrations) / len(migration_results) * 100
            },
            'by_type': migration_by_type,
            'successful_files': [r.config_file for r in successful_migrations],
            'failed_files': [
                {
                    'file': r.config_file,
                    'error': r.error_message
                } for r in failed_migrations
            ],
            'backup_locations': list(set(r.backup_path for r in migration_results if r.backup_path)),
            'timestamp': datetime.now().isoformat()
        }
        
        return report
    
    # Private helper methods
    
    def _initialize_service_templates(self) -> Dict[str, ServiceConfig]:
        """Initialize service configuration templates."""
        return {
            'instruments': ServiceConfig(
                service_name='instruments',
                service_type='api_service',
                environment_variables={
                    'SERVICE_NAME': 'instruments',
                    'SERVICE_PORT': '8001',
                    'DATABASE_URL': '${DATABASE_URL}',
                    'REDIS_URL': '${REDIS_URL}',
                    'LOG_LEVEL': 'INFO',
                    'CACHE_TTL_SECONDS': '3600',
                    'MAX_CACHE_SIZE': '1000'
                },
                dependencies=['postgres', 'redis'],
                health_check_config={
                    'endpoint': '/health',
                    'interval_seconds': 30,
                    'timeout_seconds': 10,
                    'retries': 3
                },
                cache_config={
                    'enabled': True,
                    'default_ttl': 3600,
                    'max_size': 1000,
                    'eviction_policy': 'LRU'
                },
                api_config={
                    'version': 'v1',
                    'base_path': '/api/v1/instruments',
                    'cors_enabled': True,
                    'rate_limiting': {
                        'requests_per_minute': 100,
                        'burst_size': 20
                    }
                }
            ),
            'market_data': ServiceConfig(
                service_name='market_data',
                service_type='api_service',
                environment_variables={
                    'SERVICE_NAME': 'market_data',
                    'SERVICE_PORT': '8002',
                    'DATABASE_URL': '${DATABASE_URL}',
                    'REDIS_URL': '${REDIS_URL}',
                    'LOG_LEVEL': 'INFO',
                    'CACHE_TTL_SECONDS': '300',
                    'MAX_CACHE_SIZE': '5000'
                },
                dependencies=['postgres', 'redis'],
                health_check_config={
                    'endpoint': '/health',
                    'interval_seconds': 15,
                    'timeout_seconds': 10,
                    'retries': 3
                },
                cache_config={
                    'enabled': True,
                    'default_ttl': 300,
                    'max_size': 5000,
                    'eviction_policy': 'LRU'
                },
                api_config={
                    'version': 'v1',
                    'base_path': '/api/v1/market-data',
                    'cors_enabled': True,
                    'rate_limiting': {
                        'requests_per_minute': 200,
                        'burst_size': 50
                    }
                }
            ),
            'analytics': ServiceConfig(
                service_name='analytics',
                service_type='web_service',
                environment_variables={
                    'SERVICE_NAME': 'analytics',
                    'SERVICE_PORT': '3000',
                    'DATABASE_URL': '${DATABASE_URL}',
                    'REDIS_URL': '${REDIS_URL}',
                    'LOG_LEVEL': 'INFO'
                },
                dependencies=['postgres', 'redis', 'instruments', 'market_data'],
                health_check_config={
                    'endpoint': '/health',
                    'interval_seconds': 30,
                    'timeout_seconds': 15,
                    'retries': 3
                },
                cache_config={
                    'enabled': True,
                    'default_ttl': 1800,
                    'max_size': 2000,
                    'eviction_policy': 'LRU'
                },
                api_config={
                    'version': 'v1',
                    'base_path': '/analytics',
                    'cors_enabled': True,
                    'static_files': True
                }
            ),
            'user_management': ServiceConfig(
                service_name='user_management',
                service_type='api_service',
                environment_variables={
                    'SERVICE_NAME': 'user_management',
                    'SERVICE_PORT': '8003',
                    'DATABASE_URL': '${DATABASE_URL}',
                    'REDIS_URL': '${REDIS_URL}',
                    'JWT_SECRET': '${JWT_SECRET}',
                    'LOG_LEVEL': 'INFO',
                    'SESSION_TTL_SECONDS': '3600'
                },
                dependencies=['postgres', 'redis'],
                health_check_config={
                    'endpoint': '/health',
                    'interval_seconds': 30,
                    'timeout_seconds': 10,
                    'retries': 3
                },
                cache_config={
                    'enabled': True,
                    'default_ttl': 3600,
                    'max_size': 500,
                    'eviction_policy': 'LRU'
                },
                api_config={
                    'version': 'v1',
                    'base_path': '/api/v1/users',
                    'cors_enabled': True,
                    'authentication_required': True
                }
            )
        }
    
    def _ensure_directories(self):
        """Ensure all required directories exist."""
        directories_to_create = [
            self.target_config_dir,
            self.backup_dir,
            self.target_config_dir / "docker",
            self.target_config_dir / "kubernetes",
            self.target_config_dir / "monitoring"
        ]
        
        for directory in directories_to_create:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")
    
    def _create_configuration_backup(self):
        """Create backup of existing configuration files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"config_backup_{timestamp}"
        
        if self.source_config_dir.exists():
            shutil.copytree(self.source_config_dir, backup_path)
            logger.info(f"Created configuration backup at: {backup_path}")
        else:
            logger.warning(f"Source config directory not found: {self.source_config_dir}")
    
    def _generate_service_config(self, service_name: str) -> ServiceConfig:
        """Generate service configuration from template."""
        template = self.service_config_templates.get(service_name)
        if not template:
            raise ValueError(f"No configuration template found for service: {service_name}")
        
        return template
    
    def _create_service_config_files(
        self, 
        service_name: str, 
        service_config: ServiceConfig
    ) -> List[ConfigMigrationResult]:
        """Create service-specific configuration files."""
        results = []
        service_dir = self.target_config_dir / service_name
        service_dir.mkdir(parents=True, exist_ok=True)
        
        # Create main service configuration (YAML)
        config_file = service_dir / f"{service_name}_config.yaml"
        config_data = {
            'service': {
                'name': service_config.service_name,
                'type': service_config.service_type,
                'dependencies': service_config.dependencies
            },
            'health_check': service_config.health_check_config,
            'cache': service_config.cache_config,
            'api': service_config.api_config
        }
        
        try:
            with open(config_file, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False, indent=2)
            
            results.append(ConfigMigrationResult(
                config_file=str(config_file),
                migration_type="service_yaml_config",
                status="success",
                changes=["Created service YAML configuration"],
                backup_path=None,
                error_message=None
            ))
            
        except Exception as e:
            results.append(ConfigMigrationResult(
                config_file=str(config_file),
                migration_type="service_yaml_config",
                status="failed",
                changes=[],
                backup_path=None,
                error_message=str(e)
            ))
        
        return results
    
    def _create_environment_configs(
        self, 
        service_name: str, 
        service_config: ServiceConfig
    ) -> List[ConfigMigrationResult]:
        """Create environment-specific configuration files."""
        results = []
        service_dir = self.target_config_dir / service_name
        
        for env_name, env_full_name in self.environment_mappings.items():
            try:
                env_config_file = service_dir / f"{service_name}.{env_name}.yaml"
                
                env_config = {
                    'environment': env_full_name,
                    'service': {
                        'name': service_config.service_name,
                        'replicas': 1 if env_name == 'dev' else 2 if env_name == 'staging' else 3
                    },
                    'environment_variables': self._adapt_env_vars_for_environment(
                        service_config.environment_variables, 
                        env_name
                    ),
                    'resources': self._get_resource_config_for_environment(env_name),
                    'monitoring': self._get_monitoring_config_for_environment(env_name)
                }
                
                with open(env_config_file, 'w') as f:
                    yaml.dump(env_config, f, default_flow_style=False, indent=2)
                
                results.append(ConfigMigrationResult(
                    config_file=str(env_config_file),
                    migration_type="environment_config",
                    status="success",
                    changes=[f"Created {env_name} environment configuration"],
                    backup_path=None,
                    error_message=None
                ))
                
            except Exception as e:
                results.append(ConfigMigrationResult(
                    config_file=f"{service_name}.{env_name}.yaml",
                    migration_type="environment_config",
                    status="failed",
                    changes=[],
                    backup_path=None,
                    error_message=str(e)
                ))
        
        return results
    
    def _create_docker_config(
        self, 
        service_name: str, 
        service_config: ServiceConfig
    ) -> ConfigMigrationResult:
        """Create Docker configuration for service."""
        docker_dir = self.target_config_dir / "docker"
        docker_compose_file = docker_dir / f"docker-compose.{service_name}.yml"
        
        try:
            docker_config = {
                'version': '3.8',
                'services': {
                    service_name: {
                        'image': f'ats/{service_name}:latest',
                        'container_name': f'ats-{service_name}',
                        'ports': [
                            f"{service_config.environment_variables.get('SERVICE_PORT', '8000')}:{service_config.environment_variables.get('SERVICE_PORT', '8000')}"
                        ],
                        'environment': service_config.environment_variables,
                        'depends_on': service_config.dependencies,
                        'networks': ['ats-network'],
                        'volumes': [
                            './logs:/logs',
                            './data:/data'
                        ],
                        'restart': 'unless-stopped',
                        'healthcheck': {
                            'test': f"curl -f http://localhost:{service_config.environment_variables.get('SERVICE_PORT', '8000')}{service_config.health_check_config['endpoint']} || exit 1",
                            'interval': f"{service_config.health_check_config['interval_seconds']}s",
                            'timeout': f"{service_config.health_check_config['timeout_seconds']}s",
                            'retries': service_config.health_check_config['retries'],
                            'start_period': '40s'
                        }
                    }
                },
                'networks': {
                    'ats-network': {
                        'driver': 'bridge'
                    }
                }
            }
            
            with open(docker_compose_file, 'w') as f:
                yaml.dump(docker_config, f, default_flow_style=False, indent=2)
            
            return ConfigMigrationResult(
                config_file=str(docker_compose_file),
                migration_type="docker_config",
                status="success",
                changes=["Created Docker Compose configuration"],
                backup_path=None,
                error_message=None
            )
            
        except Exception as e:
            return ConfigMigrationResult(
                config_file=str(docker_compose_file),
                migration_type="docker_config",
                status="failed",
                changes=[],
                backup_path=None,
                error_message=str(e)
            )
    
    def _create_kubernetes_config(
        self, 
        service_name: str, 
        service_config: ServiceConfig
    ) -> ConfigMigrationResult:
        """Create Kubernetes configuration for service."""
        k8s_dir = self.target_config_dir / "kubernetes"
        k8s_file = k8s_dir / f"{service_name}-deployment.yaml"
        
        try:
            k8s_config = {
                'apiVersion': 'apps/v1',
                'kind': 'Deployment',
                'metadata': {
                    'name': f'{service_name}-deployment',
                    'labels': {
                        'app': service_name,
                        'tier': 'backend'
                    }
                },
                'spec': {
                    'replicas': 2,
                    'selector': {
                        'matchLabels': {
                            'app': service_name
                        }
                    },
                    'template': {
                        'metadata': {
                            'labels': {
                                'app': service_name
                            }
                        },
                        'spec': {
                            'containers': [
                                {
                                    'name': service_name,
                                    'image': f'ats/{service_name}:latest',
                                    'ports': [
                                        {
                                            'containerPort': int(service_config.environment_variables.get('SERVICE_PORT', '8000'))
                                        }
                                    ],
                                    'env': [
                                        {'name': k, 'value': str(v)} 
                                        for k, v in service_config.environment_variables.items()
                                    ],
                                    'livenessProbe': {
                                        'httpGet': {
                                            'path': service_config.health_check_config['endpoint'],
                                            'port': int(service_config.environment_variables.get('SERVICE_PORT', '8000'))
                                        },
                                        'initialDelaySeconds': 30,
                                        'periodSeconds': service_config.health_check_config['interval_seconds']
                                    }
                                }
                            ]
                        }
                    }
                }
            }
            
            with open(k8s_file, 'w') as f:
                yaml.dump(k8s_config, f, default_flow_style=False, indent=2)
            
            return ConfigMigrationResult(
                config_file=str(k8s_file),
                migration_type="kubernetes_config",
                status="success",
                changes=["Created Kubernetes deployment configuration"],
                backup_path=None,
                error_message=None
            )
            
        except Exception as e:
            return ConfigMigrationResult(
                config_file=str(k8s_file),
                migration_type="kubernetes_config",
                status="failed",
                changes=[],
                backup_path=None,
                error_message=str(e)
            )
    
    def _create_service_discovery_config(
        self, 
        services: List[str]
    ) -> ConfigMigrationResult:
        """Create service discovery configuration."""
        discovery_file = self.target_config_dir / "service_discovery.yaml"
        
        try:
            service_endpoints = {}
            for service_name in services:
                template = self.service_config_templates.get(service_name)
                if template:
                    service_endpoints[service_name] = {
                        'host': f'{service_name}-service',
                        'port': int(template.environment_variables.get('SERVICE_PORT', '8000')),
                        'health_check': template.health_check_config['endpoint'],
                        'type': template.service_type
                    }
            
            discovery_config = {
                'service_discovery': {
                    'strategy': 'dns',
                    'services': service_endpoints,
                    'health_check_interval': 30,
                    'failure_threshold': 3
                }
            }
            
            with open(discovery_file, 'w') as f:
                yaml.dump(discovery_config, f, default_flow_style=False, indent=2)
            
            return ConfigMigrationResult(
                config_file=str(discovery_file),
                migration_type="service_discovery_config",
                status="success",
                changes=[f"Created service discovery config for {len(services)} services"],
                backup_path=None,
                error_message=None
            )
            
        except Exception as e:
            return ConfigMigrationResult(
                config_file=str(discovery_file),
                migration_type="service_discovery_config",
                status="failed",
                changes=[],
                backup_path=None,
                error_message=str(e)
            )
    
    def _create_api_gateway_config(
        self, 
        services: List[str]
    ) -> ConfigMigrationResult:
        """Create API gateway configuration."""
        gateway_file = self.target_config_dir / "api_gateway.yaml"
        
        try:
            routes = []
            for service_name in services:
                template = self.service_config_templates.get(service_name)
                if template and template.service_type == 'api_service':
                    routes.append({
                        'path': template.api_config['base_path'],
                        'service': service_name,
                        'methods': ['GET', 'POST', 'PUT', 'DELETE'],
                        'rate_limiting': template.api_config.get('rate_limiting', {})
                    })
            
            gateway_config = {
                'api_gateway': {
                    'listen_port': 8080,
                    'cors': {
                        'enabled': True,
                        'origins': ['*'],
                        'methods': ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
                    },
                    'routes': routes,
                    'middleware': [
                        'cors',
                        'rate_limiting',
                        'logging',
                        'metrics'
                    ]
                }
            }
            
            with open(gateway_file, 'w') as f:
                yaml.dump(gateway_config, f, default_flow_style=False, indent=2)
            
            return ConfigMigrationResult(
                config_file=str(gateway_file),
                migration_type="api_gateway_config",
                status="success",
                changes=[f"Created API gateway config with {len(routes)} routes"],
                backup_path=None,
                error_message=None
            )
            
        except Exception as e:
            return ConfigMigrationResult(
                config_file=str(gateway_file),
                migration_type="api_gateway_config",
                status="failed",
                changes=[],
                backup_path=None,
                error_message=str(e)
            )
    
    def _create_monitoring_config(
        self, 
        services: List[str]
    ) -> ConfigMigrationResult:
        """Create monitoring configuration."""
        monitoring_file = self.target_config_dir / "monitoring" / "prometheus.yaml"
        
        try:
            scrape_configs = []
            for service_name in services:
                template = self.service_config_templates.get(service_name)
                if template:
                    scrape_configs.append({
                        'job_name': f'{service_name}-metrics',
                        'static_configs': [
                            {
                                'targets': [
                                    f'{service_name}-service:{template.environment_variables.get("SERVICE_PORT", "8000")}'
                                ]
                            }
                        ],
                        'scrape_interval': '30s',
                        'metrics_path': '/metrics'
                    })
            
            monitoring_config = {
                'global': {
                    'scrape_interval': '15s',
                    'evaluation_interval': '15s'
                },
                'scrape_configs': scrape_configs
            }
            
            with open(monitoring_file, 'w') as f:
                yaml.dump(monitoring_config, f, default_flow_style=False, indent=2)
            
            return ConfigMigrationResult(
                config_file=str(monitoring_file),
                migration_type="monitoring_config",
                status="success",
                changes=[f"Created monitoring config for {len(services)} services"],
                backup_path=None,
                error_message=None
            )
            
        except Exception as e:
            return ConfigMigrationResult(
                config_file=str(monitoring_file),
                migration_type="monitoring_config",
                status="failed",
                changes=[],
                backup_path=None,
                error_message=str(e)
            )
    
    def _load_environment_file(self, env_file: str) -> Dict[str, str]:
        """Load environment variables from file."""
        env_vars = {}
        env_path = Path(env_file)
        
        if not env_path.exists():
            logger.warning(f"Environment file not found: {env_file}")
            return env_vars
        
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip().strip('"').strip("'")
        
        except Exception as e:
            logger.error(f"Failed to load environment file {env_file}: {e}")
        
        return env_vars
    
    def _categorize_environment_variables(
        self, 
        env_vars: Dict[str, str]
    ) -> Dict[str, Dict[str, str]]:
        """Categorize environment variables by service."""
        service_env_vars = {service: {} for service in self.service_config_templates.keys()}
        service_env_vars['shared'] = {}
        
        # Define service-specific prefixes
        service_prefixes = {
            'instruments': ['INSTRUMENT_', 'VENDOR_'],
            'market_data': ['MARKET_', 'PRICE_', 'QUOTE_'],
            'analytics': ['ANALYTICS_', 'MODEL_', 'ML_'],
            'user_management': ['USER_', 'AUTH_', 'JWT_', 'SESSION_']
        }
        
        # Common variables that all services need
        shared_vars = [
            'DATABASE_URL', 'REDIS_URL', 'LOG_LEVEL', 
            'ENVIRONMENT', 'DEBUG', 'SECRET_KEY'
        ]
        
        for key, value in env_vars.items():
            # Check if it's a shared variable
            if key in shared_vars:
                service_env_vars['shared'][key] = value
                # Also add to all services
                for service in self.service_config_templates.keys():
                    service_env_vars[service][key] = value
                continue
            
            # Check service-specific prefixes
            assigned = False
            for service, prefixes in service_prefixes.items():
                if any(key.startswith(prefix) for prefix in prefixes):
                    service_env_vars[service][key] = value
                    assigned = True
                    break
            
            # If not assigned to specific service, add to shared
            if not assigned:
                service_env_vars['shared'][key] = value
        
        return service_env_vars
    
    def _adapt_value_for_environment(
        self, 
        key: str, 
        value: str, 
        environment: str
    ) -> str:
        """Adapt environment variable value for specific environment."""
        # Database URLs - use different databases per environment
        if key == 'DATABASE_URL':
            if 'localhost' in value:
                if environment == 'dev':
                    return value.replace('dev_db', 'dev_db')
                elif environment == 'staging':
                    return value.replace('dev_db', 'staging_db')
                elif environment == 'prod':
                    return value.replace('dev_db', 'production_db')
        
        # Log levels - more verbose in dev, less in production
        if key == 'LOG_LEVEL':
            if environment == 'dev':
                return 'DEBUG'
            elif environment == 'staging':
                return 'INFO'
            elif environment == 'prod':
                return 'WARNING'
        
        # Cache TTL - shorter in dev for testing
        if key.endswith('_TTL_SECONDS'):
            if environment == 'dev':
                return str(int(value) // 4)  # Shorter TTL for dev
            elif environment == 'prod':
                return str(int(value) * 2)   # Longer TTL for prod
        
        return value
    
    def _adapt_env_vars_for_environment(
        self, 
        env_vars: Dict[str, Any], 
        environment: str
    ) -> Dict[str, Any]:
        """Adapt all environment variables for specific environment."""
        adapted_vars = {}
        
        for key, value in env_vars.items():
            if isinstance(value, str) and '${' in value:
                # Keep template variables as-is
                adapted_vars[key] = value
            else:
                adapted_vars[key] = self._adapt_value_for_environment(
                    key, str(value), environment
                )
        
        # Add environment-specific variables
        adapted_vars['ENVIRONMENT'] = environment
        adapted_vars['SERVICE_ENVIRONMENT'] = environment
        
        return adapted_vars
    
    def _get_resource_config_for_environment(self, environment: str) -> Dict[str, Any]:
        """Get resource configuration for specific environment."""
        resource_configs = {
            'dev': {
                'limits': {
                    'memory': '512Mi',
                    'cpu': '500m'
                },
                'requests': {
                    'memory': '256Mi',
                    'cpu': '250m'
                }
            },
            'staging': {
                'limits': {
                    'memory': '1Gi',
                    'cpu': '1000m'
                },
                'requests': {
                    'memory': '512Mi',
                    'cpu': '500m'
                }
            },
            'prod': {
                'limits': {
                    'memory': '2Gi',
                    'cpu': '2000m'
                },
                'requests': {
                    'memory': '1Gi',
                    'cpu': '1000m'
                }
            }
        }
        
        return resource_configs.get(environment, resource_configs['dev'])
    
    def _get_monitoring_config_for_environment(self, environment: str) -> Dict[str, Any]:
        """Get monitoring configuration for specific environment."""
        if environment == 'prod':
            return {
                'metrics_enabled': True,
                'tracing_enabled': True,
                'log_level': 'INFO',
                'alert_thresholds': {
                    'error_rate': 0.01,
                    'response_time_p95': 2000,
                    'memory_usage': 0.8
                }
            }
        elif environment == 'staging':
            return {
                'metrics_enabled': True,
                'tracing_enabled': True,
                'log_level': 'DEBUG',
                'alert_thresholds': {
                    'error_rate': 0.05,
                    'response_time_p95': 5000,
                    'memory_usage': 0.9
                }
            }
        else:  # dev
            return {
                'metrics_enabled': True,
                'tracing_enabled': False,
                'log_level': 'DEBUG'
            }
    
    def _validate_service_config(self, service_name: str) -> Dict[str, Any]:
        """Validate service configuration files."""
        service_dir = self.target_config_dir / service_name
        validation_result = {
            'service': service_name,
            'status': 'unknown',
            'issues': []
        }
        
        # Check if service directory exists
        if not service_dir.exists():
            validation_result['issues'].append(f"Service directory not found: {service_dir}")
            validation_result['status'] = 'invalid'
            return validation_result
        
        # Check required files
        required_files = [
            f"{service_name}_config.yaml",
            f"{service_name}.env"
        ]
        
        for required_file in required_files:
            file_path = service_dir / required_file
            if not file_path.exists():
                validation_result['issues'].append(f"Required file missing: {required_file}")
        
        # Validate YAML syntax
        config_file = service_dir / f"{service_name}_config.yaml"
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    yaml.safe_load(f)
            except yaml.YAMLError as e:
                validation_result['issues'].append(f"Invalid YAML syntax in {config_file.name}: {e}")
        
        # Set overall status
        if not validation_result['issues']:
            validation_result['status'] = 'valid'
        else:
            validation_result['status'] = 'invalid'
        
        return validation_result
    
    def _validate_environment_configs(self) -> List[Dict[str, Any]]:
        """Validate environment-specific configurations."""
        validation_results = []
        
        for env_name in self.environment_mappings.keys():
            env_validation = {
                'environment': env_name,
                'status': 'valid',
                'issues': []
            }
            
            # Check each service has environment config
            for service_name in self.service_config_templates.keys():
                service_dir = self.target_config_dir / service_name
                env_file = service_dir / f"{service_name}.{env_name}.yaml"
                
                if not env_file.exists():
                    env_validation['issues'].append(
                        f"Missing {env_name} config for {service_name}"
                    )
                else:
                    # Validate YAML
                    try:
                        with open(env_file, 'r') as f:
                            yaml.safe_load(f)
                    except yaml.YAMLError as e:
                        env_validation['issues'].append(
                            f"Invalid YAML in {env_file.name}: {e}"
                        )
            
            if env_validation['issues']:
                env_validation['status'] = 'invalid'
            
            validation_results.append(env_validation)
        
        return validation_results
    
    def _validate_deployment_configs(self) -> List[Dict[str, Any]]:
        """Validate deployment configurations."""
        validation_results = []
        
        # Validate Docker configs
        docker_dir = self.target_config_dir / "docker"
        if docker_dir.exists():
            for docker_file in docker_dir.glob("docker-compose.*.yml"):
                docker_validation = {
                    'file': str(docker_file),
                    'type': 'docker_compose',
                    'status': 'valid',
                    'issues': []
                }
                
                try:
                    with open(docker_file, 'r') as f:
                        yaml.safe_load(f)
                except yaml.YAMLError as e:
                    docker_validation['issues'].append(f"Invalid YAML syntax: {e}")
                    docker_validation['status'] = 'invalid'
                
                validation_results.append(docker_validation)
        
        # Validate Kubernetes configs
        k8s_dir = self.target_config_dir / "kubernetes"
        if k8s_dir.exists():
            for k8s_file in k8s_dir.glob("*-deployment.yaml"):
                k8s_validation = {
                    'file': str(k8s_file),
                    'type': 'kubernetes',
                    'status': 'valid',
                    'issues': []
                }
                
                try:
                    with open(k8s_file, 'r') as f:
                        yaml.safe_load(f)
                except yaml.YAMLError as e:
                    k8s_validation['issues'].append(f"Invalid YAML syntax: {e}")
                    k8s_validation['status'] = 'invalid'
                
                validation_results.append(k8s_validation)
        
        return validation_results