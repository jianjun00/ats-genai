"""
Instrument Service Health Integration.

This module integrates the instrument service with the health check framework,
providing comprehensive health monitoring and service registration.
"""

import asyncio
from datetime import datetime
from typing import Dict, Any
import logging

from src.infrastructure.service_discovery import (
    ServiceInstance,
    ServiceEndpoint,
    HealthCheck as ServiceHealthCheck,
    get_global_registry,
    DatabaseHealthCheck,
    SystemResourceHealthCheck,
    CustomHealthCheck,
    HealthCheckType,
    get_health_manager
)
from ..impl.instrument_service_impl import InstrumentServiceImpl

logger = logging.getLogger(__name__)


class InstrumentServiceHealthIntegration:
    """Health integration for instrument service."""

    def __init__(
        self,
        service_impl: InstrumentServiceImpl,
        service_name: str = "instrument-service",
        service_version: str = "1.0.0",
        host: str = "localhost",
        port: int = 8000
    ):
        self.service_impl = service_impl
        self.service_name = service_name
        self.service_version = service_version
        self.host = host
        self.port = port

        # Service registry and health manager
        self.registry = get_global_registry()
        self.health_manager = get_health_manager()

        # Service instance configuration
        self.service_instance = ServiceInstance(
            service_name=service_name,
            instance_id=f"{service_name}-{host}-{port}",
            version=service_version,
            endpoint=ServiceEndpoint(
                host=host,
                port=port,
                protocol="http",
                path="/"
            ),
            metadata={
                "environment": "dev",  # Can be configured
                "weight": 1,
                "tags": ["instruments", "reference-data"]
            },
            health_check=ServiceHealthCheck(
                endpoint="/health",
                interval_seconds=30,
                timeout_seconds=5,
                failure_threshold=3
            )
        )

        self._setup_health_checks()

    def _setup_health_checks(self):
        """Setup health checks for the instrument service."""

        # Database connectivity check
        database_check = DatabaseHealthCheck(
            name="instrument_database",
            connection_factory=self._get_database_connection,
            query="SELECT COUNT(*) FROM dev_instruments LIMIT 1"
        )
        self.health_manager.add_health_check(database_check)

        # System resource check
        system_check = SystemResourceHealthCheck(
            name="system_resources",
            cpu_threshold=80.0,
            memory_threshold=85.0,
            disk_threshold=90.0
        )
        self.health_manager.add_health_check(system_check)

        # Service-specific business logic checks
        business_logic_check = CustomHealthCheck(
            name="instrument_service_logic",
            check_function=self._check_business_logic,
            check_type=HealthCheckType.READINESS
        )
        self.health_manager.add_health_check(business_logic_check)

        # Data quality check
        data_quality_check = CustomHealthCheck(
            name="instrument_data_quality",
            check_function=self._check_data_quality,
            check_type=HealthCheckType.CUSTOM
        )
        self.health_manager.add_health_check(data_quality_check)

        # Service dependencies check
        dependencies_check = CustomHealthCheck(
            name="service_dependencies",
            check_function=self._check_dependencies,
            check_type=HealthCheckType.DEPENDENCY
        )
        self.health_manager.add_health_check(dependencies_check)

    async def _get_database_connection(self):
        """Get database connection for health check."""
        # Use the service's DAO connection
        return await self.service_impl.vendor_instrument_dao._get_connection()

    async def _check_business_logic(self) -> Dict[str, Any]:
        """Check business logic health."""
        try:
            # Test core service functionality
            start_time = datetime.utcnow()

            # Try to get a single instrument to test basic functionality
            criteria = {"limit": 1}
            instruments = await self.service_impl.list_vendor_instruments(criteria)

            end_time = datetime.utcnow()
            response_time_ms = (end_time - start_time).total_seconds() * 1000

            return {
                'status': 'healthy',
                'message': 'Business logic functioning normally',
                'response_time_ms': round(response_time_ms, 2),
                'test_result_count': len(instruments),
                'service_operations': {
                    'vendor_instruments': 'operational',
                    'instrument_xrefs': 'operational',
                    'unified_instruments': 'operational'
                }
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Business logic check failed: {str(e)}',
                'error': str(e),
                'error_type': type(e).__name__
            }

    async def _check_data_quality(self) -> Dict[str, Any]:
        """Check data quality metrics."""
        try:
            # Check data completeness and quality
            quality_metrics = {}

            # Check vendor instruments data quality
            vendor_criteria = {"limit": 100}
            vendor_instruments = await self.service_impl.list_vendor_instruments(vendor_criteria)

            if vendor_instruments:
                quality_metrics['vendor_instruments'] = {
                    'sample_size': len(vendor_instruments),
                    'has_symbols': sum(1 for i in vendor_instruments if i.vendor_symbol),
                    'has_names': sum(1 for i in vendor_instruments if i.instrument_name),
                    'completeness_rate': round(
                        sum(1 for i in vendor_instruments if i.vendor_symbol and i.instrument_name) / len(vendor_instruments) * 100, 2
                    )
                }

            # Check instrument cross-references
            xref_criteria = {"limit": 50}
            xrefs = await self.service_impl.list_instrument_xrefs(xref_criteria)

            if xrefs:
                quality_metrics['instrument_xrefs'] = {
                    'sample_size': len(xrefs),
                    'has_mappings': sum(1 for x in xrefs if x.vendor_symbol and x.unified_symbol),
                    'completeness_rate': round(
                        sum(1 for x in xrefs if x.vendor_symbol and x.unified_symbol) / len(xrefs) * 100, 2
                    )
                }

            # Determine overall quality status
            overall_completeness = 0
            if quality_metrics:
                completeness_rates = [
                    m.get('completeness_rate', 0)
                    for m in quality_metrics.values()
                    if isinstance(m, dict)
                ]
                if completeness_rates:
                    overall_completeness = sum(completeness_rates) / len(completeness_rates)

            status = 'healthy' if overall_completeness >= 80 else ('degraded' if overall_completeness >= 60 else 'unhealthy')

            return {
                'status': status,
                'message': f'Data quality check completed - {overall_completeness:.1f}% completeness',
                'overall_completeness_rate': round(overall_completeness, 2),
                'quality_metrics': quality_metrics
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Data quality check failed: {str(e)}',
                'error': str(e)
            }

    async def _check_dependencies(self) -> Dict[str, Any]:
        """Check service dependencies."""
        try:
            dependencies_status = {}

            # Check database dependency
            try:
                conn = await self._get_database_connection()
                result = await conn.fetch("SELECT 1")
                await conn.close()
                dependencies_status['database'] = {
                    'status': 'healthy',
                    'message': 'Database connection successful'
                }
            except Exception as e:
                dependencies_status['database'] = {
                    'status': 'unhealthy',
                    'message': f'Database connection failed: {str(e)}'
                }

            # Check if all DAOs are properly initialized
            dao_status = {
                'vendor_instrument_dao': bool(self.service_impl.vendor_instrument_dao),
                'instrument_xref_dao': bool(self.service_impl.instrument_xref_dao),
                'unified_instrument_dao': bool(self.service_impl.unified_instrument_dao)
            }

            all_daos_healthy = all(dao_status.values())
            dependencies_status['daos'] = {
                'status': 'healthy' if all_daos_healthy else 'unhealthy',
                'message': 'All DAOs initialized' if all_daos_healthy else 'Some DAOs not initialized',
                'dao_status': dao_status
            }

            # Overall dependency health
            all_dependencies_healthy = all(
                dep.get('status') == 'healthy'
                for dep in dependencies_status.values()
            )

            return {
                'status': 'healthy' if all_dependencies_healthy else 'unhealthy',
                'message': 'All dependencies healthy' if all_dependencies_healthy else 'Some dependencies unhealthy',
                'dependencies': dependencies_status
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Dependencies check failed: {str(e)}',
                'error': str(e)
            }

    async def register_service(self) -> bool:
        """Register service with service registry."""
        try:
            success = await self.registry.register_service(self.service_instance)
            if success:
                logger.info(f"Successfully registered {self.service_name} with service registry")
            return success
        except Exception as e:
            logger.error(f"Failed to register {self.service_name}: {str(e)}")
            return False

    async def deregister_service(self) -> bool:
        """Deregister service from service registry."""
        try:
            success = await self.registry.deregister_service(
                self.service_name,
                self.service_instance.instance_id
            )
            if success:
                logger.info(f"Successfully deregistered {self.service_name} from service registry")
            return success
        except Exception as e:
            logger.error(f"Failed to deregister {self.service_name}: {str(e)}")
            return False

    async def update_heartbeat(self) -> bool:
        """Update service heartbeat."""
        try:
            return await self.registry.heartbeat(
                self.service_name,
                self.service_instance.instance_id
            )
        except Exception as e:
            logger.error(f"Failed to update heartbeat for {self.service_name}: {str(e)}")
            return False

    async def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        try:
            overall_health = await self.health_manager.perform_all_checks()
            return overall_health.to_dict()
        except Exception as e:
            logger.error(f"Failed to get health status: {str(e)}")
            return {
                'status': 'unhealthy',
                'message': f'Health check failed: {str(e)}',
                'timestamp': datetime.utcnow().isoformat(),
                'checks': [],
                'summary': {'error': str(e)}
            }

    async def start_health_monitoring(self):
        """Start health monitoring background tasks."""
        # Start heartbeat task
        asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Started health monitoring for {self.service_name}")

    async def _heartbeat_loop(self):
        """Background heartbeat loop."""
        while True:
            try:
                await self.update_heartbeat()
                await asyncio.sleep(15)  # Heartbeat every 15 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {str(e)}")
                await asyncio.sleep(5)


# Context manager for service lifecycle management
async def instrument_service_with_health(service_impl: InstrumentServiceImpl, **kwargs):
    """Context manager for instrument service with health monitoring."""
    health_integration = InstrumentServiceHealthIntegration(service_impl, **kwargs)

    try:
        # Register service
        await health_integration.register_service()

        # Start health monitoring
        await health_integration.start_health_monitoring()

        yield health_integration

    finally:
        # Deregister service
        await health_integration.deregister_service()


def create_health_integrated_service(service_impl: InstrumentServiceImpl, **kwargs) -> InstrumentServiceHealthIntegration:
    """Create instrument service with health integration."""
    return InstrumentServiceHealthIntegration(service_impl, **kwargs)