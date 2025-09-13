"""
Analytics Service Container

Provides dependency injection and service configuration for the analytics domain.
This is the only place where analytics service implementations are wired to their dependencies.
"""

from typing import Dict, Any
import logging

from core.platform.config.environment import Environment, EnvironmentType

# Service interfaces
from domains.analytics.services.interfaces.analytics_service_interface import AnalyticsServiceInterface

# Service implementations
from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl

# DAO dependencies
from core.dao.analytics.events_dao import EventsDAO
from core.dao.analytics.economic_events_dao import EconomicEventsDAO

logger = logging.getLogger(__name__)


class AnalyticsServiceContainer:
    """
    Service container for analytics domain.
    
    Handles:
    1. Service lifecycle management
    2. Dependency injection
    3. Service configuration
    4. Environment-based configuration
    """
    
    def __init__(self, environment: Environment):
        self.environment = environment
        self._services: Dict[str, Any] = {}
        self._daos: Dict[str, Any] = {}
        self._initialized = False
    
    async def initialize(self):
        """Initialize all services and dependencies"""
        if self._initialized:
            return
            
        logger.info(f"Initializing AnalyticsServiceContainer for environment: {self.environment.env_type}")
        
        try:
            # Initialize DAOs first
            await self._initialize_daos()
            
            # Then initialize services that depend on DAOs
            await self._initialize_services()
            
            self._initialized = True
            logger.info("AnalyticsServiceContainer initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing AnalyticsServiceContainer: {e}")
            raise
    
    async def _initialize_daos(self):
        """Initialize all DAO dependencies"""
        
        # Analytics DAOs
        self._daos['events_dao'] = EventsDAO(self.environment)
        self._daos['economic_events_dao'] = EconomicEventsDAO(self.environment)
        
        logger.info(f"Initialized {len(self._daos)} DAO components")
    
    async def _initialize_services(self):
        """Initialize all service implementations"""
        
        # Use base service implementation
        self._services['analytics_service'] = AnalyticsServiceImpl(
            events_dao=self._daos['events_dao'],
            economic_events_dao=self._daos['economic_events_dao']
        )
        
        logger.info(f"Initialized {len(self._services)} service components")
    
    def get_analytics_service(self) -> AnalyticsServiceInterface:
        """Get the analytics service instance"""
        if not self._initialized:
            raise RuntimeError("Service container not initialized. Call initialize() first.")
        
        return self._services['analytics_service']
    
    async def shutdown(self):
        """Cleanup resources on shutdown"""
        logger.info("Shutting down AnalyticsServiceContainer")
        
        # Services don't typically need cleanup, but DAOs might need connection cleanup
        # This is where you'd add any cleanup logic
        
        self._services.clear()
        self._daos.clear()
        self._initialized = False
        
        logger.info("AnalyticsServiceContainer shutdown complete")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of service container"""
        return {
            "initialized": self._initialized,
            "environment": self.environment.env_type.value if self.environment else None,
            "services_count": len(self._services),
            "daos_count": len(self._daos),
            "status": "healthy" if self._initialized else "not_initialized"
        }


# Global container instance
_analytics_container: AnalyticsServiceContainer = None


async def get_analytics_service_container(environment: Environment = None) -> AnalyticsServiceContainer:
    """
    Get or create the global analytics service container instance.
    
    This function provides singleton access to the service container.
    In production, you might want to use a proper DI framework instead.
    """
    global _analytics_container
    
    if _analytics_container is None:
        if environment is None:
            # Default to development environment if none provided
            # In production, this should be properly configured
            environment = Environment(None, EnvironmentType.DEV)
        
        _analytics_container = AnalyticsServiceContainer(environment)
        await _analytics_container.initialize()
    
    return _analytics_container


async def shutdown_analytics_service_container():
    """Shutdown the global analytics service container"""
    global _analytics_container
    
    if _analytics_container:
        await _analytics_container.shutdown()
        _analytics_container = None


# Convenience functions for getting services
async def get_analytics_service(environment: Environment = None) -> AnalyticsServiceInterface:
    """Convenience function to get analytics service"""
    container = await get_analytics_service_container(environment)
    return container.get_analytics_service()


# FastAPI dependency provider
async def provide_analytics_service() -> AnalyticsServiceInterface:
    """
    FastAPI dependency provider for analytics service.
    
    This function can be used as a Depends() in FastAPI endpoints.
    """
    try:
        # In production, you'd inject the proper environment here
        # For now, use default environment
        container = await get_analytics_service_container()
        return container.get_analytics_service()
    
    except Exception as e:
        logger.error(f"Error providing analytics service: {e}")
        raise RuntimeError(f"Analytics service unavailable: {e}")


# Monitoring integration
async def initialize_analytics_monitoring():
    """Initialize analytics service monitoring"""
    try:
        from infrastructure.monitoring.instrument_service_monitor import initialize_instrument_service_monitoring
        await initialize_instrument_service_monitoring()
        logger.info("Analytics service monitoring initialized successfully")
    except Exception as e:
        logger.warning(f"Could not initialize analytics monitoring: {e}")


async def shutdown_analytics_monitoring():
    """Shutdown analytics service monitoring"""
    try:
        from infrastructure.monitoring.instrument_service_monitor import shutdown_instrument_service_monitoring
        await shutdown_instrument_service_monitoring()
        logger.info("Analytics service monitoring shutdown complete")
    except Exception as e:
        logger.warning(f"Error during analytics monitoring shutdown: {e}")


# Configuration factory functions
def create_development_analytics_container() -> AnalyticsServiceContainer:
    """Create analytics service container configured for development environment"""
    env = Environment(None, EnvironmentType.DEV)
    return AnalyticsServiceContainer(env)


def create_integration_analytics_container() -> AnalyticsServiceContainer:
    """Create analytics service container configured for integration environment"""
    env = Environment(None, EnvironmentType.INTG)
    return AnalyticsServiceContainer(env)


def create_production_analytics_container() -> AnalyticsServiceContainer:
    """Create analytics service container configured for production environment"""
    env = Environment(None, EnvironmentType.PROD)
    return AnalyticsServiceContainer(env)