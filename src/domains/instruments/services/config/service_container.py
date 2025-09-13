"""
Service Container for Instrument Domain

Provides dependency injection and service configuration for the instrument domain.
This is the only place where service implementations are wired to their dependencies.
"""

from typing import Dict, Any
import logging

from core.platform.config.environment import Environment, EnvironmentType

# Service interfaces
from domains.instruments.services.interfaces.instrument_service_interface import InstrumentServiceInterface

# Service implementations
from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
# from domains.instruments.services.impl.cached_instrument_service_impl import CachedInstrumentServiceImpl  # TODO: Re-enable when dependencies resolved

# DAO dependencies
from core.dao.instruments.instruments_dao import InstrumentsDAO
from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
from core.dao.infrastructure.vendors_dao import VendorsDAO
from infrastructure.vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO
# Vendor DAOs are imported dynamically as needed to avoid import errors
# from infrastructure.vendor.tiingo.dao.instruments_tiingo_dao import InstrumentsTiingoDAO  # If exists
# from infrastructure.vendor.eodhd.dao.instruments_eodhd_dao import InstrumentsEodhdDAO  # If exists

logger = logging.getLogger(__name__)


class InstrumentServiceContainer:
    """
    Service container for instrument domain.
    
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
            
        logger.info(f"Initializing InstrumentServiceContainer for environment: {self.environment.env_type}")
        
        try:
            # Initialize DAOs first
            await self._initialize_daos()
            
            # Then initialize services that depend on DAOs
            await self._initialize_services()
            
            self._initialized = True
            logger.info("InstrumentServiceContainer initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing InstrumentServiceContainer: {e}")
            raise
    
    async def _initialize_daos(self):
        """Initialize all DAO dependencies"""
        
        # Core DAOs
        self._daos['instruments_dao'] = InstrumentsDAO(self.environment)
        self._daos['xrefs_dao'] = InstrumentXrefsDAO(self.environment)
        self._daos['vendors_dao'] = VendorsDAO(self.environment)
        
        # Vendor-specific DAOs
        vendor_daos = {}
        
        # Polygon DAO
        try:
            vendor_daos['polygon'] = InstrumentPolygonDAO(self.environment)
            logger.debug("Polygon DAO initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Polygon DAO: {e}")
        
        # Add other vendor DAOs as they become available
        # try:
        #     vendor_daos['tiingo'] = InstrumentsTiingoDAO(self.environment)
        #     logger.debug("Tiingo DAO initialized")
        # except Exception as e:
        #     logger.warning(f"Could not initialize Tiingo DAO: {e}")
        
        self._daos['vendor_daos'] = vendor_daos
        
        logger.info(f"Initialized {len(self._daos)} DAO components")
    
    async def _initialize_services(self):
        """Initialize all service implementations"""
        
        # Use base service implementation for now
        # TODO: Re-enable cached service when infrastructure dependencies are resolved
        self._services['instrument_service'] = InstrumentServiceImpl(
            instruments_dao=self._daos['instruments_dao'],
            xrefs_dao=self._daos['xrefs_dao'],
            vendors_dao=self._daos['vendors_dao'],
            vendor_daos=self._daos['vendor_daos']
        )
        
        logger.info(f"Initialized {len(self._services)} service components")
    
    def get_instrument_service(self) -> InstrumentServiceInterface:
        """Get the instrument service instance"""
        if not self._initialized:
            raise RuntimeError("Service container not initialized. Call initialize() first.")
        
        return self._services['instrument_service']
    
    async def shutdown(self):
        """Cleanup resources on shutdown"""
        logger.info("Shutting down InstrumentServiceContainer")
        
        # Services don't typically need cleanup, but DAOs might need connection cleanup
        # This is where you'd add any cleanup logic
        
        self._services.clear()
        self._daos.clear()
        self._initialized = False
        
        logger.info("InstrumentServiceContainer shutdown complete")
    
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
_container: InstrumentServiceContainer = None


async def get_service_container(environment: Environment = None) -> InstrumentServiceContainer:
    """
    Get or create the global service container instance.
    
    This function provides singleton access to the service container.
    In production, you might want to use a proper DI framework instead.
    """
    global _container
    
    if _container is None:
        if environment is None:
            # Default to development environment if none provided
            # In production, this should be properly configured
            environment = Environment(None, EnvironmentType.DEV)
        
        _container = InstrumentServiceContainer(environment)
        await _container.initialize()
    
    return _container


async def shutdown_service_container():
    """Shutdown the global service container"""
    global _container
    
    if _container:
        await _container.shutdown()
        _container = None


# Convenience functions for getting services
async def get_instrument_service(environment: Environment = None) -> InstrumentServiceInterface:
    """Convenience function to get instrument service"""
    container = await get_service_container(environment)
    return container.get_instrument_service()


# FastAPI dependency provider
async def provide_instrument_service() -> InstrumentServiceInterface:
    """
    FastAPI dependency provider for instrument service.
    
    This function can be used as a Depends() in FastAPI endpoints.
    """
    try:
        # In production, you'd inject the proper environment here
        # For now, use default environment
        container = await get_service_container()
        return container.get_instrument_service()
    
    except Exception as e:
        logger.error(f"Error providing instrument service: {e}")
        raise RuntimeError(f"Service unavailable: {e}")


# Monitoring integration
async def initialize_monitoring():
    """Initialize service monitoring"""
    try:
        from infrastructure.monitoring.instrument_service_monitor import initialize_instrument_service_monitoring
        await initialize_instrument_service_monitoring()
        logger.info("Service monitoring initialized successfully")
    except Exception as e:
        logger.warning(f"Could not initialize monitoring: {e}")


async def shutdown_monitoring():
    """Shutdown service monitoring"""
    try:
        from infrastructure.monitoring.instrument_service_monitor import shutdown_instrument_service_monitoring
        await shutdown_instrument_service_monitoring()
        logger.info("Service monitoring shutdown complete")
    except Exception as e:
        logger.warning(f"Error during monitoring shutdown: {e}")


# Configuration factory functions
def create_development_container() -> InstrumentServiceContainer:
    """Create service container configured for development environment"""
    env = Environment(None, EnvironmentType.DEV)
    return InstrumentServiceContainer(env)


def create_integration_container() -> InstrumentServiceContainer:
    """Create service container configured for integration environment"""
    env = Environment(None, EnvironmentType.INTG)
    return InstrumentServiceContainer(env)


def create_production_container() -> InstrumentServiceContainer:
    """Create service container configured for production environment"""
    env = Environment(None, EnvironmentType.PROD)
    return InstrumentServiceContainer(env)