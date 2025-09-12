"""
Generic Service Container Template

Use this template to create consistent service containers across all domains.
Replace {DOMAIN} with your domain name and customize for your specific dependencies.

Example Usage:
    # For Market Data Service
    sed 's/{DOMAIN}/MarketData/g' service_container_template.py > market_data_service_container.py
"""

from typing import Dict, Any, Optional
import logging

from core.platform.config.environment import Environment, EnvironmentType

# Service interfaces
from domains.{DOMAIN.lower()}.services.interfaces.{DOMAIN.lower()}_service_interface import {DOMAIN}ServiceInterface

# Service implementations  
from domains.{DOMAIN.lower()}.services.impl.{DOMAIN.lower()}_service_impl import {DOMAIN}ServiceImpl

# DAO dependencies - TODO: Replace with your actual DAO imports
# from core.dao.{DOMAIN.lower()}.{DOMAIN.lower()}_dao import {DOMAIN}DAO
# from core.dao.infrastructure.vendors_dao import VendorsDAO
# from infrastructure.vendor.polygon.dao.{DOMAIN.lower()}_polygon_dao import {DOMAIN}PolygonDAO

logger = logging.getLogger(__name__)


class {DOMAIN}ServiceContainer:
    """
    Service container for {DOMAIN} domain.
    
    Responsibilities:
    1. Service lifecycle management (initialization, cleanup)
    2. Dependency injection configuration
    3. Environment-aware service setup
    4. Resource management and optimization
    5. Health monitoring and diagnostics
    
    Design Principles:
    - Single responsibility: Only manages {DOMAIN} services
    - Environment isolation: Different configs for dev/intg/prod
    - Lazy initialization: Services created when first needed
    - Proper cleanup: Resources released on shutdown
    - Error handling: Graceful degradation on dependency failures
    """
    
    def __init__(self, environment: Environment):
        """
        Initialize service container with environment configuration.
        
        Args:
            environment: Environment configuration (dev, intg, prod)
        """
        self.environment = environment
        self._services: Dict[str, Any] = {}
        self._daos: Dict[str, Any] = {}
        self._vendor_daos: Dict[str, Any] = {}
        self._initialized = False
        self._health_status = "not_initialized"
        
        logger.info(f"Created {DOMAIN}ServiceContainer for environment: {environment.env_type}")
    
    async def initialize(self):
        """
        Initialize all services and dependencies in proper order.
        
        Initialization Order:
        1. Core DAOs (database connections)
        2. Vendor DAOs (external integrations)
        3. Service implementations (business logic)
        4. Health checks and validation
        
        Raises:
            RuntimeError: If initialization fails
        """
        if self._initialized:
            logger.warning(f"{DOMAIN}ServiceContainer already initialized")
            return
            
        logger.info(f"Initializing {DOMAIN}ServiceContainer...")
        
        try:
            # Step 1: Initialize core DAOs
            await self._initialize_core_daos()
            
            # Step 2: Initialize vendor DAOs  
            await self._initialize_vendor_daos()
            
            # Step 3: Initialize service implementations
            await self._initialize_services()
            
            # Step 4: Validate initialization
            await self._validate_initialization()
            
            self._initialized = True
            self._health_status = "healthy"
            
            logger.info(f"{DOMAIN}ServiceContainer initialized successfully")
            logger.info(f"Initialized {{len(self._services)}} services, {{len(self._daos)}} DAOs, {{len(self._vendor_daos)}} vendor integrations")
            
        except Exception as e:
            self._health_status = "initialization_failed"
            logger.error(f"Failed to initialize {DOMAIN}ServiceContainer: {e}", exc_info=True)
            
            # Cleanup partial initialization
            await self.shutdown()
            raise RuntimeError(f"{DOMAIN}ServiceContainer initialization failed: {e}")
    
    async def _initialize_core_daos(self):
        """Initialize core DAO dependencies"""
        logger.debug("Initializing core DAOs...")
        
        # TODO: Replace with your actual DAO implementations
        # Example DAO initialization:
        # self._daos['{DOMAIN.lower()}_dao'] = {DOMAIN}DAO(self.environment)
        # self._daos['vendors_dao'] = VendorsDAO(self.environment)
        
        # Placeholder - remove when implementing actual DAOs
        logger.warning("TODO: Implement actual DAO initialization")
        
        logger.debug(f"Initialized {{len(self._daos)}} core DAOs")
    
    async def _initialize_vendor_daos(self):
        """Initialize vendor-specific DAO integrations"""
        logger.debug("Initializing vendor DAOs...")
        
        # TODO: Add vendor DAO initialization based on your domain
        # Example vendor DAO patterns:
        
        # Polygon integration (if applicable)
        # try:
        #     self._vendor_daos['polygon'] = {DOMAIN}PolygonDAO(self.environment)
        #     logger.debug("Polygon DAO initialized")
        # except Exception as e:
        #     logger.warning(f"Could not initialize Polygon DAO: {e}")
        
        # Tiingo integration (if applicable) 
        # try:
        #     self._vendor_daos['tiingo'] = {DOMAIN}TiingoDAO(self.environment)
        #     logger.debug("Tiingo DAO initialized")
        # except Exception as e:
        #     logger.warning(f"Could not initialize Tiingo DAO: {e}")
        
        # EODHD integration (if applicable)
        # try:
        #     self._vendor_daos['eodhd'] = {DOMAIN}EodhdDAO(self.environment)
        #     logger.debug("EODHD DAO initialized")
        # except Exception as e:
        #     logger.warning(f"Could not initialize EODHD DAO: {e}")
        
        # Alpha Vantage integration (if applicable)
        # try:
        #     self._vendor_daos['alpha_vantage'] = {DOMAIN}AlphaVantageDAO(self.environment)
        #     logger.debug("Alpha Vantage DAO initialized")
        # except Exception as e:
        #     logger.warning(f"Could not initialize Alpha Vantage DAO: {e}")
        
        logger.debug(f"Initialized {{len(self._vendor_daos)}} vendor DAOs")
    
    async def _initialize_services(self):
        """Initialize service implementations with their dependencies"""
        logger.debug("Initializing services...")
        
        # Primary {DOMAIN} Service Implementation
        try:
            self._services['{DOMAIN.lower()}_service'] = {DOMAIN}ServiceImpl(
                # TODO: Pass your actual DAO dependencies
                # {DOMAIN.lower()}_dao=self._daos.get('{DOMAIN.lower()}_dao'),
                # vendors_dao=self._daos.get('vendors_dao'),
                # vendor_daos=self._vendor_daos
            )
            logger.debug(f"{DOMAIN} service implementation created")
            
        except Exception as e:
            raise RuntimeError(f"Failed to create {DOMAIN} service implementation: {e}")
        
        # TODO: Add other service implementations as needed
        # Example additional services:
        # self._services['{DOMAIN.lower()}_analytics_service'] = {DOMAIN}AnalyticsServiceImpl(...)
        # self._services['{DOMAIN.lower()}_integration_service'] = {DOMAIN}IntegrationServiceImpl(...)
        
        logger.debug(f"Initialized {{len(self._services)}} services")
    
    async def _validate_initialization(self):
        """Validate that all services are properly initialized"""
        logger.debug("Validating service initialization...")
        
        # Validate primary service
        primary_service = self._services.get('{DOMAIN.lower()}_service')
        if not primary_service:
            raise RuntimeError(f"Primary {DOMAIN} service not initialized")
        
        # TODO: Add service health checks
        # try:
        #     health = await primary_service.health_check()
        #     if health.get('status') not in ['healthy', 'degraded']:
        #         logger.warning(f"{DOMAIN} service health check failed: {{health}}")
        # except Exception as e:
        #     logger.warning(f"{DOMAIN} service health check error: {e}")
        
        logger.debug("Service initialization validation completed")
    
    def get_{DOMAIN.lower()}_service(self) -> {DOMAIN}ServiceInterface:
        """
        Get the primary {DOMAIN} service instance.
        
        Returns:
            {DOMAIN}ServiceInterface: Primary service for {DOMAIN} operations
            
        Raises:
            RuntimeError: If service container not initialized
        """
        if not self._initialized:
            raise RuntimeError(f"{DOMAIN}ServiceContainer not initialized. Call initialize() first.")
        
        service = self._services.get('{DOMAIN.lower()}_service')
        if not service:
            raise RuntimeError(f"{DOMAIN} service not available")
        
        return service
    
    def get_service(self, service_name: str) -> Any:
        """
        Get a specific service by name.
        
        Args:
            service_name: Name of the service to retrieve
            
        Returns:
            Service instance if available
            
        Raises:
            RuntimeError: If service container not initialized or service not found
        """
        if not self._initialized:
            raise RuntimeError(f"{DOMAIN}ServiceContainer not initialized")
        
        service = self._services.get(service_name)
        if not service:
            available_services = list(self._services.keys())
            raise RuntimeError(f"Service '{{service_name}}' not found. Available: {{available_services}}")
        
        return service
    
    def get_dao(self, dao_name: str) -> Any:
        """
        Get a specific DAO by name (for advanced use cases).
        
        Note: Prefer using services instead of direct DAO access.
        
        Args:
            dao_name: Name of the DAO to retrieve
            
        Returns:
            DAO instance if available
        """
        if not self._initialized:
            raise RuntimeError(f"{DOMAIN}ServiceContainer not initialized")
        
        dao = self._daos.get(dao_name) or self._vendor_daos.get(dao_name)
        if not dao:
            all_daos = list(self._daos.keys()) + list(self._vendor_daos.keys())
            raise RuntimeError(f"DAO '{{dao_name}}' not found. Available: {{all_daos}}")
        
        return dao
    
    async def shutdown(self):
        """
        Cleanup all resources and shut down services gracefully.
        
        Shutdown Order:
        1. Services (business logic cleanup)
        2. Vendor DAOs (external connection cleanup)
        3. Core DAOs (database connection cleanup)
        """
        logger.info(f"Shutting down {DOMAIN}ServiceContainer...")
        
        # Shutdown services first
        for service_name, service in self._services.items():
            try:
                if hasattr(service, 'shutdown'):
                    await service.shutdown()
                logger.debug(f"Service '{{service_name}}' shut down")
            except Exception as e:
                logger.warning(f"Error shutting down service '{{service_name}}': {e}")
        
        # Shutdown vendor DAOs
        for vendor_name, dao in self._vendor_daos.items():
            try:
                if hasattr(dao, 'close') or hasattr(dao, 'shutdown'):
                    cleanup_method = getattr(dao, 'close', getattr(dao, 'shutdown', None))
                    if cleanup_method:
                        await cleanup_method()
                logger.debug(f"Vendor DAO '{{vendor_name}}' shut down")
            except Exception as e:
                logger.warning(f"Error shutting down vendor DAO '{{vendor_name}}': {e}")
        
        # Shutdown core DAOs
        for dao_name, dao in self._daos.items():
            try:
                if hasattr(dao, 'close') or hasattr(dao, 'shutdown'):
                    cleanup_method = getattr(dao, 'close', getattr(dao, 'shutdown', None))
                    if cleanup_method:
                        await cleanup_method()
                logger.debug(f"DAO '{{dao_name}}' shut down")
            except Exception as e:
                logger.warning(f"Error shutting down DAO '{{dao_name}}': {e}")
        
        # Clear all references
        self._services.clear()
        self._daos.clear()
        self._vendor_daos.clear()
        
        self._initialized = False
        self._health_status = "shutdown"
        
        logger.info(f"{DOMAIN}ServiceContainer shutdown complete")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get comprehensive health status of the service container.
        
        Returns:
            Dictionary with detailed health information
        """
        return {
            "container_name": f"{DOMAIN}ServiceContainer",
            "initialized": self._initialized,
            "health_status": self._health_status,
            "environment": self.environment.env_type.value if self.environment else None,
            "services_count": len(self._services),
            "core_daos_count": len(self._daos),
            "vendor_daos_count": len(self._vendor_daos),
            "available_services": list(self._services.keys()),
            "available_daos": list(self._daos.keys()),
            "available_vendor_daos": list(self._vendor_daos.keys()),
            "status": "healthy" if self._initialized and self._health_status == "healthy" else "unhealthy"
        }
    
    async def perform_health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check on all services and dependencies.
        
        Returns:
            Dictionary with detailed health check results
        """
        health_results = {
            "overall_status": "healthy",
            "container_health": self.get_health_status(),
            "service_health": {},
            "dao_health": {},
            "timestamp": None  # Will be set by calling code
        }
        
        if not self._initialized:
            health_results["overall_status"] = "not_initialized"
            return health_results
        
        # Check service health
        for service_name, service in self._services.items():
            try:
                if hasattr(service, 'health_check'):
                    service_health = await service.health_check()
                    health_results["service_health"][service_name] = service_health
                    
                    # Update overall status based on service health
                    if service_health.get('status') not in ['healthy', 'degraded']:
                        health_results["overall_status"] = "unhealthy"
                else:
                    health_results["service_health"][service_name] = {
                        "status": "no_health_check",
                        "message": "Service does not implement health_check method"
                    }
                    
            except Exception as e:
                health_results["service_health"][service_name] = {
                    "status": "error",
                    "error": str(e)
                }
                health_results["overall_status"] = "unhealthy"
        
        # TODO: Add DAO health checks if needed
        # for dao_name, dao in self._daos.items():
        #     try:
        #         if hasattr(dao, 'health_check'):
        #             dao_health = await dao.health_check()
        #             health_results["dao_health"][dao_name] = dao_health
        #     except Exception as e:
        #         health_results["dao_health"][dao_name] = {"status": "error", "error": str(e)}
        
        return health_results


# ========================================================================================
# GLOBAL CONTAINER MANAGEMENT
# ========================================================================================

# Global container instance for singleton pattern
_container: Optional[{DOMAIN}ServiceContainer] = None


async def get_service_container(environment: Optional[Environment] = None) -> {DOMAIN}ServiceContainer:
    """
    Get or create the global {DOMAIN} service container instance.
    
    This function provides singleton access to the service container.
    In production applications, consider using a proper DI framework instead.
    
    Args:
        environment: Environment configuration (created automatically if None)
        
    Returns:
        {DOMAIN}ServiceContainer: Initialized service container
        
    Raises:
        RuntimeError: If container initialization fails
    """
    global _container
    
    if _container is None:
        if environment is None:
            # Default to development environment if none provided
            # TODO: In production, this should be properly configured from config files
            environment = Environment(None, EnvironmentType.DEV)
            logger.info("Using default DEV environment for service container")
        
        _container = {DOMAIN}ServiceContainer(environment)
        await _container.initialize()
        
        logger.info(f"Global {DOMAIN}ServiceContainer created and initialized")
    
    return _container


async def shutdown_service_container():
    """
    Shutdown the global {DOMAIN} service container.
    
    This should be called during application shutdown to ensure proper cleanup.
    """
    global _container
    
    if _container:
        await _container.shutdown()
        _container = None
        logger.info(f"Global {DOMAIN}ServiceContainer shutdown complete")


# ========================================================================================
# CONVENIENCE FUNCTIONS FOR SERVICE ACCESS
# ========================================================================================

async def get_{DOMAIN.lower()}_service(environment: Optional[Environment] = None) -> {DOMAIN}ServiceInterface:
    """
    Convenience function to get the primary {DOMAIN} service.
    
    This is the most common way to access {DOMAIN} services in application code.
    
    Args:
        environment: Environment configuration (optional)
        
    Returns:
        {DOMAIN}ServiceInterface: Primary {DOMAIN} service
        
    Usage:
        service = await get_{DOMAIN.lower()}_service()
        result = await service.create_{DOMAIN.lower()}(dto)
    """
    container = await get_service_container(environment)
    return container.get_{DOMAIN.lower()}_service()


# ========================================================================================
# FASTAPI DEPENDENCY PROVIDERS
# ========================================================================================

async def provide_{DOMAIN.lower()}_service() -> {DOMAIN}ServiceInterface:
    """
    FastAPI dependency provider for {DOMAIN} service.
    
    This function can be used as a Depends() in FastAPI endpoints.
    It provides proper error handling and logging for API integration.
    
    Returns:
        {DOMAIN}ServiceInterface: Service instance for API endpoints
        
    Raises:
        RuntimeError: If service is unavailable
        
    Usage:
        @app.get("/api/{DOMAIN.lower()}/")
        async def get_{DOMAIN.lower()}s(
            service: {DOMAIN}ServiceInterface = Depends(provide_{DOMAIN.lower()}_service)
        ):
            return await service.list_{DOMAIN.lower()}s(criteria)
    """
    try:
        return await get_{DOMAIN.lower()}_service()
    except Exception as e:
        logger.error(f"Error providing {DOMAIN} service for API: {e}", exc_info=True)
        raise RuntimeError(f"{DOMAIN} service unavailable: {e}")


# ========================================================================================
# ENVIRONMENT-SPECIFIC CONTAINER FACTORIES
# ========================================================================================

async def create_development_container() -> {DOMAIN}ServiceContainer:
    """
    Create and initialize service container for development environment.
    
    Returns:
        {DOMAIN}ServiceContainer: Container configured for development
    """
    env = Environment(None, EnvironmentType.DEV)
    container = {DOMAIN}ServiceContainer(env)
    await container.initialize()
    return container


async def create_integration_container() -> {DOMAIN}ServiceContainer:
    """
    Create and initialize service container for integration environment.
    
    Returns:
        {DOMAIN}ServiceContainer: Container configured for integration testing
    """
    env = Environment(None, EnvironmentType.INTG)
    container = {DOMAIN}ServiceContainer(env)
    await container.initialize()
    return container


async def create_production_container() -> {DOMAIN}ServiceContainer:
    """
    Create and initialize service container for production environment.
    
    Returns:
        {DOMAIN}ServiceContainer: Container configured for production
    """
    env = Environment(None, EnvironmentType.PROD)
    container = {DOMAIN}ServiceContainer(env)
    await container.initialize()
    return container


# ========================================================================================
# USAGE EXAMPLES AND TESTING HELPERS
# ========================================================================================

"""
USAGE EXAMPLES:

1. Basic Service Access:
    service = await get_{DOMAIN.lower()}_service()
    result = await service.create_{DOMAIN.lower()}(dto)

2. Environment-Specific Access:
    dev_service = await get_{DOMAIN.lower()}_service(
        Environment(None, EnvironmentType.DEV)
    )

3. Container Management:
    container = await get_service_container()
    health = container.get_health_status()
    await shutdown_service_container()  # On app shutdown

4. FastAPI Integration:
    @router.post("/api/{DOMAIN.lower()}/")
    async def create(
        request: {DOMAIN}Request,
        service: {DOMAIN}ServiceInterface = Depends(provide_{DOMAIN.lower()}_service)
    ):
        dto = convert_request_to_dto(request)
        return await service.create_{DOMAIN.lower()}(dto)

5. Testing:
    @pytest.fixture
    async def service_container():
        container = await create_development_container()
        yield container
        await container.shutdown()
    
    async def test_{DOMAIN.lower()}_creation(service_container):
        service = service_container.get_{DOMAIN.lower()}_service()
        result = await service.create_{DOMAIN.lower()}(test_dto)
        assert result.success
"""