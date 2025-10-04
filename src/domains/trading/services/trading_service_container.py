"""
Trading Service Container

Manages dependency injection and lifecycle for TradingService components.
Provides environment-based configuration and health monitoring.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from core.platform.config.environment import Environment, EnvironmentType
from core.services.unified_service_framework import TradingPlatformService
from ...repositories.universe_dao import UniverseDAO
from ...repositories.universe_membership_dao import UniverseMembershipDAO
from ...repositories.factor_interval_dao import FactorIntervalDAO

# Optional universe state interval DAO import with fallback
try:
    from ...repositories.universe_state_interval_dao import UniverseStateIntervalDAO
except ImportError:
    UniverseStateIntervalDAO = None

# Optional market data service import with fallback
try:
    from domains.market_data.services.config.market_data_service_container import get_market_data_service
except ImportError:
    get_market_data_service = None


class TradingServiceContainer:
    """
    Container for TradingService dependencies and configuration.
    
    Manages:
    - Service lifecycle and initialization
    - DAO dependency injection
    - Environment-specific configuration
    - Health monitoring and status reporting
    """
    
    def __init__(self, environment: Environment):
        self.environment = environment
        self.logger = logging.getLogger(__name__)
        self._initialized = False
        self._service_instance: Optional[TradingServiceInterface] = None
        self._universe_dao: Optional[UniverseDAO] = None
        self._universe_membership_dao: Optional[UniverseMembershipDAO] = None
        self._factor_interval_dao: Optional[FactorIntervalDAO] = None
        self._universe_state_interval_dao: Optional[Any] = None
        self._market_data_service: Optional[Any] = None
    
    async def initialize(self) -> None:
        """Initialize the service container and all dependencies"""
        if self._initialized:
            return
        
        try:
            self.logger.info(f"Initializing TradingServiceContainer for environment: {self.environment.env_type}")
            
            # Initialize DAOs
            self._universe_dao = UniverseDAO(self.environment)
            self._universe_membership_dao = UniverseMembershipDAO(self.environment)
            self._factor_interval_dao = FactorIntervalDAO(self.environment)
            
            # Initialize optional universe state interval DAO if available
            if UniverseStateIntervalDAO:
                self._universe_state_interval_dao = UniverseStateIntervalDAO(self.environment)
                self.logger.info("Universe state interval DAO initialized")
            else:
                self.logger.warning("Universe state interval DAO not available - some functionality will be limited")
            
            # Initialize optional market data service if available
            if get_market_data_service:
                try:
                    self._market_data_service = await get_market_data_service(self.environment)
                    self.logger.info("Market data service initialized")
                except Exception as e:
                    self.logger.warning(f"Failed to initialize market data service: {e}")
            else:
                self.logger.warning("Market data service not available - portfolio analytics will be limited")
            
            # Initialize service
            self._service_instance = TradingServiceImpl(
                universe_dao=self._universe_dao,
                universe_membership_dao=self._universe_membership_dao,
                factor_interval_dao=self._factor_interval_dao,
                universe_state_interval_dao=self._universe_state_interval_dao,
                market_data_service=self._market_data_service
            )
            
            self._initialized = True
            self.logger.info("TradingServiceContainer initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize TradingServiceContainer: {e}")
            raise
    
    async def get_service(self) -> TradingServiceInterface:
        """Get the TradingService instance"""
        if not self._initialized:
            await self.initialize()
        
        if not self._service_instance:
            raise RuntimeError("TradingService not properly initialized")
        
        return self._service_instance
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the service container"""
        return {
            'service_name': 'TradingService',
            'initialized': self._initialized,
            'environment': self.environment.env_type.value if self.environment.env_type else 'unknown',
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'healthy' if self._initialized else 'not_initialized',
            'components': {
                'universe_dao': self._universe_dao is not None,
                'universe_membership_dao': self._universe_membership_dao is not None,
                'factor_interval_dao': self._factor_interval_dao is not None,
                'universe_state_interval_dao': self._universe_state_interval_dao is not None,
                'market_data_service': self._market_data_service is not None,
                'service_instance': self._service_instance is not None
            }
        }
    
    async def cleanup(self) -> None:
        """Cleanup resources and connections"""
        try:
            self.logger.info("Cleaning up TradingServiceContainer")
            
            # Cleanup DAOs if they have cleanup methods
            # Current DAOs don't have explicit cleanup, but this is where it would go
            
            self._service_instance = None
            self._universe_dao = None
            self._universe_membership_dao = None
            self._factor_interval_dao = None
            self._universe_state_interval_dao = None
            self._market_data_service = None
            self._initialized = False
            
            self.logger.info("TradingServiceContainer cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during TradingServiceContainer cleanup: {e}")


# Global container instances (one per environment type)
_containers: Dict[EnvironmentType, TradingServiceContainer] = {}


def get_trading_service_container(environment: Optional[Environment] = None) -> TradingServiceContainer:
    """
    Get or create a TradingServiceContainer for the given environment.
    
    Args:
        environment: Environment configuration. If None, creates with DEV environment.
    
    Returns:
        TradingServiceContainer instance
    """
    if environment is None:
        environment = Environment(None, EnvironmentType.DEV)
    
    env_type = environment.env_type or EnvironmentType.DEV
    
    if env_type not in _containers:
        _containers[env_type] = TradingServiceContainer(environment)
    
    return _containers[env_type]


async def get_trading_service(environment: Optional[Environment] = None) -> TradingServiceInterface:
    """
    Get a TradingService instance for the given environment.
    
    This is the main entry point for getting the trading service.
    
    Args:
        environment: Environment configuration. If None, uses DEV environment.
    
    Returns:
        TradingServiceInterface implementation
    """
    container = get_trading_service_container(environment)
    return await container.get_service()


async def cleanup_trading_service(environment: Optional[Environment] = None) -> None:
    """
    Cleanup TradingService resources for the given environment.
    
    Args:
        environment: Environment configuration. If None, cleans up DEV environment.
    """
    if environment is None:
        environment = Environment(None, EnvironmentType.DEV)
    
    env_type = environment.env_type or EnvironmentType.DEV
    
    if env_type in _containers:
        await _containers[env_type].cleanup()
        del _containers[env_type]


async def get_trading_service_health_status(environment: Optional[Environment] = None) -> Dict[str, Any]:
    """
    Get health status for TradingService in the given environment.
    
    Args:
        environment: Environment configuration. If None, uses DEV environment.
    
    Returns:
        Health status dictionary
    """
    try:
        container = get_trading_service_container(environment)
        return container.get_health_status()
    except Exception as e:
        return {
            'service_name': 'TradingService',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


# Development and testing utilities

async def reset_trading_service_container(environment: Optional[Environment] = None) -> None:
    """
    Reset the TradingService container (useful for testing).
    
    Args:
        environment: Environment configuration. If None, resets DEV environment.
    """
    await cleanup_trading_service(environment)
    # Container will be recreated on next access


def list_trading_service_containers() -> Dict[str, Dict[str, Any]]:
    """
    List all active TradingService containers and their status.
    
    Returns:
        Dictionary mapping environment names to health status
    """
    containers_status = {}
    
    for env_type, container in _containers.items():
        containers_status[env_type.value] = container.get_health_status()
    
    return containers_status


# Configuration and diagnostics

def validate_trading_service_configuration(environment: Environment) -> Dict[str, Any]:
    """
    Validate TradingService configuration for the given environment.
    
    Args:
        environment: Environment configuration to validate
    
    Returns:
        Validation results with issues and recommendations
    """
    issues = []
    recommendations = []
    
    # Check environment configuration
    if not environment:
        issues.append("Environment configuration is missing")
    elif not environment.env_type:
        issues.append("Environment type is not specified")
    
    # Check database configuration
    try:
        db_url = environment.get_database_url()
        if not db_url:
            issues.append("Database URL is not configured")
    except Exception as e:
        issues.append(f"Database configuration error: {e}")
    
    # Check table name configuration
    try:
        universe_table = environment.get_table_name('universe')
        membership_table = environment.get_table_name('universe_membership')
        factor_table = environment.get_table_name('factor_interval')
        
        if not universe_table:
            issues.append("Universe table name not configured")
        if not membership_table:
            issues.append("Universe membership table name not configured")
        if not factor_table:
            issues.append("Factor interval table name not configured")
            
    except Exception as e:
        issues.append(f"Table configuration error: {e}")
    
    # Recommendations
    if UniverseStateIntervalDAO is None:
        recommendations.append("Consider implementing universe state interval DAO for full state management")
    
    if get_market_data_service is None:
        recommendations.append("Consider integrating market data service for portfolio analytics")
    
    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'recommendations': recommendations,
        'environment_type': environment.env_type.value if environment.env_type else 'unknown',
        'validation_timestamp': datetime.utcnow().isoformat()
    }