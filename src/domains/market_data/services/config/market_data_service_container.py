"""
Market Data Service Container

Manages dependency injection and lifecycle for MarketDataService components.
Provides environment-based configuration and health monitoring.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from core.platform.config.environment import Environment, EnvironmentType
from ..interfaces.market_data_service_interface import MarketDataServiceInterface
from ..impl.market_data_service_impl import MarketDataServiceImpl
from ...repositories.daily_price_polygon_dao import DailyPricesDAO
from ...repositories.fundamentals_dao import FundamentalsDAO

# Optional instruments DAO import with fallback
try:
    from domains.instruments.repositories.instruments_dao import InstrumentsDAO
except ImportError:
    InstrumentsDAO = None


class MarketDataServiceContainer:
    """
    Container for MarketDataService dependencies and configuration.

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
        self._service_instance: Optional[MarketDataServiceInterface] = None
        self._daily_price_polygon_dao: Optional[DailyPricesDAO] = None
        self._fundamentals_dao: Optional[FundamentalsDAO] = None
        self._instruments_dao: Optional[Any] = None

    async def initialize(self) -> None:
        """Initialize the service container and all dependencies"""
        if self._initialized:
            return

        try:
            self.logger.info(f"Initializing MarketDataServiceContainer for environment: {self.environment.env_type}")

            # Initialize DAOs
            self._daily_price_polygon_dao = DailyPricesDAO(self.environment)
            self._fundamentals_dao = FundamentalsDAO(self.environment)

            # Initialize instruments DAO if available
            if InstrumentsDAO:
                self._instruments_dao = InstrumentsDAO(self.environment)
                self.logger.info("Instruments DAO initialized")
            else:
                self.logger.warning("Instruments DAO not available - some functionality will be limited")

            # Initialize service
            self._service_instance = MarketDataServiceImpl(
                daily_price_polygon_dao=self._daily_price_polygon_dao,
                fundamentals_dao=self._fundamentals_dao,
                instruments_dao=self._instruments_dao
            )

            self._initialized = True
            self.logger.info("MarketDataServiceContainer initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize MarketDataServiceContainer: {e}")
            raise

    async def get_service(self) -> MarketDataServiceInterface:
        """Get the MarketDataService instance"""
        if not self._initialized:
            await self.initialize()

        if not self._service_instance:
            raise RuntimeError("MarketDataService not properly initialized")

        return self._service_instance

    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the service container"""
        return {
            'service_name': 'MarketDataService',
            'initialized': self._initialized,
            'environment': self.environment.env_type.value if self.environment.env_type else 'unknown',
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'healthy' if self._initialized else 'not_initialized',
            'components': {
                'daily_price_polygon_dao': self._daily_price_polygon_dao is not None,
                'fundamentals_dao': self._fundamentals_dao is not None,
                'instruments_dao': self._instruments_dao is not None,
                'service_instance': self._service_instance is not None
            }
        }

    async def cleanup(self) -> None:
        """Cleanup resources and connections"""
        try:
            self.logger.info("Cleaning up MarketDataServiceContainer")

            # Cleanup DAOs if they have cleanup methods
            # Current DAOs don't have explicit cleanup, but this is where it would go

            self._service_instance = None
            self._daily_price_polygon_dao = None
            self._fundamentals_dao = None
            self._instruments_dao = None
            self._initialized = False

            self.logger.info("MarketDataServiceContainer cleanup completed")

        except Exception as e:
            self.logger.error(f"Error during MarketDataServiceContainer cleanup: {e}")


# Global container instances (one per environment type)
_containers: Dict[EnvironmentType, MarketDataServiceContainer] = {}


def get_market_data_service_container(environment: Optional[Environment] = None) -> MarketDataServiceContainer:
    """
    Get or create a MarketDataServiceContainer for the given environment.

    Args:
        environment: Environment configuration. If None, creates with DEV environment.

    Returns:
        MarketDataServiceContainer instance
    """
    if environment is None:
        environment = Environment(None, EnvironmentType.DEV)

    env_type = environment.env_type or EnvironmentType.DEV

    if env_type not in _containers:
        _containers[env_type] = MarketDataServiceContainer(environment)

    return _containers[env_type]


async def get_market_data_service(environment: Optional[Environment] = None) -> MarketDataServiceInterface:
    """
    Get a MarketDataService instance for the given environment.

    This is the main entry point for getting the market data service.

    Args:
        environment: Environment configuration. If None, uses DEV environment.

    Returns:
        MarketDataServiceInterface implementation
    """
    container = get_market_data_service_container(environment)
    return await container.get_service()


async def cleanup_market_data_service(environment: Optional[Environment] = None) -> None:
    """
    Cleanup MarketDataService resources for the given environment.

    Args:
        environment: Environment configuration. If None, cleans up DEV environment.
    """
    if environment is None:
        environment = Environment(None, EnvironmentType.DEV)

    env_type = environment.env_type or EnvironmentType.DEV

    if env_type in _containers:
        await _containers[env_type].cleanup()
        del _containers[env_type]


async def get_market_data_service_health_status(environment: Optional[Environment] = None) -> Dict[str, Any]:
    """
    Get health status for MarketDataService in the given environment.

    Args:
        environment: Environment configuration. If None, uses DEV environment.

    Returns:
        Health status dictionary
    """
    try:
        container = get_market_data_service_container(environment)
        return container.get_health_status()
    except Exception as e:
        return {
            'service_name': 'MarketDataService',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


# Development and testing utilities

async def reset_market_data_service_container(environment: Optional[Environment] = None) -> None:
    """
    Reset the MarketDataService container (useful for testing).

    Args:
        environment: Environment configuration. If None, resets DEV environment.
    """
    await cleanup_market_data_service(environment)
    # Container will be recreated on next access


def list_market_data_service_containers() -> Dict[str, Dict[str, Any]]:
    """
    List all active MarketDataService containers and their status.

    Returns:
        Dictionary mapping environment names to health status
    """
    containers_status = {}

    for env_type, container in _containers.items():
        containers_status[env_type.value] = container.get_health_status()

    return containers_status


# Configuration and diagnostics

def validate_market_data_service_configuration(environment: Environment) -> Dict[str, Any]:
    """
    Validate MarketDataService configuration for the given environment.

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
        daily_price_polygon_table = environment.get_table_name('daily_price_polygon')
        fundamentals_table = environment.get_table_name('fundamentals')

        if not daily_price_polygon_table:
            issues.append("Daily prices table name not configured")
        if not fundamentals_table:
            issues.append("Fundamentals table name not configured")

    except Exception as e:
        issues.append(f"Table configuration error: {e}")

    # Recommendations
    if InstrumentsDAO is None:
        recommendations.append("Consider installing instruments DAO for full functionality")

    return {
        'valid': len(issues) == 0,
        'issues': issues,
        'recommendations': recommendations,
        'environment_type': environment.env_type.value if environment.env_type else 'unknown',
        'validation_timestamp': datetime.utcnow().isoformat()
    }