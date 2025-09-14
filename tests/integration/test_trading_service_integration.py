"""
Integration tests for Trading Service architecture.

Tests end-to-end functionality including:
- Trading service layer integration
- Universe and membership management
- Factor interval operations
- Service container functionality
"""

import pytest
import asyncio
from datetime import datetime, date, timedelta
from decimal import Decimal

from domains.trading.services.interfaces.trading_service_interface import (
    TradingServiceInterface,
    UniverseDTO,
    UniverseMembershipDTO,
    FactorIntervalDTO,
    UniverseSearchCriteria,
    FactorSearchCriteria,
    PortfolioOptimizationRequest,
    TradingOperationResult
)
from core.platform.config.environment import Environment, EnvironmentType


class TestTradingServiceArchitecture:
    """Test trading service architecture components work correctly"""

    @pytest.fixture
    def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)

    @pytest.fixture
    def sample_universe(self):
        """Sample universe for testing"""
        return UniverseDTO(
            name="Tech_Universe",
            description="Technology stocks universe for testing"
        )

    @pytest.fixture
    def sample_membership(self):
        """Sample universe membership for testing"""
        return UniverseMembershipDTO(
            universe_id=1,
            symbol="AAPL",
            instrument_id=123,
            start_date=datetime.utcnow() - timedelta(days=30)
        )

    @pytest.fixture
    def sample_factor(self):
        """Sample factor interval for testing"""
        return FactorIntervalDTO(
            universe_state_interval_id=1,
            factor_name="momentum",
            factor_value=Decimal("0.75")
        )

    def test_service_interface_compliance(self):
        """Test service implements interface correctly"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl

        # Test implementation implements interface
        assert issubclass(TradingServiceImpl, TradingServiceInterface)

        # Test all interface methods exist
        interface_methods = [
            'create_universe',
            'get_universe_by_id',
            'get_universe_by_name',
            'list_universes',
            'update_universe',
            'delete_universe',
            'add_universe_member',
            'remove_universe_member',
            'get_universe_members',
            'get_active_memberships',
            'update_membership_batch',
            'create_factor_interval',
            'get_factor_interval_by_id',
            'list_factor_intervals',
            'create_factor_intervals_batch',
            'delete_factor_interval',
            'get_factors_by_universe_state',
            'create_universe_state_interval',
            'get_universe_state_interval',
            'get_universe_states_by_period',
            'optimize_portfolio',
            'calculate_portfolio_metrics',
            'get_universe_correlation_matrix',
            'calculate_factor_exposures',
            'get_universe_analytics',
            'calculate_universe_returns',
            'get_factor_performance',
            'detect_universe_anomalies',
            'calculate_var',
            'stress_test_portfolio',
            'calculate_portfolio_beta',
            'validate_universe_data',
            'get_universe_coverage_report',
            'reconcile_universe_memberships',
            'export_universe_data',
            'clone_universe',
            'merge_universes'
        ]

        for method_name in interface_methods:
            assert hasattr(TradingServiceImpl, method_name)
            assert callable(getattr(TradingServiceImpl, method_name))

    def test_dto_models_work(self):
        """Test DTO models work correctly"""

        # Test UniverseDTO
        universe = UniverseDTO(
            name="Test Universe",
            description="Test universe description"
        )
        assert universe.name == "Test Universe"
        assert universe.description == "Test universe description"

        # Test UniverseMembershipDTO
        membership = UniverseMembershipDTO(
            universe_id=1,
            symbol="TSLA",
            instrument_id=456,
            start_date=datetime.utcnow()
        )
        assert membership.universe_id == 1
        assert membership.symbol == "TSLA"
        assert membership.instrument_id == 456

        # Test FactorIntervalDTO
        factor = FactorIntervalDTO(
            universe_state_interval_id=2,
            factor_name="value",
            factor_value=Decimal("1.25")
        )
        assert factor.universe_state_interval_id == 2
        assert factor.factor_name == "value"
        assert factor.factor_value == Decimal("1.25")

        # Test UniverseSearchCriteria
        criteria = UniverseSearchCriteria(
            name_pattern="Tech",
            active_only=True,
            limit=10
        )
        assert criteria.name_pattern == "Tech"
        assert criteria.active_only is True
        assert criteria.limit == 10

        # Test FactorSearchCriteria
        factor_criteria = FactorSearchCriteria(
            universe_state_interval_id=1,
            factor_names=["momentum", "value"],
            limit=100
        )
        assert factor_criteria.universe_state_interval_id == 1
        assert factor_criteria.factor_names == ["momentum", "value"]

        # Test TradingOperationResult
        result = TradingOperationResult(
            success=True,
            record_id=123,
            created_count=1
        )
        assert result.success is True
        assert result.record_id == 123
        assert result.created_count == 1

    def test_service_container_integration(self):
        """Test service container integrates correctly"""
        from domains.trading.services.config.trading_service_container import (
            TradingServiceContainer
        )
        from unittest.mock import Mock

        # Create mock environment to avoid database dependency issues
        mock_env = Mock()
        mock_env.env_type = EnvironmentType.DEV
        mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
        mock_env.get_table_name.side_effect = lambda table: f"test_{table}"

        container = TradingServiceContainer(mock_env)

        assert container is not None
        assert container.environment == mock_env
        assert not container._initialized

        # Test health status
        health = container.get_health_status()
        assert "initialized" in health
        assert "environment" in health
        assert health["status"] == "not_initialized"

    def test_import_paths_work(self):
        """Test all service imports work correctly"""

        # Test service interface import
        from domains.trading.services.interfaces.trading_service_interface import TradingServiceInterface
        assert TradingServiceInterface is not None

        # Test service implementation import
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        assert TradingServiceImpl is not None

        # Test service container import
        from domains.trading.services.config.trading_service_container import get_trading_service
        assert get_trading_service is not None


class TestTradingServiceLogic:
    """Test trading service business logic"""

    @pytest.fixture
    def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)

    def test_universe_validation_logic(self):
        """Test universe validation business logic"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock, AsyncMock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        universe_dao.get_universe_by_name = AsyncMock(return_value=None)
        universe_dao.create_universe = AsyncMock(return_value=1)

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test valid universe creation
        valid_universe = UniverseDTO(
            name="Valid Universe",
            description="A valid test universe"
        )

        result = asyncio.run(service.create_universe(valid_universe))
        assert result.success is True
        assert result.record_id == 1
        assert result.created_count == 1

        # Test invalid universe creation (missing name)
        invalid_universe = UniverseDTO(
            name="",  # Empty name
            description="Invalid universe"
        )

        result = asyncio.run(service.create_universe(invalid_universe))
        assert result.success is False
        assert "name is required" in result.error_message

        # Test duplicate universe creation
        universe_dao.get_universe_by_name = AsyncMock(return_value={'id': 1, 'name': 'Existing'})

        duplicate_universe = UniverseDTO(
            name="Existing",
            description="Duplicate universe"
        )

        result = asyncio.run(service.create_universe(duplicate_universe))
        assert result.success is False
        assert "already exists" in result.error_message

    def test_dto_conversion_logic(self):
        """Test DAO to DTO conversion logic"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test universe DAO to DTO conversion
        dao_universe = {
            'id': 1,
            'name': 'Tech Universe',
            'description': 'Technology stocks',
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }

        universe_dto = service._dao_to_universe_dto(dao_universe)
        assert isinstance(universe_dto, UniverseDTO)
        assert universe_dto.id == 1
        assert universe_dto.name == 'Tech Universe'
        assert universe_dto.description == 'Technology stocks'

        # Test membership DAO to DTO conversion
        dao_membership = {
            'id': 2,
            'universe_id': 1,
            'symbol': 'AAPL',
            'instrument_id': 123,
            'start_date': datetime.utcnow(),
            'end_date': None
        }

        membership_dto = service._dao_to_membership_dto(dao_membership)
        assert isinstance(membership_dto, UniverseMembershipDTO)
        assert membership_dto.id == 2
        assert membership_dto.universe_id == 1
        assert membership_dto.symbol == 'AAPL'
        assert membership_dto.instrument_id == 123

        # Test factor DAO to DTO conversion
        dao_factor = {
            'id': 3,
            'universe_state_interval_id': 1,
            'factor_name': 'momentum',
            'factor_value': 0.85,
            'created_at': datetime.utcnow()
        }

        factor_dto = service._dao_to_factor_dto(dao_factor)
        assert isinstance(factor_dto, FactorIntervalDTO)
        assert factor_dto.id == 3
        assert factor_dto.universe_state_interval_id == 1
        assert factor_dto.factor_name == 'momentum'
        assert factor_dto.factor_value == Decimal('0.85')

    def test_business_logic_error_handling(self):
        """Test business logic handles errors gracefully"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock, AsyncMock

        # Create service with mock DAOs that raise exceptions
        universe_dao = Mock()
        universe_dao.get_universe_by_name = AsyncMock(side_effect=Exception("Database error"))

        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test universe creation error handling
        test_universe = UniverseDTO(
            name="Test Universe",
            description="Test universe"
        )

        result = asyncio.run(service.create_universe(test_universe))
        assert result.success is False
        assert result.error_message is not None
        assert "Database error" in result.error_message

    def test_portfolio_optimization_logic(self):
        """Test portfolio optimization logic"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock, AsyncMock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        # Mock active memberships
        sample_memberships = [
            {'universe_id': 1, 'symbol': 'AAPL', 'instrument_id': 1, 'end_date': None, 'start_date': datetime.utcnow()},
            {'universe_id': 1, 'symbol': 'GOOGL', 'instrument_id': 2, 'end_date': None, 'start_date': datetime.utcnow()},
            {'universe_id': 1, 'symbol': 'MSFT', 'instrument_id': 3, 'end_date': None, 'start_date': datetime.utcnow()}
        ]

        universe_membership_dao.get_active_memberships = AsyncMock(return_value=sample_memberships)

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test portfolio optimization
        optimization_request = PortfolioOptimizationRequest(
            universe_id=1,
            objective='max_return',
            target_date=date.today()
        )

        result = asyncio.run(service.optimize_portfolio(optimization_request))

        # Should return equal weights for the three stocks
        assert result.universe_id == 1
        assert len(result.weights) == 3
        assert 'AAPL' in result.weights
        assert 'GOOGL' in result.weights
        assert 'MSFT' in result.weights

        # Each weight should be approximately 1/3
        expected_weight = Decimal('1.0') / 3
        for weight in result.weights.values():
            assert abs(weight - expected_weight) < Decimal('0.01')

    def test_universe_analytics_logic(self):
        """Test universe analytics calculations"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock, AsyncMock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        # Mock universe and memberships
        universe_dao.get_universe = AsyncMock(return_value={
            'id': 1,
            'name': 'Test Universe',
            'description': 'Test description',
            'created_at': datetime.utcnow()
        })

        sample_memberships = [
            {'universe_id': 1, 'symbol': 'AAPL', 'end_date': None, 'start_date': datetime.utcnow()},
            {'universe_id': 1, 'symbol': 'GOOGL', 'end_date': None, 'start_date': datetime.utcnow()},
            {'universe_id': 1, 'symbol': 'MSFT', 'end_date': datetime.utcnow() - timedelta(days=30), 'start_date': datetime.utcnow()}  # Inactive
        ]

        universe_membership_dao.get_memberships_by_universe = AsyncMock(return_value=sample_memberships)

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test analytics calculation
        analytics = asyncio.run(service.get_universe_analytics(1))

        assert analytics['universe_id'] == 1
        assert analytics['universe_name'] == 'Test Universe'
        assert analytics['total_members'] == 3
        assert analytics['active_members'] == 2  # Only AAPL and GOOGL are active


class TestTradingServiceMigrationValidation:
    """Validate the trading service migration is working correctly"""

    def test_trading_migration_completeness(self):
        """Test migration touched all necessary components"""

        # Test files exist
        import os

        service_files = [
            'src/domains/trading/services/interfaces/trading_service_interface.py',
            'src/domains/trading/services/impl/trading_service_impl.py',
            'src/domains/trading/services/config/trading_service_container.py'
        ]

        for file_path in service_files:
            full_path = os.path.join('/home/jianjun/ats-genai-data', file_path)
            assert os.path.exists(full_path), f"Trading service file missing: {file_path}"

    def test_trading_service_patterns(self):
        """Test trading service follows proper architectural patterns"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test service coordinates DAOs
        assert hasattr(service, 'universe_dao')
        assert hasattr(service, 'universe_membership_dao')
        assert hasattr(service, 'factor_interval_dao')

        # Test service provides proper methods
        assert hasattr(service, 'create_universe')
        assert hasattr(service, 'add_universe_member')
        assert hasattr(service, 'create_factor_interval')

        # Test service has conversion methods
        assert hasattr(service, '_dao_to_universe_dto')
        assert hasattr(service, '_dao_to_membership_dto')
        assert hasattr(service, '_dao_to_factor_dto')

    def test_trading_service_business_logic_patterns(self):
        """Test trading service implements business logic patterns correctly"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock, AsyncMock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_dao.get_universe_by_name = AsyncMock(return_value=None)
        universe_dao.create_universe = AsyncMock(return_value=1)

        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test business validation patterns
        invalid_universe = UniverseDTO(name="")  # Missing required field
        result = asyncio.run(service.create_universe(invalid_universe))

        assert result.success is False
        assert "required" in result.error_message.lower()

        # Test successful business operation patterns
        valid_universe = UniverseDTO(
            name="Valid Universe",
            description="A valid test universe"
        )
        result = asyncio.run(service.create_universe(valid_universe))

        # Should succeed with mock DAO
        assert result.success is True
        assert result.created_count == 1
        assert result.record_id == 1

    def test_trading_service_utility_operations(self):
        """Test trading service utility operations"""
        from domains.trading.services.impl.trading_service_impl import TradingServiceImpl
        from unittest.mock import Mock, AsyncMock

        # Create service with mock DAOs
        universe_dao = Mock()
        universe_membership_dao = Mock()
        factor_interval_dao = Mock()

        # Mock universe and memberships for export test
        universe_dao.get_universe = AsyncMock(return_value={
            'id': 1,
            'name': 'Export Test Universe',
            'description': 'Test universe for export'
        })

        sample_memberships = [
            {'universe_id': 1, 'symbol': 'AAPL', 'instrument_id': 1, 'start_date': datetime.utcnow(), 'end_date': None},
            {'universe_id': 1, 'symbol': 'GOOGL', 'instrument_id': 2, 'start_date': datetime.utcnow(), 'end_date': None}
        ]

        universe_membership_dao.get_memberships_by_universe = AsyncMock(return_value=sample_memberships)

        service = TradingServiceImpl(
            universe_dao=universe_dao,
            universe_membership_dao=universe_membership_dao,
            factor_interval_dao=factor_interval_dao
        )

        # Test CSV export
        csv_export = asyncio.run(service.export_universe_data(1, format="csv"))
        assert isinstance(csv_export, str)
        assert 'symbol,instrument_id,start_date,end_date' in csv_export
        assert 'AAPL' in csv_export
        assert 'GOOGL' in csv_export

        # Test JSON export
        json_export = asyncio.run(service.export_universe_data(1, format="json"))
        assert isinstance(json_export, dict)
        assert 'universe' in json_export
        assert 'members' in json_export
        assert json_export['universe']['name'] == 'Export Test Universe'
        assert len(json_export['members']) == 2


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_trading_service_integration.py -v
    pytest.main([__file__, "-v", "--tb=short"])