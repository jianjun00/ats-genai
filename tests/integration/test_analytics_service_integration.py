"""
Integration tests for Analytics Service architecture.

Tests end-to-end functionality including:
- Analytics service layer integration
- Events and economic events management 
- Data quality validation
- Service container functionality
"""

import pytest
import asyncio
from datetime import datetime, date
from decimal import Decimal

from domains.analytics.services.interfaces.analytics_service_interface import (
    AnalyticsServiceInterface,
    EventDTO,
    EconomicEventTypeDTO,
    EconomicEventDTO,
    EventSearchCriteria,
    EconomicEventSearchCriteria
)
from core.platform.config.environment import Environment, EnvironmentType


class TestAnalyticsServiceArchitecture:
    """Test analytics service architecture components work correctly"""
    
    @pytest.fixture
    def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    @pytest.fixture
    def sample_event(self):
        """Sample event for testing"""
        return EventDTO(
            event_type="earnings_announcement",
            instrument_id=1,
            event_time=datetime.utcnow(),
            reported_time=datetime.utcnow(),
            source="test_source",
            data={"eps": 2.45, "revenue": 1000000000}
        )
    
    @pytest.fixture
    def sample_economic_event_type(self):
        """Sample economic event type for testing"""
        return EconomicEventTypeDTO(
            name="GDP Growth Rate",
            description="Quarterly GDP growth rate",
            category="Economic Growth",
            country="US",
            importance_level=5,
            frequency="Quarterly",
            typical_release_time="08:30"
        )
    
    @pytest.fixture
    def sample_economic_event(self):
        """Sample economic event for testing"""
        return EconomicEventDTO(
            event_type_id=1,
            date=date.today(),
            release_time=datetime.utcnow(),
            estimate=Decimal("2.1"),
            actual=Decimal("2.3"),
            previous=Decimal("2.0"),
            unit="percent",
            currency="USD",
            source_vendor="test_vendor",
            source_event_id="GDP_Q1_2024"
        )
    
    def test_service_interface_compliance(self):
        """Test service implements interface correctly"""
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        
        # Test implementation implements interface
        assert issubclass(AnalyticsServiceImpl, AnalyticsServiceInterface)
        
        # Test all interface methods exist
        interface_methods = [
            'create_event',
            'get_event_by_id',
            'list_events',
            'get_events_count',
            'create_events_batch',
            'create_economic_event_type',
            'get_economic_event_type_by_id',
            'list_economic_event_types',
            'update_economic_event_type',
            'create_economic_event',
            'get_economic_event_by_id',
            'list_economic_events',
            'update_economic_event',
            'create_economic_events_batch',
            'get_economic_calendar',
            'get_events_by_instrument',
            'get_analytics_summary',
            'validate_event_data',
            'get_data_quality_report'
        ]
        
        for method_name in interface_methods:
            assert hasattr(AnalyticsServiceImpl, method_name)
            assert callable(getattr(AnalyticsServiceImpl, method_name))
    
    def test_dto_models_work(self):
        """Test DTO models work correctly"""
        
        # Test EventDTO
        event = EventDTO(
            event_type="test_event",
            instrument_id=123,
            event_time=datetime.utcnow()
        )
        assert event.event_type == "test_event"
        assert event.instrument_id == 123
        assert event.event_time is not None
        
        # Test EconomicEventTypeDTO
        event_type = EconomicEventTypeDTO(
            name="Test Event Type",
            category="Test Category",
            importance_level=3
        )
        assert event_type.name == "Test Event Type"
        assert event_type.category == "Test Category"
        assert event_type.importance_level == 3
        
        # Test EconomicEventDTO
        economic_event = EconomicEventDTO(
            event_type_id=1,
            date=date.today(),
            actual=Decimal("1.5")
        )
        assert economic_event.event_type_id == 1
        assert economic_event.date == date.today()
        assert economic_event.actual == Decimal("1.5")
        
        # Test EventSearchCriteria
        criteria = EventSearchCriteria(
            instrument_id=123,
            event_type="earnings",
            limit=50
        )
        assert criteria.instrument_id == 123
        assert criteria.event_type == "earnings"
        assert criteria.limit == 50
        
        # Test EconomicEventSearchCriteria
        econ_criteria = EconomicEventSearchCriteria(
            countries=["US", "UK"],
            importance_levels=[4, 5],
            start_date=date.today()
        )
        assert econ_criteria.countries == ["US", "UK"]
        assert econ_criteria.importance_levels == [4, 5]
        assert econ_criteria.start_date == date.today()
    
    def test_service_container_integration(self):
        """Test service container integrates correctly"""
        from domains.analytics.services.config.analytics_service_container import (
            AnalyticsServiceContainer
        )
        
        # Test container can be created
        env = Environment(None, EnvironmentType.DEV)
        container = AnalyticsServiceContainer(env)
        
        assert container is not None
        assert container.environment == env
        assert not container._initialized
        
        # Test health status
        health = container.get_health_status()
        assert "initialized" in health
        assert "environment" in health
        assert health["status"] == "not_initialized"
    
    def test_import_paths_work(self):
        """Test all service imports work correctly"""
        
        # Test service interface import
        from domains.analytics.services.interfaces.analytics_service_interface import AnalyticsServiceInterface
        assert AnalyticsServiceInterface is not None
        
        # Test service implementation import  
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        assert AnalyticsServiceImpl is not None
        
        # Test service container import
        from domains.analytics.services.config.analytics_service_container import get_analytics_service
        assert get_analytics_service is not None


class TestAnalyticsServiceLogic:
    """Test analytics service business logic"""
    
    @pytest.fixture
    def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    def test_event_validation_logic(self):
        """Test event validation business logic"""
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        events_dao = Mock()
        economic_events_dao = Mock()
        service = AnalyticsServiceImpl(events_dao, economic_events_dao)
        
        # Test validation logic
        valid_event = EventDTO(
            event_type="test_event",
            event_time=datetime.utcnow(),
            source="test_source"
        )
        
        validation_result = asyncio.run(service.validate_event_data(valid_event))
        assert validation_result["valid"] is True
        assert len(validation_result["issues"]) == 0
        
        # Test invalid event
        invalid_event = EventDTO(
            event_type="",  # Empty event type
            event_time=None,  # Missing event time
            data="invalid_data"  # Wrong data type
        )
        
        validation_result = asyncio.run(service.validate_event_data(invalid_event))
        assert validation_result["valid"] is False
        assert len(validation_result["issues"]) > 0
    
    def test_dto_conversion_logic(self):
        """Test DAO to DTO conversion logic"""
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        events_dao = Mock()
        economic_events_dao = Mock()
        service = AnalyticsServiceImpl(events_dao, economic_events_dao)
        
        # Test event DAO to DTO conversion
        dao_event = {
            'id': 1,
            'event_type': 'earnings',
            'instrument_id': 123,
            'event_time': datetime.utcnow(),
            'reported_time': datetime.utcnow(),
            'source': 'bloomberg',
            'data': {'eps': 2.45}
        }
        
        event_dto = service._dao_to_event_dto(dao_event)
        assert isinstance(event_dto, EventDTO)
        assert event_dto.id == 1
        assert event_dto.event_type == 'earnings'
        assert event_dto.instrument_id == 123
        assert event_dto.source == 'bloomberg'
        
        # Test economic event type DAO to DTO conversion
        dao_event_type = {
            'id': 1,
            'name': 'GDP Growth Rate',
            'description': 'Quarterly GDP growth',
            'category': 'Economic Growth',
            'country': 'US',
            'importance_level': 5,
            'frequency': 'Quarterly',
            'typical_release_time': '08:30'
        }
        
        event_type_dto = service._dao_to_economic_event_type_dto(dao_event_type)
        assert isinstance(event_type_dto, EconomicEventTypeDTO)
        assert event_type_dto.id == 1
        assert event_type_dto.name == 'GDP Growth Rate'
        assert event_type_dto.country == 'US'
        assert event_type_dto.importance_level == 5
        
        # Test economic event DAO to DTO conversion
        dao_economic_event = {
            'id': 1,
            'event_type_id': 1,
            'date': date.today(),
            'release_time': datetime.utcnow(),
            'estimate': Decimal('2.1'),
            'actual': Decimal('2.3'),
            'previous': Decimal('2.0'),
            'unit': 'percent',
            'currency': 'USD',
            'source_vendor': 'fred',
            'is_preliminary': False
        }
        
        economic_event_dto = service._dao_to_economic_event_dto(dao_economic_event)
        assert isinstance(economic_event_dto, EconomicEventDTO)
        assert economic_event_dto.id == 1
        assert economic_event_dto.event_type_id == 1
        assert economic_event_dto.actual == Decimal('2.3')
        assert economic_event_dto.currency == 'USD'
    
    def test_business_logic_error_handling(self):
        """Test business logic handles errors gracefully"""
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        from unittest.mock import Mock, AsyncMock
        
        # Create service with mock DAOs that raise exceptions
        events_dao = Mock()
        events_dao.insert_event = AsyncMock(side_effect=Exception("Database error"))
        
        economic_events_dao = Mock()
        economic_events_dao.create_event_type = AsyncMock(side_effect=Exception("Database error"))
        
        service = AnalyticsServiceImpl(events_dao, economic_events_dao)
        
        # Test event creation error handling
        test_event = EventDTO(
            event_type="test_event",
            event_time=datetime.utcnow()
        )
        
        result = asyncio.run(service.create_event(test_event))
        assert result.success is False
        assert result.error_message is not None
        assert "Database error" in result.error_message
        
        # Test economic event type creation error handling
        test_event_type = EconomicEventTypeDTO(
            name="Test Event Type"
        )
        
        result = asyncio.run(service.create_economic_event_type(test_event_type))
        assert result.success is False
        assert result.error_message is not None


class TestAnalyticsServiceMigrationValidation:
    """Validate the analytics service migration is working correctly"""
    
    def test_analytics_migration_completeness(self):
        """Test migration touched all necessary components"""
        
        # Test files exist
        import os
        
        service_files = [
            'src/domains/analytics/services/interfaces/analytics_service_interface.py',
            'src/domains/analytics/services/impl/analytics_service_impl.py',
            'src/domains/analytics/services/config/analytics_service_container.py'
        ]
        
        for file_path in service_files:
            full_path = os.path.join('/home/jianjun/ats-genai-data', file_path)
            assert os.path.exists(full_path), f"Analytics service file missing: {file_path}"
    
    def test_analytics_service_patterns(self):
        """Test analytics service follows proper architectural patterns"""
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        events_dao = Mock()
        economic_events_dao = Mock()
        service = AnalyticsServiceImpl(events_dao, economic_events_dao)
        
        # Test service coordinates DAOs
        assert hasattr(service, 'events_dao')
        assert hasattr(service, 'economic_events_dao')
        
        # Test service provides proper methods
        assert hasattr(service, 'create_event')
        assert hasattr(service, 'create_economic_event')
        assert hasattr(service, 'get_analytics_summary')
        
        # Test service has conversion methods
        assert hasattr(service, '_dao_to_event_dto')
        assert hasattr(service, '_dao_to_economic_event_dto')
        assert hasattr(service, '_dao_to_economic_event_type_dto')
    
    def test_analytics_service_business_logic_patterns(self):
        """Test analytics service implements business logic patterns correctly"""
        from domains.analytics.services.impl.analytics_service_impl import AnalyticsServiceImpl
        from unittest.mock import Mock, AsyncMock
        
        # Create service with mock DAOs
        events_dao = Mock()
        events_dao.insert_event = AsyncMock(return_value={'id': 1})
        
        economic_events_dao = Mock()
        economic_events_dao.create_event_type = AsyncMock(return_value=1)
        economic_events_dao.get_event_types = AsyncMock(return_value=[])
        
        service = AnalyticsServiceImpl(events_dao, economic_events_dao)
        
        # Test business validation patterns
        invalid_event = EventDTO(event_type="")  # Missing required field
        result = asyncio.run(service.create_event(invalid_event))
        
        assert result.success is False
        assert "required" in result.error_message.lower()
        
        # Test successful business operation patterns
        valid_event = EventDTO(
            event_type="test_event",
            event_time=datetime.utcnow(),
            source="test"
        )
        result = asyncio.run(service.create_event(valid_event))
        
        # Should succeed with mock DAO
        assert result.success is True
        assert result.created_count == 1
        assert result.event_id == 1


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_analytics_service_integration.py -v
    pytest.main([__file__, "-v", "--tb=short"])