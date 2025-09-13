"""
Service Architecture Validation Tests.

Simple integration tests to validate the service architecture
works correctly without complex environment dependencies.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    InstrumentSearchCriteria,
    InstrumentOperationResult
)
from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl


class TestServiceArchitecture:
    """Test service architecture components work correctly"""
    
    @pytest.fixture
    def mock_daos(self):
        """Create mock DAOs for testing"""
        instruments_dao = Mock()
        instruments_dao.get_instrument = AsyncMock(return_value=None)
        instruments_dao.get_instrument_by_symbol = AsyncMock(return_value=None)
        instruments_dao.list_instruments = AsyncMock(return_value=[])
        instruments_dao.count_instruments = AsyncMock(return_value=0)
        instruments_dao.create_instrument = AsyncMock(return_value=1)
        
        xrefs_dao = Mock()
        xrefs_dao.resolve_instrument_id_by_symbol = AsyncMock(return_value=None)
        xrefs_dao.list_xrefs_for_instrument = AsyncMock(return_value=[])
        xrefs_dao.create_xref = AsyncMock(return_value=1)
        
        vendors_dao = Mock()
        vendors_dao.get_vendor_by_name = AsyncMock(return_value={'id': 1, 'name': 'ticker'})
        
        return {
            'instruments_dao': instruments_dao,
            'xrefs_dao': xrefs_dao,
            'vendors_dao': vendors_dao,
            'vendor_daos': {}
        }
    
    @pytest.fixture
    def instrument_service(self, mock_daos):
        """Create instrument service with mocked DAOs"""
        return InstrumentServiceImpl(
            instruments_dao=mock_daos['instruments_dao'],
            xrefs_dao=mock_daos['xrefs_dao'],
            vendors_dao=mock_daos['vendors_dao'],
            vendor_daos=mock_daos['vendor_daos']
        )
    
    @pytest.mark.asyncio
    async def test_service_interface_compliance(self, instrument_service):
        """Test service implements interface correctly"""
        assert isinstance(instrument_service, InstrumentServiceInterface)
        
        # Test all interface methods exist
        interface_methods = [
            'get_instrument_by_id',
            'get_instrument_by_symbol',
            'list_instruments',
            'get_instrument_count',
            'validate_symbol',
            'create_instrument',
            'create_cross_reference',
            'get_cross_references'
        ]
        
        for method_name in interface_methods:
            assert hasattr(instrument_service, method_name)
            assert callable(getattr(instrument_service, method_name))
    
    @pytest.mark.asyncio
    async def test_basic_operations(self, instrument_service):
        """Test basic service operations work"""
        
        # Test get_instrument_count
        count = await instrument_service.get_instrument_count()
        assert isinstance(count, int)
        assert count >= 0
        
        # Test list_instruments
        criteria = InstrumentSearchCriteria(limit=10)
        results = await instrument_service.list_instruments(criteria)
        assert isinstance(results, list)
        
        # Test validate_symbol
        is_valid = await instrument_service.validate_symbol("TEST")
        assert isinstance(is_valid, bool)
        
        # Test get_instrument_by_id
        instrument = await instrument_service.get_instrument_by_id(1)
        # Should return None with mock data
        assert instrument is None
    
    @pytest.mark.asyncio
    async def test_create_operations(self, instrument_service):
        """Test create operations work"""
        
        # Test create instrument
        instrument_dto = InstrumentDTO(
            symbol="TEST",
            name="Test Instrument",
            exchange="NYSE"
        )
        
        result = await instrument_service.create_instrument(instrument_dto)
        assert isinstance(result, InstrumentOperationResult)
        assert hasattr(result, 'success')
        assert hasattr(result, 'instrument_id')
    
    @pytest.mark.asyncio
    async def test_error_handling(self, instrument_service):
        """Test service handles errors gracefully"""
        
        # Test with invalid data
        invalid_instrument = InstrumentDTO(symbol="")  # Empty symbol
        result = await instrument_service.create_instrument(invalid_instrument)
        
        assert isinstance(result, InstrumentOperationResult)
        assert result.success is False
        assert result.error_message is not None
    
    @pytest.mark.asyncio
    async def test_dto_conversion(self, instrument_service, mock_daos):
        """Test DTO conversion works correctly"""
        
        # Mock DAO to return raw database record
        mock_record = {
            'id': 1,
            'symbol': 'AAPL', 
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'type': 'stock',
            'currency': 'USD'
        }
        
        mock_daos['instruments_dao'].get_instrument.return_value = mock_record
        
        result = await instrument_service.get_instrument_by_id(1)
        
        # Should convert to DTO
        assert isinstance(result, InstrumentDTO)
        assert result.symbol == 'AAPL'
        assert result.name == 'Apple Inc.'
        assert result.instrument_type == 'stock'
    
    def test_service_patterns(self, instrument_service):
        """Test service follows proper architectural patterns"""
        
        # Test service coordinates DAOs
        assert hasattr(instrument_service, 'instruments_dao')
        assert hasattr(instrument_service, 'xrefs_dao')
        assert hasattr(instrument_service, 'vendors_dao')
        
        # Test service provides proper methods
        assert hasattr(instrument_service, 'create_instrument')
        assert hasattr(instrument_service, 'get_instrument_by_id')


class TestServiceMigrationSuccess:
    """Validate service migration was successful"""
    
    def test_import_paths_work(self):
        """Test all service imports work correctly"""
        
        # Test service interface import
        from domains.instruments.services.interfaces.instrument_service_interface import InstrumentServiceInterface
        assert InstrumentServiceInterface is not None
        
        # Test service implementation import  
        from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
        assert InstrumentServiceImpl is not None
        
        # Test service container import
        from domains.instruments.services.config.service_container import get_instrument_service
        assert get_instrument_service is not None
    
    def test_dto_models_work(self):
        """Test DTO models work correctly"""
        
        from domains.instruments.services.interfaces.instrument_service_interface import (
            InstrumentDTO,
            InstrumentXrefDTO, 
            InstrumentSearchCriteria,
            InstrumentOperationResult
        )
        
        # Test InstrumentDTO
        instrument = InstrumentDTO(symbol="AAPL", name="Apple Inc.")
        assert instrument.symbol == "AAPL"
        assert instrument.name == "Apple Inc."
        
        # Test InstrumentSearchCriteria
        criteria = InstrumentSearchCriteria(symbols=["AAPL"], limit=10)
        assert criteria.symbols == ["AAPL"]
        assert criteria.limit == 10
        
        # Test InstrumentOperationResult
        result = InstrumentOperationResult(success=True, instrument_id=1)
        assert result.success is True
        assert result.instrument_id == 1
    
    def test_migration_completeness(self):
        """Test migration touched all necessary components"""
        
        # Test files exist
        import os
        
        service_files = [
            'src/domains/instruments/services/interfaces/instrument_service_interface.py',
            'src/domains/instruments/services/impl/instrument_service_impl.py',
            'src/domains/instruments/services/config/service_container.py'
        ]
        
        for file_path in service_files:
            full_path = os.path.join('/home/jianjun/ats-genai-data', file_path)
            assert os.path.exists(full_path), f"Service file missing: {file_path}"
    
    def test_api_integration(self):
        """Test API integrates with service layer"""
        
        from services.web_services.api.enhanced_instruments_api import app
        
        # Test API can be created
        assert app is not None
        assert app.title == "ATS Instruments API (Enhanced)"
        
        # Test routes are registered
        routes = [route.path for route in app.routes if hasattr(route, 'path')]
        assert len(routes) > 0
        
        # Test OpenAPI schema can be generated
        schema = app.openapi()
        assert 'openapi' in schema


if __name__ == "__main__":
    pytest.main([__file__, "-v"])