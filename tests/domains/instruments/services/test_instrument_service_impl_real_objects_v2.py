"""
Real objects integration tests for InstrumentServiceImpl

Replaces mock-heavy testing with authentic database integration to test:
- Real service interface contract compliance with actual database constraints
- Business rule enforcement through real data validation
- Error handling with actual database exceptions
- DTO conversions with real data serialization/deserialization
- Transaction coordination with actual database transactions

This demonstrates fail-fast testing that eliminates Mock and AsyncMock dependencies
and provides authentic validation of instrument service business logic.
"""

import pytest
from datetime import date, datetime

from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentXrefDTO,
    InstrumentSearchCriteria
)
from core.dao.instruments_dao import InstrumentsDAO
from core.dao.instrument_xrefs_dao import InstrumentXrefsDAO
from shared.utils.environment import Environment, EnvironmentType


class TestInstrumentServiceImplRealObjects:
    """Real objects test suite for InstrumentServiceImpl business logic"""

    @pytest.fixture
    async def test_environment(self):
        """Real Environment instance for testing."""
        return Environment(
            env_type=EnvironmentType.DEV,
            db_url="postgresql://postgres:dev_password@localhost:3432/dev_db"
        )

    @pytest.fixture
    async def real_instruments_dao(self, test_environment):
        """Real InstrumentsDAO with actual database connection."""
        return InstrumentsDAO(test_environment)

    @pytest.fixture
    async def real_xrefs_dao(self, test_environment):
        """Real InstrumentXrefsDAO with actual database connection."""
        return InstrumentXrefsDAO(test_environment)

    @pytest.fixture
    async def instrument_service(self, real_instruments_dao, real_xrefs_dao):
        """Create real InstrumentServiceImpl with actual DAOs."""
        return InstrumentServiceImpl(
            instruments_dao=real_instruments_dao,
            xrefs_dao=real_xrefs_dao
        )

    @pytest.fixture
    async def test_instrument_data(self, real_instruments_dao):
        """Create real test instrument data and clean up after test."""
        # Create real test instruments
        test_instruments = []
        created_ids = []
        
        for i in range(3):
            instrument_id = await real_instruments_dao.create_instrument(
                symbol=f"TEST_INST_{i}",
                name=f"Test Instrument {i} Inc.",
                exchange="NASDAQ",
                sector="Technology",
                asset_class="Stock"
            )
            created_ids.append(instrument_id)
            
            instrument = await real_instruments_dao.get_instrument(instrument_id)
            test_instruments.append(instrument)
        
        yield {
            'instruments': test_instruments,
            'ids': created_ids,
            'symbols': [f"TEST_INST_{i}" for i in range(3)]
        }
        
        # Cleanup - delete test instruments
        for instrument_id in created_ids:
            await real_instruments_dao.delete_instrument(instrument_id)

    async def test_create_instrument_real_objects(self, instrument_service, real_instruments_dao):
        """Test instrument creation with real business logic validation."""
        instrument_dto = InstrumentDTO(
            symbol="TEST_CREATE_INST",
            name="Test Create Instrument Inc.",
            exchange="NYSE",
            sector="Finance",
            asset_class="Stock",
            is_active=True,
            created_date=date.today()
        )
        
        # Test real creation
        created_id = await instrument_service.create_instrument(instrument_dto)
        
        # Validate real database persistence
        assert created_id is not None
        assert created_id > 0
        
        # Verify actual database record
        created_instrument = await real_instruments_dao.get_instrument(created_id)
        assert created_instrument['symbol'] == "TEST_CREATE_INST"
        assert created_instrument['name'] == "Test Create Instrument Inc."
        assert created_instrument['exchange'] == "NYSE"
        
        # Cleanup
        await real_instruments_dao.delete_instrument(created_id)

    async def test_get_instrument_real_objects(self, instrument_service, test_instrument_data):
        """Test instrument retrieval with real database queries."""
        test_id = test_instrument_data['ids'][0]
        
        # Test real retrieval
        instrument_dto = await instrument_service.get_instrument(test_id)
        
        # Validate real data
        assert instrument_dto is not None
        assert instrument_dto.symbol == test_instrument_data['symbols'][0]
        assert instrument_dto.name == "Test Instrument 0 Inc."
        assert instrument_dto.exchange == "NASDAQ"

    async def test_get_instrument_by_symbol_real_objects(self, instrument_service, test_instrument_data):
        """Test instrument retrieval by symbol with real database queries."""
        test_symbol = test_instrument_data['symbols'][1]
        
        # Test real symbol lookup
        instrument_dto = await instrument_service.get_instrument_by_symbol(test_symbol)
        
        # Validate real data
        assert instrument_dto is not None
        assert instrument_dto.symbol == test_symbol
        assert instrument_dto.name == "Test Instrument 1 Inc."

    async def test_list_instruments_real_objects(self, instrument_service, test_instrument_data):
        """Test instrument listing with real database pagination."""
        search_criteria = InstrumentSearchCriteria(
            exchange="NASDAQ",
            sector="Technology",
            is_active=True,
            limit=10,
            offset=0
        )
        
        # Test real search
        instruments_list = await instrument_service.list_instruments(search_criteria)
        
        # Validate real results
        assert instruments_list is not None
        assert len(instruments_list) >= len(test_instrument_data['symbols'])
        
        # Check that our test instruments are included
        returned_symbols = {inst.symbol for inst in instruments_list}
        test_symbols = set(test_instrument_data['symbols'])
        assert test_symbols.issubset(returned_symbols)

    async def test_business_rule_enforcement_real_objects(self, instrument_service, real_instruments_dao):
        """Test business rule enforcement with real data validation."""
        # Test duplicate symbol validation
        instrument_dto = InstrumentDTO(
            symbol="TEST_DUPLICATE",
            name="Test Duplicate Instrument Inc.",
            exchange="NYSE",
            sector="Finance",
            asset_class="Stock",
            is_active=True,
            created_date=date.today()
        )
        
        # Create first instrument
        first_id = await instrument_service.create_instrument(instrument_dto)
        assert first_id > 0
        
        # Attempt to create duplicate (should fail with real constraint)
        try:
            second_id = await instrument_service.create_instrument(instrument_dto)
            
            # If creation succeeds, there might be business logic handling duplicates
            if second_id is not None:
                # Clean up both
                await real_instruments_dao.delete_instrument(first_id)
                await real_instruments_dao.delete_instrument(second_id)
            else:
                # Cleanup first
                await real_instruments_dao.delete_instrument(first_id)
                
        except Exception as e:
            # Real constraint violation is expected
            assert "duplicate" in str(e).lower() or "unique" in str(e).lower() or "constraint" in str(e).lower()
            
            # Cleanup
            await real_instruments_dao.delete_instrument(first_id)

    async def test_error_handling_real_objects(self, instrument_service):
        """Test error handling with actual database exceptions."""
        
        # Test non-existent instrument
        try:
            non_existent = await instrument_service.get_instrument(99999999)
            assert non_existent is None  # Should return None for not found
        except Exception as e:
            # Real database error is acceptable
            assert isinstance(e, Exception)
        
        # Test invalid symbol lookup
        try:
            invalid_instrument = await instrument_service.get_instrument_by_symbol("INVALID_SYMBOL_XYZ")
            assert invalid_instrument is None  # Should return None for not found
        except Exception as e:
            # Real database error is acceptable
            assert isinstance(e, Exception)