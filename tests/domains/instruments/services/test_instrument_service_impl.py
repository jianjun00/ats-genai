"""
Comprehensive tests for InstrumentServiceImpl

Tests the business logic layer with mocked DAOs to verify:
1. Service interface contract compliance
2. Business rule enforcement
3. Error handling
4. DTO conversions
5. Transaction coordination
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import date
from typing import List, Optional, Dict, Any

from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentXrefDTO,
    InstrumentSearchCriteria,
    InstrumentOperationResult
)


class TestInstrumentServiceImpl:
    """Test suite for InstrumentServiceImpl business logic"""

    @pytest.fixture
    def mock_instruments_dao(self):
        """Mock InstrumentsDAO"""
        dao = Mock()
        dao.create_instrument = AsyncMock()
        dao.get_instrument = AsyncMock()
        dao.get_instrument_by_symbol = AsyncMock()
        dao.list_instruments = AsyncMock()
        dao.count_instruments = AsyncMock()
        dao.create_instruments_batch = AsyncMock()
        return dao

    @pytest.fixture
    def mock_xrefs_dao(self):
        """Mock InstrumentXrefsDAO"""
        dao = Mock()
        dao.create_xref = AsyncMock()
        dao.get_xref = AsyncMock()
        dao.list_xrefs_for_instrument = AsyncMock()
        dao.find_xref = AsyncMock()
        dao.resolve_instrument_id_by_symbol = AsyncMock()
        dao.resolve_instrument_id = AsyncMock()
        dao.get_all_symbols = AsyncMock()
        dao.create_xrefs_batch = AsyncMock()
        return dao

    @pytest.fixture
    def mock_vendors_dao(self):
        """Mock VendorsDAO"""
        dao = Mock()
        dao.get_vendor_by_name = AsyncMock()
        return dao

    @pytest.fixture
    def service(self, mock_instruments_dao, mock_xrefs_dao, mock_vendors_dao):
        """Create InstrumentServiceImpl with mocked dependencies"""
        return InstrumentServiceImpl(
            instruments_dao=mock_instruments_dao,
            xrefs_dao=mock_xrefs_dao,
            vendors_dao=mock_vendors_dao,
            vendor_daos={}
        )

    @pytest.fixture
    def sample_instrument_dto(self):
        """Sample instrument DTO for testing"""
        return InstrumentDTO(
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            instrument_type="stock",
            currency="USD",
            list_date=date(2020, 1, 1),
            delist_date=None
        )

    @pytest.fixture
    def sample_xref_dto(self):
        """Sample cross-reference DTO for testing"""
        return InstrumentXrefDTO(
            instrument_id=1,
            vendor_name="ticker",
            vendor_symbol="AAPL",
            xref_type="equity",
            start_date=date(2020, 1, 1),
            end_date=None
        )

    # Test create_instrument business logic

    @pytest.mark.asyncio
    async def test_create_instrument_success(self, service, mock_instruments_dao, sample_instrument_dto):
        """Test successful instrument creation"""
        # Setup mocks
        mock_instruments_dao.get_instrument_by_symbol.return_value = None  # No existing instrument
        mock_instruments_dao.create_instrument.return_value = 123

        # Execute
        result = await service.create_instrument(sample_instrument_dto)

        # Verify
        assert result.success is True
        assert result.instrument_id == 123
        assert result.created_count == 1
        assert result.error_message is None

        # Verify DAO calls
        mock_instruments_dao.get_instrument_by_symbol.assert_called_once_with("AAPL")
        mock_instruments_dao.create_instrument.assert_called_once_with(
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            type_="stock",
            currency="USD",
            list_date=date(2020, 1, 1),
            delist_date=None
        )

    @pytest.mark.asyncio
    async def test_create_instrument_validation_error(self, service, mock_instruments_dao):
        """Test instrument creation with validation error"""
        # Create invalid DTO (missing required symbol)
        invalid_dto = InstrumentDTO(name="Apple Inc.")

        # Execute
        result = await service.create_instrument(invalid_dto)

        # Verify
        assert result.success is False
        assert "Symbol is required" in result.error_message
        assert result.instrument_id is None

        # Verify DAO not called
        mock_instruments_dao.create_instrument.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_instrument_duplicate_error(self, service, mock_instruments_dao, sample_instrument_dto):
        """Test instrument creation with duplicate symbol"""
        # Setup mocks - existing instrument found
        existing_instrument = {'id': 999, 'symbol': 'AAPL'}
        mock_instruments_dao.get_instrument_by_symbol.return_value = existing_instrument

        # Execute
        result = await service.create_instrument(sample_instrument_dto)

        # Verify
        assert result.success is False
        assert "already exists" in result.error_message
        assert result.instrument_id == 999  # Returns existing ID

        # Verify create not called
        mock_instruments_dao.create_instrument.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_instrument_dao_exception(self, service, mock_instruments_dao, sample_instrument_dto):
        """Test instrument creation with DAO exception"""
        # Setup mocks
        mock_instruments_dao.get_instrument_by_symbol.return_value = None
        mock_instruments_dao.create_instrument.side_effect = Exception("Database error")

        # Execute
        result = await service.create_instrument(sample_instrument_dto)

        # Verify
        assert result.success is False
        assert "Database error" in result.error_message
        assert result.instrument_id is None

    # Test get_instrument_by_symbol business logic

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol_via_xref(self, service, mock_xrefs_dao, mock_instruments_dao):
        """Test getting instrument by symbol via cross-reference (preferred method)"""
        # Setup mocks
        mock_xrefs_dao.resolve_instrument_id_by_symbol.return_value = 123
        mock_instruments_dao.get_instrument.return_value = {
            'id': 123, 'symbol': 'AAPL', 'name': 'Apple Inc.', 'exchange': 'NASDAQ',
            'type': 'stock', 'currency': 'USD', 'list_date': date(2020, 1, 1), 'delist_date': None
        }

        # Execute
        result = await service.get_instrument_by_symbol("AAPL", "ticker")

        # Verify
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.name == "Apple Inc."
        assert result.id == 123

        # Verify preferred method used
        mock_xrefs_dao.resolve_instrument_id_by_symbol.assert_called_once_with("AAPL")
        mock_instruments_dao.get_instrument.assert_called_once_with(123)

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol_fallback(self, service, mock_xrefs_dao, mock_instruments_dao):
        """Test getting instrument by symbol with fallback to direct lookup"""
        # Setup mocks - xref lookup fails, direct lookup succeeds
        mock_xrefs_dao.resolve_instrument_id_by_symbol.return_value = None
        mock_instruments_dao.get_instrument_by_symbol.return_value = {
            'id': 456, 'symbol': 'AAPL', 'name': 'Apple Inc.', 'exchange': 'NASDAQ',
            'type': 'stock', 'currency': 'USD', 'list_date': date(2020, 1, 1), 'delist_date': None
        }

        # Execute
        result = await service.get_instrument_by_symbol("AAPL", "ticker")

        # Verify
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.id == 456

        # Verify fallback method used
        mock_xrefs_dao.resolve_instrument_id_by_symbol.assert_called_once_with("AAPL")
        mock_instruments_dao.get_instrument_by_symbol.assert_called_once_with("AAPL")

    # Test cross-reference business logic

    @pytest.mark.asyncio
    async def test_create_cross_reference_success(self, service, mock_vendors_dao, mock_xrefs_dao, sample_xref_dto):
        """Test successful cross-reference creation"""
        # Setup mocks
        mock_vendors_dao.get_vendor_by_name.return_value = {'id': 1, 'name': 'ticker'}
        mock_xrefs_dao.find_xref.return_value = None  # No existing xref
        mock_xrefs_dao.create_xref.return_value = 789

        # Execute
        result = await service.create_cross_reference(sample_xref_dto)

        # Verify
        assert result.success is True
        assert result.instrument_id == 789
        assert result.created_count == 1

        # Verify DAO calls
        mock_vendors_dao.get_vendor_by_name.assert_called_once_with("ticker")
        mock_xrefs_dao.find_xref.assert_called_once_with(1, "AAPL")
        mock_xrefs_dao.create_xref.assert_called_once_with(
            instrument_id=1,
            vendor_id=1,
            symbol="AAPL",
            type="equity",
            start_at=date(2020, 1, 1),
            end_at=None
        )

    @pytest.mark.asyncio
    async def test_create_cross_reference_vendor_not_found(self, service, mock_vendors_dao, sample_xref_dto):
        """Test cross-reference creation with unknown vendor"""
        # Setup mocks
        mock_vendors_dao.get_vendor_by_name.return_value = None

        # Execute
        result = await service.create_cross_reference(sample_xref_dto)

        # Verify
        assert result.success is False
        assert "Vendor 'ticker' not found" in result.error_message

    @pytest.mark.asyncio
    async def test_create_cross_reference_validation_error(self, service, mock_vendors_dao):
        """Test cross-reference creation with validation error"""
        # Setup mocks
        mock_vendors_dao.get_vendor_by_name.return_value = {'id': 1, 'name': 'ticker'}

        # Create invalid DTO (missing required fields)
        invalid_dto = InstrumentXrefDTO(vendor_name="ticker")

        # Execute
        result = await service.create_cross_reference(invalid_dto)

        # Verify
        assert result.success is False
        assert "instrument_id and vendor_symbol are required" in result.error_message

    @pytest.mark.asyncio
    async def test_create_cross_reference_duplicate_error(self, service, mock_vendors_dao, mock_xrefs_dao, sample_xref_dto):
        """Test cross-reference creation with duplicate"""
        # Setup mocks
        mock_vendors_dao.get_vendor_by_name.return_value = {'id': 1, 'name': 'ticker'}
        mock_xrefs_dao.find_xref.return_value = {'id': 999}  # Existing xref

        # Execute
        result = await service.create_cross_reference(sample_xref_dto)

        # Verify
        assert result.success is False
        assert "already exists" in result.error_message
        assert result.skipped_count == 1

    # Test list_instruments filtering logic

    @pytest.mark.asyncio
    async def test_list_instruments_with_filtering(self, service, mock_instruments_dao):
        """Test instrument listing with business logic filtering"""
        # Setup mock data
        mock_instruments = [
            {'id': 1, 'symbol': 'AAPL', 'exchange': 'NASDAQ', 'type': 'stock', 'currency': 'USD'},
            {'id': 2, 'symbol': 'GOOGL', 'exchange': 'NASDAQ', 'type': 'stock', 'currency': 'USD'},
            {'id': 3, 'symbol': 'TSLA', 'exchange': 'NASDAQ', 'type': 'stock', 'currency': 'USD'},
            {'id': 4, 'symbol': 'BTC', 'exchange': 'CRYPTO', 'type': 'crypto', 'currency': 'USD'}
        ]
        mock_instruments_dao.list_instruments.return_value = mock_instruments

        # Execute with filtering
        criteria = InstrumentSearchCriteria(
            symbols=['AAPL', 'TSLA'],
            exchanges=['NASDAQ'],
            limit=10
        )
        result = await service.list_instruments(criteria)

        # Verify filtering applied
        assert len(result) == 2
        symbols = {dto.symbol for dto in result}
        assert symbols == {'AAPL', 'TSLA'}

        # Verify all results match criteria
        for dto in result:
            assert dto.exchange == 'NASDAQ'
            assert dto.symbol in ['AAPL', 'TSLA']

    # Test batch operations

    @pytest.mark.asyncio
    async def test_create_instruments_batch_success(self, service, mock_instruments_dao):
        """Test successful batch instrument creation"""
        # Setup
        instruments = [
            InstrumentDTO(symbol="AAPL", name="Apple Inc."),
            InstrumentDTO(symbol="GOOGL", name="Alphabet Inc."),
        ]
        mock_instruments_dao.create_instruments_batch.return_value = [1, 2]

        # Execute
        result = await service.create_instruments_batch(instruments)

        # Verify
        assert result.success is True
        assert result.created_count == 2

        # Verify DAO called with correct format
        mock_instruments_dao.create_instruments_batch.assert_called_once()
        call_args = mock_instruments_dao.create_instruments_batch.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0]['symbol'] == 'AAPL'
        assert call_args[1]['symbol'] == 'GOOGL'

    @pytest.mark.asyncio
    async def test_create_instruments_batch_empty(self, service, mock_instruments_dao):
        """Test batch creation with empty list"""
        # Execute
        result = await service.create_instruments_batch([])

        # Verify
        assert result.success is True
        assert result.created_count == 0

        # Verify DAO not called
        mock_instruments_dao.create_instruments_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_instruments_batch_filters_invalid(self, service, mock_instruments_dao):
        """Test batch creation filters out invalid instruments"""
        # Setup - one valid, one invalid (no symbol)
        instruments = [
            InstrumentDTO(symbol="AAPL", name="Apple Inc."),
            InstrumentDTO(name="Invalid - no symbol"),
        ]
        mock_instruments_dao.create_instruments_batch.return_value = [1]

        # Execute
        result = await service.create_instruments_batch(instruments)

        # Verify
        assert result.success is True
        assert result.created_count == 1

        # Verify only valid instrument passed to DAO
        call_args = mock_instruments_dao.create_instruments_batch.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0]['symbol'] == 'AAPL'

    # Test utility operations

    @pytest.mark.asyncio
    async def test_get_instrument_count(self, service, mock_instruments_dao):
        """Test getting instrument count"""
        mock_instruments_dao.count_instruments.return_value = 1500

        result = await service.get_instrument_count()

        assert result == 1500
        mock_instruments_dao.count_instruments.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_symbol_valid(self, service, mock_xrefs_dao, mock_instruments_dao):
        """Test symbol validation with valid symbol"""
        # Setup - symbol found via xref
        mock_xrefs_dao.resolve_instrument_id_by_symbol.return_value = 123
        mock_instruments_dao.get_instrument.return_value = {'id': 123, 'symbol': 'AAPL'}

        result = await service.validate_symbol("AAPL", "ticker")

        assert result is True

    @pytest.mark.asyncio
    async def test_validate_symbol_invalid(self, service, mock_xrefs_dao, mock_instruments_dao):
        """Test symbol validation with invalid symbol"""
        # Setup - symbol not found
        mock_xrefs_dao.resolve_instrument_id_by_symbol.return_value = None
        mock_instruments_dao.get_instrument_by_symbol.return_value = None

        result = await service.validate_symbol("INVALID", "ticker")

        assert result is False

    # Test error handling and logging

    @pytest.mark.asyncio
    async def test_dao_exception_handling(self, service, mock_instruments_dao):
        """Test proper exception handling when DAO throws exception"""
        mock_instruments_dao.count_instruments.side_effect = Exception("Database connection failed")

        result = await service.get_instrument_count()

        # Should return default value and log error (not crash)
        assert result == 0

    @pytest.mark.asyncio
    async def test_vendor_id_caching(self, service, mock_vendors_dao, mock_xrefs_dao):
        """Test vendor ID caching optimization"""
        # Setup
        mock_vendors_dao.get_vendor_by_name.return_value = {'id': 1, 'name': 'ticker'}
        mock_xrefs_dao.find_xref.return_value = None
        mock_xrefs_dao.create_xref.return_value = 1

        # Create multiple xrefs with same vendor
        xref1 = InstrumentXrefDTO(instrument_id=1, vendor_name="ticker", vendor_symbol="AAPL")
        xref2 = InstrumentXrefDTO(instrument_id=2, vendor_name="ticker", vendor_symbol="GOOGL")

        await service.create_cross_reference(xref1)
        await service.create_cross_reference(xref2)

        # Verify vendor looked up only once (cached)
        assert mock_vendors_dao.get_vendor_by_name.call_count == 1

    # Test DTO conversion logic

    def test_dao_to_instrument_dto_conversion(self, service):
        """Test DAO record to InstrumentDTO conversion"""
        dao_record = {
            'id': 1,
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'type': 'stock',
            'currency': 'USD',
            'list_date': date(2020, 1, 1),
            'delist_date': None
        }

        dto = service._dao_to_instrument_dto(dao_record)

        assert dto.id == 1
        assert dto.symbol == 'AAPL'
        assert dto.name == 'Apple Inc.'
        assert dto.instrument_type == 'stock'  # Note: type -> instrument_type
        assert dto.list_date == date(2020, 1, 1)

    def test_dao_to_instrument_dto_none(self, service):
        """Test DAO to DTO conversion with None input"""
        result = service._dao_to_instrument_dto(None)
        assert result is None