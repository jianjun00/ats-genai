"""
Comprehensive tests for InstrumentServiceImpl using REAL OBJECTS

Tests the business logic layer with real DAOs to verify:
1. Service interface contract compliance  
2. Business rule enforcement
3. Real database integration
4. DTO conversions with actual data
5. Transaction coordination with real transactions

This replaces mock objects with real database-backed DAOs to catch
actual integration issues and verify real business logic.
"""

import pytest
from datetime import date
from typing import AsyncGenerator

from core.platform.config.environment import Environment, EnvironmentType
from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentXrefDTO,
    InstrumentSearchCriteria
)
from core.dao.instruments.instruments_dao import InstrumentsDAO
from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
from core.dao.infrastructure.vendors_dao import VendorsDAO


class TestInstrumentServiceImplRealObjects:
    """Test suite for InstrumentServiceImpl with real database integration"""

    @pytest.fixture(scope="session")
    async def test_environment(self) -> Environment:
        """Real test environment with actual database connection"""
        return Environment(
            env_type=EnvironmentType.TEST,
            db_url="postgresql://test:test@localhost/test_instruments_db"
        )

    @pytest.fixture
    async def clean_database(self, test_environment: Environment) -> AsyncGenerator[Environment, None]:
        """Clean database state for each test"""
        # Clean up before test
        async with test_environment.get_connection() as conn:
            await conn.execute("TRUNCATE TABLE instrument_xrefs, instruments, vendors RESTART IDENTITY CASCADE")
        
        yield test_environment
        
        # Clean up after test
        async with test_environment.get_connection() as conn:
            await conn.execute("TRUNCATE TABLE instrument_xrefs, instruments, vendors RESTART IDENTITY CASCADE")

    @pytest.fixture
    async def real_instruments_dao(self, clean_database: Environment) -> InstrumentsDAO:
        """Real InstrumentsDAO with test database"""
        return InstrumentsDAO(clean_database)

    @pytest.fixture
    async def real_xrefs_dao(self, clean_database: Environment) -> InstrumentXrefsDAO:
        """Real InstrumentXrefsDAO with test database"""
        return InstrumentXrefsDAO(clean_database)

    @pytest.fixture
    async def real_vendors_dao(self, clean_database: Environment) -> VendorsDAO:
        """Real VendorsDAO with test database"""
        dao = VendorsDAO(clean_database)
        # Set up test vendors
        await dao.create_vendor("ticker", "Primary ticker symbols")
        await dao.create_vendor("polygon", "Polygon.io data provider")
        return dao

    @pytest.fixture
    async def service(
        self, 
        real_instruments_dao: InstrumentsDAO,
        real_xrefs_dao: InstrumentXrefsDAO, 
        real_vendors_dao: VendorsDAO
    ) -> InstrumentServiceImpl:
        """Create InstrumentServiceImpl with real dependencies"""
        return InstrumentServiceImpl(
            instruments_dao=real_instruments_dao,
            xrefs_dao=real_xrefs_dao,
            vendors_dao=real_vendors_dao,
            vendor_daos={}  # Real vendor DAOs would be injected here
        )

    @pytest.fixture
    def sample_instrument_dto(self) -> InstrumentDTO:
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

    # Test create_instrument with real database integration

    @pytest.mark.asyncio
    async def test_create_instrument_success_real_db(
        self, 
        service: InstrumentServiceImpl, 
        sample_instrument_dto: InstrumentDTO
    ):
        """Test successful instrument creation with real database"""
        # Execute - no mocking needed, tests real database interaction
        result = await service.create_instrument(sample_instrument_dto)

        # Verify result structure
        assert result.success is True
        assert result.instrument_id is not None
        assert result.instrument_id > 0  # Real database generates positive IDs
        assert result.created_count == 1
        assert result.error_message is None

        # Verify actual database state by querying directly
        created_instrument = await service.get_instrument_by_id(result.instrument_id)
        assert created_instrument is not None
        assert created_instrument.symbol == "AAPL"
        assert created_instrument.name == "Apple Inc."
        assert created_instrument.exchange == "NASDAQ"
        assert created_instrument.instrument_type == "stock"

    @pytest.mark.asyncio
    async def test_create_instrument_duplicate_error_real_db(
        self, 
        service: InstrumentServiceImpl, 
        sample_instrument_dto: InstrumentDTO
    ):
        """Test instrument creation with real duplicate detection"""
        # Create first instrument
        first_result = await service.create_instrument(sample_instrument_dto)
        assert first_result.success is True
        
        # Attempt to create duplicate - real database constraints will catch this
        duplicate_result = await service.create_instrument(sample_instrument_dto)
        
        # Verify duplicate handling
        assert duplicate_result.success is False
        assert "already exists" in duplicate_result.error_message
        assert duplicate_result.instrument_id == first_result.instrument_id

    @pytest.mark.asyncio  
    async def test_create_instrument_validation_error_real_business_logic(
        self, 
        service: InstrumentServiceImpl
    ):
        """Test instrument creation validation with real business rules"""
        # Create invalid DTO - real validation will catch this
        invalid_dto = InstrumentDTO(
            symbol="",  # Invalid empty symbol
            name="Apple Inc.",
            exchange="NASDAQ",
            instrument_type="stock",
            currency="USD"
        )

        # Execute - real validation logic runs
        result = await service.create_instrument(invalid_dto)

        # Verify real validation worked
        assert result.success is False
        assert "Symbol is required" in result.error_message
        assert result.instrument_id is None

    # Test get_instrument_by_symbol with real cross-reference logic

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol_via_real_xref(
        self, 
        service: InstrumentServiceImpl, 
        sample_instrument_dto: InstrumentDTO
    ):
        """Test getting instrument by symbol via real cross-reference lookup"""
        # Create instrument and xref in database
        create_result = await service.create_instrument(sample_instrument_dto)
        assert create_result.success is True

        xref_dto = InstrumentXrefDTO(
            instrument_id=create_result.instrument_id,
            vendor_name="ticker",
            vendor_symbol="AAPL",
            xref_type="equity",
            start_date=date(2020, 1, 1),
            end_date=None
        )
        xref_result = await service.create_cross_reference(xref_dto)
        assert xref_result.success is True

        # Test real cross-reference resolution
        result = await service.get_instrument_by_symbol("AAPL", "ticker")

        # Verify real lookup worked  
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.name == "Apple Inc."
        assert result.id == create_result.instrument_id

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol_fallback_real_lookup(
        self,
        service: InstrumentServiceImpl,
        sample_instrument_dto: InstrumentDTO
    ):
        """Test real fallback lookup when cross-reference doesn't exist"""
        # Create instrument without cross-reference
        create_result = await service.create_instrument(sample_instrument_dto)
        assert create_result.success is True

        # Test fallback to direct symbol lookup (real database query)
        result = await service.get_instrument_by_symbol("AAPL", "ticker")

        # Verify fallback worked with real database
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.id == create_result.instrument_id

    # Test cross-reference creation with real vendor validation

    @pytest.mark.asyncio
    async def test_create_cross_reference_success_real_vendors(
        self,
        service: InstrumentServiceImpl,
        sample_instrument_dto: InstrumentDTO
    ):
        """Test successful cross-reference creation with real vendor validation"""
        # Create instrument first
        create_result = await service.create_instrument(sample_instrument_dto)
        assert create_result.success is True

        # Create cross-reference with real vendor validation
        xref_dto = InstrumentXrefDTO(
            instrument_id=create_result.instrument_id,
            vendor_name="ticker", # Real vendor exists in database
            vendor_symbol="AAPL",
            xref_type="equity",
            start_date=date(2020, 1, 1),
            end_date=None
        )

        result = await service.create_cross_reference(xref_dto)

        # Verify real database transaction worked
        assert result.success is True
        assert result.instrument_id is not None
        assert result.created_count == 1

        # Verify real cross-reference exists in database
        xrefs = await service.list_cross_references_for_instrument(create_result.instrument_id)
        assert len(xrefs) == 1
        assert xrefs[0].vendor_name == "ticker"
        assert xrefs[0].vendor_symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_create_cross_reference_vendor_not_found_real_validation(
        self,
        service: InstrumentServiceImpl,
        sample_instrument_dto: InstrumentDTO
    ):
        """Test cross-reference creation with real vendor validation failure"""
        # Create instrument
        create_result = await service.create_instrument(sample_instrument_dto)
        assert create_result.success is True

        # Attempt cross-reference with non-existent vendor
        xref_dto = InstrumentXrefDTO(
            instrument_id=create_result.instrument_id,
            vendor_name="nonexistent_vendor", # Real validation will catch this
            vendor_symbol="AAPL",
            xref_type="equity",
            start_date=date(2020, 1, 1),
            end_date=None
        )

        result = await service.create_cross_reference(xref_dto)

        # Verify real vendor validation worked
        assert result.success is False
        assert "Vendor 'nonexistent_vendor' not found" in result.error_message

    # Test list_instruments with real filtering and pagination

    @pytest.mark.asyncio
    async def test_list_instruments_with_real_filtering(
        self,
        service: InstrumentServiceImpl
    ):
        """Test instrument listing with real database filtering"""
        # Create multiple instruments in real database
        instruments = [
            InstrumentDTO(symbol="AAPL", name="Apple Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),
            InstrumentDTO(symbol="GOOGL", name="Alphabet Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),
            InstrumentDTO(symbol="TSLA", name="Tesla Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),
            InstrumentDTO(symbol="BTC-USD", name="Bitcoin", exchange="CRYPTO", instrument_type="crypto", currency="USD")
        ]
        
        created_ids = []
        for instrument in instruments:
            result = await service.create_instrument(instrument)
            assert result.success is True
            created_ids.append(result.instrument_id)

        # Test real database filtering
        criteria = InstrumentSearchCriteria(
            symbols=['AAPL', 'TSLA'],
            exchanges=['NASDAQ'],
            limit=10
        )
        result = await service.list_instruments(criteria)

        # Verify real filtering worked
        assert len(result) == 2
        symbols = {dto.symbol for dto in result}
        assert symbols == {'AAPL', 'TSLA'}

        # Verify all results match real criteria
        for dto in result:
            assert dto.exchange == 'NASDAQ'
            assert dto.symbol in ['AAPL', 'TSLA']
            assert dto.id in created_ids

    # Test batch operations with real transactions

    @pytest.mark.asyncio
    async def test_create_instruments_batch_real_transaction(
        self,
        service: InstrumentServiceImpl
    ):
        """Test successful batch instrument creation with real database transaction"""
        instruments = [
            InstrumentDTO(symbol="AAPL", name="Apple Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),
            InstrumentDTO(symbol="GOOGL", name="Alphabet Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),
            InstrumentDTO(symbol="MSFT", name="Microsoft Corp.", exchange="NASDAQ", instrument_type="stock", currency="USD")
        ]

        # Execute real batch transaction
        result = await service.create_instruments_batch(instruments)

        # Verify real transaction succeeded
        assert result.success is True
        assert result.created_count == 3
        assert len(result.instrument_ids) == 3

        # Verify all instruments exist in real database
        for i, instrument_id in enumerate(result.instrument_ids):
            created_instrument = await service.get_instrument_by_id(instrument_id)
            assert created_instrument is not None
            assert created_instrument.symbol == instruments[i].symbol
            assert created_instrument.name == instruments[i].name

    @pytest.mark.asyncio
    async def test_create_instruments_batch_partial_failure_real_transaction(
        self,
        service: InstrumentServiceImpl
    ):
        """Test batch creation with partial failure and real transaction rollback"""
        # Create one instrument first
        existing = InstrumentDTO(symbol="AAPL", name="Apple Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD")
        await service.create_instrument(existing)

        # Attempt batch with duplicate (real constraint violation)
        instruments = [
            InstrumentDTO(symbol="GOOGL", name="Alphabet Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),
            InstrumentDTO(symbol="AAPL", name="Apple Inc.", exchange="NASDAQ", instrument_type="stock", currency="USD"),  # Duplicate
            InstrumentDTO(symbol="MSFT", name="Microsoft Corp.", exchange="NASDAQ", instrument_type="stock", currency="USD")
        ]

        # Real transaction should handle this appropriately
        result = await service.create_instruments_batch(instruments)

        # Verify real transaction behavior
        # (Behavior depends on implementation - either all fail or non-duplicates succeed)
        if result.success:
            # If implementation filters duplicates
            assert result.created_count == 2  # GOOGL and MSFT
        else:
            # If implementation fails entire batch on duplicate
            assert result.created_count == 0
            assert "duplicate" in result.error_message.lower() or "already exists" in result.error_message.lower()

    # Test real error scenarios without mocking

    @pytest.mark.asyncio
    async def test_database_constraint_violation_real_error(
        self,
        service: InstrumentServiceImpl
    ):
        """Test real database constraint violations are handled properly"""
        # Create instrument with constraint-violating data
        invalid_dto = InstrumentDTO(
            symbol="TOOLONG" * 10,  # Violates symbol length constraint
            name="Valid Name",
            exchange="NASDAQ",
            instrument_type="stock", 
            currency="USD"
        )

        # Real database constraint will be triggered
        result = await service.create_instrument(invalid_dto)

        # Verify real constraint violation handling
        assert result.success is False
        assert result.error_message is not None
        # Error message should contain database constraint details

    @pytest.mark.asyncio
    async def test_concurrent_access_real_database(
        self,
        service: InstrumentServiceImpl,
        sample_instrument_dto: InstrumentDTO
    ):
        """Test concurrent access patterns with real database"""
        import asyncio
        
        # Create multiple concurrent requests to real database
        tasks = [
            service.create_instrument(sample_instrument_dto)
            for _ in range(3)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify real concurrency handling
        successful_results = [r for r in results if not isinstance(r, Exception) and r.success]
        failed_results = [r for r in results if not isinstance(r, Exception) and not r.success]
        
        # Exactly one should succeed, others should fail with duplicate error
        assert len(successful_results) == 1
        assert len(failed_results) == 2
        
        for failed_result in failed_results:
            assert "already exists" in failed_result.error_message

    # Test real performance characteristics

    @pytest.mark.asyncio
    async def test_large_batch_performance_real_database(
        self,
        service: InstrumentServiceImpl
    ):
        """Test performance with large batch operations on real database"""
        import time
        
        # Create large batch of instruments
        instruments = [
            InstrumentDTO(
                symbol=f"TEST{i:04d}",
                name=f"Test Instrument {i}",
                exchange="NASDAQ",
                instrument_type="stock",
                currency="USD"
            )
            for i in range(100)  # Reasonable size for test
        ]
        
        # Measure real database performance
        start_time = time.time()
        result = await service.create_instruments_batch(instruments)
        end_time = time.time()
        
        # Verify real performance characteristics
        assert result.success is True
        assert result.created_count == 100
        assert end_time - start_time < 10.0  # Should complete within 10 seconds
        
        # Verify all instruments exist in real database
        criteria = InstrumentSearchCriteria(limit=150)
        all_instruments = await service.list_instruments(criteria)
        test_instruments = [i for i in all_instruments if i.symbol.startswith("TEST")]
        assert len(test_instruments) == 100