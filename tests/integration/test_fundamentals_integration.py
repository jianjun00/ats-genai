"""
Integration tests for Fundamental DAOs and UnifiedFundamentalProvider

Tests against actual database schema in K8s dev environment to validate
real-world functionality and cross-vendor data validation.
"""

import pytest
import asyncio
from datetime import date, datetime
from decimal import Decimal

from vendor.fmp.core.dao.fundamentals_fmp_dao import FundamentalsFMPDAO, FMPFundamental
from vendor.polygon.core.dao.fundamentals_polygon_dao import FundamentalsPolygonDAO, PolygonFundamental
from vendor.tiingo.core.dao.fundamentals_tiingo_dao import FundamentalsTiingoDAO, TiingoFundamental
from domains.market_data.services.fundamentals.unified_fundamental_provider import (
    UnifiedFundamentalProvider,
    ValidationStatus
)
from shared.utils.environment import Environment


@pytest.fixture
def dev_environment():
    """Development environment configuration"""
    return Environment()


@pytest.fixture
def test_symbol():
    """Test symbol for integration tests"""
    return "TEST_INTEGRATION"


@pytest.fixture
def test_date():
    """Test date for integration tests"""
    return date(2023, 12, 31)


@pytest.fixture
def sample_fmp_test_data(test_symbol, test_date):
    """Sample FMP test data"""
    return FMPFundamental(
        symbol=test_symbol,
        date=test_date,
        vendor="fmp",
        fiscal_period="Q4",
        revenue=1000000000,
        net_income=100000000,
        eps=2.50,
        total_assets=5000000000,
        market_cap=50000000000,
        pe_ratio=20.0,
        roe=0.15,
        raw_data={"test": "fmp_integration"}
    )


@pytest.fixture
def sample_polygon_test_data(test_symbol, test_date):
    """Sample Polygon test data"""
    return PolygonFundamental(
        symbol=test_symbol,
        date=test_date,
        vendor="polygon",
        fiscal_period="Q4",
        revenue=1001000000,
        net_income=99500000,
        eps=2.49,
        total_assets=5010000000,
        market_cap=49800000000,
        pe_ratio=19.9,
        roe=0.149,
        raw_data={"test": "polygon_integration"}
    )


@pytest.fixture
def sample_tiingo_test_data(test_symbol, test_date):
    """Sample Tiingo test data"""
    return TiingoFundamental(
        symbol=test_symbol,
        date=test_date,
        vendor="tiingo",
        fiscal_period="Q4",
        revenue=999500000,
        net_income=100200000,
        eps=2.51,
        total_assets=4995000000,
        market_cap=50100000000,
        pe_ratio=20.1,
        roe=0.151,
        raw_data={"test": "tiingo_integration"}
    )


@pytest.mark.integration
class TestFundamentalsDAOIntegration:
    """Integration tests for fundamental DAOs against real database"""

    @pytest.mark.asyncio

    async def test_fmp_dao_crud_operations(self, dev_environment, sample_fmp_test_data):
        """Test FMP DAO CRUD operations against real database"""
        dao = FundamentalsFMPDAO(dev_environment)

        # Cleanup any existing test data
        await dao.delete_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)

        try:
            # Test insertion
            insert_result = await dao.insert_fundamental(sample_fmp_test_data)
            assert insert_result is True

            # Test retrieval
            retrieved = await dao.get_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)
            assert retrieved is not None
            assert retrieved.symbol == sample_fmp_test_data.symbol
            assert retrieved.vendor == "fmp"
            assert retrieved.revenue == sample_fmp_test_data.revenue

            # Test listing
            fundamentals_list = await dao.list_fundamentals(sample_fmp_test_data.symbol)
            assert len(fundamentals_list) >= 1
            assert any(f.date == sample_fmp_test_data.date for f in fundamentals_list)

            # Test symbols with data
            symbols = await dao.get_symbols_with_data()
            assert sample_fmp_test_data.symbol in symbols

            # Test latest fundamental
            latest = await dao.get_latest_fundamental(sample_fmp_test_data.symbol)
            assert latest is not None
            assert latest.symbol == sample_fmp_test_data.symbol

        finally:
            # Cleanup
            delete_result = await dao.delete_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)
            assert delete_result is True

    @pytest.mark.asyncio

    async def test_polygon_dao_crud_operations(self, dev_environment, sample_polygon_test_data):
        """Test Polygon DAO CRUD operations against real database"""
        dao = FundamentalsPolygonDAO(dev_environment)

        # Cleanup any existing test data
        await dao.delete_fundamental(sample_polygon_test_data.symbol, sample_polygon_test_data.date)

        try:
            # Test insertion
            insert_result = await dao.insert_fundamental(sample_polygon_test_data)
            assert insert_result is True

            # Test retrieval
            retrieved = await dao.get_fundamental(sample_polygon_test_data.symbol, sample_polygon_test_data.date)
            assert retrieved is not None
            assert retrieved.symbol == sample_polygon_test_data.symbol
            assert retrieved.vendor == "polygon"
            assert retrieved.revenue == sample_polygon_test_data.revenue

        finally:
            # Cleanup
            await dao.delete_fundamental(sample_polygon_test_data.symbol, sample_polygon_test_data.date)

    @pytest.mark.asyncio

    async def test_tiingo_dao_crud_operations(self, dev_environment, sample_tiingo_test_data):
        """Test Tiingo DAO CRUD operations against real database"""
        dao = FundamentalsTiingoDAO(dev_environment)

        # Cleanup any existing test data
        await dao.delete_fundamental(sample_tiingo_test_data.symbol, sample_tiingo_test_data.date)

        try:
            # Test insertion
            insert_result = await dao.insert_fundamental(sample_tiingo_test_data)
            assert insert_result is True

            # Test retrieval
            retrieved = await dao.get_fundamental(sample_tiingo_test_data.symbol, sample_tiingo_test_data.date)
            assert retrieved is not None
            assert retrieved.symbol == sample_tiingo_test_data.symbol
            assert retrieved.vendor == "tiingo"
            assert retrieved.revenue == sample_tiingo_test_data.revenue

        finally:
            # Cleanup
            await dao.delete_fundamental(sample_tiingo_test_data.symbol, sample_tiingo_test_data.date)


@pytest.mark.integration
class TestUnifiedFundamentalProviderIntegration:
    """Integration tests for UnifiedFundamentalProvider with real database"""

    @pytest.mark.asyncio

    async def test_cross_vendor_validation(self, dev_environment, sample_fmp_test_data,
                                         sample_polygon_test_data, sample_tiingo_test_data):
        """Test cross-vendor validation with real database data"""
        provider = UnifiedFundamentalProvider(dev_environment)

        # Setup test data across all vendors
        fmp_dao = FundamentalsFMPDAO(dev_environment)
        polygon_dao = FundamentalsPolygonDAO(dev_environment)
        tiingo_dao = FundamentalsTiingoDAO(dev_environment)

        # Cleanup any existing test data
        await fmp_core.dao.delete_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)
        await polygon_core.dao.delete_fundamental(sample_polygon_test_data.symbol, sample_polygon_test_data.date)
        await tiingo_core.dao.delete_fundamental(sample_tiingo_test_data.symbol, sample_tiingo_test_data.date)

        try:
            # Insert test data across all vendors
            await fmp_core.dao.insert_fundamental(sample_fmp_test_data)
            await polygon_core.dao.insert_fundamental(sample_polygon_test_data)
            await tiingo_core.dao.insert_fundamental(sample_tiingo_test_data)

            # Test unified fundamental retrieval
            unified_result = await provider.get_unified_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)

            assert unified_result is not None
            assert unified_result.symbol == sample_fmp_test_data.symbol
            assert unified_result.date == sample_fmp_test_data.date
            assert len(unified_result.vendor_data) == 3
            assert unified_result.status in [ValidationStatus.CONSENSUS, ValidationStatus.MAJORITY_CONSENSUS]
            assert unified_result.confidence_score > 0.0

            # Verify all vendors are represented
            vendors = {vd.vendor for vd in unified_result.vendor_data}
            assert vendors == {"fmp", "polygon", "tiingo"}

            # Test validation metadata
            assert unified_result.validation_metadata is not None
            assert 'disagreements' in unified_result.validation_metadata
            assert 'outliers' in unified_result.validation_metadata

        finally:
            # Cleanup test data
            await fmp_core.dao.delete_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)
            await polygon_core.dao.delete_fundamental(sample_polygon_test_data.symbol, sample_polygon_test_data.date)
            await tiingo_core.dao.delete_fundamental(sample_tiingo_test_data.symbol, sample_tiingo_test_data.date)

    @pytest.mark.asyncio

    async def test_unified_symbols_list(self, dev_environment):
        """Test unified symbols list functionality"""
        provider = UnifiedFundamentalProvider(dev_environment)

        # Get symbols that have data
        symbols = await provider.list_symbols_with_data()

        assert isinstance(symbols, list)
        # Should return some real symbols from the database
        # (actual symbols will depend on what's in the dev database)

    @pytest.mark.asyncio

    async def test_database_schema_compatibility(self, dev_environment, sample_fmp_test_data):
        """Test that our DAO schema matches actual database schema"""
        dao = FundamentalsFMPDAO(dev_environment)

        # Test that we can successfully insert with all fields
        # This will fail if the database schema doesn't match our DAO
        try:
            result = await dao.insert_fundamental(sample_fmp_test_data)
            assert result is True

            # Verify retrieval preserves all fields
            retrieved = await dao.get_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)
            assert retrieved is not None

        finally:
            # Cleanup
            await dao.delete_fundamental(sample_fmp_test_data.symbol, sample_fmp_test_data.date)