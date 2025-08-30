"""
Unit tests for Fundamental DAOs (FMP, Polygon, Tiingo)

Tests all fundamental DAO operations including CRUD operations,
data validation, error handling, and vendor-specific behavior.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date, datetime
from decimal import Decimal

from src.dao.fundamentals_fmp_dao import FundamentalsFMPDAO, FMPFundamental
from src.dao.fundamentals_polygon_dao import FundamentalsPolygonDAO, PolygonFundamental  
from src.dao.fundamentals_tiingo_dao import FundamentalsTiingoDAO, TiingoFundamental
from src.config.environment import Environment


@pytest.fixture
def mock_environment():
    """Mock environment for testing"""
    env = MagicMock(spec=Environment)
    env.get_table_name.return_value = "test_fundamentals_comprehensive"
    env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test_db"
    return env


@pytest.fixture  
def sample_fmp_fundamental():
    """Sample FMP fundamental data"""
    return FMPFundamental(
        symbol="AAPL",
        date=date(2023, 12, 31),
        vendor="fmp",
        fiscal_period="Q4",
        revenue=123456000000,
        net_income=10000000000,
        eps=5.67,
        total_assets=400000000000,
        market_cap=3000000000000,
        pe_ratio=25.5,
        roe=0.28,
        raw_data={"source": "fmp_api", "timestamp": "2024-01-15T10:00:00Z"}
    )


@pytest.fixture
def sample_polygon_fundamental():
    """Sample Polygon fundamental data"""
    return PolygonFundamental(
        symbol="AAPL", 
        date=date(2023, 12, 31),
        vendor="polygon",
        fiscal_period="Q4",
        revenue=123500000000,
        net_income=9900000000,
        eps=5.65,
        total_assets=398000000000,
        market_cap=2980000000000,
        pe_ratio=25.2,
        roe=0.27,
        raw_data={"source": "polygon_api", "timestamp": "2024-01-15T11:00:00Z"}
    )


@pytest.fixture
def sample_tiingo_fundamental():
    """Sample Tiingo fundamental data"""
    return TiingoFundamental(
        symbol="AAPL",
        date=date(2023, 12, 31), 
        vendor="tiingo",
        fiscal_period="Q4",
        revenue=123400000000,
        net_income=10100000000,
        eps=5.68,
        total_assets=401000000000,
        market_cap=3020000000000,
        pe_ratio=25.8,
        roe=0.29,
        raw_data={"source": "tiingo_api", "timestamp": "2024-01-15T12:00:00Z"}
    )


class TestFundamentalsFMPDAO:
    """Test suite for FMP Fundamentals DAO"""
    
    def test_init(self, mock_environment):
        """Test DAO initialization"""
        dao = FundamentalsFMPDAO(mock_environment)
        
        assert dao.env == mock_environment
        assert dao.vendor == "fmp"
        assert dao.table_name == "test_fundamentals_comprehensive"
        assert dao.db_url == "postgresql://test:test@localhost:5432/test_db"
        assert dao.logger is not None
    
    @patch('asyncpg.create_pool')
    async def test_insert_fundamental_success(self, mock_pool, mock_environment, sample_fmp_fundamental):
        """Test successful fundamental insertion"""
        # Setup mocks
        mock_conn = AsyncMock()
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test insertion
        result = await dao.insert_fundamental(sample_fmp_fundamental)
        
        assert result is True
        mock_conn.execute.assert_called_once()
        
        # Verify SQL parameters
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO test_fundamentals_comprehensive" in call_args[0][0]
        assert "ON CONFLICT (symbol, date, vendor)" in call_args[0][0]
        assert call_args[0][1] == "AAPL"  # symbol
        assert call_args[0][2] == date(2023, 12, 31)  # date
        assert call_args[0][3] == "fmp"  # vendor
    
    @patch('asyncpg.create_pool')
    async def test_insert_fundamental_failure(self, mock_pool, mock_environment, sample_fmp_fundamental):
        """Test fundamental insertion failure"""
        # Setup mocks to raise exception
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test insertion failure
        result = await dao.insert_fundamental(sample_fmp_fundamental)
        
        assert result is False
        mock_conn.execute.assert_called_once()
    
    @patch('asyncpg.create_pool')
    async def test_get_fundamental_found(self, mock_pool, mock_environment):
        """Test getting existing fundamental data"""
        # Setup mock data
        mock_row = {
            'symbol': 'AAPL',
            'date': date(2023, 12, 31),
            'vendor': 'fmp',
            'fiscal_period': 'Q4',
            'revenue': 123456000000,
            'gross_profit': None,
            'operating_income': None,
            'net_income': 10000000000,
            'ebitda': None,
            'eps': 5.67,
            'total_assets': 400000000000,
            'total_liabilities': None,
            'shareholders_equity': None,
            'current_assets': None,
            'current_liabilities': None,
            'total_debt': None,
            'cash_and_equivalents': None,
            'operating_cash_flow': None,
            'investing_cash_flow': None,
            'financing_cash_flow': None,
            'free_cash_flow': None,
            'market_cap': 3000000000000,
            'pe_ratio': 25.5,
            'pb_ratio': None,
            'debt_to_equity': None,
            'roe': 0.28,
            'roa': None,
            'current_ratio': None,
            'quick_ratio': None,
            'raw_data': {"source": "fmp_api"}
        }
        
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = mock_row
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test getting fundamental
        result = await dao.get_fundamental("AAPL", date(2023, 12, 31))
        
        assert result is not None
        assert isinstance(result, FMPFundamental)
        assert result.symbol == "AAPL"
        assert result.vendor == "fmp"
        assert result.revenue == 123456000000
        assert result.eps == 5.67
        
        mock_conn.fetchrow.assert_called_once()
    
    @patch('asyncpg.create_pool')
    async def test_get_fundamental_not_found(self, mock_pool, mock_environment):
        """Test getting non-existent fundamental data"""
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test getting non-existent fundamental
        result = await dao.get_fundamental("NONEXISTENT", date(2023, 12, 31))
        
        assert result is None
        mock_conn.fetchrow.assert_called_once()
    
    @patch('asyncpg.create_pool')
    async def test_list_fundamentals(self, mock_pool, mock_environment):
        """Test listing fundamentals for a symbol"""
        mock_rows = [
            {
                'symbol': 'AAPL', 'date': date(2023, 12, 31), 'vendor': 'fmp',
                'fiscal_period': 'Q4', 'revenue': 123456000000, 'net_income': 10000000000,
                'eps': 5.67, 'total_assets': 400000000000, 'market_cap': 3000000000000,
                'pe_ratio': 25.5, 'roe': 0.28, 'raw_data': None,
                'gross_profit': None, 'operating_income': None, 'ebitda': None,
                'total_liabilities': None, 'shareholders_equity': None, 'current_assets': None,
                'current_liabilities': None, 'total_debt': None, 'cash_and_equivalents': None,
                'operating_cash_flow': None, 'investing_cash_flow': None, 'financing_cash_flow': None,
                'free_cash_flow': None, 'pb_ratio': None, 'debt_to_equity': None,
                'roa': None, 'current_ratio': None, 'quick_ratio': None
            },
            {
                'symbol': 'AAPL', 'date': date(2023, 9, 30), 'vendor': 'fmp',
                'fiscal_period': 'Q3', 'revenue': 120000000000, 'net_income': 9500000000,
                'eps': 5.40, 'total_assets': 390000000000, 'market_cap': 2900000000000,
                'pe_ratio': 24.8, 'roe': 0.26, 'raw_data': None,
                'gross_profit': None, 'operating_income': None, 'ebitda': None,
                'total_liabilities': None, 'shareholders_equity': None, 'current_assets': None,
                'current_liabilities': None, 'total_debt': None, 'cash_and_equivalents': None,
                'operating_cash_flow': None, 'investing_cash_flow': None, 'financing_cash_flow': None,
                'free_cash_flow': None, 'pb_ratio': None, 'debt_to_equity': None,
                'roa': None, 'current_ratio': None, 'quick_ratio': None
            }
        ]
        
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_rows
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test listing fundamentals
        result = await dao.list_fundamentals("AAPL", limit=10)
        
        assert len(result) == 2
        assert all(isinstance(f, FMPFundamental) for f in result)
        assert result[0].symbol == "AAPL"
        assert result[0].vendor == "fmp"
        assert result[0].date == date(2023, 12, 31)
        assert result[1].date == date(2023, 9, 30)
        
        mock_conn.fetch.assert_called_once()
    
    @patch('asyncpg.create_pool')
    async def test_delete_fundamental_success(self, mock_pool, mock_environment):
        """Test successful fundamental deletion"""
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "DELETE 1"  # PostgreSQL DELETE result format
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test deletion
        result = await dao.delete_fundamental("AAPL", date(2023, 12, 31))
        
        assert result is True
        mock_conn.execute.assert_called_once()
        
        # Verify SQL parameters
        call_args = mock_conn.execute.call_args
        assert "DELETE FROM test_fundamentals_comprehensive" in call_args[0][0]
        assert call_args[0][1] == "AAPL"
        assert call_args[0][2] == date(2023, 12, 31)
        assert call_args[0][3] == "fmp"
    
    @patch('asyncpg.create_pool')
    async def test_delete_fundamental_not_found(self, mock_pool, mock_environment):
        """Test deletion of non-existent fundamental"""
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "DELETE 0"  # No rows deleted
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test deletion of non-existent record
        result = await dao.delete_fundamental("NONEXISTENT", date(2023, 12, 31))
        
        assert result is False
        mock_conn.execute.assert_called_once()
    
    @patch('asyncpg.create_pool')
    async def test_get_symbols_with_data(self, mock_pool, mock_environment):
        """Test getting symbols that have fundamental data"""
        mock_rows = [
            {'symbol': 'AAPL'},
            {'symbol': 'GOOGL'},
            {'symbol': 'MSFT'}
        ]
        
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_rows
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test getting symbols
        result = await dao.get_symbols_with_data()
        
        assert result == ['AAPL', 'GOOGL', 'MSFT']
        mock_conn.fetch.assert_called_once()


class TestFundamentalsPolygonDAO:
    """Test suite for Polygon Fundamentals DAO"""
    
    def test_init(self, mock_environment):
        """Test DAO initialization"""
        dao = FundamentalsPolygonDAO(mock_environment)
        
        assert dao.env == mock_environment
        assert dao.vendor == "polygon"
        assert dao.table_name == "test_fundamentals_comprehensive"
        assert dao.db_url == "postgresql://test:test@localhost:5432/test_db"
    
    @patch('asyncpg.create_pool')
    async def test_insert_and_retrieve(self, mock_pool, mock_environment, sample_polygon_fundamental):
        """Test inserting and retrieving Polygon fundamental data"""
        # Mock for insertion
        mock_conn = AsyncMock()
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsPolygonDAO(mock_environment)
        
        # Test insertion
        result = await dao.insert_fundamental(sample_polygon_fundamental)
        assert result is True
        
        # Verify vendor is correct in call
        call_args = mock_conn.execute.call_args
        assert call_args[0][3] == "polygon"  # vendor parameter


class TestFundamentalsTiingoDAO:
    """Test suite for Tiingo Fundamentals DAO"""
    
    def test_init(self, mock_environment):
        """Test DAO initialization"""
        dao = FundamentalsTiingoDAO(mock_environment)
        
        assert dao.env == mock_environment
        assert dao.vendor == "tiingo"
        assert dao.table_name == "test_fundamentals_comprehensive"
        assert dao.db_url == "postgresql://test:test@localhost:5432/test_db"
    
    @patch('asyncpg.create_pool')
    async def test_vendor_specific_operations(self, mock_pool, mock_environment, sample_tiingo_fundamental):
        """Test Tiingo-specific operations"""
        mock_conn = AsyncMock()
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsTiingoDAO(mock_environment)
        
        # Test insertion with Tiingo vendor
        result = await dao.insert_fundamental(sample_tiingo_fundamental)
        assert result is True
        
        # Verify correct vendor parameter
        call_args = mock_conn.execute.call_args
        assert call_args[0][3] == "tiingo"


class TestVendorFundamentalDataClasses:
    """Test data class functionality"""
    
    def test_fmp_fundamental_creation(self, sample_fmp_fundamental):
        """Test FMP fundamental data class"""
        assert sample_fmp_fundamental.vendor == "fmp"
        assert sample_fmp_fundamental.symbol == "AAPL"
        assert sample_fmp_fundamental.revenue == 123456000000
        assert sample_fmp_fundamental.eps == 5.67
        assert sample_fmp_fundamental.raw_data["source"] == "fmp_api"
    
    def test_polygon_fundamental_creation(self, sample_polygon_fundamental):
        """Test Polygon fundamental data class"""
        assert sample_polygon_fundamental.vendor == "polygon"
        assert sample_polygon_fundamental.symbol == "AAPL"
        assert sample_polygon_fundamental.revenue == 123500000000
        assert sample_polygon_fundamental.eps == 5.65
    
    def test_tiingo_fundamental_creation(self, sample_tiingo_fundamental):
        """Test Tiingo fundamental data class"""
        assert sample_tiingo_fundamental.vendor == "tiingo"
        assert sample_tiingo_fundamental.symbol == "AAPL"
        assert sample_tiingo_fundamental.revenue == 123400000000
        assert sample_tiingo_fundamental.eps == 5.68


class TestDAOErrorHandling:
    """Test error handling across all DAOs"""
    
    @patch('asyncpg.create_pool')
    async def test_database_connection_failure(self, mock_pool, mock_environment):
        """Test handling of database connection failures"""
        mock_pool.side_effect = Exception("Connection failed")
        
        dao = FundamentalsFMPDAO(mock_environment)
        
        # Test that operations gracefully handle connection failures
        result = await dao.get_fundamental("AAPL", date(2023, 12, 31))
        assert result is None
    
    @patch('asyncpg.create_pool')
    async def test_query_execution_failure(self, mock_pool, mock_environment):
        """Test handling of query execution failures"""
        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = Exception("Query execution failed")
        mock_pool_instance = AsyncMock()
        mock_pool_instance.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool_instance.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.return_value = mock_pool_instance
        
        dao = FundamentalsPolygonDAO(mock_environment)
        
        # Test that query failures are handled gracefully
        result = await dao.get_fundamental("AAPL", date(2023, 12, 31))
        assert result is None


@pytest.mark.integration
class TestDAOIntegration:
    """Integration tests for DAO operations"""
    
    def test_all_vendors_have_same_interface(self, mock_environment):
        """Test that all vendor DAOs have consistent interfaces"""
        fmp_dao = FundamentalsFMPDAO(mock_environment)
        polygon_dao = FundamentalsPolygonDAO(mock_environment)
        tiingo_dao = FundamentalsTiingoDAO(mock_environment)
        
        # Test that all DAOs have the same methods
        expected_methods = [
            'insert_fundamental',
            'get_fundamental', 
            'list_fundamentals',
            'get_latest_fundamental',
            'delete_fundamental',
            'get_symbols_with_data'
        ]
        
        for method in expected_methods:
            assert hasattr(fmp_dao, method)
            assert hasattr(polygon_dao, method)
            assert hasattr(tiingo_dao, method)
            assert callable(getattr(fmp_dao, method))
            assert callable(getattr(polygon_dao, method))
            assert callable(getattr(tiingo_dao, method))


class TestUnifiedFundamentalProvider:
    """Test suite for UnifiedFundamentalProvider"""
    
    @pytest.fixture
    def mock_unified_provider(self, mock_environment):
        """Mock UnifiedFundamentalProvider for testing"""
        from src.market_data.fundamentals.unified_fundamental_provider import UnifiedFundamentalProvider
        
        with patch.multiple(
            'src.market_data.fundamentals.unified_fundamental_provider',
            FundamentalsFMPDAO=MagicMock(),
            FundamentalsPolygonDAO=MagicMock(),
            FundamentalsTiingoDAO=MagicMock()
        ):
            provider = UnifiedFundamentalProvider(mock_environment)
            return provider
    
    @pytest.fixture
    def sample_vendor_fundamentals(self, sample_fmp_fundamental, sample_polygon_fundamental, sample_tiingo_fundamental):
        """Sample vendor fundamental data for testing"""
        from src.market_data.fundamentals.unified_fundamental_provider import VendorFundamental
        
        return [
            VendorFundamental(
                vendor="fmp",
                symbol=sample_fmp_fundamental.symbol,
                date=sample_fmp_fundamental.date,
                revenue=sample_fmp_fundamental.revenue,
                net_income=sample_fmp_fundamental.net_income,
                eps=sample_fmp_fundamental.eps,
                market_cap=sample_fmp_fundamental.market_cap,
                pe_ratio=sample_fmp_fundamental.pe_ratio,
                roe=sample_fmp_fundamental.roe,
                raw_data=sample_fmp_fundamental.raw_data
            ),
            VendorFundamental(
                vendor="polygon", 
                symbol=sample_polygon_fundamental.symbol,
                date=sample_polygon_fundamental.date,
                revenue=sample_polygon_fundamental.revenue,
                net_income=sample_polygon_fundamental.net_income,
                eps=sample_polygon_fundamental.eps,
                market_cap=sample_polygon_fundamental.market_cap,
                pe_ratio=sample_polygon_fundamental.pe_ratio,
                roe=sample_polygon_fundamental.roe,
                raw_data=sample_polygon_fundamental.raw_data
            ),
            VendorFundamental(
                vendor="tiingo",
                symbol=sample_tiingo_fundamental.symbol,
                date=sample_tiingo_fundamental.date,
                revenue=sample_tiingo_fundamental.revenue,
                net_income=sample_tiingo_fundamental.net_income,
                eps=sample_tiingo_fundamental.eps,
                market_cap=sample_tiingo_fundamental.market_cap,
                pe_ratio=sample_tiingo_fundamental.pe_ratio,
                roe=sample_tiingo_fundamental.roe,
                raw_data=sample_tiingo_fundamental.raw_data
            )
        ]
    
    def test_init(self, mock_environment):
        """Test UnifiedFundamentalProvider initialization"""
        from src.market_data.fundamentals.unified_fundamental_provider import UnifiedFundamentalProvider
        
        with patch.multiple(
            'src.market_data.fundamentals.unified_fundamental_provider',
            FundamentalsFMPDAO=MagicMock(),
            FundamentalsPolygonDAO=MagicMock(),
            FundamentalsTiingoDAO=MagicMock()
        ):
            provider = UnifiedFundamentalProvider(mock_environment)
            
            assert provider.env == mock_environment
            assert hasattr(provider, 'fmp_dao')
            assert hasattr(provider, 'polygon_dao')
            assert hasattr(provider, 'tiingo_dao')
            assert provider.logger is not None
    
    async def test_get_unified_fundamental_all_vendors(self, mock_unified_provider, sample_vendor_fundamentals):
        """Test getting unified fundamental with data from all vendors"""
        from src.market_data.fundamentals.unified_fundamental_provider import UnifiedFundamental, ValidationStatus
        
        # Mock DAO responses
        mock_unified_provider.fmp_dao.get_fundamental = AsyncMock(return_value=sample_vendor_fundamentals[0])
        mock_unified_provider.polygon_dao.get_fundamental = AsyncMock(return_value=sample_vendor_fundamentals[1])
        mock_unified_provider.tiingo_dao.get_fundamental = AsyncMock(return_value=sample_vendor_fundamentals[2])
        
        # Test getting unified fundamental
        result = await mock_unified_provider.get_unified_fundamental("AAPL", date(2023, 12, 31))
        
        assert result is not None
        assert isinstance(result, UnifiedFundamental)
        assert result.symbol == "AAPL"
        assert result.date == date(2023, 12, 31)
        assert result.status in [ValidationStatus.CONSENSUS, ValidationStatus.MAJORITY_CONSENSUS]
        assert result.confidence_score > 0.0
        assert len(result.vendor_data) == 3
        
        # Verify vendor data
        vendors = {vd.vendor for vd in result.vendor_data}
        assert vendors == {"fmp", "polygon", "tiingo"}
    
    async def test_get_unified_fundamental_partial_data(self, mock_unified_provider, sample_vendor_fundamentals):
        """Test getting unified fundamental with partial vendor data"""
        from src.market_data.fundamentals.unified_fundamental_provider import ValidationStatus
        
        # Mock DAO responses - only 2 out of 3 vendors have data
        mock_unified_provider.fmp_dao.get_fundamental = AsyncMock(return_value=sample_vendor_fundamentals[0])
        mock_unified_provider.polygon_dao.get_fundamental = AsyncMock(return_value=sample_vendor_fundamentals[1])
        mock_unified_provider.tiingo_dao.get_fundamental = AsyncMock(return_value=None)
        
        # Test getting unified fundamental
        result = await mock_unified_provider.get_unified_fundamental("AAPL", date(2023, 12, 31))
        
        assert result is not None
        assert result.status == ValidationStatus.MAJORITY_CONSENSUS
        assert len(result.vendor_data) == 2
        assert result.confidence_score > 0.0
    
    async def test_get_unified_fundamental_no_data(self, mock_unified_provider):
        """Test getting unified fundamental when no vendors have data"""
        # Mock DAO responses - no data from any vendor
        mock_unified_provider.fmp_dao.get_fundamental = AsyncMock(return_value=None)
        mock_unified_provider.polygon_dao.get_fundamental = AsyncMock(return_value=None)
        mock_unified_provider.tiingo_dao.get_fundamental = AsyncMock(return_value=None)
        
        # Test getting unified fundamental
        result = await mock_unified_provider.get_unified_fundamental("NONEXISTENT", date(2023, 12, 31))
        
        assert result is None
    
    async def test_get_unified_fundamental_single_vendor(self, mock_unified_provider, sample_vendor_fundamentals):
        """Test getting unified fundamental with only one vendor"""
        from src.market_data.fundamentals.unified_fundamental_provider import ValidationStatus
        
        # Mock DAO responses - only FMP has data
        mock_unified_provider.fmp_dao.get_fundamental = AsyncMock(return_value=sample_vendor_fundamentals[0])
        mock_unified_provider.polygon_dao.get_fundamental = AsyncMock(return_value=None)
        mock_unified_provider.tiingo_dao.get_fundamental = AsyncMock(return_value=None)
        
        # Test getting unified fundamental
        result = await mock_unified_provider.get_unified_fundamental("AAPL", date(2023, 12, 31))
        
        assert result is not None
        assert result.status == ValidationStatus.SINGLE_VENDOR
        assert len(result.vendor_data) == 1
        assert result.confidence_score == 0.5  # Single vendor has moderate confidence
    
    def test_calculate_statistical_metrics(self, mock_unified_provider):
        """Test statistical calculation for cross-vendor validation"""
        # Test with sample data
        values = [100.0, 102.0, 98.0, 101.0]
        
        mean, std, outliers = mock_unified_provider._calculate_statistical_metrics(values)
        
        assert abs(mean - 100.25) < 0.01
        assert std > 0
        assert len(outliers) == 0  # No outliers in this tight range
    
    def test_calculate_statistical_metrics_with_outliers(self, mock_unified_provider):
        """Test statistical calculation with outliers"""
        # Test with data containing outliers
        values = [100.0, 101.0, 99.0, 200.0]  # 200 is an outlier
        
        mean, std, outliers = mock_unified_provider._calculate_statistical_metrics(values)
        
        assert len(outliers) == 1
        assert 200.0 in outliers
        assert mean > 100  # Mean affected by outlier
        assert std > 30   # High standard deviation due to outlier
    
    def test_detect_disagreements(self, mock_unified_provider, sample_vendor_fundamentals):
        """Test disagreement detection across vendors"""
        # Create disagreement scenario
        disagreement_data = sample_vendor_fundamentals.copy()
        disagreement_data[2].revenue = 200000000000  # Tiingo has very different revenue
        
        disagreements = mock_unified_provider._detect_disagreements(disagreement_data)
        
        assert len(disagreements) > 0
        assert any('revenue' in d['metric'] for d in disagreements)
        assert any(d['severity'] == 'high' for d in disagreements)
    
    def test_calculate_confidence_score(self, mock_unified_provider, sample_vendor_fundamentals):
        """Test confidence score calculation"""
        # Test with consistent data (high confidence)
        score = mock_unified_provider._calculate_confidence_score(sample_vendor_fundamentals, [])
        assert score > 0.8  # High confidence with multiple agreeing vendors
        
        # Test with single vendor (moderate confidence)
        single_vendor_score = mock_unified_provider._calculate_confidence_score(sample_vendor_fundamentals[:1], [])
        assert single_vendor_score == 0.5  # Single vendor baseline
        
        # Test with disagreements (lower confidence)  
        disagreements = [{'metric': 'revenue', 'severity': 'high'}]
        low_score = mock_unified_provider._calculate_confidence_score(sample_vendor_fundamentals, disagreements)
        assert low_score < 0.8  # Lower confidence with disagreements
    
    async def test_dao_error_handling(self, mock_unified_provider):
        """Test handling of DAO errors"""
        # Mock DAO to raise exception
        mock_unified_provider.fmp_dao.get_fundamental = AsyncMock(side_effect=Exception("DAO Error"))
        mock_unified_provider.polygon_dao.get_fundamental = AsyncMock(return_value=None)
        mock_unified_provider.tiingo_dao.get_fundamental = AsyncMock(return_value=None)
        
        # Should handle DAO errors gracefully
        result = await mock_unified_provider.get_unified_fundamental("AAPL", date(2023, 12, 31))
        
        assert result is None  # No data available due to errors and missing data
    
    async def test_list_symbols_with_data(self, mock_unified_provider):
        """Test listing symbols that have fundamental data"""
        # Mock DAO responses
        mock_unified_provider.fmp_dao.get_symbols_with_data = AsyncMock(return_value=["AAPL", "GOOGL", "MSFT"])
        mock_unified_provider.polygon_dao.get_symbols_with_data = AsyncMock(return_value=["AAPL", "GOOGL", "TSLA"])
        mock_unified_provider.tiingo_dao.get_symbols_with_data = AsyncMock(return_value=["AAPL", "MSFT", "TSLA"])
        
        # Test getting combined symbol list
        result = await mock_unified_provider.list_symbols_with_data()
        
        assert result is not None
        assert isinstance(result, list)
        # Should return union of all vendor symbols
        assert "AAPL" in result
        assert "GOOGL" in result
        assert "MSFT" in result
        assert "TSLA" in result