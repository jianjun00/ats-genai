"""
Unit tests for UnifiedMarketCapProvider

Tests market cap calculation logic, cross-source validation, and data reconciliation
using mocked dependencies for reliable, fast testing.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date

from domains.market_data.services.market_cap.unified_market_cap_provider import (
    UnifiedMarketCapProvider,
    MarketCapSource,
    UnifiedMarketCap,
    MarketCapValidationStatus
)
from domains.market_data.services.vendor_adapters.fundamentals.unified_fundamental_provider import (
    UnifiedFundamental,
    VendorFundamental,
    ValidationStatus as FundamentalValidationStatus
)
from domains.trading.services.core.eod.unified_daily_price_validator import (
    UnifiedPrice,
    ValidationResult,
    ValidationStatus as PriceValidationStatus
)
from core.platform.config.environment import Environment


@pytest.fixture
def mock_environment():
    """Mock environment for testing"""
    env = MagicMock(spec=Environment)
    env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test_db"
    return env


@pytest.fixture
def sample_fundamental_data():
    """Sample fundamental data with market cap"""
    return [
        VendorFundamental(
            vendor="fmp",
            symbol="AAPL",
            date=date(2023, 12, 31),
            market_cap=3000000000000,  # $3T
            confidence=0.9,
            raw_data={"source": "fmp_api"}
        ),
        VendorFundamental(
            vendor="polygon",
            symbol="AAPL",
            date=date(2023, 12, 31),
            market_cap=2980000000000,  # $2.98T
            confidence=0.85,
            raw_data={"source": "polygon_api"}
        )
    ]


@pytest.fixture
def sample_unified_fundamental(sample_fundamental_data):
    """Sample unified fundamental data"""
    return UnifiedFundamental(
        symbol="AAPL",
        date=date(2023, 12, 31),
        status=FundamentalValidationStatus.CONSENSUS,
        confidence_score=0.9,
        vendor_data=sample_fundamental_data,
        validation_metadata={"disagreements": [], "outliers": []}
    )


@pytest.fixture
def sample_unified_price():
    """Sample unified price data"""
    validation_result = ValidationResult(
        is_valid=True,
        status=PriceValidationStatus.VALID,
        confidence_score=0.9,
        statistical_score=1.2,
        price_variance=0.01,
        validation_notes="Consensus from 3 vendors"
    )

    return UnifiedPrice(
        instrument_id=1,
        date=date(2023, 12, 31),
        open_price=195.00,
        high_price=196.50,
        low_price=194.00,
        close=195.89,
        adj_close=195.89,
        volume=50000000,
        primary_vendor="polygon",
        secondary_vendors=["tiingo", "fmp"],
        vendor_count=3,
        validation_result=validation_result,
        vendor_prices={"polygon": 195.89, "tiingo": 195.85, "fmp": 195.92}
    )


class TestUnifiedMarketCapProvider:
    """Test suite for UnifiedMarketCapProvider"""

    @pytest.fixture
    def mock_provider(self, mock_environment):
        """Mock UnifiedMarketCapProvider with mocked dependencies"""
        with patch('src.market_data.market_cap.unified_market_cap_provider.UnifiedFundamentalProvider') as mock_fundamental, \
             patch('src.market_data.market_cap.unified_market_cap_provider.UnifiedDailyPriceValidator') as mock_price:

            provider = UnifiedMarketCapProvider(mock_environment)
            provider.fundamental_provider = mock_fundamental.return_value
            provider.price_validator = mock_price.return_value
            provider.conn = AsyncMock()

            return provider

    def test_init(self, mock_environment):
        """Test UnifiedMarketCapProvider initialization"""
        with patch('src.market_data.market_cap.unified_market_cap_provider.UnifiedFundamentalProvider'), \
             patch('src.market_data.market_cap.unified_market_cap_provider.UnifiedDailyPriceValidator'):

            provider = UnifiedMarketCapProvider(mock_environment)

            assert provider.env == mock_environment
            assert provider.disagreement_threshold == 0.15
            assert provider.outlier_threshold_sigma == 3.0
            assert provider.min_confidence_threshold == 0.3
            assert provider.logger is not None

    @pytest.mark.asyncio

    async def test_connect_and_disconnect(self, mock_provider):
        """Test database connection management"""
        # Mock database connection
        with patch('asyncpg.connect') as mock_connect:
            mock_connect.return_value = AsyncMock()

            await mock_provider.connect()
            assert mock_provider.conn is not None
            mock_provider.price_validator.connect.assert_called_once()

            await mock_provider.disconnect()
            mock_provider.conn.close.assert_called_once()
            mock_provider.price_validator.disconnect.assert_called_once()

    @pytest.mark.asyncio

    async def test_get_fundamental_market_cap_sources(self, mock_provider, sample_unified_fundamental):
        """Test getting market cap from fundamental data sources"""
        # Mock fundamental provider
        mock_provider.fundamental_provider.get_unified_fundamental = AsyncMock(
            return_value=sample_unified_fundamental
        )

        sources = await mock_provider._get_fundamental_market_cap_sources("AAPL", date(2023, 12, 31))

        assert len(sources) == 2
        assert all(isinstance(s, MarketCapSource) for s in sources)
        assert sources[0].source_type == "fundamental"
        assert sources[0].vendor == "fmp"
        assert sources[0].market_cap == 3000000000000
        assert sources[1].vendor == "polygon"
        assert sources[1].market_cap == 2980000000000

    @pytest.mark.asyncio

    async def test_get_price_based_market_cap(self, mock_provider, sample_unified_price):
        """Test calculating market cap from price * shares outstanding"""
        # Mock price validator
        mock_provider.price_validator.validate_and_unify_price = AsyncMock(
            return_value=sample_unified_price
        )

        # Mock shares outstanding lookup
        mock_provider._get_shares_outstanding = AsyncMock(return_value=15000000000)  # 15B shares

        source = await mock_provider._get_price_based_market_cap("AAPL", date(2023, 12, 31))

        assert source is not None
        assert isinstance(source, MarketCapSource)
        assert source.source_type == "price_calculated"
        assert source.vendor == "calculated"
        assert source.calculation_method == "price_shares"
        assert source.price_used == 195.89
        assert source.shares_outstanding == 15000000000
        # Market cap = 195.89 * 15B = ~2.94T
        assert abs(source.market_cap - (195.89 * 15000000000)) < 100

    @pytest.mark.asyncio

    async def test_get_shares_outstanding_from_fundamental(self, mock_provider):
        """Test extracting shares outstanding from fundamental data"""
        # Create mock fundamental data with shares in raw_data
        mock_fundamental = UnifiedFundamental(
            symbol="AAPL",
            date=date(2023, 12, 31),
            status=FundamentalValidationStatus.CONSENSUS,
            confidence_score=0.9,
            vendor_data=[
                VendorFundamental(
                    vendor="fmp",
                    symbol="AAPL",
                    date=date(2023, 12, 31),
                    raw_data={"shares_outstanding": 15000000000}
                )
            ],
            validation_metadata={}
        )

        mock_provider.fundamental_provider.get_unified_fundamental = AsyncMock(
            return_value=mock_fundamental
        )

        shares = await mock_provider._get_shares_outstanding("AAPL", date(2023, 12, 31))

        assert shares == 15000000000

    @pytest.mark.asyncio

    async def test_get_shares_outstanding_from_database(self, mock_provider):
        """Test getting shares outstanding from database fallback"""
        # Mock no fundamental data
        mock_provider.fundamental_provider.get_unified_fundamental = AsyncMock(return_value=None)

        # Mock database query
        mock_provider.conn.fetchrow = AsyncMock(
            return_value={'shares_outstanding': 14500000000}
        )

        shares = await mock_provider._get_shares_outstanding("AAPL", date(2023, 12, 31))

        assert shares == 14500000000
        mock_provider.conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio

    async def test_get_historical_market_cap_estimate(self, mock_provider):
        """Test getting historical market cap estimate"""
        # Mock database query for historical data
        historical_date = date(2023, 12, 25)  # 6 days before target
        mock_provider.conn.fetchrow = AsyncMock(
            return_value={
                'market_cap': 2950000000000,
                'date': historical_date
            }
        )

        source = await mock_provider._get_historical_market_cap_estimate("AAPL", date(2023, 12, 31))

        assert source is not None
        assert isinstance(source, MarketCapSource)
        assert source.source_type == "historical"
        assert source.vendor == "database"
        assert source.market_cap == 2950000000000
        assert source.calculation_method == "historical_estimate"
        assert source.confidence < 1.0  # Lower confidence for historical data
        assert source.raw_data["days_difference"] == 6

    @pytest.mark.asyncio

    async def test_create_unified_market_cap_single_source(self, mock_provider):
        """Test creating unified market cap with single source"""
        sources = [
            MarketCapSource(
                source_type="fundamental",
                vendor="fmp",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=3000000000000,
                calculation_method="reported",
                confidence=0.9
            )
        ]

        result = await mock_provider._create_unified_market_cap("AAPL", date(2023, 12, 31), sources)

        assert result is not None
        assert isinstance(result, UnifiedMarketCap)
        assert result.symbol == "AAPL"
        assert result.market_cap == 3000000000000
        assert result.status == MarketCapValidationStatus.SINGLE_SOURCE
        assert result.confidence_score == 0.9 * 0.7  # Reduced for single source
        assert result.primary_source == "fundamental_fmp"

    @pytest.mark.asyncio

    async def test_create_unified_market_cap_consensus(self, mock_provider):
        """Test creating unified market cap with consensus from multiple sources"""
        sources = [
            MarketCapSource(
                source_type="fundamental",
                vendor="fmp",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=3000000000000,
                calculation_method="reported",
                confidence=0.9
            ),
            MarketCapSource(
                source_type="fundamental",
                vendor="polygon",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=2980000000000,
                calculation_method="reported",
                confidence=0.85
            ),
            MarketCapSource(
                source_type="price_calculated",
                vendor="calculated",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=2990000000000,
                calculation_method="price_shares",
                confidence=0.8
            )
        ]

        result = await mock_provider._create_unified_market_cap("AAPL", date(2023, 12, 31), sources)

        assert result is not None
        assert result.status == MarketCapValidationStatus.CONSENSUS
        assert result.confidence_score > 0.8
        assert len(result.source_data) == 3
        assert result.validation_metadata["total_sources"] == 3
        assert result.validation_metadata["consensus_sources"] == 3
        assert "Consensus from 3 sources" in result.calculation_notes

    @pytest.mark.asyncio

    async def test_create_unified_market_cap_disagreement(self, mock_provider):
        """Test handling vendor disagreement in market cap data"""
        sources = [
            MarketCapSource(
                source_type="fundamental",
                vendor="fmp",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=3000000000000,  # $3T
                calculation_method="reported",
                confidence=0.9
            ),
            MarketCapSource(
                source_type="fundamental",
                vendor="polygon",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=2400000000000,  # $2.4T - 20% disagreement
                calculation_method="reported",
                confidence=0.85
            )
        ]

        result = await mock_provider._create_unified_market_cap("AAPL", date(2023, 12, 31), sources)

        assert result is not None
        assert result.status == MarketCapValidationStatus.VENDOR_DISAGREEMENT
        assert result.confidence_score == 0.4  # Low confidence due to disagreement
        assert result.validation_metadata["max_deviation_pct"] > 0.15

    @pytest.mark.asyncio

    async def test_create_unified_market_cap_outlier_detection(self, mock_provider):
        """Test outlier detection in market cap data"""
        sources = [
            MarketCapSource(
                source_type="fundamental",
                vendor="fmp",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=3000000000000,  # Normal
                calculation_method="reported",
                confidence=0.9
            ),
            MarketCapSource(
                source_type="fundamental",
                vendor="polygon",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=2990000000000,  # Normal
                calculation_method="reported",
                confidence=0.85
            ),
            MarketCapSource(
                source_type="price_calculated",
                vendor="calculated",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=1000000000000,  # Outlier - much lower
                calculation_method="price_shares",
                confidence=0.8
            )
        ]

        result = await mock_provider._create_unified_market_cap("AAPL", date(2023, 12, 31), sources)

        assert result is not None
        assert result.status == MarketCapValidationStatus.OUTLIER_DETECTED
        assert result.validation_metadata["outliers_detected"] == 1
        assert result.validation_metadata["consensus_sources"] == 2  # Outlier excluded

    @pytest.mark.asyncio

    async def test_get_unified_market_cap_integration(self, mock_provider, sample_unified_fundamental, sample_unified_price):
        """Test full integration of get_unified_market_cap"""
        # Mock fundamental data
        mock_provider.fundamental_provider.get_unified_fundamental = AsyncMock(
            return_value=sample_unified_fundamental
        )

        # Mock price data and shares outstanding
        mock_provider.price_validator.validate_and_unify_price = AsyncMock(
            return_value=sample_unified_price
        )
        mock_provider._get_shares_outstanding = AsyncMock(return_value=15000000000)

        result = await mock_provider.get_unified_market_cap("AAPL", date(2023, 12, 31))

        assert result is not None
        assert isinstance(result, UnifiedMarketCap)
        assert result.symbol == "AAPL"
        assert result.date == date(2023, 12, 31)
        assert result.market_cap > 0
        assert 0.0 <= result.confidence_score <= 1.0
        assert len(result.source_data) >= 2  # Fundamental + price-calculated

    @pytest.mark.asyncio

    async def test_list_symbols_with_market_cap_data(self, mock_provider):
        """Test listing symbols with market cap data"""
        # Mock fundamental symbols
        mock_provider.fundamental_provider.list_symbols_with_data = AsyncMock(
            return_value=["AAPL", "GOOGL", "MSFT"]
        )

        # Mock price symbols from database
        mock_provider.conn.fetch = AsyncMock(
            return_value=[
                {"symbol": "AAPL"},
                {"symbol": "TSLA"},
                {"symbol": "META"}
            ]
        )

        symbols = await mock_provider.list_symbols_with_market_cap_data()

        # Should return union of fundamental and price symbols
        expected_symbols = ["AAPL", "GOOGL", "META", "MSFT", "TSLA"]
        assert sorted(symbols) == sorted(expected_symbols)

    @pytest.mark.asyncio

    async def test_get_market_cap_history(self, mock_provider):
        """Test getting market cap history over date range"""
        start_date = date(2023, 12, 29)
        end_date = date(2023, 12, 31)

        # Mock get_unified_market_cap to return different values for different dates
        async def mock_get_market_cap(symbol, target_date):
            if target_date == date(2023, 12, 29):
                return UnifiedMarketCap(
                    symbol=symbol,
                    date=target_date,
                    market_cap=2950000000000,
                    confidence_score=0.9,
                    status=MarketCapValidationStatus.CONSENSUS,
                    primary_source="fundamental_fmp",
                    source_data=[],
                    validation_metadata={},
                    calculation_notes="Mock data"
                )
            elif target_date == date(2023, 12, 30):
                return None  # Missing data
            elif target_date == date(2023, 12, 31):
                return UnifiedMarketCap(
                    symbol=symbol,
                    date=target_date,
                    market_cap=3000000000000,
                    confidence_score=0.85,
                    status=MarketCapValidationStatus.CONSENSUS,
                    primary_source="fundamental_polygon",
                    source_data=[],
                    validation_metadata={},
                    calculation_notes="Mock data"
                )
            return None

        mock_provider.get_unified_market_cap = AsyncMock(side_effect=mock_get_market_cap)

        history = await mock_provider.get_market_cap_history("AAPL", start_date, end_date)

        assert len(history) == 2  # Only 2 out of 3 dates have data
        assert history[0].date == date(2023, 12, 29)
        assert history[0].market_cap == 2950000000000
        assert history[1].date == date(2023, 12, 31)
        assert history[1].market_cap == 3000000000000

    @pytest.mark.asyncio

    async def test_error_handling(self, mock_provider):
        """Test error handling in various scenarios"""
        # Test with no sources
        result = await mock_provider._create_unified_market_cap("AAPL", date(2023, 12, 31), [])
        assert result is None

        # Test with connection error
        mock_provider.fundamental_provider.get_unified_fundamental = AsyncMock(
            side_effect=Exception("Connection error")
        )
        mock_provider.price_validator.validate_and_unify_price = AsyncMock(
            side_effect=Exception("Price error")
        )
        mock_provider._get_historical_market_cap_estimate = AsyncMock(return_value=None)

        result = await mock_provider.get_unified_market_cap("INVALID", date(2023, 12, 31))
        assert result is None

    @pytest.mark.asyncio

    async def test_confidence_filtering(self, mock_provider):
        """Test filtering sources by minimum confidence threshold"""
        # Sources with varying confidence levels
        sources = [
            MarketCapSource(
                source_type="fundamental",
                vendor="fmp",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=3000000000000,
                calculation_method="reported",
                confidence=0.9  # High confidence
            ),
            MarketCapSource(
                source_type="historical",
                vendor="database",
                symbol="AAPL",
                date=date(2023, 12, 31),
                market_cap=2800000000000,
                calculation_method="historical_estimate",
                confidence=0.2  # Below threshold
            )
        ]

        result = await mock_provider._create_unified_market_cap("AAPL", date(2023, 12, 31), sources)

        # Should only use the high-confidence source
        assert result is not None
        assert result.status == MarketCapValidationStatus.SINGLE_SOURCE
        assert len(result.source_data) == 1
        assert result.source_data[0].confidence == 0.9