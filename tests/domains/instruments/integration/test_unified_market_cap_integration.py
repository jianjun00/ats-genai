"""
Integration tests for UnifiedMarketCapProvider

Tests against real database schema and data to validate market cap calculations
using actual fundamental and price data from multiple vendors.
"""

import pytest
from datetime import date, timedelta

from domains.market_data.services.market_cap.unified_market_cap_provider import (
    UnifiedMarketCapProvider,
    MarketCapValidationStatus,
    UnifiedMarketCap
)
from core.shared.utils.environment import Environment


@pytest.fixture
def dev_environment():
    """Development environment configuration"""
    return Environment()


@pytest.fixture
def test_symbols():
    """Symbols known to have both fundamental and price data"""
    return ["AAPL", "GOOGL", "MSFT"]  # Major stocks likely to have multi-vendor data


@pytest.fixture
def test_dates():
    """Recent test dates for integration testing"""
    return [
        date(2024, 9, 30),  # End of Q3 2024
        date(2024, 6, 30),  # End of Q2 2024
        date(2024, 3, 31)   # End of Q1 2024
    ]


@pytest.mark.integration
class TestUnifiedMarketCapProviderIntegration:
    """Integration tests for UnifiedMarketCapProvider with real database"""

    @pytest.mark.asyncio

    async def test_provider_initialization_and_connection(self, dev_environment):
        """Test provider initialization and database connection"""
        provider = UnifiedMarketCapProvider(dev_environment)

        assert provider.env == dev_environment
        assert provider.fundamental_provider is not None
        assert provider.price_validator is not None

        try:
            await provider.connect()
            assert provider.conn is not None

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_list_symbols_with_market_cap_data(self, dev_environment):
        """Test listing symbols that have market cap data"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            # Test without date filters
            all_symbols = await provider.list_symbols_with_market_cap_data()
            assert isinstance(all_symbols, list)
            assert len(all_symbols) > 0
            assert all(isinstance(symbol, str) for symbol in all_symbols)

            # Test with date filters
            start_date = date(2024, 1, 1)
            end_date = date(2024, 12, 31)
            filtered_symbols = await provider.list_symbols_with_market_cap_data(start_date, end_date)
            assert isinstance(filtered_symbols, list)

            # Filtered list should be subset of all symbols
            assert set(filtered_symbols).issubset(set(all_symbols))

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_get_fundamental_market_cap_sources_real_data(self, dev_environment, test_symbols):
        """Test getting market cap from real fundamental data"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            for symbol in test_symbols:
                # Use a recent quarter end date likely to have data
                test_date = date(2024, 9, 30)

                sources = await provider._get_fundamental_market_cap_sources(symbol, test_date)

                # May or may not have fundamental market cap data, but should not error
                assert isinstance(sources, list)

                for source in sources:
                    assert source.symbol == symbol
                    assert source.date == test_date
                    assert source.source_type == "fundamental"
                    assert source.vendor in ["fmp", "polygon", "tiingo"]
                    assert source.market_cap > 0
                    assert 0.0 <= source.confidence <= 1.0

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_get_price_based_market_cap_real_data(self, dev_environment, test_symbols):
        """Test calculating market cap from real price data"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            for symbol in test_symbols:
                # Use recent date likely to have price data
                test_date = date(2024, 9, 30)

                source = await provider._get_price_based_market_cap(symbol, test_date)

                if source:  # May not have complete data for all symbols
                    assert source.symbol == symbol
                    assert source.date == test_date
                    assert source.source_type == "price_calculated"
                    assert source.vendor == "calculated"
                    assert source.calculation_method == "price_shares"
                    assert source.market_cap > 0
                    assert source.price_used is not None
                    assert source.price_used > 0
                    assert source.shares_outstanding is not None
                    assert source.shares_outstanding > 0
                    assert 0.0 <= source.confidence <= 1.0

                    # Validate calculation: market_cap = price * shares
                    expected_market_cap = int(source.price_used * source.shares_outstanding)
                    assert abs(source.market_cap - expected_market_cap) < 1000  # Allow for rounding

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_get_shares_outstanding_real_data(self, dev_environment, test_symbols):
        """Test getting shares outstanding from real data sources"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            for symbol in test_symbols:
                test_date = date(2024, 9, 30)

                shares = await provider._get_shares_outstanding(symbol, test_date)

                if shares:  # May not have data for all symbols
                    assert isinstance(shares, int)
                    assert shares > 0
                    # Reasonable range for major companies (millions to billions of shares)
                    assert 1_000_000 <= shares <= 100_000_000_000

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_get_historical_market_cap_estimate_real_data(self, dev_environment):
        """Test getting historical market cap estimates from real database"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            # Use AAPL which is likely to have historical data
            symbol = "AAPL"
            test_date = date(2024, 10, 15)  # Date that might need historical lookup

            source = await provider._get_historical_market_cap_estimate(symbol, test_date)

            if source:  # May not find historical data
                assert source.symbol == symbol
                assert source.date == test_date
                assert source.source_type == "historical"
                assert source.vendor == "database"
                assert source.calculation_method == "historical_estimate"
                assert source.market_cap > 0
                assert 0.0 <= source.confidence <= 1.0
                assert source.raw_data is not None
                assert "original_date" in source.raw_data
                assert "days_difference" in source.raw_data

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_get_unified_market_cap_real_data(self, dev_environment, test_symbols, test_dates):
        """Test full unified market cap calculation with real data"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            for symbol in test_symbols:
                for test_date in test_dates:
                    result = await provider.get_unified_market_cap(symbol, test_date)

                    if result:  # May not have data for all symbol/date combinations
                        assert isinstance(result, UnifiedMarketCap)
                        assert result.symbol == symbol
                        assert result.date == test_date
                        assert result.market_cap > 0
                        assert 0.0 <= result.confidence_score <= 1.0
                        assert isinstance(result.status, MarketCapValidationStatus)
                        assert result.primary_source is not None
                        assert isinstance(result.source_data, list)
                        assert len(result.source_data) > 0
                        assert isinstance(result.validation_metadata, dict)
                        assert isinstance(result.calculation_notes, str)

                        # Validate market cap is reasonable for major companies
                        # (between $1B and $10T)
                        assert 1_000_000_000 <= result.market_cap <= 10_000_000_000_000

                        # Validate all source data
                        for source in result.source_data:
                            assert source.symbol == symbol
                            assert source.date == test_date
                            assert source.market_cap > 0
                            assert 0.0 <= source.confidence <= 1.0

                        # Log successful result for manual verification
                        print(f"✅ {symbol} on {test_date}: ${result.market_cap:,} "
                              f"(confidence: {result.confidence_score:.2f}, "
                              f"status: {result.status.value}, "
                              f"sources: {len(result.source_data)})")

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_market_cap_data_consistency(self, dev_environment):
        """Test consistency of market cap data across multiple calls"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            symbol = "AAPL"
            test_date = date(2024, 9, 30)

            # Get market cap multiple times
            results = []
            for _ in range(3):
                result = await provider.get_unified_market_cap(symbol, test_date)
                if result:
                    results.append(result)

            if len(results) > 1:
                # Results should be consistent across calls
                first_result = results[0]
                for result in results[1:]:
                    assert result.symbol == first_result.symbol
                    assert result.date == first_result.date
                    assert result.market_cap == first_result.market_cap
                    assert result.confidence_score == first_result.confidence_score
                    assert result.status == first_result.status

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_market_cap_history_real_data(self, dev_environment):
        """Test getting market cap history with real data"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            symbol = "AAPL"
            start_date = date(2024, 9, 28)  # Friday
            end_date = date(2024, 9, 30)    # Monday (3 days)

            history = await provider.get_market_cap_history(symbol, start_date, end_date)

            assert isinstance(history, list)

            if history:  # May not have data for all dates
                for market_cap in history:
                    assert isinstance(market_cap, UnifiedMarketCap)
                    assert market_cap.symbol == symbol
                    assert start_date <= market_cap.date <= end_date
                    assert market_cap.market_cap > 0
                    assert 0.0 <= market_cap.confidence_score <= 1.0

                # History should be in chronological order
                dates = [mc.date for mc in history]
                assert dates == sorted(dates)

                print(f"✅ Market cap history for {symbol}: {len(history)} data points")
                for mc in history:
                    print(f"   {mc.date}: ${mc.market_cap:,} ({mc.status.value})")

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_cross_source_validation_real_data(self, dev_environment):
        """Test cross-source validation with real market cap data"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            # Find a symbol/date combination with multiple data sources
            symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "META"]
            test_date = date(2024, 9, 30)

            multi_source_results = []

            for symbol in symbols:
                result = await provider.get_unified_market_cap(symbol, test_date)
                if result and len(result.source_data) > 1:
                    multi_source_results.append(result)
                    break  # Found one, that's enough for the test

            if multi_source_results:
                result = multi_source_results[0]

                # Should have validation metadata for multi-source data
                assert "total_sources" in result.validation_metadata
                assert "consensus_sources" in result.validation_metadata
                assert "source_breakdown" in result.validation_metadata

                # Status should reflect multi-source validation
                assert result.status in [
                    MarketCapValidationStatus.CONSENSUS,
                    MarketCapValidationStatus.MAJORITY_CONSENSUS,
                    MarketCapValidationStatus.VENDOR_DISAGREEMENT,
                    MarketCapValidationStatus.OUTLIER_DETECTED
                ]

                # Confidence should be higher for consensus
                if result.status == MarketCapValidationStatus.CONSENSUS:
                    assert result.confidence_score > 0.7

                print(f"✅ Multi-source validation for {result.symbol}:")
                print(f"   Status: {result.status.value}")
                print(f"   Confidence: {result.confidence_score:.2f}")
                print(f"   Sources: {len(result.source_data)}")
                print(f"   Market cap: ${result.market_cap:,}")

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_error_handling_real_database(self, dev_environment):
        """Test error handling with real database connection"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            # Test with invalid symbol
            result = await provider.get_unified_market_cap("INVALID_SYMBOL_XYZ", date(2024, 1, 1))
            assert result is None

            # Test with very old date (unlikely to have data)
            result = await provider.get_unified_market_cap("AAPL", date(1900, 1, 1))
            assert result is None

            # Test with future date
            future_date = date.today() + timedelta(days=365)
            result = await provider.get_unified_market_cap("AAPL", future_date)
            assert result is None

        finally:
            await provider.disconnect()

    @pytest.mark.asyncio

    async def test_database_schema_compatibility(self, dev_environment):
        """Test compatibility with actual database schema"""
        provider = UnifiedMarketCapProvider(dev_environment)

        try:
            await provider.connect()

            # Test that all database queries work without schema errors
            # This indirectly validates our schema assumptions

            # Test fundamental data access
            fundamental_sources = await provider._get_fundamental_market_cap_sources("AAPL", date(2024, 9, 30))
            # Should not raise exception even if no data found

            # Test shares outstanding lookup
            shares = await provider._get_shares_outstanding("AAPL", date(2024, 9, 30))
            # Should not raise exception even if no data found

            # Test historical market cap lookup
            historical = await provider._get_historical_market_cap_estimate("AAPL", date(2024, 10, 15))
            # Should not raise exception even if no data found

            # Test symbols listing
            symbols = await provider.list_symbols_with_market_cap_data()
            assert isinstance(symbols, list)

            print("✅ All database schema compatibility tests passed")

        finally:
            await provider.disconnect()