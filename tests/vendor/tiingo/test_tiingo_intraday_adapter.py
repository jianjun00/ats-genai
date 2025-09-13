"""
Comprehensive tests for Tiingo Intraday Adapter.

Tests both mock scenarios and integration with real API when keys are available.
"""

import pytest
import aiohttp
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from domains.market_data.services.agent.tiingo_intraday_adapter import (
    TiingoIntradayAdapter,
    TiingoMinuteBar,
    fetch_tiingo_minute_data,
    backfill_tiingo_minute_data
)


class TestTiingoMinuteBar:
    """Test TiingoMinuteBar data structure."""

    def test_minute_bar_creation(self):
        """Test basic minute bar creation."""
        timestamp = datetime(2024, 1, 1, 9, 30)
        bar = TiingoMinuteBar(
            symbol='AAPL',
            timestamp=timestamp,
            open=180.00,
            high=181.00,
            low=179.50,
            close=180.50,
            volume=1000000
        )

        assert bar.symbol == 'AAPL'
        assert bar.timestamp == timestamp
        assert bar.open == 180.00
        assert bar.high == 181.00
        assert bar.low == 179.50
        assert bar.close == 180.50
        assert bar.volume == 1000000
        assert bar.vendor == 'tiingo'

    def test_minute_bar_defaults(self):
        """Test default values."""
        bar = TiingoMinuteBar(
            symbol='MSFT',
            timestamp=datetime.now(),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=500000
        )

        assert bar.vendor == 'tiingo'


class TestTiingoIntradayAdapter:
    """Test TiingoIntradayAdapter functionality."""

    def test_adapter_initialization(self):
        """Test adapter initialization."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        assert adapter.api_key == 'test_key'
        assert adapter.vendor_name == 'tiingo'
        assert 'iex_intraday' in adapter.base_urls
        assert adapter.session is None

    def test_adapter_initialization_no_key(self):
        """Test adapter initialization without API key."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="TIINGO_API_KEY"):
                TiingoIntradayAdapter()

    def test_adapter_initialization_from_env(self):
        """Test adapter initialization from environment."""
        with patch.dict('os.environ', {'TIINGO_API_KEY': 'env_key'}):
            adapter = TiingoIntradayAdapter()
            assert adapter.api_key == 'env_key'

    def test_get_intraday_url(self):
        """Test URL construction."""
        adapter = TiingoIntradayAdapter(api_key='test_key')
        url = adapter.get_intraday_url('AAPL', '2024-01-01', '2024-01-01')

        expected = (
            "https://api.tiingo.com/iex/AAPL/prices"
            "?startDate=2024-01-01&endDate=2024-01-01"
            "&resampleFreq=1min&token=test_key"
        )
        assert url == expected

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        async with adapter:
            assert adapter.session is not None
            assert isinstance(adapter.session, aiohttp.ClientSession)

        # Session should be closed after context
        assert adapter.session.closed

    def test_parse_intraday_data_empty(self):
        """Test parsing empty data."""
        adapter = TiingoIntradayAdapter(api_key='test_key')
        result = adapter._parse_intraday_data('AAPL', [])

        assert result == []

    def test_parse_intraday_data_valid(self):
        """Test parsing valid intraday data."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_data = [
            {
                'date': '2024-01-01T09:30:00Z',
                'open': 180.00,
                'high': 181.00,
                'low': 179.50,
                'close': 180.50,
                'volume': 1000000
            },
            {
                'date': '2024-01-01T09:31:00Z',
                'open': 180.50,
                'high': 181.50,
                'low': 180.00,
                'close': 181.00,
                'volume': 800000
            }
        ]

        result = adapter._parse_intraday_data('AAPL', mock_data)

        assert len(result) == 2
        assert all(isinstance(bar, TiingoMinuteBar) for bar in result)
        assert result[0].symbol == 'AAPL'
        assert result[0].open == 180.00
        assert result[0].volume == 1000000
        assert result[1].close == 181.00

    def test_parse_intraday_data_invalid_entries(self):
        """Test parsing data with invalid entries."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_data = [
            {
                'date': '2024-01-01T09:30:00Z',
                'open': 180.00,
                'high': 181.00,
                'low': 179.50,
                'close': 180.50,
                'volume': 1000000
            },
            {
                'date': '2024-01-01T09:31:00Z',
                'open': 'invalid',  # Invalid data
                'high': 181.50,
                'low': 180.00,
                'close': 181.00,
                'volume': 800000
            },
            {
                # Missing required fields
                'date': '2024-01-01T09:32:00Z',
                'open': 181.00
            }
        ]

        with patch('market_data.agent.tiingo_intraday_adapter.logger') as mock_logger:
            result = adapter._parse_intraday_data('AAPL', mock_data)

        assert len(result) == 1  # Only valid entry
        assert mock_logger.warning.call_count == 2  # Two invalid entries

    def test_parse_daily_resampled(self):
        """Test parsing daily resampled data."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_data = [
            {
                'date': '2024-01-01T09:30:00Z',
                'open': 180.00,
                'high': 181.00,
                'low': 179.50,
                'close': 180.50,
                'volume': 1000000
            }
        ]

        result = adapter._parse_daily_resampled('AAPL', mock_data)

        assert len(result) == 1
        assert isinstance(result[0], TiingoMinuteBar)
        assert result[0].symbol == 'AAPL'

    def test_validate_data_quality_empty(self):
        """Test data quality validation with empty data."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        result = adapter.validate_data_quality([])

        assert result['valid'] is False
        assert result['reason'] == 'No data'

    def test_validate_data_quality_valid_data(self):
        """Test data quality validation with valid data."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        bars = []
        base_time = datetime(2024, 1, 1, 9, 30)

        # Create 10 consecutive minute bars
        for i in range(10):
            bars.append(TiingoMinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=180.00 + i * 0.1,
                high=181.00 + i * 0.1,
                low=179.50 + i * 0.1,
                close=180.50 + i * 0.1,
                volume=1000000 - i * 10000
            ))

        result = adapter.validate_data_quality(bars)

        assert result['valid'] is True
        assert result['total_bars'] == 10
        assert result['time_gaps'] == 0
        assert result['price_outliers'] == 0
        assert result['vendor'] == 'tiingo'
        assert result['data_completeness'] == 1.0

    def test_validate_data_quality_with_gaps(self):
        """Test data quality validation with time gaps."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        bars = [
            TiingoMinuteBar(
                symbol='AAPL',
                timestamp=datetime(2024, 1, 1, 9, 30),
                open=180.00, high=181.00, low=179.50, close=180.50, volume=1000000
            ),
            TiingoMinuteBar(
                symbol='AAPL',
                timestamp=datetime(2024, 1, 1, 9, 35),  # 5-minute gap
                open=180.50, high=181.50, low=180.00, close=181.00, volume=800000
            )
        ]

        result = adapter.validate_data_quality(bars)

        assert result['time_gaps'] == 1
        assert len(result['gap_details']) == 1

    def test_validate_data_quality_with_outliers(self):
        """Test data quality validation with price outliers."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        bars = [
            TiingoMinuteBar(
                symbol='AAPL',
                timestamp=datetime(2024, 1, 1, 9, 30),
                open=180.00, high=181.00, low=179.50, close=180.50, volume=1000000
            ),
            TiingoMinuteBar(
                symbol='AAPL',
                timestamp=datetime(2024, 1, 1, 9, 31),
                open=180.50, high=181.50, low=180.00, close=200.00, volume=800000  # 10%+ jump
            )
        ]

        result = adapter.validate_data_quality(bars)

        assert result['price_outliers'] >= 1

    def test_fetch_instruments(self):
        """Test fetching instrument metadata."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_response_data = [
            {
                'ticker': 'AAPL',
                'name': 'Apple Inc.',
                'exchange': 'NASDAQ',
                'sector': 'Technology'
            },
            {
                'ticker': 'MSFT',
                'name': 'Microsoft Corporation',
                'exchange': 'NASDAQ',
                'sector': 'Technology'
            }
        ]

        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            instruments = adapter.fetch_instruments()

        assert len(instruments) == 2
        assert instruments[0].symbol == 'AAPL'
        assert instruments[0].name == 'Apple Inc.'
        assert instruments[0].vendor == 'tiingo'
        assert instruments[1].symbol == 'MSFT'

    def test_fetch_eod_not_implemented(self):
        """Test that EOD fetch raises NotImplementedError."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        with pytest.raises(NotImplementedError):
            adapter.fetch_eod(['AAPL'], datetime.now(), datetime.now())

    def test_fetch_ticks_not_implemented(self):
        """Test that tick fetch raises NotImplementedError."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        with pytest.raises(NotImplementedError):
            adapter.fetch_ticks('AAPL', datetime.now(), datetime.now())

    def test_fetch_interval_unsupported(self):
        """Test that unsupported intervals raise ValueError."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        with pytest.raises(ValueError, match="Only 1-minute intervals supported"):
            adapter.fetch_interval('AAPL', '5min', datetime.now(), datetime.now())


class TestTiingoAsyncMethods:
    """Test async methods with mocked HTTP responses."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_single_day_success(self):
        """Test successful single day fetch."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_response_data = [
            {
                'date': '2024-01-01T09:30:00Z',
                'open': 180.00,
                'high': 181.00,
                'low': 179.50,
                'close': 180.50,
                'volume': 1000000
            }
        ]

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        # Properly mock the context manager
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_response
        mock_context_manager.__aexit__.return_value = None

        mock_session = AsyncMock()
        mock_session.get.return_value = mock_context_manager

        adapter.session = mock_session

        result = await adapter._fetch_single_day('AAPL', '2024-01-01')

        assert len(result) == 1
        assert result[0].symbol == 'AAPL'
        assert result[0].open == 180.00

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_single_day_rate_limit(self):
        """Test rate limit handling."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        # First response: rate limited
        mock_response_429 = AsyncMock()
        mock_response_429.status = 429

        # Second response: success
        mock_response_200 = AsyncMock()
        mock_response_200.status = 200
        mock_response_200.json = AsyncMock(return_value=[])

        # Create proper context managers
        mock_cm_429 = AsyncMock()
        mock_cm_429.__aenter__.return_value = mock_response_429
        mock_cm_429.__aexit__.return_value = None

        mock_cm_200 = AsyncMock()
        mock_cm_200.__aenter__.return_value = mock_response_200
        mock_cm_200.__aexit__.return_value = None

        mock_session = AsyncMock()
        mock_session.get.side_effect = [mock_cm_429, mock_cm_200]

        adapter.session = mock_session

        with patch('asyncio.sleep') as mock_sleep:
            result = await adapter._fetch_single_day('AAPL', '2024-01-01')

        assert len(result) == 0
        mock_sleep.assert_called_once_with(60)  # Rate limit delay

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_single_day_not_found(self):
        """Test handling of 404 responses."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_response = AsyncMock()
        mock_response.status = 404

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        adapter.session = mock_session

        result = await adapter._fetch_single_day('AAPL', '2024-01-01')

        assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_single_day_error(self):
        """Test error handling."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        mock_response = AsyncMock()
        mock_response.status = 500

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        adapter.session = mock_session

        result = await adapter._fetch_single_day('AAPL', '2024-01-01')

        assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_minute_bars_async_no_session(self):
        """Test fetch without session raises error."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        with pytest.raises(RuntimeError, match="Must use async context manager"):
            await adapter.fetch_minute_bars_async(
                'AAPL',
                datetime(2024, 1, 1),
                datetime(2024, 1, 1)
            )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_multiple_symbols_async(self):
        """Test fetching multiple symbols concurrently."""
        adapter = TiingoIntradayAdapter(api_key='test_key')

        # Mock successful responses for all symbols
        with patch.object(adapter, 'fetch_minute_bars_async') as mock_fetch:
            mock_fetch.side_effect = [
                [TiingoMinuteBar('AAPL', datetime.now(), 180, 181, 179, 180.5, 1000)],
                [TiingoMinuteBar('MSFT', datetime.now(), 100, 101, 99, 100.5, 800)],
                []  # Empty result for third symbol
            ]

            adapter.session = AsyncMock()  # Mock session

            result = await adapter.fetch_multiple_symbols_async(
                ['AAPL', 'MSFT', 'GOOGL'],
                datetime(2024, 1, 1),
                datetime(2024, 1, 1),
                max_concurrent=2
            )

        assert len(result) == 3
        assert 'AAPL' in result
        assert 'MSFT' in result
        assert 'GOOGL' in result
        assert len(result['AAPL']) == 1
        assert len(result['MSFT']) == 1
        assert len(result['GOOGL']) == 0


class TestConvenienceFunctions:
    """Test convenience functions."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_tiingo_minute_data(self):
        """Test convenience function for fetching minute data."""
        symbols = ['AAPL', 'MSFT']
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        with patch.object(TiingoIntradayAdapter, 'fetch_multiple_symbols_async') as mock_fetch:
            mock_fetch.return_value = {
                'AAPL': [TiingoMinuteBar('AAPL', start_date, 180, 181, 179, 180.5, 1000)],
                'MSFT': [TiingoMinuteBar('MSFT', start_date, 100, 101, 99, 100.5, 800)]
            }

            result = await fetch_tiingo_minute_data(symbols, start_date, end_date, 'test_key')

        assert len(result) == 2
        assert 'AAPL' in result
        assert 'MSFT' in result

    def test_backfill_tiingo_minute_data(self):
        """Test backfill convenience function."""
        with patch.object(TiingoIntradayAdapter, 'fetch_minute_bars_sync') as mock_fetch:
            mock_fetch.return_value = [
                TiingoMinuteBar('AAPL', datetime.now(), 180, 181, 179, 180.5, 1000)
            ]

            result = backfill_tiingo_minute_data('AAPL', days_back=30, api_key='test_key')

        assert len(result) == 1
        assert result[0].symbol == 'AAPL'


@pytest.mark.integration
class TestTiingoIntegration:
    """Integration tests with real API (requires TIINGO_API_KEY)."""

    @pytest.mark.skipif(
        not pytest.importorskip("os").getenv("TIINGO_API_KEY"),
        reason="TIINGO_API_KEY not available"
    )
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_real_api_fetch(self):
        """Test with real Tiingo API."""
        import os
        api_key = os.getenv("TIINGO_API_KEY")

        async with TiingoIntradayAdapter(api_key) as adapter:
            # Fetch recent data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)

            bars = await adapter.fetch_minute_bars_async('AAPL', start_date, end_date)

            # Basic validation
            if bars:  # Data might not be available for weekends/holidays
                assert all(bar.symbol == 'AAPL' for bar in bars)
                assert all(bar.vendor == 'tiingo' for bar in bars)
                assert all(bar.volume >= 0 for bar in bars)

                # Validate data quality
                quality_metrics = adapter.validate_data_quality(bars)
                assert 'total_bars' in quality_metrics
                assert quality_metrics['vendor'] == 'tiingo'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])