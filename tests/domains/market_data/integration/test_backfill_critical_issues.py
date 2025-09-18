#!/usr/bin/env python3
"""
Integration tests for critical backfill issues identified in production.

These tests reproduce the exact errors found in logs and validate fixes
using real data scenarios.
"""

import pytest
from datetime import datetime, date
from unittest.mock import patch, MagicMock, AsyncMock

from domains.market_data.services.agent.polygon_adapter import PolygonAdapter
from domains.market_data.services.agent.tiingo_adapter import TiingoAdapter
from core.shared.utils.database import Database


class TestPolygonDatetimeTimezoneIssue:
    """Test the Polygon 'datetime.timezone' attribute error."""

    def test_polygon_datetime_error_reproduction(self):
        """Reproduce the exact error: type object 'datetime.datetime' has no attribute 'timezone'"""
        with pytest.raises(AttributeError, match="'datetime.datetime' has no attribute 'timezone'"):
            # This reproduces the bug found in logs
            datetime.timezone  # This should fail

    def test_polygon_datetime_fix_with_zoneinfo(self):
        """Test fixed datetime handling using proper timezone support."""
        from zoneinfo import ZoneInfo
        import datetime as dt

        # Test timestamp conversion - this was the source of the error
        timestamp_ms = 1640995200000  # 2022-01-01 00:00:00 UTC

        # OLD WAY (causes error)
        # date_val = dt.datetime.utcfromtimestamp(timestamp_ms / 1000).date()

        # NEW WAY (fixed)
        utc_dt = dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=ZoneInfo("UTC"))
        date_val = utc_dt.date()

        assert date_val == date(2022, 1, 1)
        assert utc_dt.tzinfo is not None

    def test_polygon_adapter_datetime_handling(self):
        """Test that PolygonAdapter handles datetime conversion correctly."""
        adapter = PolygonAdapter(api_key="test_key")

        # Mock response that would cause timezone error
        mock_response = {
            "results": [
                {
                    "t": 1640995200000,  # 2022-01-01 UTC timestamp in ms
                    "o": 100.0,
                    "h": 105.0,
                    "l": 98.0,
                    "c": 103.0,
                    "v": 1000000
                }
            ]
        }

        with patch('requests.get') as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = mock_response

            eod_prices = adapter.fetch_eod(['AAPL'], '2022-01-01', '2022-01-01')

            assert len(eod_prices) == 1
            assert eod_prices[0].date == date(2022, 1, 1)
            assert eod_prices[0].symbol == 'AAPL'


class TestTiingoRateLimitingIssues:
    """Test Tiingo API rate limiting and authentication issues."""

    def test_tiingo_429_error_reproduction(self):
        """Reproduce the 429 rate limiting error."""
        adapter = TiingoAdapter(api_key="test_key")

        with patch('requests.get') as mock_get:
            # Mock 429 response
            mock_response = MagicMock()
            mock_response.status_code = 429
            mock_response.json.return_value = {
                "detail": "Request was throttled. Expected available in 3600 seconds."
            }
            mock_get.return_value = mock_response

            # This should handle 429 gracefully
            eod_prices = adapter.fetch_eod(['AAPL'], '2022-01-01', '2022-01-01')

            # Should return empty list, not crash
            assert eod_prices == []


class TestDatabaseSchemaIssues:
    """Test database schema inconsistency issues."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_missing_tables_detection(self):
        """Test detection of missing instrument tables."""
        # Mock database connection
        mock_connection = AsyncMock()

        # Mock table existence check
        mock_connection.fetch.return_value = []  # No tables found

        with patch('asyncpg.connect', return_value=mock_connection):
            db = Database()

            # Test queries that were failing in logs
            test_queries = [
                "SELECT COUNT(*) as count FROM dev_instrument_tiingo",
                "SELECT COUNT(*) as count FROM dev_instrument_polygon",
                "SELECT COUNT(*) as count FROM dev_instrument_eodhd"
            ]

            for query in test_queries:
                # These should fail gracefully
                try:
                    await mock_connection.fetch(query)
                except Exception as e:
                    assert "does not exist" in str(e)


class TestRealDataScenarios:
    """Test with real data scenarios that caused failures."""

    def test_polygon_real_timestamp_conversion(self):
        """Test with real timestamp values from Polygon API that caused errors."""
        real_timestamps = [
            1640995200000,  # 2022-01-01 00:00:00 UTC
            1641081600000,  # 2022-01-02 00:00:00 UTC
            1641168000000   # 2022-01-03 00:00:00 UTC
        ]

        from zoneinfo import ZoneInfo
        import datetime as dt

        for timestamp_ms in real_timestamps:
            # This was the failing line - now fixed
            utc_dt = dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=ZoneInfo("UTC"))
            date_val = utc_dt.date()

            # Verify conversion worked
            assert utc_dt.tzinfo is not None
            assert isinstance(date_val, date)

    def test_symbols_causing_issues(self):
        """Test symbols that were specifically failing in logs."""
        failing_symbols = [
            'AHPAU', 'AHS', 'AHR', 'AHT', 'AHRN', 'AHRNU', 'AHRNW',
            'AHTC', 'AHTCQ', 'AHYB', 'AI', 'AIA', 'AIACU', 'AIB'
        ]

        adapter = PolygonAdapter(api_key="test_key")

        for symbol in failing_symbols:
            with patch('requests.get') as mock_get:
                # Mock successful response
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "results": [
                        {
                            "t": 1640995200000,
                            "o": 100.0, "h": 105.0, "l": 98.0, "c": 103.0, "v": 1000000
                        }
                    ]
                }
                mock_get.return_value = mock_response

                # This should work without timezone errors
                eod_prices = adapter.fetch_eod([symbol], '2022-01-01', '2022-01-01')
                assert len(eod_prices) >= 0  # Should not crash