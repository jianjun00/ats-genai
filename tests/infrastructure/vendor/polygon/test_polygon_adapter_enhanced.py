import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime, date
from domains.market_data.services.vendor_adapters.market_cap.unified_market_cap_provider import PolygonAdapter


class TestPolygonAdapterEnhanced:
    """Comprehensive test coverage for PolygonAdapter."""

    def test_init_with_api_key(self):
        """Test adapter initialization with explicit API key."""
        api_key = "test_polygon_key"
        adapter = PolygonAdapter(api_key=api_key)
        assert adapter.api_key == api_key
        assert adapter.vendor_name == "polygon"

    def test_init_from_env_var(self):
        """Test adapter initialization from environment variable."""
        with patch.dict('os.environ', {'POLYGON_API_KEY': 'env_polygon_key'}):
            adapter = PolygonAdapter()
            assert adapter.api_key == "env_polygon_key"

    def test_init_no_api_key_raises_exception(self):
        """Test that missing API key raises exception."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(Exception, match="Please set your POLYGON_API_KEY environment variable"):
                PolygonAdapter()

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_instruments_success(self, mock_get):
        """Test successful instrument metadata retrieval."""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    "primary_exchange": "NASDAQ",
                    "sic_description": "Electronic Computers",
                    "list_date": "1980-12-12",
                    "delisted_utc": None,
                    "cik": "0000320193"
                },
                {
                    "ticker": "GOOGL",
                    "name": "Alphabet Inc.",
                    "primary_exchange": "NASDAQ",
                    "sic_description": "Computer Programming Services",
                    "list_date": "2004-08-19",
                    "delisted_utc": None,
                    "cik": "0001652044"
                }
            ]
        }
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()

        assert len(instruments) == 2

        # Check first instrument
        inst1 = instruments[0]
        assert inst1.instrument_id == "0000320193"  # Uses CIK as instrument_id
        assert inst1.symbol == "AAPL"
        assert inst1.name == "Apple Inc."
        assert inst1.exchange == "NASDAQ"
        assert inst1.sector == "Electronic Computers"
        from datetime import date
        assert inst1.list_date == date(1980, 12, 12)  # InstrumentMetadata converts string to date object
        assert inst1.delist_date is None
        assert inst1.vendor == "polygon"
        assert inst1.extra["ticker"] == "AAPL"

        # Check second instrument
        inst2 = instruments[1]
        assert inst2.symbol == "GOOGL"
        assert inst2.name == "Alphabet Inc."

        # Verify API call
        expected_url = "https://api.polygon.io/v3/reference/tickers?active=true&apiKey=test_key"
        mock_get.assert_called_once_with(expected_url)

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_instruments_fallback_instrument_id(self, mock_get):
        """Test fetch_instruments uses ticker as fallback when CIK is missing."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    # No CIK field
                }
            ]
        }
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()

        assert len(instruments) == 1
        assert instruments[0].instrument_id == "AAPL"  # Falls back to ticker
        assert instruments[0].symbol == "AAPL"

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_instruments_empty_results(self, mock_get):
        """Test fetch_instruments with empty results."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()

        assert instruments == []

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_instruments_missing_results_key(self, mock_get):
        """Test fetch_instruments when results key is missing."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {}  # No results key
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()

        assert instruments == []

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_instruments_http_error(self, mock_get):
        """Test fetch_instruments handles HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("API Error")
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")

        with pytest.raises(requests.HTTPError):
            adapter.fetch_instruments()

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_success(self, mock_get):
        """Test successful EOD data retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1702598400000,  # 2023-12-15 00:00:00 UTC in ms
                    "o": 195.18,
                    "h": 197.08,
                    "l": 194.83,
                    "c": 197.57,
                    "v": 55012853
                },
                {
                    "t": 1702684800000,  # 2023-12-16 00:00:00 UTC in ms
                    "o": 196.90,
                    "h": 196.94,
                    "l": 193.85,
                    "c": 194.83,
                    "v": 66561513
                }
            ]
        }
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")

        assert len(prices) == 2

        # Check first price
        price1 = prices[0]
        assert price1.instrument_id == "AAPL"
        assert price1.date == date(2023, 12, 15)
        assert price1.open == 195.18
        assert price1.high == 197.08
        assert price1.low == 194.83
        assert price1.close == 197.57
        assert price1.adj_close is None  # Polygon doesn't provide adjusted close directly
        assert price1.volume == 55012853
        assert price1.vendor == "polygon"
        assert price1.provenance["polygon_row"]["o"] == 195.18

        # Check second price
        price2 = prices[1]
        assert price2.date == date(2023, 12, 16)
        assert price2.close == 194.83

        # Verify API call format
        expected_url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-12-15/2023-12-16?adjusted=true&sort=asc&limit=50000&apiKey=test_key"
        mock_get.assert_called_once_with(expected_url)

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_multiple_symbols(self, mock_get):
        """Test EOD data retrieval for multiple symbols."""
        def mock_response_side_effect(url):
            response = MagicMock()
            response.status_code = 200
            if "AAPL" in url:
                response.json.return_value = {
                    "results": [
                        {
                            "t": 1702598400000,  # 2023-12-15
                            "o": 195.18,
                            "h": 197.08,
                            "l": 194.83,
                            "c": 197.57,
                            "v": 55012853
                        }
                    ]
                }
            elif "GOOGL" in url:
                response.json.return_value = {
                    "results": [
                        {
                            "t": 1702598400000,  # 2023-12-15
                            "o": 140.05,
                            "h": 141.35,
                            "l": 139.64,
                            "c": 140.93,
                            "v": 25891013
                        }
                    ]
                }
            return response

        mock_get.side_effect = mock_response_side_effect

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL", "GOOGL"], "2023-12-15", "2023-12-15")

        assert len(prices) == 2
        symbols = [p.instrument_id for p in prices]
        assert "AAPL" in symbols
        assert "GOOGL" in symbols

        # Verify both API calls were made
        assert mock_get.call_count == 2

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_http_error(self, mock_get):
        """Test fetch_eod handles HTTP errors gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 500  # Server error
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")

        # Should continue processing despite error (doesn't raise exception)
        assert prices == []

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_empty_results(self, mock_get):
        """Test fetch_eod with empty results."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")

        assert prices == []

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_missing_results_key(self, mock_get):
        """Test fetch_eod when results key is missing."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}  # No results key
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")

        assert prices == []

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_timezone_handling(self, mock_get):
        """Test fetch_eod correctly handles timezone conversion."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1702598400000,  # 2023-12-15 00:00:00 UTC in ms
                    "o": 195.18,
                    "h": 197.08,
                    "l": 194.83,
                    "c": 197.57,
                    "v": 55012853
                }
            ]
        }
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-15")

        assert len(prices) == 1
        # Verify timezone-aware datetime was properly converted to date
        assert prices[0].date == date(2023, 12, 15)

    @patch('market_data.agent.polygon_adapter.requests.get')
    @patch('os.makedirs')
    @patch('builtins.open', create=True)
    def test_fetch_eod_logging_mechanism(self, mock_open, mock_makedirs, mock_get):
        """Test EOD data logging for specific tickers and date ranges."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1702598400000,
                    "o": 195.18,
                    "h": 197.08,
                    "l": 194.83,
                    "c": 197.57,
                    "v": 55012853
                }
            ]
        }
        mock_get.return_value = mock_response

        # Mock file writing context manager
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        adapter = PolygonAdapter(api_key="test_key")
        # AAPL should trigger logging (it's in log_tickers)
        prices = adapter.fetch_eod(["AAPL"], "2023-06-15", "2023-06-16")

        # Verify logging directory was created
        mock_makedirs.assert_called_with("tests/data", exist_ok=True)

        # Verify files were opened for writing (request and response logs)
        assert mock_open.call_count >= 2

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_missing_optional_fields(self, mock_get):
        """Test fetch_eod handles missing optional OHLCV fields."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1702598400000,
                    "c": 197.57,
                    # Missing o, h, l, v
                }
            ]
        }
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-15")

        assert len(prices) == 1
        price = prices[0]
        assert price.close == 197.57
        assert price.open is None
        assert price.high is None
        assert price.low is None
        assert price.volume is None

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_json_serialization_error_handling(self, mock_get):
        """Test fetch_eod handles JSON serialization errors in logging."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")  # First call fails
        mock_get.return_value = mock_response

        with patch('os.makedirs'):
            with patch('builtins.open', create=True) as mock_open:
                mock_file = MagicMock()
                mock_open.return_value.__enter__.return_value = mock_file

                adapter = PolygonAdapter(api_key="test_key")

                # The adapter will fail on JSON parsing, so we expect a ValueError
                with pytest.raises(ValueError, match="Invalid JSON"):
                    adapter.fetch_eod(["AAPL"], "2023-06-15", "2023-06-16")

                # Verify error was written to response file (logging mechanism)
                mock_file.write.assert_called()

    def test_fetch_ticks_not_implemented(self):
        """Test that fetch_ticks raises NotImplementedError."""
        adapter = PolygonAdapter(api_key="test_key")

        with pytest.raises(NotImplementedError, match="PolygonAdapter.fetch_ticks is not implemented yet"):
            adapter.fetch_ticks("AAPL", datetime.now(), datetime.now())

    def test_fetch_interval_not_implemented(self):
        """Test that fetch_interval raises NotImplementedError."""
        adapter = PolygonAdapter(api_key="test_key")

        with pytest.raises(NotImplementedError, match="PolygonAdapter.fetch_interval is not implemented yet"):
            adapter.fetch_interval("AAPL", "1h", datetime.now(), datetime.now())

    def test_base_url_format(self):
        """Test that BASE_URL format is correct."""
        adapter = PolygonAdapter(api_key="test_key")
        expected_base = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
        assert adapter.BASE_URL == expected_base

    def test_vendor_name(self):
        """Test vendor_name is set correctly."""
        adapter = PolygonAdapter(api_key="test_key")
        assert adapter.vendor_name == "polygon"

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_instruments_partial_data(self, mock_get):
        """Test fetch_instruments handles instruments with missing fields."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc.",
                    # Missing all other fields
                },
                {
                    "ticker": "GOOGL",
                    # Missing name and other fields
                }
            ]
        }
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()

        assert len(instruments) == 2
        assert instruments[0].symbol == "AAPL"
        assert instruments[0].name == "Apple Inc."
        assert instruments[0].exchange is None
        assert instruments[0].sector is None

        assert instruments[1].symbol == "GOOGL"
        assert instruments[1].name is None

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_url_format_validation(self, mock_get):
        """Test that EOD requests use correct URL format."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="secret_polygon_key")
        adapter.fetch_eod(["TSLA"], "2023-01-01", "2023-01-02")

        # Verify URL format
        call_args = mock_get.call_args[0][0]
        assert "api.polygon.io/v2/aggs/ticker/TSLA/range/1/day" in call_args
        assert "2023-01-01/2023-01-02" in call_args
        assert "adjusted=true" in call_args
        assert "sort=asc" in call_args
        assert "limit=50000" in call_args
        assert "apiKey=secret_polygon_key" in call_args

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_in_log_range_function(self, mock_get):
        """Test the in_log_range helper function behavior."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")

        # Test with dates outside log range (should not trigger logging)
        with patch('os.makedirs') as mock_makedirs:
            adapter.fetch_eod(["AAPL"], "2019-01-01", "2019-01-02")
            # Should not create logging directory for dates outside range
            mock_makedirs.assert_not_called()

    @patch('market_data.agent.polygon_adapter.requests.get')
    def test_fetch_eod_handles_large_datasets(self, mock_get):
        """Test fetch_eod with large number of data points."""
        # Create large dataset
        large_results = []
        for i in range(1000):
            large_results.append({
                "t": 1702598400000 + (i * 86400000),  # Each day apart
                "o": 190.0 + i * 0.1,
                "h": 195.0 + i * 0.1,
                "l": 189.0 + i * 0.1,
                "c": 192.0 + i * 0.1,
                "v": 1000000 + i * 1000
            })

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": large_results}
        mock_get.return_value = mock_response

        adapter = PolygonAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2026-11-10")  # Long date range

        assert len(prices) == 1000
        # Verify first and last entries
        assert prices[0].open == 190.0
        assert prices[-1].open == 289.9  # 190.0 + 999 * 0.1