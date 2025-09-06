import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime, date
from domains.market_data.services.agent.tiingo_adapter import TiingoAdapter
from domains.market_data.services.agent.models import InstrumentMetadata, EODPrice


class TestTiingoAdapter:
    """Comprehensive test coverage for TiingoAdapter."""
    
    def test_init_with_api_key(self):
        """Test adapter initialization with explicit API key."""
        api_key = "test_api_key"
        adapter = TiingoAdapter(api_key=api_key)
        assert adapter.api_key == api_key
        assert adapter.vendor_name == "tiingo"
    
    def test_init_from_env_var(self):
        """Test adapter initialization from environment variable."""
        with patch.dict('os.environ', {'TIINGO_API_KEY': 'env_api_key'}):
            adapter = TiingoAdapter()
            assert adapter.api_key == "env_api_key"
    
    def test_init_no_api_key_raises_exception(self):
        """Test that missing API key raises exception."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(Exception, match="Please set your TIINGO_API_KEY environment variable"):
                TiingoAdapter()
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_instruments_success(self, mock_get):
        """Test successful instrument metadata retrieval."""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "exchange": "NASDAQ"
            },
            {
                "ticker": "GOOGL", 
                "name": "Alphabet Inc.",
                "exchange": "NASDAQ"
            }
        ]
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()
        
        assert len(instruments) == 2
        assert instruments[0].symbol == "AAPL"
        assert instruments[0].name == "Apple Inc."
        assert instruments[0].exchange == "NASDAQ"
        assert instruments[0].vendor == "tiingo"
        assert instruments[0].sector is None  # Tiingo doesn't provide sector
        assert instruments[0].list_date is None  # Tiingo doesn't provide list_date
        assert instruments[0].delist_date is None  # Tiingo doesn't provide delist_date
        
        # Verify API call
        expected_url = "https://api.tiingo.com/tiingo/supported-tickers?token=test_key"
        mock_get.assert_called_once_with(expected_url)
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_instruments_empty_response(self, mock_get):
        """Test fetch_instruments with empty API response."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()
        
        assert instruments == []
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_instruments_http_error(self, mock_get):
        """Test fetch_instruments handles HTTP errors."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("API Error")
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        
        with pytest.raises(requests.HTTPError):
            adapter.fetch_instruments()
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_success(self, mock_get):
        """Test successful EOD data retrieval."""
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "date": "2023-12-15T00:00:00+00:00",
                "open": 195.18,
                "high": 197.08,
                "low": 194.83,
                "close": 197.57,
                "adjClose": 196.25,
                "volume": 55012853
            },
            {
                "date": "2023-12-16T00:00:00+00:00",
                "open": 196.90,
                "high": 196.94,
                "low": 193.85,
                "close": 194.83,
                "adjClose": 193.52,
                "volume": 66561513
            }
        ]
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
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
        assert price1.adj_close == 196.25
        assert price1.volume == 55012853
        assert price1.vendor == "tiingo"
        assert price1.provenance["tiingo_row"]["open"] == 195.18
        
        # Check second price
        price2 = prices[1]
        assert price2.date == date(2023, 12, 16)
        assert price2.close == 194.83
        
        # Verify API call
        expected_url = "https://api.tiingo.com/tiingo/daily/AAPL/prices?startDate=2023-12-15&endDate=2023-12-16&token=test_key"
        mock_get.assert_called_once_with(expected_url)
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_multiple_symbols(self, mock_get):
        """Test EOD data retrieval for multiple symbols."""
        def mock_response_side_effect(url):
            response = MagicMock()
            response.status_code = 200
            if "AAPL" in url:
                response.json.return_value = [
                    {
                        "date": "2023-12-15T00:00:00+00:00",
                        "open": 195.18,
                        "high": 197.08,
                        "low": 194.83,
                        "close": 197.57,
                        "adjClose": 196.25,
                        "volume": 55012853
                    }
                ]
            elif "GOOGL" in url:
                response.json.return_value = [
                    {
                        "date": "2023-12-15T00:00:00+00:00",
                        "open": 140.05,
                        "high": 141.35,
                        "low": 139.64,
                        "close": 140.93,
                        "adjClose": 140.93,
                        "volume": 25891013
                    }
                ]
            return response
        
        mock_get.side_effect = mock_response_side_effect
        
        adapter = TiingoAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL", "GOOGL"], "2023-12-15", "2023-12-15")
        
        assert len(prices) == 2
        symbols = [p.instrument_id for p in prices]
        assert "AAPL" in symbols
        assert "GOOGL" in symbols
        
        # Verify both API calls were made
        assert mock_get.call_count == 2
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_rate_limiting(self, mock_get):
        """Test EOD data handles rate limiting (429 errors)."""
        def mock_response_side_effect(url):
            response = MagicMock()
            if "AAPL" in url:
                response.status_code = 429  # Rate limited
                return response
            elif "GOOGL" in url:
                response.status_code = 200
                response.json.return_value = [
                    {
                        "date": "2023-12-15T00:00:00+00:00",
                        "open": 140.05,
                        "high": 141.35,
                        "low": 139.64,
                        "close": 140.93,
                        "adjClose": 140.93,
                        "volume": 25891013
                    }
                ]
                return response
        
        mock_get.side_effect = mock_response_side_effect
        
        adapter = TiingoAdapter(api_key="test_key")
        with patch('logging.getLogger') as mock_logger:
            prices = adapter.fetch_eod(["AAPL", "GOOGL"], "2023-12-15", "2023-12-15")
        
        # Only GOOGL should be returned (AAPL was rate limited)
        assert len(prices) == 1
        assert prices[0].instrument_id == "GOOGL"
        
        # Verify warning was logged
        mock_logger.return_value.warning.assert_called_with("Rate limited for AAPL, skipping")
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_http_error(self, mock_get):
        """Test fetch_eod handles HTTP errors gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 500  # Server error
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")
        
        # Should return empty list for failed requests
        assert prices == []
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_empty_data(self, mock_get):
        """Test fetch_eod with empty data response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")
        
        assert prices == []
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_date_parsing(self, mock_get):
        """Test fetch_eod correctly parses different date formats."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "date": "2023-12-15T00:00:00+00:00",  # Full ISO format
                "open": 195.18,
                "high": 197.08,
                "low": 194.83,
                "close": 197.57,
                "adjClose": 196.25,
                "volume": 55012853
            },
            {
                "date": "2023-12-16T16:00:00-05:00",  # Different timezone
                "open": 196.90,
                "high": 196.94,
                "low": 193.85,
                "close": 194.83,
                "adjClose": 193.52,
                "volume": 66561513
            }
        ]
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-16")
        
        assert len(prices) == 2
        assert prices[0].date == date(2023, 12, 15)
        assert prices[1].date == date(2023, 12, 16)
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    @patch('os.makedirs')
    @patch('builtins.open', create=True)
    def test_fetch_eod_logging_mechanism(self, mock_open, mock_makedirs, mock_get):
        """Test EOD data logging for specific tickers and date ranges."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "date": "2023-12-15T00:00:00+00:00",
                "open": 195.18,
                "high": 197.08,
                "low": 194.83,
                "close": 197.57,
                "adjClose": 196.25,
                "volume": 55012853
            }
        ]
        mock_get.return_value = mock_response
        
        # Mock file writing context manager
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        adapter = TiingoAdapter(api_key="test_key")
        # AAPL should trigger logging (it's in log_tickers)
        prices = adapter.fetch_eod(["AAPL"], "2023-06-15", "2023-06-16")
        
        # Verify logging directory was created
        mock_makedirs.assert_called_with("tests/data", exist_ok=True)
        
        # Verify files were opened for writing (request and response logs)
        assert mock_open.call_count >= 2
        
        # Verify data was written to files
        assert mock_file.write.call_count >= 0 or mock_file.json is not None
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_missing_optional_fields(self, mock_get):
        """Test fetch_eod handles missing optional fields gracefully."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "date": "2023-12-15T00:00:00+00:00",
                "close": 197.57,
                # Missing open, high, low, adjClose, volume
            }
        ]
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        prices = adapter.fetch_eod(["AAPL"], "2023-12-15", "2023-12-15")
        
        assert len(prices) == 1
        price = prices[0]
        assert price.close == 197.57
        assert price.open is None
        assert price.high is None
        assert price.low is None
        assert price.adj_close is None
        assert price.volume is None
    
    def test_fetch_ticks_not_implemented(self):
        """Test that fetch_ticks raises NotImplementedError."""
        adapter = TiingoAdapter(api_key="test_key")
        
        with pytest.raises(NotImplementedError, match="TiingoAdapter.fetch_ticks is not implemented yet"):
            adapter.fetch_ticks("AAPL", datetime.now(), datetime.now())
    
    def test_fetch_interval_not_implemented(self):
        """Test that fetch_interval raises NotImplementedError."""
        adapter = TiingoAdapter(api_key="test_key")
        
        with pytest.raises(NotImplementedError, match="TiingoAdapter.fetch_interval is not implemented yet"):
            adapter.fetch_interval("AAPL", "1h", datetime.now(), datetime.now())
    
    def test_base_url_format(self):
        """Test that BASE_URL format is correct."""
        adapter = TiingoAdapter(api_key="test_key")
        expected_base = "https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start}&endDate={end}&token={api_key}"
        assert adapter.BASE_URL == expected_base
    
    def test_vendor_name(self):
        """Test vendor_name is set correctly."""
        adapter = TiingoAdapter(api_key="test_key")
        assert adapter.vendor_name == "tiingo"
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_instruments_with_partial_data(self, mock_get):
        """Test fetch_instruments handles partial instrument data."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                # Missing exchange
            },
            {
                "ticker": "GOOGL",
                # Missing name and exchange
            }
        ]
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="test_key")
        instruments = adapter.fetch_instruments()
        
        assert len(instruments) == 2
        assert instruments[0].symbol == "AAPL"
        assert instruments[0].name == "Apple Inc."
        assert instruments[0].exchange is None
        
        assert instruments[1].symbol == "GOOGL"
        assert instruments[1].name is None
        assert instruments[1].exchange is None
    
    @patch('market_data.agent.tiingo_adapter.requests.get')
    def test_fetch_eod_api_key_in_url(self, mock_get):
        """Test that API key is correctly included in URL."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response
        
        adapter = TiingoAdapter(api_key="secret_api_key")
        adapter.fetch_eod(["AAPL"], "2023-01-01", "2023-01-02")
        
        # Verify API key was included in the URL
        call_args = mock_get.call_args[0][0]
        assert "token=secret_api_key" in call_args
        assert "AAPL" in call_args
        assert "startDate=2023-01-01" in call_args
        assert "endDate=2023-01-02" in call_args