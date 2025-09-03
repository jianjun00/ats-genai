import os
import pytest
from unittest import mock
from datetime import date, datetime
import json

from domains.market_data.services.agent.polygon_adapter import PolygonAdapter
from domains.market_data.services.agent.tiingo_adapter import TiingoAdapter
from domains.market_data.services.agent.models import EODPrice

# Sample response data for mocking API calls
POLYGON_SAMPLE_RESPONSE = {
    "ticker": "AAPL",
    "status": "OK",
    "results": [
        {
            "v": 123456,
            "o": 150.0,
            "c": 155.0,
            "h": 156.0,
            "l": 149.0,
            "t": 1609459200000  # 2021-01-01
        },
        {
            "v": 234567,
            "o": 155.0,
            "c": 160.0,
            "h": 161.0,
            "l": 154.0,
            "t": 1609545600000  # 2021-01-02
        }
    ]
}

TIINGO_SAMPLE_RESPONSE = [
    {
        "date": "2021-01-01T00:00:00.000Z",
        "open": 150.0,
        "high": 156.0,
        "low": 149.0,
        "close": 155.0,
        "adjClose": 155.0,
        "volume": 123456,
        "adjVolume": 123456
    },
    {
        "date": "2021-01-02T00:00:00.000Z",
        "open": 155.0,
        "high": 161.0,
        "low": 154.0,
        "close": 160.0,
        "adjClose": 160.0,
        "volume": 234567,
        "adjVolume": 234567
    }
]

class TestPolygonAdapter:
    @pytest.fixture
    def adapter(self):
        """Create a polygon adapter with a mock API key"""
        with mock.patch.dict(os.environ, {"POLYGON_API_KEY": "test_key"}):
            return PolygonAdapter()
    
    @mock.patch('requests.get')
    def test_fetch_eod(self, mock_get, adapter):
        """Test fetching EOD prices from Polygon"""
        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = POLYGON_SAMPLE_RESPONSE
        mock_get.return_value = mock_response
        
        # Call the method
        result = adapter.fetch_eod(["AAPL"], "2021-01-01", "2021-01-02")
        
        # Assertions
        assert len(result) == 2
        assert isinstance(result[0], EODPrice)
        assert result[0].instrument_id == "AAPL"
        assert result[0].date == date(2021, 1, 1)
        assert result[0].open == 150.0
        assert result[0].close == 155.0
        assert result[0].vendor == "polygon"
        
        # Verify API call
        mock_get.assert_called_once()
        call_args = mock_get.call_args[0][0]
        assert "AAPL" in call_args
        assert "2021-01-01" in call_args or "2021-01-02" in call_args
        assert "test_key" in call_args
    
    @mock.patch('requests.get')
    def test_fetch_eod_error(self, mock_get, adapter):
        """Test handling API errors"""
        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Call the method
        result = adapter.fetch_eod(["AAPL"], "2021-01-01", "2021-01-02")
        
        # Should return empty list on error
        assert result == []
        
        # Verify API call was made
        mock_get.assert_called_once()


class TestTiingoAdapter:
    @pytest.fixture
    def adapter(self):
        """Create a tiingo adapter with a mock API key"""
        with mock.patch.dict(os.environ, {"TIINGO_API_KEY": "test_key"}):
            return TiingoAdapter()
    
    @mock.patch('requests.get')
    def test_fetch_eod(self, mock_get, adapter):
        """Test fetching EOD prices from Tiingo"""
        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = TIINGO_SAMPLE_RESPONSE
        mock_get.return_value = mock_response
        
        # Call the method
        result = adapter.fetch_eod(["AAPL"], "2021-01-01", "2021-01-02")
        
        # Assertions
        assert len(result) == 2
        assert isinstance(result[0], EODPrice)
        assert result[0].instrument_id == "AAPL"
        assert result[0].date == date(2021, 1, 1)
        assert result[0].open == 150.0
        assert result[0].close == 155.0
        assert result[0].adj_close == 155.0
        assert result[0].vendor == "tiingo"
        
        # Verify API call
        mock_get.assert_called_once()
        call_args = mock_get.call_args[0][0]
        assert "AAPL" in call_args
        assert "2021-01-01" in call_args or "2021-01-02" in call_args
        assert "test_key" in call_args
    
    @mock.patch('requests.get')
    def test_fetch_eod_error(self, mock_get, adapter):
        """Test handling API errors"""
        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Call the method
        result = adapter.fetch_eod(["AAPL"], "2021-01-01", "2021-01-02")
        
        # Should return empty list on error
        assert result == []
        
        # Verify API call was made
        mock_get.assert_called_once()
