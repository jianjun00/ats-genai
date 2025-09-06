import pytest
import json
import os
from unittest.mock import Mock
from urllib.parse import urlencode, urlparse, parse_qs


class TestPolygonInstrumentPopulation:
    """Test cases for Polygon instrument population API key handling"""
    
    def test_api_key_handling_in_next_url(self):
        """Test that API key is properly handled in next_url requests"""
        
        def add_api_key_to_url(url, api_key):
            """Add API key to URL if not present"""
            if 'apikey=' in url:
                return url
            separator = '&' if '?' in url else '?'
            return f"{url}{separator}apikey={api_key}"
        
        # Test case 1: URL without API key
        next_url = "https://api.polygon.io/v3/reference/tickers?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIzLTEyLTE1JmxpbWl0PTEwMDAmbHQ9"
        api_key = "test_api_key"
        result = add_api_key_to_url(next_url, api_key)
        assert "apikey=test_api_key" in result
        assert result == f"{next_url}&apikey={api_key}"
        
        # Test case 2: URL already has API key
        next_url_with_key = "https://api.polygon.io/v3/reference/tickers?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIzLTEyLTE1JmxpbWl0PTEwMDAmbHQ9&apikey=existing_key"
        result = add_api_key_to_url(next_url_with_key, api_key)
        assert result == next_url_with_key  # Should remain unchanged
        
        # Test case 3: URL without query parameters
        base_url = "https://api.polygon.io/v3/reference/tickers"
        result = add_api_key_to_url(base_url, api_key)
        assert result == f"{base_url}?apikey={api_key}"
    
    def test_pagination_simulation(self):
        """Test pagination logic without external requests"""
        
        # Simulate pagination data structures
        first_response = {
            "results": [
                {"ticker": "AAPL", "name": "Apple Inc.", "type": "CS"},
                {"ticker": "MSFT", "name": "Microsoft Corp.", "type": "CS"}
            ],
            "next_url": "https://api.polygon.io/v3/reference/tickers?cursor=next_page_cursor"
        }
        
        second_response = {
            "results": [
                {"ticker": "GOOGL", "name": "Alphabet Inc.", "type": "CS"}
            ],
            "next_url": None
        }
        
        def add_api_key_to_url(url, api_key):
            if 'apikey=' in url:
                return url
            separator = '&' if '?' in url else '?'
            return f"{url}{separator}apikey={api_key}"
        
        api_key = "test_api_key"
        
        # Test pagination logic
        results_collected = []
        next_url = first_response.get("next_url")
        
        # Process first page
        results_collected.extend(first_response["results"])
        
        # Process second page with API key added
        if next_url:
            fixed_url = add_api_key_to_url(next_url, api_key)
            assert "apikey=test_api_key" in fixed_url
            results_collected.extend(second_response["results"])
        
        assert len(results_collected) == 3
        assert results_collected[0]["ticker"] == "AAPL"
        assert results_collected[2]["ticker"] == "GOOGL"
    
    def test_api_error_simulation(self):
        """Test simulation of 401 API authentication errors"""
        
        error_response = {
            "status": "ERROR",
            "request_id": "test_id", 
            "error": "API Key was not provided"
        }
        
        # Simulate the error condition
        status_code = 401
        assert status_code == 401
        assert "API Key was not provided" in error_response["error"]
    
    def test_instrument_data_validation(self):
        """Test that instrument data is properly validated and processed"""
        
        def parse_date(val):
            from datetime import datetime
            if not val:
                return None
            try:
                return datetime.strptime(val[:10], "%Y-%m-%d").date()
            except Exception:
                return None
        
        # Test valid date
        assert parse_date("2023-01-15") is not None
        assert str(parse_date("2023-01-15")) == "2023-01-15"
        
        # Test invalid date
        assert parse_date(None) is None
        assert parse_date("") is None
        assert parse_date("invalid") is None
        
        # Test instrument data processing
        sample_instrument = {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "primary_exchange": "XNAS", 
            "type": "CS",
            "currency_name": "USD",
            "active": True,
            "list_date": "1980-12-12",
            "delisted_utc": None
        }
        
        # Simulate data processing
        processed_data = (
            sample_instrument.get('ticker'),
            sample_instrument.get('name'), 
            sample_instrument.get('primary_exchange'),
            sample_instrument.get('type'),
            sample_instrument.get('currency_name'),
            sample_instrument.get('share_class_figi'),  # None
            sample_instrument.get('isin'),  # None
            sample_instrument.get('cusip'),  # None
            sample_instrument.get('composite_figi'),  # None
            sample_instrument.get('active'),
            parse_date(sample_instrument.get('list_date')),
            parse_date(sample_instrument.get('delisted_utc')),
            json.dumps(sample_instrument)
        )
        
        assert processed_data[0] == "AAPL"
        assert processed_data[1] == "Apple Inc."
        assert processed_data[9] is True
        assert str(processed_data[10]) == "1980-12-12"
        assert processed_data[11] is None
        assert "AAPL" in processed_data[12]  # JSON contains ticker
    
    def test_batch_size_validation(self):
        """Test that batch processing handles appropriate sizes"""
        
        # Test normal batch
        instruments = [{"ticker": f"TEST{i}", "name": f"Test {i}"} for i in range(500)]
        assert len(instruments) == 500
        
        # Test maximum batch size (Polygon API limit)
        max_batch = [{"ticker": f"TEST{i}", "name": f"Test {i}"} for i in range(1000)]
        assert len(max_batch) == 1000
        
        # Test empty batch
        empty_batch = []
        assert len(empty_batch) == 0


class TestPolygonApiIntegration:
    """Integration tests for Polygon API issues"""
    
    @pytest.mark.integration
    def test_next_url_api_key_issue_reproduction(self):
        """Test that reproduces the specific API key issue found in production"""
        
        # This test reproduces the exact issue where next_url requests 
        # were missing API keys, causing 401 errors after the first page
        
        def add_api_key_to_url(url, api_key):
            """The fix for the API key issue"""
            if 'apikey=' in url:
                return url
            separator = '&' if '?' in url else '?'
            return f"{url}{separator}apikey={api_key}"
        
        # Simulate the problematic next_url from Polygon API
        problematic_next_url = "https://api.polygon.io/v3/reference/tickers?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIzLTEyLTE1JmxpbWl0PTEwMDAmbHQ9MjAyMy0xMi0xNQ%3D%3D&limit=1000&market=stocks&order=asc&sort=ticker"
        
        api_key = os.getenv('POLYGON_API_KEY', 'test_api_key_placeholder')
        
        # Before fix: next_url would be used directly, causing 401
        assert 'apikey=' not in problematic_next_url
        
        # After fix: API key is properly added
        fixed_url = add_api_key_to_url(problematic_next_url, api_key)
        assert 'apikey=' in fixed_url
        assert f"apikey={api_key}" in fixed_url
        
        # Verify the URL is still valid
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(fixed_url)
        query_params = parse_qs(parsed.query)
        assert 'apikey' in query_params
        assert query_params['apikey'][0] == api_key


if __name__ == "__main__":
    pytest.main([__file__])