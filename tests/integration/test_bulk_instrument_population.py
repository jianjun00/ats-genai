#!/usr/bin/env python3
"""
Integration tests for bulk instrument population scripts.

Tests comprehensive vendor-native instrument population:
- Tiingo: Uses TiingoClient.list_stock_tickers() for all 60K+ stocks
- EODHD: Uses exchange-symbol-list/US API for all 50K+ US stocks
- Verifies inclusion of delisted/historical securities
"""

import pytest
import subprocess
import sys
import os
import tempfile
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

class TestBulkInstrumentPopulation:
    """Test comprehensive bulk instrument population from vendor APIs."""
    
    def test_tiingo_bulk_script_exists(self):
        """Test that Tiingo bulk script exists and is executable."""
        script_path = "/home/jianjun/ats-genai-admin/scripts/run_tiingo_bulk.py"
        assert os.path.exists(script_path), f"Tiingo bulk script not found at {script_path}"
        assert os.access(script_path, os.X_OK), "Tiingo bulk script is not executable"
    
    def test_eodhd_bulk_script_exists(self):
        """Test that EODHD bulk script exists and is executable."""
        script_path = "/home/jianjun/ats-genai-admin/scripts/run_eodhd_bulk.py"
        assert os.path.exists(script_path), f"EODHD bulk script not found at {script_path}"
        assert os.access(script_path, os.X_OK), "EODHD bulk script is not executable"
    
    @patch('tiingo.TiingoClient')
    def test_tiingo_bulk_api_integration(self, mock_tiingo_client):
        """Test Tiingo bulk script uses official client API correctly."""
        # Mock the TiingoClient
        mock_client_instance = MagicMock()
        mock_tiingo_client.return_value = mock_client_instance
        
        # Mock comprehensive stock list response
        mock_stock_tickers = [
            {'ticker': 'AAPL', 'assetType': 'Stock', 'priceCurrency': 'USD', 'startDate': '1980-12-12'},
            {'ticker': 'MSFT', 'assetType': 'Stock', 'priceCurrency': 'USD', 'startDate': '1986-03-13'},
            {'ticker': 'TSLA', 'assetType': 'Stock', 'priceCurrency': 'USD', 'startDate': '2010-06-29'},
            {'ticker': 'VIAC', 'assetType': 'Stock', 'priceCurrency': 'USD', 'startDate': '2005-12-05', 'endDate': '2019-12-04'},  # Delisted
        ]
        mock_client_instance.list_stock_tickers.return_value = mock_stock_tickers
        
        # Import and test the function
        from scripts.run_tiingo_bulk import get_tiingo_supported_symbols
        
        symbols = get_tiingo_supported_symbols()
        
        # Verify API was called correctly
        mock_tiingo_client.assert_called_once()
        mock_client_instance.list_stock_tickers.assert_called_once()
        
        # Verify results include all symbols (including delisted)
        expected_symbols = ['AAPL', 'MSFT', 'TSLA', 'VIAC']
        assert symbols == expected_symbols
        assert 'VIAC' in symbols, "Should include delisted stocks"
    
    @patch('requests.get')
    def test_eodhd_bulk_api_integration(self, mock_requests):
        """Test EODHD bulk script uses exchange-symbol-list API correctly."""
        # Mock EODHD API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'Code': 'AAPL', 'Name': 'Apple Inc', 'Country': 'USA', 'Exchange': 'NASDAQ', 'Currency': 'USD'},
            {'Code': 'MSFT', 'Name': 'Microsoft Corp', 'Country': 'USA', 'Exchange': 'NASDAQ', 'Currency': 'USD'},
            {'Code': 'JCP', 'Name': 'J.C. Penney Company Inc', 'Country': 'USA', 'Exchange': 'OTCGREY', 'Currency': 'USD'},  # Delisted/Penny stock
        ]
        mock_requests.return_value = mock_response
        
        # Import and test the function
        from scripts.run_eodhd_bulk import get_eodhd_supported_symbols
        
        symbols = get_eodhd_supported_symbols()
        
        # Verify API was called with correct endpoint
        mock_requests.assert_called_once()
        call_args = mock_requests.call_args[0][0]
        assert 'eodhd.com/api/exchange-symbol-list/US' in call_args
        assert 'api_token=' in call_args
        assert 'fmt=json' in call_args
        
        # Verify results include all symbols with .US suffix
        expected_symbols = ['AAPL.US', 'JCP.US', 'MSFT.US']
        assert symbols == expected_symbols
        assert 'JCP.US' in symbols, "Should include delisted/penny stocks"
    
    def test_tiingo_script_comprehensive_coverage(self):
        """Test that Tiingo script is designed for comprehensive coverage."""
        script_path = "/home/jianjun/ats-genai-admin/scripts/run_tiingo_bulk.py"
        
        with open(script_path, 'r') as f:
            content = f.read()
        
        # Verify no hardcoded stock lists
        assert 'AAPL", "MSFT"' not in content, "Should not contain hardcoded stock lists"
        assert 'curated_stocks' not in content, "Should not use curated stock lists"
        
        # Verify uses official API methods
        assert 'list_stock_tickers()' in content, "Should use official list_stock_tickers() method"
        assert 'TiingoClient' in content, "Should use official TiingoClient"
        
        # Verify comprehensive approach
        assert 'ALL supported' in content or 'comprehensive' in content, "Should indicate comprehensive approach"
    
    def test_eodhd_script_comprehensive_coverage(self):
        """Test that EODHD script is designed for comprehensive coverage."""
        script_path = "/home/jianjun/ats-genai-admin/scripts/run_eodhd_bulk.py"
        
        with open(script_path, 'r') as f:
            content = f.read()
        
        # Verify uses native EODHD API
        assert 'exchange-symbol-list/US' in content, "Should use EODHD's native exchange list API"
        assert 'eodhd.com/api' in content, "Should use official EODHD API"
        
        # Verify no hardcoded lists
        assert '["AAPL"' not in content, "Should not contain hardcoded symbol lists"
        
        # Verify handles US symbols correctly
        assert '.US' in content, "Should handle EODHD's .US suffix format"
    
    @pytest.mark.integration
    def test_tiingo_bulk_population_integration(self):
        """Integration test for Tiingo bulk population (requires API key)."""
        # Skip if no API key
        api_key = os.getenv('TIINGO_API_KEY')
        if not api_key:
            pytest.skip("TIINGO_API_KEY not available")
        
        # Test with limited batch size for integration testing
        from scripts.run_tiingo_bulk import get_tiingo_supported_symbols
        
        symbols = get_tiingo_supported_symbols()
        
        # Verify comprehensive results
        assert len(symbols) > 50000, f"Expected 50K+ symbols, got {len(symbols)}"
        assert len(symbols) < 100000, f"Unexpected symbol count: {len(symbols)}"
        
        # Verify includes major stocks
        major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        for stock in major_stocks:
            assert stock in symbols, f"Major stock {stock} missing from comprehensive list"
    
    @pytest.mark.integration  
    def test_eodhd_bulk_population_integration(self):
        """Integration test for EODHD bulk population (requires API key)."""
        # Skip if no API key
        api_key = os.getenv('EODHD_API_KEY')
        if not api_key:
            pytest.skip("EODHD_API_KEY not available")
        
        from scripts.run_eodhd_bulk import get_eodhd_supported_symbols
        
        symbols = get_eodhd_supported_symbols()
        
        # Verify comprehensive results  
        assert len(symbols) > 40000, f"Expected 40K+ symbols, got {len(symbols)}"
        assert len(symbols) < 80000, f"Unexpected symbol count: {len(symbols)}"
        
        # Verify format and includes major stocks
        major_stocks_us = ['AAPL.US', 'MSFT.US', 'GOOGL.US', 'AMZN.US', 'TSLA.US']
        for stock in major_stocks_us:
            assert stock in symbols, f"Major stock {stock} missing from comprehensive list"
        
        # Verify all symbols have .US suffix
        for symbol in symbols[:100]:  # Check first 100
            assert symbol.endswith('.US'), f"Symbol {symbol} should end with .US"
    
    def test_batch_processing_configuration(self):
        """Test that both scripts use appropriate batch sizes for API rate limits."""
        # Test Tiingo batch size
        tiingo_path = "/home/jianjun/ats-genai-admin/scripts/run_tiingo_bulk.py"
        with open(tiingo_path, 'r') as f:
            tiingo_content = f.read()
        
        assert 'batch_size=100' in tiingo_content, "Tiingo should use batch_size=100"
        assert 'time.sleep(' in tiingo_content, "Should include rate limiting delays"
        
        # Test EODHD batch size
        eodhd_path = "/home/jianjun/ats-genai-admin/scripts/run_eodhd_bulk.py"
        with open(eodhd_path, 'r') as f:
            eodhd_content = f.read()
        
        assert 'batch_size=100' in eodhd_content, "EODHD should use batch_size=100"
        assert 'time.sleep(' in eodhd_content, "Should include rate limiting delays"
    
    def test_error_handling_and_logging(self):
        """Test that scripts have proper error handling and logging."""
        scripts = [
            "/home/jianjun/ats-genai-admin/scripts/run_tiingo_bulk.py",
            "/home/jianjun/ats-genai-admin/scripts/run_eodhd_bulk.py"
        ]
        
        for script_path in scripts:
            with open(script_path, 'r') as f:
                content = f.read()
            
            # Verify logging setup
            assert 'logging.basicConfig' in content, f"{script_path} should have logging configured"
            assert 'logger = logging.getLogger' in content, f"{script_path} should create logger"
            
            # Verify error handling
            assert 'try:' in content, f"{script_path} should have try/catch blocks"
            assert 'except Exception as e:' in content, f"{script_path} should handle exceptions"
            assert 'logger.error' in content, f"{script_path} should log errors"
    
    def test_no_hardcoded_fallbacks(self):
        """Test that scripts don't fall back to hardcoded lists."""
        tiingo_path = "/home/jianjun/ats-genai-admin/scripts/run_tiingo_bulk.py"
        
        with open(tiingo_path, 'r') as f:
            content = f.read()
        
        # Should raise RuntimeError instead of using fallback lists
        assert 'raise RuntimeError' in content, "Should raise error instead of using fallbacks"
        assert 'cannot get comprehensive stock list' in content.lower(), "Should indicate failure to get comprehensive list"

class TestInstrumentDataQuality:
    """Test quality of populated instrument data."""
    
    @pytest.mark.database
    def test_tiingo_instruments_include_delisted(self):
        """Test that Tiingo instruments include delisted securities."""
        import psycopg2
        
        # This would require database connection - mock for unit test
        # In real integration test, would connect to test database
        pass  # Placeholder for database integration test
    
    @pytest.mark.database  
    def test_instrument_metadata_completeness(self):
        """Test that populated instruments have complete metadata."""
        # Would test database for:
        # - start_date populated
        # - end_date for delisted stocks
        # - exchange information
        # - proper symbol formats
        pass  # Placeholder for database integration test

if __name__ == "__main__":
    pytest.main([__file__, "-v"])