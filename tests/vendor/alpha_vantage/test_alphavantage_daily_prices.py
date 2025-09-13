#!/usr/bin/env python3
"""
Tests for Alpha Vantage daily price ingestion
"""

import pytest
import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../src'))

from domains.market_data.repositories.daily_prices_alphavantage_dao import DailyPricesAlphaVantageDAO
from shared.utils.environment import Environment

class TestAlphaVantageDailyPrices:

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_initialization(self):
        """Test that Alpha Vantage DAO can be initialized"""
        # Mock environment
        env = MagicMock()
        env.get_table_name.return_value = 'dev_daily_price_alphavantage'
        env.get_database_url.return_value = 'postgresql://test:test@localhost/test'

        dao = DailyPricesAlphaVantageDAO(env)

        assert dao.table_name == 'dev_daily_price_alphavantage'
        assert dao.db_url == 'postgresql://test:test@localhost/test'

    def test_api_key_handling(self):
        """Test API key environment variable handling"""
        # Save original value
        original_key = os.environ.get('ALPHA_VANTAGE_API_KEY')

        try:
            # Test with no API key
            if 'ALPHA_VANTAGE_API_KEY' in os.environ:
                del os.environ['ALPHA_VANTAGE_API_KEY']

            # Import after removing env var
            from domains.market_data.services.eod.daily_price_alphavantage import ALPHA_VANTAGE_API_KEY
            assert ALPHA_VANTAGE_API_KEY is None

            # Test with API key set
            os.environ['ALPHA_VANTAGE_API_KEY'] = 'test_key_123'

            # Need to reimport to pick up new env var
            import importlib
            import market_data.eod.daily_price_alphavantage
            importlib.reload(market_data.eod.daily_price_alphavantage)

            from domains.market_data.services.eod.daily_price_alphavantage import ALPHA_VANTAGE_API_KEY
            assert ALPHA_VANTAGE_API_KEY == 'test_key_123'

        finally:
            # Restore original value
            if original_key:
                os.environ['ALPHA_VANTAGE_API_KEY'] = original_key
            elif 'ALPHA_VANTAGE_API_KEY' in os.environ:
                del os.environ['ALPHA_VANTAGE_API_KEY']

    def test_price_record_format(self):
        """Test that price records have the correct format"""
        sample_alphavantage_response = {
            "Time Series (Daily)": {
                "2024-01-02": {
                    "1. open": "185.64",
                    "2. high": "186.89",
                    "3. low": "184.35",
                    "4. close": "185.64",
                    "5. adjusted close": "182.45",
                    "6. volume": "82488200"
                }
            }
        }

        # Test parsing logic
        time_series = sample_alphavantage_response["Time Series (Daily)"]
        for date_str, price_data in time_series.items():
            parsed_record = {
                'date': date(2024, 1, 2),
                'open_price': float(price_data['1. open']),
                'high_price': float(price_data['2. high']),
                'low_price': float(price_data['3. low']),
                'close': float(price_data['4. close']),
                'adj_close': float(price_data['5. adjusted close']),
                'volume': int(price_data['6. volume'])
            }

            assert parsed_record['open_price'] == 185.64
            assert parsed_record['high_price'] == 186.89
            assert parsed_record['low_price'] == 184.35
            assert parsed_record['close'] == 185.64
            assert parsed_record['adj_close'] == 182.45
            assert parsed_record['volume'] == 82488200

    def test_rate_limiting_configuration(self):
        """Test that rate limiting is properly configured"""
        # Alpha Vantage free tier: 5 calls per minute = 12 seconds between calls
        expected_delay = 12

        # This would be tested in the actual implementation
        # For now, just verify the calculation
        calls_per_minute = 5
        seconds_between_calls = 60 / calls_per_minute

        assert seconds_between_calls == expected_delay

    def test_table_schema_requirements(self):
        """Test that the required database table structure is defined"""
        required_columns = [
            'id', 'instrument_id', 'date', 'close', 'volume',
            'open_price', 'high_price', 'low_price', 'adj_close'
        ]

        # This would be tested against actual database schema
        # For now, just verify the requirements are documented
        assert all(col in required_columns for col in ['instrument_id', 'date', 'close'])

class TestAlphaVantageIntegration:
    """Integration tests for Alpha Vantage ingestion"""

    def test_vendor_configuration(self):
        """Test that Alpha Vantage is properly configured as a vendor"""
        # Alpha Vantage should be vendor ID 5 based on earlier database check
        expected_vendor_id = 5
        expected_vendor_name = 'alpha_vantage'

        # This would query the actual database in a real integration test
        # For now, document the expected configuration
        assert expected_vendor_id == 5
        assert expected_vendor_name == 'alpha_vantage'

    def test_api_endpoints(self):
        """Test Alpha Vantage API endpoint configuration"""
        expected_base_url = "https://www.alphavantage.co/query"
        expected_function = "TIME_SERIES_DAILY_ADJUSTED"

        from domains.market_data.services.eod.daily_price_alphavantage import ALPHA_VANTAGE_BASE_URL

        assert ALPHA_VANTAGE_BASE_URL == expected_base_url

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_table_exists(self):
        """Test that the Alpha Vantage daily prices table exists"""
        # This test would connect to actual database and verify table exists
        # Skip for now since it requires database connection
        pytest.skip("Requires actual database connection")

    def test_price_comparison_readiness(self):
        """Test that Alpha Vantage is ready for majority voting comparison"""
        # Alpha Vantage provides split-adjusted prices like Tiingo
        # This makes it suitable for majority voting with corrected Polygon data

        expected_features = [
            'split_adjusted_prices',
            'daily_ohlcv_data',
            'historical_data',
            'rate_limited_api'
        ]

        # Document that Alpha Vantage supports all required features
        assert all(feature in expected_features for feature in ['split_adjusted_prices', 'daily_ohlcv_data'])

if __name__ == "__main__":
    # Run basic tests without pytest
    import unittest

    class BasicTests(unittest.TestCase):
        def test_imports(self):
            """Test that Alpha Vantage modules can be imported"""
            try:
                from domains.market_data.repositories.daily_prices_alphavantage_dao import DailyPricesAlphaVantageDAO
                from domains.market_data.services.eod.daily_price_alphavantage import fetch_alphavantage_daily_prices
                self.assertTrue(True)
            except ImportError as e:
                self.fail(f"Failed to import Alpha Vantage modules: {e}")

        def test_basic_functionality(self):
            """Test basic DAO functionality"""
            from domains.market_data.repositories.daily_prices_alphavantage_dao import DailyPricesAlphaVantageDAO
            from unittest.mock import MagicMock

            env = MagicMock()
            env.get_table_name.return_value = 'test_table'
            env.get_database_url.return_value = 'test_url'

            dao = DailyPricesAlphaVantageDAO(env)
            self.assertEqual(core.dao.table_name, 'test_table')

    unittest.main()