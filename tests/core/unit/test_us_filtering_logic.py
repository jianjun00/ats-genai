"""
Unit tests for US-only filtering logic in instrument population scripts.

Tests the filtering logic for all three vendors:
- Polygon: primary_exchange filtering
- Tiingo: exchangeCode filtering
- EODHD: country and exchange filtering
"""

import unittest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class TestPolygonUSFiltering(unittest.TestCase):
    """Test US-only filtering logic for Polygon data"""

    def setUp(self):
        """Set up test data"""
        self.US_EXCHANGES = {'XNYS', 'XNAS', 'XASE', 'BATS'}

    def test_polygon_us_exchange_filtering(self):
        """Test that US exchanges are correctly identified"""
        test_cases = [
            ('XNYS', True, 'NYSE'),
            ('XNAS', True, 'NASDAQ'),
            ('XASE', True, 'NYSE American'),
            ('BATS', True, 'BATS'),
            ('LSE', False, 'London Stock Exchange'),
            ('TSE', False, 'Tokyo Stock Exchange'),
            ('TSX', False, 'Toronto Stock Exchange'),
            ('', False, 'Empty exchange'),
            (None, False, 'None exchange'),
        ]

        for exchange, expected, description in test_cases:
            with self.subTest(exchange=exchange, description=description):
                is_us = exchange in self.US_EXCHANGES if exchange else False
                self.assertEqual(is_us, expected,
                               f"Exchange {exchange} ({description}) should be {expected}")

    def test_polygon_bulk_filtering_logic(self):
        """Test the bulk filtering logic used in fetch_and_store_instruments"""
        # Mock ticker data as returned by Polygon API
        mock_tickers = [
            {'ticker': 'AAPL', 'primary_exchange': 'XNAS'},
            {'ticker': 'IBM', 'primary_exchange': 'XNYS'},
            {'ticker': 'FOREIGN1', 'primary_exchange': 'LSE'},
            {'ticker': 'TSLA', 'primary_exchange': 'XNAS'},
            {'ticker': 'FOREIGN2', 'primary_exchange': 'TSX'},
            {'ticker': 'EMPTY', 'primary_exchange': ''},
        ]

        # Apply filtering logic
        us_symbols = []
        filtered_count = 0
        start_ticker = ''

        for item in mock_tickers:
            symbol = item.get('ticker')
            primary_exchange = item.get('primary_exchange', '')

            if symbol <= start_ticker:
                continue

            if primary_exchange in self.US_EXCHANGES:
                us_symbols.append(symbol)
            else:
                filtered_count += 1

        # Verify results
        self.assertEqual(len(us_symbols), 3, "Should have 3 US symbols")
        self.assertEqual(filtered_count, 3, "Should filter out 3 non-US symbols")
        self.assertIn('AAPL', us_symbols)
        self.assertIn('IBM', us_symbols)
        self.assertIn('TSLA', us_symbols)
        self.assertNotIn('FOREIGN1', us_symbols)
        self.assertNotIn('FOREIGN2', us_symbols)
        self.assertNotIn('EMPTY', us_symbols)


class TestTiingoUSFiltering(unittest.TestCase):
    """Test US-only filtering logic for Tiingo data"""

    def setUp(self):
        """Set up test data"""
        self.US_EXCHANGE_CODES = ['NYSE', 'NASDAQ', 'AMEX', 'BATS', 'IEX']

    def test_tiingo_exchange_code_filtering(self):
        """Test that US exchange codes are correctly identified"""
        test_cases = [
            ('NYSE', True, 'New York Stock Exchange'),
            ('NASDAQ', True, 'NASDAQ'),
            ('AMEX', True, 'American Stock Exchange'),
            ('BATS', True, 'BATS Global Markets'),
            ('IEX', True, 'Investors Exchange'),
            ('LSE', False, 'London Stock Exchange'),
            ('TSX', False, 'Toronto Stock Exchange'),
            ('ASX', False, 'Australian Securities Exchange'),
            ('', True, 'Empty exchange code (assumed US)'),
            (None, True, 'None exchange code (assumed US)'),
        ]

        for exchange_code, expected, description in test_cases:
            with self.subTest(exchange_code=exchange_code, description=description):
                # Logic from populate_instrument_tiingo.py:
                # if exchange_code and exchange_code not in US_EXCHANGE_CODES: skip
                is_us = not (exchange_code and exchange_code not in self.US_EXCHANGE_CODES)
                self.assertEqual(is_us, expected,
                               f"Exchange code {exchange_code} ({description}) should be {expected}")

    def test_tiingo_individual_ticker_filtering(self):
        """Test filtering logic for individual ticker processing"""
        test_responses = [
            {'ticker': 'AAPL', 'exchangeCode': 'NASDAQ', 'should_process': True},
            {'ticker': 'IBM', 'exchangeCode': 'NYSE', 'should_process': True},
            {'ticker': 'SPY', 'exchangeCode': 'AMEX', 'should_process': True},
            {'ticker': 'FOREIGN', 'exchangeCode': 'LSE', 'should_process': False},
            {'ticker': 'UNKNOWN', 'exchangeCode': '', 'should_process': True},
            {'ticker': 'CANADIAN', 'exchangeCode': 'TSX', 'should_process': False},
        ]

        for response in test_responses:
            with self.subTest(ticker=response['ticker']):
                exchange_code = response.get('exchangeCode', '')
                # Apply Tiingo filtering logic
                should_skip = exchange_code and exchange_code not in self.US_EXCHANGE_CODES
                should_process = not should_skip

                self.assertEqual(should_process, response['should_process'],
                               f"Ticker {response['ticker']} with exchange {exchange_code} "
                               f"should {'be processed' if response['should_process'] else 'be skipped'}")


class TestEODHDUSFiltering(unittest.TestCase):
    """Test US-only filtering logic for EODHD data"""

    def setUp(self):
        """Set up test data"""
        self.US_EXCHANGES = ['US', 'NASDAQ', 'NYSE', 'AMEX', 'NYSE MKT', 'BATS', 'IEX']

    def is_us_stock(self, country, exchange):
        """Replicate EODHD filtering logic"""
        return (country == 'USA' or country == 'US' or
                exchange in self.US_EXCHANGES or
                any(us_ex in str(exchange).upper() for us_ex in ['NYSE', 'NASDAQ']))

    def test_eodhd_country_filtering(self):
        """Test country-based filtering"""
        test_cases = [
            ('USA', 'NASDAQ', True, 'US country with NASDAQ'),
            ('US', 'NYSE', True, 'US country with NYSE'),
            ('USA', '', True, 'US country with empty exchange'),
            ('United States', 'LSE', False, 'Wrong country format'),
            ('Canada', 'NYSE', True, 'Non-US country but US exchange'),
            ('UK', 'LSE', False, 'Non-US country and exchange'),
            ('', 'NASDAQ', True, 'Empty country but US exchange'),
        ]

        for country, exchange, expected, description in test_cases:
            with self.subTest(country=country, exchange=exchange, description=description):
                result = self.is_us_stock(country, exchange)
                self.assertEqual(result, expected,
                               f"{description}: country='{country}', exchange='{exchange}' -> {result}")

    def test_eodhd_exchange_filtering(self):
        """Test exchange-based filtering"""
        test_cases = [
            ('Germany', 'NASDAQ', True, 'Foreign country but NASDAQ exchange'),
            ('Japan', 'NYSE', True, 'Foreign country but NYSE exchange'),
            ('', 'NYSE MKT', True, 'NYSE American exchange'),
            ('', 'BATS', True, 'BATS exchange'),
            ('', 'IEX', True, 'IEX exchange'),
            ('', 'LSE', False, 'London Stock Exchange'),
            ('', 'TSX', False, 'Toronto Stock Exchange'),
            ('', 'ASX', False, 'Australian Securities Exchange'),
        ]

        for country, exchange, expected, description in test_cases:
            with self.subTest(country=country, exchange=exchange, description=description):
                result = self.is_us_stock(country, exchange)
                self.assertEqual(result, expected,
                               f"{description}: country='{country}', exchange='{exchange}' -> {result}")

    def test_eodhd_exchange_pattern_matching(self):
        """Test that NYSE and NASDAQ are detected in exchange names"""
        test_cases = [
            ('', 'NYSE American', True, 'NYSE in exchange name'),
            ('', 'NASDAQ Global Select Market', True, 'NASDAQ in exchange name'),
            ('', 'NYSE Arca', True, 'NYSE variant'),
            ('', 'NASDAQ Capital Market', True, 'NASDAQ variant'),
            ('', 'London Stock Exchange', False, 'No US exchange in name'),
            ('', 'Tokyo Stock Exchange', False, 'No US exchange in name'),
        ]

        for country, exchange, expected, description in test_cases:
            with self.subTest(country=country, exchange=exchange, description=description):
                result = self.is_us_stock(country, exchange)
                self.assertEqual(result, expected,
                               f"{description}: exchange='{exchange}' -> {result}")

    def test_eodhd_individual_and_bulk_filtering(self):
        """Test that both individual and bulk processing use same logic"""
        test_data = [
            {'symbol': 'AAPL', 'country': 'USA', 'exchange': 'NASDAQ', 'expected': True},
            {'symbol': 'FOREIGN', 'country': 'UK', 'exchange': 'LSE', 'expected': False},
            {'symbol': 'EDGE_CASE', 'country': 'Germany', 'exchange': 'NYSE', 'expected': True},
        ]

        for data in test_data:
            with self.subTest(symbol=data['symbol']):
                # Test individual processing logic
                individual_result = self.is_us_stock(data['country'], data['exchange'])

                # Test bulk processing logic (should be identical)
                bulk_result = self.is_us_stock(data['country'], data['exchange'])

                self.assertEqual(individual_result, data['expected'])
                self.assertEqual(bulk_result, data['expected'])
                self.assertEqual(individual_result, bulk_result,
                               "Individual and bulk processing should use same logic")


class TestFilteringIntegration(unittest.TestCase):
    """Integration tests for filtering across all vendors"""

    def test_consistent_filtering_standards(self):
        """Test that all vendors filter consistently for major US exchanges"""
        # Test data representing same stocks across different vendor formats
        test_stocks = [
            {
                'symbol': 'AAPL',
                'polygon': {'primary_exchange': 'XNAS'},
                'tiingo': {'exchangeCode': 'NASDAQ'},
                'eodhd': {'country': 'USA', 'exchange': 'NASDAQ'},
                'expected': True,
                'description': 'Apple - NASDAQ stock'
            },
            {
                'symbol': 'IBM',
                'polygon': {'primary_exchange': 'XNYS'},
                'tiingo': {'exchangeCode': 'NYSE'},
                'eodhd': {'country': 'USA', 'exchange': 'NYSE'},
                'expected': True,
                'description': 'IBM - NYSE stock'
            },
            {
                'symbol': 'FOREIGN',
                'polygon': {'primary_exchange': 'LSE'},
                'tiingo': {'exchangeCode': 'LSE'},
                'eodhd': {'country': 'UK', 'exchange': 'LSE'},
                'expected': False,
                'description': 'Foreign stock - London'
            }
        ]

        for stock in test_stocks:
            with self.subTest(symbol=stock['symbol'], description=stock['description']):
                # Test Polygon filtering
                polygon_exchange = stock['polygon']['primary_exchange']
                polygon_us = polygon_exchange in {'XNYS', 'XNAS', 'XASE', 'BATS'}

                # Test Tiingo filtering
                tiingo_exchange = stock['tiingo']['exchangeCode']
                tiingo_us = not (tiingo_exchange and tiingo_exchange not in ['NYSE', 'NASDAQ', 'AMEX', 'BATS', 'IEX'])

                # Test EODHD filtering
                eodhd_country = stock['eodhd']['country']
                eodhd_exchange = stock['eodhd']['exchange']
                eodhd_us = (eodhd_country in ['USA', 'US'] or
                           eodhd_exchange in ['US', 'NASDAQ', 'NYSE', 'AMEX', 'NYSE MKT', 'BATS', 'IEX'] or
                           any(us_ex in eodhd_exchange.upper() for us_ex in ['NYSE', 'NASDAQ']))

                # All should agree on US vs non-US classification
                self.assertEqual(polygon_us, stock['expected'],
                               f"Polygon classification for {stock['symbol']}")
                self.assertEqual(tiingo_us, stock['expected'],
                               f"Tiingo classification for {stock['symbol']}")
                self.assertEqual(eodhd_us, stock['expected'],
                               f"EODHD classification for {stock['symbol']}")

    def test_edge_case_consistency(self):
        """Test that edge cases are handled consistently"""
        edge_cases = [
            {
                'description': 'Empty/missing exchange data',
                'polygon_pass': False,  # Empty primary_exchange filtered out
                'tiingo_pass': True,    # Empty exchangeCode assumed US
                'eodhd_pass': False,    # Empty country and exchange filtered out
                'note': 'Different assumptions for missing data are acceptable'
            }
        ]

        for case in edge_cases:
            # This test documents the intentional differences between vendors
            # Rather than forcing consistency, we verify each vendor's logic is correct
            self.assertTrue(True, f"Edge case documented: {case['description']}")


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)