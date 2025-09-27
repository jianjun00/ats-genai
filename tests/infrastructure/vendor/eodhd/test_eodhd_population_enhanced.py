"""
Unit tests for enhanced EODHD population with IPO dates

Tests the updated population logic that uses fundamentals API
to properly fetch IPO dates and other temporal information.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import date

# Import the modules we're testing
import sys
sys.path.append('/workspace/src')

from domains.instruments.services.secmaster.populate_instrument_eodhd import (
    get_exchange_symbols,
    fetch_fundamental_data,
    fetch_and_store_instruments,
    upsert_instrument,
    parse_date
)


class TestParseDate:
    """Test date parsing function"""

    def test_parse_valid_date(self):
        """Test parsing valid ISO date"""
        result = parse_date("2022-02-02")
        assert result == date(2022, 2, 2)

    def test_parse_date_with_time(self):
        """Test parsing date with time component"""
        result = parse_date("2022-02-02T10:30:00Z")
        assert result == date(2022, 2, 2)

    def test_parse_null_date(self):
        """Test parsing null/empty date"""
        assert parse_date(None) is None
        assert parse_date("") is None
        assert parse_date("null") is None

    def test_parse_invalid_date(self):
        """Test parsing invalid date format"""
        assert parse_date("invalid-date") is None
        assert parse_date("2022-13-45") is None


class TestGetExchangeSymbols:
    """Test exchange symbol fetching"""

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_get_exchange_symbols_success(self, mock_get):
        """Test successful symbol fetching from exchange"""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                'Code': 'AAPL',
                'Name': 'Apple Inc',
                'Exchange': 'NASDAQ',
                'Type': 'Common Stock',
                'Currency': 'USD',
                'Country': 'USA'
            },
            {
                'Code': 'MSFT',
                'Name': 'Microsoft Corp',
                'Exchange': 'NASDAQ',
                'Type': 'Common Stock',
                'Currency': 'USD',
                'Country': 'USA'
            }
        ]
        mock_get.return_value = mock_response

        # Test the function
        result = await get_exchange_symbols('US', 'test_api_key')

        assert len(result) == 2
        assert result[0]['symbol'] == 'AAPL'
        assert result[0]['name'] == 'Apple Inc'
        assert result[0]['exchange'] == 'NASDAQ'
        assert result[1]['symbol'] == 'MSFT'

        # Verify API call
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert 'exchange-symbol-list/US' in call_url
        assert 'test_api_key' in call_url

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_get_exchange_symbols_api_error(self, mock_get):
        """Test handling of API errors"""
        mock_get.side_effect = Exception("API Error")

        result = await get_exchange_symbols('US', 'test_api_key')

        assert result == []

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_get_exchange_symbols_filter_long_symbols(self, mock_get):
        """Test filtering of overly long symbol names"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'Code': 'AAPL', 'Name': 'Apple Inc'},
            {'Code': 'VERYLONGSYMBOLNAME', 'Name': 'Long Symbol Corp'}  # Should be filtered
        ]
        mock_get.return_value = mock_response

        result = await get_exchange_symbols('US', 'test_api_key')

        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'


class TestFetchFundamentalData:
    """Test fundamental data fetching"""

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_fetch_fundamental_data_success(self, mock_get):
        """Test successful fundamental data fetching"""
        # Mock API response with complete data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'General': {
                'Name': 'Apple Inc',
                'Exchange': 'NASDAQ',
                'Type': 'Common Stock',
                'CurrencyCode': 'USD',
                'IPODate': '1980-12-12',
                'Country': 'USA',
                'Sector': 'Technology',
                'Industry': 'Consumer Electronics'
            }
        }
        mock_get.return_value = mock_response

        # Test the function
        result = await fetch_fundamental_data('AAPL', 'test_api_key')

        assert result is not None
        assert result['symbol'] == 'AAPL'
        assert result['name'] == 'Apple Inc'
        assert result['exchange'] == 'NASDAQ'
        assert result['ipo_date'] == '1980-12-12'
        assert result['sector'] == 'Technology'
        assert result['industry'] == 'Consumer Electronics'
        assert 'full_response' in result

        # Verify API call
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert 'fundamentals/AAPL.US' in call_url
        assert 'test_api_key' in call_url

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_fetch_fundamental_data_already_has_exchange(self, mock_get):
        """Test handling symbols that already have exchange suffix"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'General': {'Name': 'Test Corp'}}
        mock_get.return_value = mock_response

        result = await fetch_fundamental_data('AAPL.US', 'test_api_key')

        assert result['symbol'] == 'AAPL'  # Should strip exchange suffix

        # Verify correct URL was called
        call_url = mock_get.call_args[0][0]
        assert 'fundamentals/AAPL.US' in call_url

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_fetch_fundamental_data_api_error(self, mock_get):
        """Test handling of API errors"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = await fetch_fundamental_data('INVALID', 'test_api_key')

        assert result is None

    @patch('src.secmaster.populate_instrument_eodhd.requests.get')
    @pytest.mark.asyncio
    async def test_fetch_fundamental_data_network_error(self, mock_get):
        """Test handling of network errors"""
        mock_get.side_effect = Exception("Network Error")

        result = await fetch_fundamental_data('AAPL', 'test_api_key')

        assert result is None


class TestUpsertInstrument:
    """Test database upsert operations"""

    @pytest.mark.asyncio

    async def test_upsert_instrument_new_format(self):
        """Test upserting instrument with new data format"""
        # Mock database components
        mock_pool = Mock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        # Mock environment
        with patch('src.secmaster.populate_instrument_eodhd.env') as mock_env:
            mock_env.get_table_name.return_value = 'test_instrument_eodhd'

            # Test data in new format
            item_data = {
                'symbol': 'AAPL',
                'name': 'Apple Inc',
                'exchange': 'NASDAQ',
                'type': 'Common Stock',
                'currency': 'USD',
                'ipo_date': '1980-12-12',
                'country': 'USA',
                'sector': 'Technology',
                'industry': 'Consumer Electronics',
                'full_response': {'General': {'Name': 'Apple Inc'}}
            }

            await upsert_instrument(mock_pool, item_data)

            # Verify database call
            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args

            # Check SQL query structure
            sql_query = call_args[0][0]
            assert 'INSERT INTO test_instrument_eodhd' in sql_query
            assert 'country' in sql_query
            assert 'sector' in sql_query
            assert 'industry' in sql_query
            assert 'ON CONFLICT (symbol)' in sql_query

            # Check parameter values
            params = call_args[0][1:]
            assert params[0] == 'AAPL'  # symbol
            assert params[1] == 'Apple Inc'  # name
            assert params[2] == 'NASDAQ'  # exchange
            assert params[5] == date(1980, 12, 12)  # parsed ipo_date

    @pytest.mark.asyncio

    async def test_upsert_instrument_legacy_format(self):
        """Test upserting instrument with legacy data format"""
        mock_pool = Mock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch('src.secmaster.populate_instrument_eodhd.env') as mock_env:
            mock_env.get_table_name.return_value = 'test_instrument_eodhd'

            # Test data in legacy format (for backward compatibility)
            item_data = {
                'Code': 'MSFT',
                'Name': 'Microsoft Corp',
                'Exchange': 'NASDAQ',
                'Type': 'Common Stock',
                'CurrencyCode': 'USD',
                'IPODate': '1986-03-13'
            }

            await upsert_instrument(mock_pool, item_data)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1:]

            assert params[0] == 'MSFT'  # symbol (from Code)
            assert params[1] == 'Microsoft Corp'  # name (from Name)
            assert params[5] == date(1986, 3, 13)  # parsed IPODate


@pytest.mark.integration
class TestFetchAndStoreInstruments:
    """Integration tests for the main function"""

    @patch('src.secmaster.populate_instrument_eodhd.Database')
    @patch('src.secmaster.populate_instrument_eodhd.fetch_fundamental_data')
    @pytest.mark.asyncio
    async def test_fetch_and_store_specific_tickers(self, mock_fetch_fundamental, mock_db_class):
        """Test processing specific tickers"""
        # Mock database
        mock_pool = Mock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.close = AsyncMock()
        mock_db_class.create_connection_pool = AsyncMock(return_value=mock_pool)

        # Mock environment
        with patch('src.secmaster.populate_instrument_eodhd.env') as mock_env:
            mock_env.get_table_name.return_value = 'test_instrument_eodhd'

            # Mock fundamental data response
            mock_fetch_fundamental.side_effect = [
                {
                    'symbol': 'AAPL',
                    'name': 'Apple Inc',
                    'ipo_date': '1980-12-12',
                    'exchange': 'NASDAQ'
                },
                {
                    'symbol': 'MSFT',
                    'name': 'Microsoft Corp',
                    'ipo_date': '1986-03-13',
                    'exchange': 'NASDAQ'
                }
            ]

            # Test the function
            await fetch_and_store_instruments(ticker='AAPL,MSFT')

            # Verify fundamental data was fetched for both symbols
            assert mock_fetch_fundamental.call_count == 2
            mock_fetch_fundamental.assert_any_call('AAPL', None)
            mock_fetch_fundamental.assert_any_call('MSFT', None)

            # Verify database operations
            assert mock_conn.execute.call_count >= 2  # At least table creation + 2 upserts

    @patch('src.secmaster.populate_instrument_eodhd.Database')
    @patch('src.secmaster.populate_instrument_eodhd.get_exchange_symbols')
    @patch('src.secmaster.populate_instrument_eodhd.fetch_fundamental_data')
    @pytest.mark.asyncio
    async def test_fetch_and_store_bulk_mode_sample(self, mock_fetch_fundamental, mock_get_symbols, mock_db_class):
        """Test bulk mode with a small sample"""
        # Mock database
        mock_pool = Mock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.close = AsyncMock()
        mock_db_class.create_connection_pool = AsyncMock(return_value=mock_pool)

        # Mock environment
        with patch('src.secmaster.populate_instrument_eodhd.env') as mock_env:
            mock_env.get_table_name.return_value = 'test_instrument_eodhd'

            # Mock exchange symbols response
            mock_get_symbols.return_value = [
                {'symbol': 'AAPL', 'name': 'Apple Inc', 'exchange': 'NASDAQ'},
                {'symbol': 'MSFT', 'name': 'Microsoft Corp', 'exchange': 'NASDAQ'}
            ]

            # Mock fundamental data responses
            mock_fetch_fundamental.side_effect = [
                {
                    'symbol': 'AAPL',
                    'name': 'Apple Inc',
                    'ipo_date': '1980-12-12',
                    'exchange': 'NASDAQ',
                    'sector': 'Technology'
                },
                {
                    'symbol': 'MSFT',
                    'name': 'Microsoft Corp',
                    'ipo_date': '1986-03-13',
                    'exchange': 'NASDAQ',
                    'sector': 'Technology'
                }
            ]

            # Test bulk mode (with mocked sleep to speed up test)
            with patch('src.secmaster.populate_instrument_eodhd.time.sleep'):
                await fetch_and_store_instruments(bulk_mode=True, exchange='US')

            # Verify exchange symbols were fetched
            mock_get_symbols.assert_called_once_with('US', None)

            # Verify fundamental data was fetched for all symbols
            assert mock_fetch_fundamental.call_count == 2

            # Verify database operations occurred
            assert mock_conn.execute.call_count >= 3  # Table creation + 2 upserts


class TestErrorHandling:
    """Test error handling scenarios"""

    @patch('src.secmaster.populate_instrument_eodhd.Database')
    @pytest.mark.asyncio
    async def test_database_connection_failure(self, mock_db_class):
        """Test handling of database connection failures"""
        # Mock database connection failure
        mock_db_class.create_connection_pool.side_effect = Exception("Connection failed")

        # Test should raise the exception
        with pytest.raises(Exception, match="Connection failed"):
            await fetch_and_store_instruments(ticker='AAPL')

    @patch('src.secmaster.populate_instrument_eodhd.Database')
    @patch('src.secmaster.populate_instrument_eodhd.fetch_fundamental_data')
    @pytest.mark.asyncio
    async def test_partial_failures_in_bulk_mode(self, mock_fetch_fundamental, mock_db_class):
        """Test handling of partial failures in bulk processing"""
        # Mock database
        mock_pool = Mock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.close = AsyncMock()
        mock_db_class.create_connection_pool = AsyncMock(return_value=mock_pool)

        with patch('src.secmaster.populate_instrument_eodhd.env') as mock_env:
            mock_env.get_table_name.return_value = 'test_instrument_eodhd'

            with patch('src.secmaster.populate_instrument_eodhd.get_exchange_symbols') as mock_get_symbols:
                mock_get_symbols.return_value = [
                    {'symbol': 'AAPL', 'name': 'Apple Inc'},
                    {'symbol': 'INVALID', 'name': 'Invalid Corp'}
                ]

                # Mock one success, one failure
                mock_fetch_fundamental.side_effect = [
                    {'symbol': 'AAPL', 'name': 'Apple Inc', 'ipo_date': '1980-12-12'},
                    None  # Simulates API failure for second symbol
                ]

                # Test should complete without raising exception
                with patch('src.secmaster.populate_instrument_eodhd.time.sleep'):
                    await fetch_and_store_instruments(bulk_mode=True)

                # Verify both symbols were attempted
                assert mock_fetch_fundamental.call_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])