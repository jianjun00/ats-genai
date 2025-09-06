#!/usr/bin/env python3
"""
Integration tests for Tiingo fundamentals collection

Tests the complete Tiingo fundamentals collection pipeline including:
- API endpoint validation
- DOW 30 symbol restrictions
- Database schema creation
- Data insertion and idempotency
- Error handling for non-DOW symbols
"""

import pytest
import asyncio
import os
import sys
from datetime import datetime, date, timedelta
from unittest.mock import patch, AsyncMock, MagicMock

sys.path.append('/workspace/src')

# Import the class we're testing
import importlib.util
spec = importlib.util.spec_from_file_location(
    "tiingo_fundamentals", 
    "/workspace/scripts/tiingo_30_year_fundamentals_backfill.py"
)
tiingo_fundamentals = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tiingo_fundamentals)

class TestTiingoFundamentalsCollection:
    """Test Tiingo fundamentals collection functionality."""
    
    @pytest.fixture
    def collector(self):
        """Create a TiingoFundamentalsCollector instance for testing."""
        return tiingo_fundamentals.TiingoFundamentalsCollector("test_api_key")
    
    @pytest.fixture 
    def mock_db_connection(self):
        """Mock database connection."""
        conn = AsyncMock()
        conn.execute = AsyncMock()
        conn.executemany = AsyncMock() 
        conn.fetchval = AsyncMock()
        conn.fetchrow = AsyncMock()
        conn.fetch = AsyncMock()
        return conn
    
    def test_collector_initialization(self, collector):
        """Test collector initializes with correct parameters."""
        assert collector.api_key == "test_api_key"
        assert collector.base_url == "https://api.tiingo.com/tiingo/fundamentals"
        assert collector.request_delay == 1.0
        assert collector.stats['total_instruments'] == 0
    
    @pytest.mark.asyncio
    
    async def test_get_instruments_returns_dow_30_only(self, collector, mock_db_connection):
        """Test that instrument selection returns only DOW 30 symbols."""
        instruments = await collector.get_instruments_for_backfill(mock_db_connection, limit=10)
        
        # Should return exactly 10 DOW 30 symbols
        assert len(instruments) == 10
        
        # Check DOW 30 symbols are included
        dow_30_symbols = ['AAPL', 'MSFT', 'UNH', 'GS', 'HD', 'CAT', 'AMGN', 'MCD', 'CRM', 'V']
        returned_symbols = [inst['symbol'] for inst in instruments]
        
        for symbol in returned_symbols:
            assert symbol in dow_30_symbols
        
        # Check structure matches expected format
        for inst in instruments:
            assert 'id' in inst
            assert 'symbol' in inst
            assert 'active' in inst
            assert inst['active'] is True
    
    @pytest.mark.asyncio
    
    async def test_get_instruments_respects_limit(self, collector, mock_db_connection):
        """Test that limit parameter is respected."""
        # Test with various limits
        for limit in [5, 15, 30]:
            instruments = await collector.get_instruments_for_backfill(mock_db_connection, limit=limit)
            expected_count = min(limit, 30)  # DOW 30 has max 30 symbols
            assert len(instruments) == expected_count
    
    @pytest.mark.asyncio
    
    async def test_table_creation(self, collector, mock_db_connection):
        """Test that database tables are created with correct schema."""
        await collector.ensure_fundamentals_tables(mock_db_connection)
        
        # Verify both tables are created
        assert mock_db_connection.execute.call_count == 4  # 2 tables + 2 indexes
        
        calls = mock_db_connection.execute.call_args_list
        
        # Check daily fundamentals table creation
        daily_table_sql = calls[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS dev_tiingo_fundamentals_daily" in daily_table_sql
        assert "market_cap DOUBLE PRECISION" in daily_table_sql
        assert "pe_ratio DOUBLE PRECISION" in daily_table_sql
        assert "PRIMARY KEY (date, symbol)" in daily_table_sql
        
        # Check statements table creation
        statements_table_sql = calls[1][0][0]
        assert "CREATE TABLE IF NOT EXISTS dev_tiingo_fundamentals_statements" in statements_table_sql
        assert "statement_type TEXT NOT NULL" in statements_table_sql
        assert "data_code TEXT NOT NULL" in statements_table_sql
        assert "value DOUBLE PRECISION" in statements_table_sql
    
    def test_daily_fundamentals_api_call(self, collector):
        """Test daily fundamentals API call structure."""
        symbol = "AAPL"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        
        with patch('requests.get') as mock_get:
            # Mock successful response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {
                    'date': '2024-01-01T00:00:00Z',
                    'marketCap': 3000000000000,
                    'enterpriseVal': 2900000000000,
                    'peRatio': 29.5,
                    'pbRatio': 8.2,
                    'trailingPE': 28.1
                }
            ]
            mock_get.return_value = mock_response
            
            result = collector.fetch_daily_fundamentals(symbol, start_date, end_date)
            
            # Verify API call was made correctly
            mock_get.assert_called_once()
            args, kwargs = mock_get.call_args
            
            assert args[0] == f"{collector.base_url}/{symbol}/daily"
            assert kwargs['params']['token'] == "test_api_key"
            assert kwargs['params']['startDate'] == '2024-01-01'
            assert kwargs['params']['endDate'] == '2024-12-31'
            assert kwargs['params']['format'] == 'json'
            
            # Verify response parsing
            assert len(result) == 1
            assert result[0]['date'] == '2024-01-01T00:00:00Z'
            assert result[0]['marketCap'] == 3000000000000
    
    def test_statements_api_call(self, collector):
        """Test financial statements API call structure."""
        symbol = "AAPL"
        
        with patch('requests.get') as mock_get:
            # Mock successful response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {
                    'date': '2024-01-01',
                    'year': 2024,
                    'quarter': 1,
                    'statementData': {
                        'balanceSheet': [
                            {'dataCode': 'totalAssets', 'value': 100000000000},
                            {'dataCode': 'totalLiabilities', 'value': 50000000000}
                        ],
                        'incomeStatement': [
                            {'dataCode': 'totalRevenue', 'value': 25000000000},
                            {'dataCode': 'netIncome', 'value': 5000000000}
                        ]
                    }
                }
            ]
            mock_get.return_value = mock_response
            
            result = collector.fetch_statements(symbol)
            
            # Verify API call was made correctly
            mock_get.assert_called_once()
            args, kwargs = mock_get.call_args
            
            assert args[0] == f"{collector.base_url}/{symbol}/statements"
            assert kwargs['params']['token'] == "test_api_key"
            assert kwargs['params']['format'] == 'json'
            
            # Verify response parsing
            assert len(result) == 1
            assert result[0]['year'] == 2024
            assert 'statementData' in result[0]
    
    def test_api_error_handling(self, collector):
        """Test API error handling for various HTTP status codes."""
        symbol = "AAPL"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        
        with patch('requests.get') as mock_get:
            # Test 404 (not found)
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            
            result = collector.fetch_daily_fundamentals(symbol, start_date, end_date)
            assert result == []
            
            # Test 400 (bad request - non-DOW symbol)
            mock_response.status_code = 400
            mock_response.text = '{"detail":"Error: Free and Power plans are limited to the DOW 30..."}'
            
            result = collector.fetch_daily_fundamentals(symbol, start_date, end_date)
            assert result == []
    
    @pytest.mark.asyncio
    
    async def test_daily_fundamentals_insertion(self, collector, mock_db_connection):
        """Test daily fundamentals database insertion."""
        symbol = "AAPL"
        instrument_id = 1
        daily_data = [
            {
                'date': '2024-01-01T00:00:00Z',
                'marketCap': 3000000000000,
                'enterpriseVal': 2900000000000,
                'peRatio': 29.5,
                'pbRatio': 8.2,
                'trailingPE': 28.1
            }
        ]
        
        result = await collector.insert_daily_fundamentals(
            mock_db_connection, symbol, instrument_id, daily_data
        )
        
        # Should return count of inserted records
        assert result == 1
        
        # Verify database insertion
        mock_db_connection.executemany.assert_called_once()
        
        call_args = mock_db_connection.executemany.call_args
        sql_query = call_args[0][0]
        data_rows = call_args[0][1]
        
        # Check SQL structure
        assert "INSERT INTO dev_tiingo_fundamentals_daily" in sql_query
        assert "ON CONFLICT (date, symbol) DO UPDATE SET" in sql_query
        
        # Check data structure
        assert len(data_rows) == 1
        row = data_rows[0]
        assert row[1] == symbol  # symbol
        assert row[2] == instrument_id  # instrument_id
        assert row[3] == 3000000000000  # market_cap
    
    @pytest.mark.asyncio
    
    async def test_statements_insertion(self, collector, mock_db_connection):
        """Test financial statements database insertion.""" 
        symbol = "AAPL"
        instrument_id = 1
        statements_data = [
            {
                'date': '2024-01-01',
                'year': 2024,
                'quarter': 1,
                'statementData': {
                    'balanceSheet': [
                        {'dataCode': 'totalAssets', 'value': 100000000000}
                    ],
                    'incomeStatement': [
                        {'dataCode': 'totalRevenue', 'value': 25000000000}
                    ]
                }
            }
        ]
        
        result = await collector.insert_statements(
            mock_db_connection, symbol, instrument_id, statements_data
        )
        
        # Should return count of inserted records (2 statements)
        assert result == 2
        
        # Verify database insertion
        mock_db_connection.executemany.assert_called_once()
        
        call_args = mock_db_connection.executemany.call_args
        sql_query = call_args[0][0]
        data_rows = call_args[0][1]
        
        # Check SQL structure
        assert "INSERT INTO dev_tiingo_fundamentals_statements" in sql_query
        assert "ON CONFLICT (date, symbol, statement_type, data_code) DO UPDATE SET" in sql_query
        
        # Check data structure - should have 2 rows (balance sheet + income statement)
        assert len(data_rows) == 2
        
        # Check balance sheet record
        bs_row = data_rows[0]
        assert bs_row[1] == symbol  # symbol
        assert bs_row[5] == 'balanceSheet'  # statement_type
        assert bs_row[6] == 'totalAssets'  # data_code
        assert bs_row[7] == 100000000000  # value
        
        # Check income statement record
        is_row = data_rows[1] 
        assert is_row[1] == symbol  # symbol
        assert is_row[5] == 'incomeStatement'  # statement_type
        assert is_row[6] == 'totalRevenue'  # data_code
        assert is_row[7] == 25000000000  # value
    
    @pytest.mark.asyncio
    
    async def test_existing_data_check(self, collector, mock_db_connection):
        """Test checking for existing data to skip duplicate collection."""
        symbol = "AAPL"
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        
        # Mock existing data found
        mock_db_connection.fetchval.side_effect = [5, 10]  # daily_count, statements_count
        
        result = await collector.check_existing_data(mock_db_connection, symbol, start_date, end_date)
        assert result is True
        
        # Mock no existing data
        mock_db_connection.fetchval.side_effect = [0, 0]
        
        result = await collector.check_existing_data(mock_db_connection, symbol, start_date, end_date)
        assert result is False
    
    @pytest.mark.asyncio
    
    async def test_statistics_tracking(self, collector, mock_db_connection):
        """Test that statistics are properly tracked during collection."""
        initial_stats = collector.stats.copy()
        
        # Mock successful collection
        instruments = await collector.get_instruments_for_backfill(mock_db_connection, limit=5)
        
        # Stats should be updated
        assert collector.stats['total_instruments'] == 5
        assert collector.stats['total_instruments'] > initial_stats['total_instruments']
    
    def test_dow_30_symbol_validation(self, collector, mock_db_connection):
        """Test that only DOW 30 symbols are processed and others would fail appropriately."""
        # DOW 30 symbols (subset for testing)
        dow_30_symbols = [
            'AAPL', 'MSFT', 'UNH', 'GS', 'HD', 'CAT', 'AMGN', 'MCD', 'CRM', 'V',
            'BA', 'JPM', 'JNJ', 'HON', 'AXP', 'PG', 'CVX', 'IBM', 'MRK', 'DIS',
            'WMT', 'MMM', 'TRV', 'NKE', 'KO', 'DOW', 'CSCO', 'INTC', 'WBA', 'VZ'
        ]
        
        # Test with API that would return 400 for non-DOW symbols
        non_dow_symbols = ['GOOGL', 'AMZN', 'TSLA', 'NVDA']
        
        with patch('requests.get') as mock_get:
            for symbol in dow_30_symbols[:5]:  # Test first 5 DOW symbols
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = [{'date': '2024-01-01', 'marketCap': 1000000}]
                mock_get.return_value = mock_response
                
                result = collector.fetch_daily_fundamentals(symbol, date(2024, 1, 1), date(2024, 1, 31))
                assert len(result) == 1  # Should succeed
            
            for symbol in non_dow_symbols:  # Test non-DOW symbols
                mock_response = MagicMock()
                mock_response.status_code = 400
                mock_response.text = '{"detail":"Error: Free and Power plans are limited to the DOW 30..."}'
                mock_get.return_value = mock_response
                
                result = collector.fetch_daily_fundamentals(symbol, date(2024, 1, 1), date(2024, 1, 31))
                assert result == []  # Should return empty for non-DOW symbols

@pytest.mark.asyncio
class TestTiingoFundamentalsIntegration:
    """Integration tests requiring database connections."""
    
    @pytest.mark.asyncio
    
    async def test_end_to_end_collection_flow(self):
        """Test complete collection flow with mocked database."""
        collector = tiingo_fundamentals.TiingoFundamentalsCollector("test_api_key")
        
        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.executemany = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=0)  # No existing data
        
        # Mock successful API calls
        with patch.object(collector, 'fetch_daily_fundamentals') as mock_daily, \
             patch.object(collector, 'fetch_statements') as mock_statements:
            
            mock_daily.return_value = [
                {'date': '2024-01-01T00:00:00Z', 'marketCap': 3000000000000}
            ]
            mock_statements.return_value = [
                {'date': '2024-01-01', 'year': 2024, 'quarter': 1, 
                 'statementData': {'balanceSheet': [{'dataCode': 'assets', 'value': 1000}]}}
            ]
            
            # Test single instrument backfill
            instrument = {'id': 1, 'symbol': 'AAPL', 'active': True}
            start_date = date(2024, 1, 1)
            end_date = date(2024, 1, 31)
            
            result = await collector.backfill_instrument_fundamentals(
                mock_conn, instrument, start_date, end_date, skip_existing=False
            )
            
            # Should have collected and inserted data
            assert result > 0
            mock_daily.assert_called_once_with('AAPL', start_date, end_date)
            mock_statements.assert_called_once_with('AAPL')

if __name__ == "__main__":
    pytest.main([__file__, "-v"])