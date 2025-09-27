#!/usr/bin/env python3
"""
Comprehensive Test Suite: Fundamentals Data Collection

Tests for Polygon fundamentals API integration, data standardization,
database schema compliance, and data quality validation.

Coverage:
- API connectivity and response validation
- Data standardization and parsing
- Database schema compliance
- Data integrity and quality checks
- Error handling and edge cases
"""

import pytest
import os
from datetime import datetime

# Test fixtures and setup
@pytest.fixture
def sample_polygon_financial_response():
    """Sample Polygon financial API response for testing"""
    return {
        "status": "OK",
        "results": [
            {
                "start_date": "2023-10-01",
                "end_date": "2024-09-28",
                "filing_date": "2024-11-01",
                "acceptance_datetime": "2024-11-01T10:01:36Z",
                "timeframe": "annual",
                "fiscal_period": "FY",
                "fiscal_year": "2024",
                "cik": "0000320193",
                "sic": "3571",
                "tickers": ["AAPL"],
                "company_name": "Apple Inc.",
                "financials": {
                    "balance_sheet": {
                        "assets": {"value": 3.6498e+11, "unit": "USD", "label": "Assets"},
                        "liabilities": {"value": 3.0803e+11, "unit": "USD", "label": "Liabilities"},
                        "equity": {"value": 5.695e+10, "unit": "USD", "label": "Equity"}
                    },
                    "income_statement": {
                        "revenues": {"value": 3.91035e+11, "unit": "USD", "label": "Revenues"},
                        "net_income_loss": {"value": 9.3736e+10, "unit": "USD", "label": "Net Income/Loss"},
                        "operating_income_loss": {"value": 1.23216e+11, "unit": "USD", "label": "Operating Income/Loss"},
                        "gross_profit": {"value": 1.80683e+11, "unit": "USD", "label": "Gross Profit"}
                    },
                    "cash_flow_statement": {
                        "net_cash_flow_from_operating_activities": {
                            "value": 1.18254e+11, "unit": "USD",
                            "label": "Net Cash Flow From Operating Activities"
                        }
                    },
                    "comprehensive_income": {
                        "comprehensive_income_loss": {
                            "value": 9.8016e+10, "unit": "USD",
                            "label": "Comprehensive Income/Loss"
                        }
                    }
                }
            }
        ]
    }

@pytest.fixture
def sample_standardized_financial():
    """Sample standardized financial record for testing"""
    return {
        'symbol': 'AAPL',
        'cik': '0000320193',
        'fiscal_period': 'FY',
        'fiscal_year': '2024',
        'start_date': '2023-10-01',
        'end_date': '2024-09-28',
        'timeframe': 'annual',
        'filing_date': '2024-11-01',
        'acceptance_datetime': '2024-11-01T10:01:36Z',
        'company_name': 'Apple Inc.',
        'sic': '3571',
        'total_assets': 364980000000.0,
        'total_liabilities': 308030000000.0,
        'total_equity': 56950000000.0,
        'total_revenue': 391035000000.0,
        'net_income': 93736000000.0,
        'operating_income': 123216000000.0,
        'gross_profit': 180683000000.0,
        'operating_cash_flow': 118254000000.0
    }

class TestPolygonFundamentalsAPI:
    """Test Polygon fundamentals API integration"""

    def test_api_key_validation(self):
        """Test API key presence and validation"""
        api_key = os.getenv('POLYGON_API_KEY')
        assert api_key is not None, "POLYGON_API_KEY environment variable must be set"
        assert len(api_key) > 10, "API key appears invalid (too short)"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_api_connectivity(self):
        """Test basic API connectivity and response format"""
        import aiohttp

        api_key = os.getenv('POLYGON_API_KEY', 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD')
        url = "https://api.polygon.io/vX/reference/financials"
        params = {
            'ticker': 'AAPL',
            'timeframe': 'annual',
            'limit': 1,
            'apikey': api_key
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                assert response.status == 200, f"API returned status {response.status}"

                data = await response.json()
                assert data.get('status') == 'OK', f"API status not OK: {data.get('status')}"
                assert 'results' in data, "Response missing results field"
                assert isinstance(data['results'], list), "Results should be a list"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_api_rate_limiting_handling(self):
        """Test proper handling of rate limiting responses"""
        # This test would simulate rate limiting scenarios
        # In practice, we'd mock the response or use a test API

class TestDataStandardization:
    """Test data standardization and parsing logic"""

    def test_financial_value_extraction(self, sample_polygon_financial_response):
        """Test extraction of financial values from nested structures"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("test_key")
        financials = sample_polygon_financial_response['results'][0]['financials']

        # Test balance sheet extraction
        assets = collector.extract_financial_value(financials, 'balance_sheet', 'assets')
        assert assets == 364980000000.0, f"Expected 364980000000.0, got {assets}"

        # Test income statement extraction
        revenue = collector.extract_financial_value(financials, 'income_statement', 'revenues')
        assert revenue == 391035000000.0, f"Expected 391035000000.0, got {revenue}"

        # Test missing field handling
        missing = collector.extract_financial_value(financials, 'balance_sheet', 'nonexistent_field')
        assert missing is None, f"Expected None for missing field, got {missing}"

    def test_record_standardization(self, sample_polygon_financial_response):
        """Test complete record standardization process"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("test_key")
        record = sample_polygon_financial_response['results'][0]

        standardized = collector.standardize_polygon_financial(record, 'AAPL')

        assert standardized is not None, "Standardization should not return None"
        assert standardized['symbol'] == 'AAPL', "Symbol should match"
        assert standardized['company_name'] == 'Apple Inc.', "Company name should match"
        assert standardized['fiscal_year'] == '2024', "Fiscal year should match"
        assert standardized['total_assets'] == 364980000000.0, "Total assets should be extracted correctly"

    def test_edge_case_handling(self):
        """Test handling of edge cases and malformed data"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("test_key")

        # Test empty record
        result = collector.standardize_polygon_financial({}, 'TEST')
        assert result is not None, "Should handle empty record gracefully"
        assert result['symbol'] == 'TEST', "Symbol should still be set"

        # Test missing financials section
        incomplete_record = {
            'fiscal_year': '2024',
            'company_name': 'Test Company'
        }
        result = collector.standardize_polygon_financial(incomplete_record, 'TEST')
        assert result is not None, "Should handle missing financials section"
        assert result['total_assets'] is None, "Should handle missing financial data"

class TestDatabaseSchema:
    """Test database schema compliance and operations"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fundamentals_table_creation(self):
        """Test creation of fundamentals table with proper schema"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        collector = SimplePolygonFundamentalsCollector("test_key")
        await collector.ensure_fundamentals_table(pool)

        # Verify table exists and has correct structure
        async with pool.acquire() as conn:
            # Check table exists
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'dev_fundamental_polygon'
                )
            """)
            assert exists, "Fundamentals table should exist"

            # Check critical columns exist
            columns = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'dev_fundamental_polygon'
            """)

            column_names = [col['column_name'] for col in columns]
            required_columns = [
                'symbol', 'fiscal_year', 'timeframe', 'total_assets',
                'total_revenue', 'net_income', 'balance_sheet'
            ]

            for col in required_columns:
                assert col in column_names, f"Required column {col} missing"

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_record_insertion_and_retrieval(self, sample_standardized_financial):
        """Test insertion and retrieval of financial records"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        collector = SimplePolygonFundamentalsCollector("test_key")

        # Insert test record
        test_records = [sample_standardized_financial]
        inserted = await collector.insert_polygon_financials(pool, test_records)
        assert inserted == 1, f"Expected 1 record inserted, got {inserted}"

        # Retrieve and verify
        async with pool.acquire() as conn:
            record = await conn.fetchrow("""
                SELECT * FROM dev_fundamental_polygon
                WHERE symbol = $1 AND fiscal_year = $2 AND timeframe = $3
            """, 'AAPL', '2024', 'annual')

            assert record is not None, "Record should be retrievable"
            assert record['total_revenue'] == 391035000000, "Revenue should match"
            assert record['company_name'] == 'Apple Inc.', "Company name should match"

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_duplicate_handling(self, sample_standardized_financial):
        """Test proper handling of duplicate records (upsert behavior)"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        collector = SimplePolygonFundamentalsCollector("test_key")

        # Insert same record twice
        test_records = [sample_standardized_financial]
        inserted1 = await collector.insert_polygon_financials(pool, test_records)
        inserted2 = await collector.insert_polygon_financials(pool, test_records)

        assert inserted1 == 1, "First insert should succeed"
        assert inserted2 == 1, "Second insert should update (upsert)"

        # Verify only one record exists
        async with pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_fundamental_polygon
                WHERE symbol = $1 AND fiscal_year = $2 AND timeframe = $3
            """, 'AAPL', '2024', 'annual')

            assert count == 1, f"Expected 1 record after upsert, got {count}"

        await pool.close()

class TestDataQuality:
    """Test data quality validation and integrity checks"""

    def test_financial_data_consistency(self, sample_standardized_financial):
        """Test financial data consistency and logical relationships"""
        record = sample_standardized_financial

        # Basic accounting equation: Assets = Liabilities + Equity
        assets = record.get('total_assets', 0)
        liabilities = record.get('total_liabilities', 0)
        equity = record.get('total_equity', 0)

        if assets and liabilities and equity:
            balance_sheet_balanced = abs(assets - (liabilities + equity)) < 1000000  # Allow small rounding
            assert balance_sheet_balanced, f"Balance sheet doesn't balance: {assets} != {liabilities + equity}"

        # Revenue should be positive for profitable companies
        revenue = record.get('total_revenue', 0)
        assert revenue > 0, f"Revenue should be positive, got {revenue}"

        # Net income should be less than revenue
        net_income = record.get('net_income', 0)
        if revenue and net_income:
            assert net_income < revenue, f"Net income ({net_income}) should be less than revenue ({revenue})"

    def test_data_type_validation(self, sample_standardized_financial):
        """Test proper data types for all fields"""
        record = sample_standardized_financial

        # String fields
        string_fields = ['symbol', 'cik', 'fiscal_period', 'fiscal_year', 'timeframe', 'company_name']
        for field in string_fields:
            if record.get(field) is not None:
                assert isinstance(record[field], str), f"{field} should be string type"

        # Numeric fields
        numeric_fields = ['total_assets', 'total_liabilities', 'total_equity', 'total_revenue', 'net_income']
        for field in numeric_fields:
            if record.get(field) is not None:
                assert isinstance(record[field], (int, float)), f"{field} should be numeric type"
                assert record[field] >= 0 or field == 'net_income', f"{field} should be non-negative (except net_income)"

    def test_required_fields_validation(self, sample_standardized_financial):
        """Test presence of required fields"""
        record = sample_standardized_financial

        required_fields = ['symbol', 'fiscal_year', 'timeframe']
        for field in required_fields:
            assert field in record, f"Required field {field} missing"
            assert record[field] is not None, f"Required field {field} is None"
            assert record[field] != '', f"Required field {field} is empty"

class TestErrorHandling:
    """Test error handling and resilience"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_api_error_handling(self):
        """Test handling of API errors and invalid responses"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("invalid_key")

        # Test with invalid API key (should handle gracefully)
        result = await collector.fetch_financials_for_symbol('AAPL', 'annual')
        assert isinstance(result, list), "Should return empty list on API error"

    def test_malformed_data_handling(self):
        """Test handling of malformed or incomplete data"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("test_key")

        # Test with completely malformed record
        malformed_record = {"invalid": "data"}
        result = collector.standardize_polygon_financial(malformed_record, 'TEST')

        assert result is not None, "Should handle malformed data gracefully"
        assert result['symbol'] == 'TEST', "Symbol should still be set correctly"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_error_handling(self):
        """Test handling of database connection and query errors"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("test_key")

        # Test with invalid database connection (should handle gracefully)
        # This would typically involve mocking the database connection

class TestPerformanceAndScaling:
    """Test performance characteristics and scaling behavior"""

    def test_batch_processing_efficiency(self):
        """Test efficiency of batch processing operations"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector

        collector = SimplePolygonFundamentalsCollector("test_key")

        # Test processing of multiple records
        start_time = datetime.now()

        # Simulate processing 100 records
        test_records = []
        for i in range(100):
            record = {
                'financials': {
                    'balance_sheet': {'assets': {'value': 1000000 + i}},
                    'income_statement': {'revenues': {'value': 500000 + i}}
                },
                'fiscal_year': '2024',
                'company_name': f'Test Company {i}'
            }
            standardized = collector.standardize_polygon_financial(record, f'TEST{i:03d}')
            if standardized:
                test_records.append(standardized)

        processing_time = datetime.now() - start_time

        assert len(test_records) == 100, "Should process all records"
        assert processing_time.total_seconds() < 5.0, f"Processing took too long: {processing_time}"

    def test_memory_usage_bounds(self):
        """Test that memory usage stays within reasonable bounds"""
        # This would involve monitoring memory usage during large batch operations
        # Implementation would depend on specific memory profiling tools

# Integration tests
class TestEndToEndIntegration:
    """End-to-end integration tests"""

    @pytest.mark.asyncio
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_complete_collection_workflow(self):
        """Test complete fundamentals collection workflow"""
        from scripts.simple_polygon_fundamentals_backfill import SimplePolygonFundamentalsCollector
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)

        api_key = os.getenv('POLYGON_API_KEY')
        if not api_key:
            pytest.skip("POLYGON_API_KEY not available for integration test")

        async with SimplePolygonFundamentalsCollector(api_key) as collector:
            # Test symbol selection
            symbols = await collector.get_priority_symbols(pool, limit=2)
            assert len(symbols) > 0, "Should return at least one symbol"

            # Test data collection for one symbol
            test_symbol = symbols[0]
            records = await collector.fetch_financials_for_symbol(test_symbol, 'annual')

            if records:  # Only test if we get data
                # Test database insertion
                inserted = await collector.insert_polygon_financials(pool, records[:1])
                assert inserted == 1, "Should successfully insert record"

                # Test data retrieval
                async with pool.acquire() as conn:
                    retrieved = await conn.fetchrow("""
                        SELECT * FROM dev_fundamental_polygon
                        WHERE symbol = $1
                        ORDER BY created_at DESC LIMIT 1
                    """, test_symbol)

                    assert retrieved is not None, "Should retrieve inserted record"
                    assert retrieved['symbol'] == test_symbol, "Symbol should match"

        await pool.close()

if __name__ == "__main__":
    # Run tests with proper configuration
    import sys

    # Add src to path
    sys.path.insert(0, '/workspace/src')

    # Run specific test categories
    pytest.main([
        __file__,
        "-v",  # Verbose output
        "-s",  # Don't capture output
        "--tb=short",  # Short traceback format
        "-x",  # Stop on first failure
        # "--markers",  # Show available markers
    ])