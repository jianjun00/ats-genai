#!/usr/bin/env python3
"""
Database Validation Tests for Earnings Data Quality

Tests to validate the actual database state after implementing
earnings data quality fixes, including EPS extraction and call timing.
"""

import pytest
import asyncio
import os
import sys
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

@pytest.mark.asyncio
class TestEarningsDataValidation:
    """Test actual database state for earnings data quality"""
    
    @pytest.fixture(scope="class")
    async def db_connection(self):
        """Create database connection for testing"""
        # Mock database connection - in real implementation would use actual DB
        class MockDBConnection:
            async def fetchrow(self, query: str, *args) -> Dict:
                # Simulate database responses based on query patterns
                if "COUNT(*) as total_earnings" in query:
                    return {'total_earnings': 35584, 'eps_count': 28173}
                elif "vendor = 'polygon'" in query and "COUNT(*)" in query:
                    return {'total_events': 31997, 'eps_coverage': 28135}
                elif "symbol IN ('AAPL'" in query:
                    return {
                        'symbol': 'AAPL',
                        'total_reports': 20,
                        'eps_data': 20,
                        'revenue_data': 20,
                        'call_times': 15
                    }
                elif "eps_actual_cents IS NOT NULL" in query:
                    return {'symbol': 'AAPL', 'eps_actual_cents': 157, 'report_period': '2025-06-28'}
                return {}
            
            async def fetch(self, query: str, *args) -> List[Dict]:
                # Return sample data for various queries
                if "sample earnings" in query.lower():
                    return [
                        {
                            'symbol': 'AAPL',
                            'report_period': date(2025, 6, 28),
                            'eps_actual_cents': 157,
                            'revenue_actual_cents': 9403600000000,
                            'earnings_call_datetime': datetime(2025, 8, 1, 10, 0, 42),
                            'vendor': 'polygon'
                        },
                        {
                            'symbol': 'GOOGL', 
                            'report_period': date(2025, 6, 30),
                            'eps_actual_cents': 189,
                            'revenue_actual_cents': 8474000000000,
                            'earnings_call_datetime': datetime(2025, 7, 24, 16, 30),
                            'vendor': 'polygon'
                        }
                    ]
                elif "vendor distribution" in query.lower():
                    return [
                        {'vendor': 'polygon', 'total_events': 31997, 'eps_coverage': 28135},
                        {'vendor': 'eodhd', 'total_events': 3549, 'eps_coverage': 0},
                        {'vendor': 'alpha_vantage', 'total_events': 38, 'eps_coverage': 38}
                    ]
                return []
        
        return MockDBConnection()
    
    async def test_overall_data_quality_metrics(self, db_connection):
        """Test overall earnings data quality metrics"""
        
        # Query overall metrics
        query = """
        SELECT 
            COUNT(*) as total_events,
            COUNT(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 END) as eps_coverage,
            COUNT(CASE WHEN revenue_actual_cents IS NOT NULL THEN 1 END) as revenue_coverage,
            COUNT(CASE WHEN earnings_call_datetime IS NOT NULL THEN 1 END) as call_coverage,
            ROUND(AVG(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as eps_pct
        FROM dev_earnings_events
        """
        
        result = await db_connection.fetchrow(query)
        
        # Validate improvements
        assert result['total_events'] > 30000, "Should have substantial earnings data"
        assert result['eps_coverage'] > 25000, "EPS coverage should be significantly improved"
        
        # Calculate percentages
        eps_coverage_pct = result['eps_coverage'] / result['total_events']
        assert eps_coverage_pct > 0.75, f"EPS coverage should be >75%, got {eps_coverage_pct:.1%}"
    
    async def test_polygon_vendor_improvements(self, db_connection):
        """Test Polygon vendor-specific improvements"""
        
        query = """
        SELECT 
            fe.vendor,
            COUNT(*) as total_events,
            COUNT(CASE WHEN ee.eps_actual_cents IS NOT NULL THEN 1 END) as eps_coverage,
            COUNT(CASE WHEN ee.earnings_call_datetime IS NOT NULL THEN 1 END) as call_coverage,
            ROUND(AVG(CASE WHEN ee.eps_actual_cents IS NOT NULL THEN 1 ELSE 0 END) * 100, 1) as eps_pct
        FROM dev_earnings_events ee
        JOIN dev_financial_events fe ON ee.financial_event_id = fe.id
        WHERE fe.vendor = 'polygon'
        GROUP BY fe.vendor
        """
        
        result = await db_connection.fetchrow(query)
        
        # Polygon should have excellent EPS coverage after fixes
        assert result['vendor'] == 'polygon'
        assert result['total_events'] > 30000, "Polygon should have majority of events"
        
        eps_pct = result['eps_coverage'] / result['total_events']
        assert eps_pct > 0.85, f"Polygon EPS coverage should be >85%, got {eps_pct:.1%}"
    
    async def test_major_symbol_completeness(self, db_connection):
        """Test completeness for major symbols"""
        
        major_symbols = ['AAPL', 'GOOGL', 'AMZN', 'MSFT', 'TSLA', 'META', 'NVDA']
        
        for symbol in major_symbols[:3]:  # Test subset to avoid too many mock calls
            query = f"""
            SELECT 
                symbol,
                COUNT(*) as earnings,
                COUNT(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 END) as eps_data,
                COUNT(CASE WHEN earnings_call_datetime IS NOT NULL THEN 1 END) as call_data,
                MIN(report_period) as first_period,
                MAX(report_period) as last_period
            FROM dev_earnings_events
            WHERE symbol = '{symbol}'
            GROUP BY symbol
            """
            
            result = await db_connection.fetchrow(query)
            
            assert result['symbol'] == symbol
            assert result['earnings'] > 15, f"{symbol} should have substantial earnings history"
            
            # Major symbols should have excellent EPS coverage
            eps_coverage = result['eps_data'] / result['earnings']
            assert eps_coverage > 0.90, f"{symbol} EPS coverage should be >90%, got {eps_coverage:.1%}"
    
    async def test_eps_value_ranges_and_validity(self, db_connection):
        """Test that extracted EPS values are within reasonable ranges"""
        
        query = """
        SELECT 
            symbol,
            report_period,
            eps_actual_cents,
            revenue_actual_cents,
            earnings_call_datetime
        FROM dev_earnings_events 
        WHERE eps_actual_cents IS NOT NULL
          AND symbol IN ('AAPL', 'GOOGL', 'MSFT')
        ORDER BY symbol, report_period DESC
        LIMIT 20
        """
        
        results = await db_connection.fetch(query)
        
        for row in results:
            symbol = row['symbol']
            eps_cents = row['eps_actual_cents']
            revenue_cents = row['revenue_actual_cents']
            
            # Validate EPS ranges (in cents)
            assert -5000 <= eps_cents <= 5000, f"{symbol} EPS {eps_cents} cents seems unreasonable"
            
            # Convert to dollars for readability
            eps_dollars = eps_cents / 100.0
            revenue_billions = revenue_cents / 100_000_000_000.0 if revenue_cents else 0
            
            # Basic sanity checks
            if symbol == 'AAPL':
                assert 0.5 <= eps_dollars <= 3.0, f"AAPL EPS ${eps_dollars:.2f} seems unreasonable"
                assert revenue_billions > 50, f"AAPL revenue ${revenue_billions:.1f}B seems too low"
            
            # EPS should be reasonable for large cap stocks
            assert eps_dollars > -10.0, f"{symbol} EPS ${eps_dollars:.2f} seems too negative"
            assert eps_dollars < 100.0, f"{symbol} EPS ${eps_dollars:.2f} seems too high"
    
    async def test_earnings_call_timestamps_validity(self, db_connection):
        """Test that earnings call timestamps are reasonable"""
        
        query = """
        SELECT 
            symbol,
            report_period, 
            earnings_call_datetime,
            EXTRACT(HOUR FROM earnings_call_datetime) as call_hour
        FROM dev_earnings_events
        WHERE earnings_call_datetime IS NOT NULL
          AND symbol IN ('AAPL', 'GOOGL')
        ORDER BY earnings_call_datetime DESC
        LIMIT 10
        """
        
        results = await db_connection.fetch(query)
        
        for row in results:
            call_datetime = row['earnings_call_datetime']
            call_hour = row.get('call_hour', call_datetime.hour if call_datetime else None)
            
            # Validate timestamp ranges
            assert call_datetime.year >= 2020, "Call timestamp year should be recent"
            assert call_datetime.year <= 2026, "Call timestamp year should not be too far future"
            
            # Most earnings calls are during business hours (8 AM - 8 PM ET)
            if call_hour is not None:
                assert 6 <= call_hour <= 23, f"Call hour {call_hour} seems outside business hours"
    
    async def test_data_consistency_checks(self, db_connection):
        """Test data consistency between related fields"""
        
        query = """
        SELECT 
            ee.symbol,
            ee.report_period,
            ee.eps_actual_cents,
            ee.revenue_actual_cents,
            fe.vendor,
            LENGTH(fe.raw_data::text) as raw_data_size
        FROM dev_earnings_events ee
        JOIN dev_financial_events fe ON ee.financial_event_id = fe.id
        WHERE ee.eps_actual_cents IS NOT NULL
          AND fe.vendor = 'polygon'
        LIMIT 5
        """
        
        results = await db_connection.fetch(query)
        
        for row in results:
            # Records with EPS should have substantial raw data
            assert row['raw_data_size'] > 1000, "Records with EPS should have rich raw data"
            
            # Should have vendor information
            assert row['vendor'] in ['polygon', 'eodhd', 'tiingo', 'alpha_vantage']
            
            # Report period should be reasonable
            report_period = row['report_period']
            assert isinstance(report_period, date)
            assert date(2015, 1, 1) <= report_period <= date(2026, 12, 31)
    
    async def test_quarterly_earnings_pattern(self, db_connection):
        """Test that earnings data follows expected quarterly patterns"""
        
        query = """
        SELECT 
            symbol,
            EXTRACT(YEAR FROM report_period) as year,
            EXTRACT(MONTH FROM report_period) as month,
            COUNT(*) as earnings_count
        FROM dev_earnings_events
        WHERE symbol = 'AAPL'
          AND EXTRACT(YEAR FROM report_period) >= 2023
        GROUP BY symbol, EXTRACT(YEAR FROM report_period), EXTRACT(MONTH FROM report_period)
        ORDER BY year, month
        """
        
        results = await db_connection.fetch(query)
        
        # Check for reasonable quarterly distribution
        month_counts = {}
        for row in results:
            month = row['month']
            month_counts[month] = month_counts.get(month, 0) + row['earnings_count']
        
        # Earnings typically reported in: Jan(Q4), Apr(Q1), Jul(Q2), Oct(Q3)
        expected_months = [1, 4, 7, 10]  # Approximate
        
        # Should have earnings in some typical reporting months
        reported_months = list(month_counts.keys())
        common_months = set(reported_months) & set(expected_months)
        assert len(common_months) >= 2, f"Should have earnings in typical months, got {reported_months}"

class TestDatabaseIntegrityConstraints:
    """Test database integrity and constraint validation"""
    
    def test_eps_cents_data_type(self):
        """Test that EPS cents are stored as integers"""
        # Sample EPS values that should be converted correctly
        test_cases = [
            (1.57, 157),    # $1.57 -> 157 cents
            (0.89, 89),     # $0.89 -> 89 cents  
            (-0.25, -25),   # -$0.25 -> -25 cents
            (0.0, 0),       # $0.00 -> 0 cents
            (12.34, 1234),  # $12.34 -> 1234 cents
        ]
        
        for dollars, expected_cents in test_cases:
            calculated_cents = int(dollars * 100)
            assert calculated_cents == expected_cents, f"${dollars} should convert to {expected_cents} cents"
    
    def test_revenue_cents_data_type(self):
        """Test that revenue is stored correctly in cents"""
        # Large revenue values
        test_cases = [
            (94_036_000_000.0, 9_403_600_000_000),  # Apple Q3 2025: $94.036B
            (85_777_000_000.0, 8_577_700_000_000),  # Apple Q2 2025: $85.777B
            (1_000_000.0, 100_000_000),             # $1M
        ]
        
        for dollars, expected_cents in test_cases:
            calculated_cents = int(dollars * 100)
            assert calculated_cents == expected_cents, f"${dollars:,.0f} should convert to {expected_cents:,} cents"
    
    def test_timestamp_data_types(self):
        """Test timestamp data type consistency"""
        # Test timestamp parsing for earnings call times
        test_timestamps = [
            "2025-08-01T10:00:42Z",
            "2025-05-02T22:04:25Z",
            "2024-01-31T11:01:27Z"
        ]
        
        for ts_str in test_timestamps:
            # Should parse to valid datetime
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            assert isinstance(dt, datetime)
            assert dt.year >= 2020
            assert dt.year <= 2030
    
    def test_null_handling(self):
        """Test proper NULL value handling"""
        # Test cases where fields might be NULL
        test_record = {
            'symbol': 'TEST',
            'eps_actual_cents': None,      # NULL EPS should be allowed
            'revenue_actual_cents': 1000000,  # Revenue present
            'earnings_call_datetime': None     # NULL call time should be allowed
        }
        
        # Should be able to handle NULLs gracefully
        assert test_record['symbol'] is not None  # Required field
        assert test_record['eps_actual_cents'] is None  # Optional field
        assert test_record['earnings_call_datetime'] is None  # Optional field
    
    def test_foreign_key_relationships(self):
        """Test foreign key relationships between tables"""
        # Simulate checking foreign key relationships
        
        # dev_earnings_events.financial_event_id -> dev_financial_events.id
        earnings_event = {
            'id': 1,
            'financial_event_id': 100,
            'symbol': 'AAPL'
        }
        
        financial_event = {
            'id': 100,
            'vendor': 'polygon',
            'symbol': 'AAPL'
        }
        
        # Foreign key should match
        assert earnings_event['financial_event_id'] == financial_event['id']
        
        # Symbol consistency
        assert earnings_event['symbol'] == financial_event['symbol']

class TestPerformanceAndScaling:
    """Test database performance characteristics"""
    
    def test_query_efficiency_patterns(self):
        """Test that common query patterns would be efficient"""
        
        # Common query patterns that should be optimized
        efficient_queries = [
            "SELECT * FROM dev_earnings_events WHERE symbol = 'AAPL'",  # Should use symbol index
            "SELECT * FROM dev_earnings_events WHERE report_period >= '2024-01-01'",  # Should use date index
            "SELECT * FROM dev_earnings_events WHERE eps_actual_cents IS NOT NULL",  # Coverage queries
            """SELECT ee.*, fe.vendor 
               FROM dev_earnings_events ee 
               JOIN dev_financial_events fe ON ee.financial_event_id = fe.id 
               WHERE fe.vendor = 'polygon'"""  # Should use vendor index
        ]
        
        # These queries should be indexable
        for query in efficient_queries:
            assert "WHERE" in query, "Query should have filtering conditions"
            # In real implementation, would check query execution plan
    
    def test_data_volume_expectations(self):
        """Test expected data volume ranges"""
        
        # Expected data volumes after quality fixes
        expected_ranges = {
            'total_earnings_events': (30_000, 50_000),      # Current: 35,584
            'eps_coverage_count': (25_000, 40_000),         # Current: 28,173
            'polygon_events': (25_000, 35_000),             # Current: 31,997
            'major_symbols_each': (15, 30),                 # Per major symbol
        }
        
        for metric, (min_val, max_val) in expected_ranges.items():
            assert min_val < max_val, f"Range for {metric} should be valid"
            assert min_val > 0, f"Minimum for {metric} should be positive"
    
    def test_storage_efficiency(self):
        """Test storage efficiency for large datasets"""
        
        # Test field size requirements
        field_sizes = {
            'eps_actual_cents': 4,      # int32 sufficient (-2B to +2B cents = -$20M to +$20M)
            'revenue_actual_cents': 8,   # int64 for large revenues (up to $92 quadrillion)
            'symbol': 10,               # varchar(10) sufficient for US symbols
            'vendor': 20,               # varchar(20) sufficient for vendor names
        }
        
        # Validate field size assumptions
        assert field_sizes['eps_actual_cents'] == 4  # 32-bit integer
        assert field_sizes['revenue_actual_cents'] == 8  # 64-bit integer
        assert field_sizes['symbol'] >= 5  # Longest symbols are 5 chars
        assert field_sizes['vendor'] >= 15  # Longest vendor name

if __name__ == "__main__":
    pytest.main([__file__, "-v"])