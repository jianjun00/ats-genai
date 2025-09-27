#!/usr/bin/env python3
"""
Test Cases for Tiingo End Date Fix

Validates that Tiingo end_date field is correctly interpreted:
- Recent end_date (within 7 days) = active data feed, should be NULL
- Old end_date (> 7 days) = actual delisting date, should be preserved
- NULL end_date = active instrument, should remain NULL

Critical test cases based on known market facts:
- GM: Should have NULL end_date (actively traded)
- DELL: Should have NULL end_date (actively traded)
- F (Ford): Should have NULL end_date (actively traded)
- BBBYQ: Should have 2023-09-29 end_date (actually delisted)
"""

import sys
import os
import pytest
import asyncpg
from datetime import datetime, date, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

class TestTiingoEndDateFix:
    """Test cases for Tiingo end_date interpretation fix"""

    @pytest.fixture
    async def db_connection(self):
        """Database connection fixture"""
        conn = await asyncpg.connect(
            host='localhost',
            port=5433,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        yield conn
        await conn.close()

    @pytest.mark.asyncio

    async def test_active_companies_have_null_end_date(self, db_connection):
        """Test that actively traded companies have NULL end_date after fix"""

        # Known actively traded companies that should have NULL end_date
        active_symbols = ['GM', 'DELL', 'F', 'AAPL', 'MSFT', 'GOOGL', 'TSLA']

        query = """
        SELECT symbol, end_date,
               CASE WHEN end_date IS NULL THEN 'NULL' ELSE end_date::text END as end_date_str
        FROM dev_instrument_tiingo
        WHERE symbol = ANY($1)
        ORDER BY symbol
        """

        results = await db_connection.fetch(query, active_symbols)

        # Validate results
        for row in results:
            symbol = row['symbol']
            end_date = row['end_date']

            # These companies should have NULL end_date (actively traded)
            assert end_date is None, f"❌ {symbol} should have NULL end_date but has {row['end_date_str']}"
            print(f"✅ {symbol}: Correctly has NULL end_date (actively traded)")

    @pytest.mark.asyncio

    async def test_delisted_companies_have_correct_end_date(self, db_connection):
        """Test that actually delisted companies have correct end_date preserved"""

        # Known delisted companies with actual delisting dates
        expected_delistings = {
            'BBBYQ': date(2023, 9, 29),  # Bed Bath & Beyond - Sept 29, 2023
            # Add more when we find them in the data
        }

        for symbol, expected_date in expected_delistings.items():
            query = "SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = $1"
            result = await db_connection.fetchrow(query, symbol)

            if result:
                actual_date = result['end_date']
                assert actual_date == expected_date, \
                    f"❌ {symbol} should have end_date {expected_date} but has {actual_date}"
                print(f"✅ {symbol}: Correctly has end_date {actual_date} (actually delisted)")
            else:
                print(f"⚠️ {symbol}: Not found in database")

    @pytest.mark.asyncio

    async def test_no_recent_end_dates_for_active_stocks(self, db_connection):
        """Test that no actively traded stocks have recent end_dates (within 7 days)"""

        cutoff_date = datetime.now().date() - timedelta(days=7)

        query = """
        SELECT symbol, end_date, name
        FROM dev_instrument_tiingo
        WHERE end_date > $1
        AND symbol IN (
            -- Major active stocks that should never have recent end_dates
            'GM', 'DELL', 'F', 'AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'AMZN', 'META'
        )
        ORDER BY symbol
        """

        results = await db_connection.fetch(query, cutoff_date)

        # Should be no results - active stocks shouldn't have recent end_dates
        assert len(results) == 0, \
            f"❌ Found {len(results)} active stocks with recent end_dates: {[r['symbol'] for r in results]}"

        print(f"✅ No actively traded stocks have end_dates within last 7 days")

    @pytest.mark.asyncio

    async def test_end_date_interpretation_logic(self, db_connection):
        """Test the core logic for interpreting Tiingo end_dates"""

        today = datetime.now().date()
        week_ago = today - timedelta(days=7)

        query = """
        SELECT
            symbol,
            end_date,
            CASE
                WHEN end_date IS NULL THEN 'active_null'
                WHEN end_date > $1 THEN 'recent_should_be_null'
                ELSE 'old_preserve_date'
            END as interpretation,
            CASE
                WHEN end_date IS NULL THEN NULL
                WHEN end_date > $1 THEN NULL  -- Recent dates should become NULL
                ELSE end_date  -- Old dates preserved
            END as corrected_end_date
        FROM dev_instrument_tiingo
        WHERE symbol IN ('GM', 'DELL', 'F', 'BBBYQ')
        ORDER BY symbol
        """

        results = await db_connection.fetch(query, week_ago)

        expected_interpretations = {
            'GM': 'active_null',      # Should be NULL (actively traded)
            'DELL': 'active_null',    # Should be NULL (actively traded)
            'F': 'active_null',       # Should be NULL (actively traded)
            'BBBYQ': 'old_preserve_date'  # Should preserve actual delisting date
        }

        for row in results:
            symbol = row['symbol']
            interpretation = row['interpretation']
            expected = expected_interpretations.get(symbol)

            if expected:
                if expected == 'active_null':
                    assert interpretation in ['active_null', 'recent_should_be_null'], \
                        f"❌ {symbol}: Expected NULL or recent date, got {interpretation}"
                    assert row['corrected_end_date'] is None, \
                        f"❌ {symbol}: Corrected end_date should be NULL"
                elif expected == 'old_preserve_date':
                    assert interpretation == 'old_preserve_date', \
                        f"❌ {symbol}: Expected old date preservation, got {interpretation}"
                    assert row['corrected_end_date'] is not None, \
                        f"❌ {symbol}: Corrected end_date should be preserved"

                print(f"✅ {symbol}: {interpretation} -> corrected_end_date: {row['corrected_end_date']}")

    @pytest.mark.asyncio

    async def test_historical_accuracy_validation(self, db_connection):
        """Test against known historical market events"""

        # Test cases with known market facts
        historical_validations = [
            {
                'symbol': 'GM',
                'expected_start_date': date(2010, 11, 18),  # GM IPO
                'expected_end_date': None,  # Still active
                'description': 'General Motors IPO November 18, 2010'
            },
            {
                'symbol': 'DELL',
                'expected_start_date': date(2018, 12, 21),  # Dell return to public markets
                'expected_end_date': None,  # Still active
                'description': 'Dell Technologies return December 21, 2018'
            },
            {
                'symbol': 'BBBYQ',
                'expected_end_date': date(2023, 9, 29),  # Bed Bath & Beyond delisting
                'description': 'Bed Bath & Beyond final delisting September 29, 2023'
            }
        ]

        for validation in historical_validations:
            symbol = validation['symbol']
            query = "SELECT symbol, start_date, end_date, name FROM dev_instrument_tiingo WHERE symbol = $1"
            result = await db_connection.fetchrow(query, symbol)

            if result:
                # Check start_date if specified
                if 'expected_start_date' in validation:
                    expected_start = validation['expected_start_date']
                    actual_start = result['start_date']
                    assert actual_start == expected_start, \
                        f"❌ {symbol} start_date: expected {expected_start}, got {actual_start}"

                # Check end_date
                expected_end = validation['expected_end_date']
                actual_end = result['end_date']
                assert actual_end == expected_end, \
                    f"❌ {symbol} end_date: expected {expected_end}, got {actual_end}"

                print(f"✅ {symbol}: {validation['description']} - VALIDATED")
            else:
                print(f"⚠️ {symbol}: Not found in database")

    @pytest.mark.asyncio

    async def test_data_quality_metrics(self, db_connection):
        """Test overall data quality metrics after fix"""

        query = """
        SELECT
            COUNT(*) as total_instruments,
            COUNT(CASE WHEN end_date IS NULL THEN 1 END) as active_instruments,
            COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) as delisted_instruments,
            COUNT(CASE WHEN end_date > CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_end_dates,
            ROUND(
                COUNT(CASE WHEN end_date IS NULL THEN 1 END)::numeric / COUNT(*)::numeric * 100, 2
            ) as active_percentage
        FROM dev_instrument_tiingo
        """

        result = await db_connection.fetchrow(query)

        total = result['total_instruments']
        active = result['active_instruments']
        delisted = result['delisted_instruments']
        recent_end_dates = result['recent_end_dates']
        active_pct = result['active_percentage']

        print(f"📊 Tiingo Data Quality Metrics:")
        print(f"   Total instruments: {total:,}")
        print(f"   Active (NULL end_date): {active:,} ({active_pct}%)")
        print(f"   Delisted (has end_date): {delisted:,}")
        print(f"   Recent end_dates (problematic): {recent_end_dates:,}")

        # Quality thresholds
        assert active_pct > 70.0, f"❌ Active percentage too low: {active_pct}% (expected >70%)"
        assert recent_end_dates == 0, f"❌ Found {recent_end_dates} instruments with recent end_dates"

        print(f"✅ Data quality metrics within acceptable ranges")

if __name__ == "__main__":
    import asyncio

    async def run_tests():
        test_instance = TestTiingoEndDateFix()

        # Get database connection
        conn = await asyncpg.connect(
            host='localhost',
            port=5433,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )

        print("🧪 Running Tiingo End Date Fix Test Suite")
        print("=" * 60)

        # Run all tests
        await test_instance.test_active_companies_have_null_end_date(conn)
        await test_instance.test_delisted_companies_have_correct_end_date(conn)
        await test_instance.test_no_recent_end_dates_for_active_stocks(conn)
        await test_instance.test_end_date_interpretation_logic(conn)
        await test_instance.test_historical_accuracy_validation(conn)
        await test_instance.test_data_quality_metrics(conn)

        print("=" * 60)
        print("✅ ALL TESTS PASSED - Tiingo end_date fix working correctly")

    asyncio.run(run_tests())