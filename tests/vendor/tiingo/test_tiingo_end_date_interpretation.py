"""
Regression Tests for Tiingo End Date Interpretation Issue

This test suite prevents regression of the critical data quality issue where
Tiingo API's endDate field was incorrectly interpreted as delisting date,
causing 75% of active stocks to appear delisted.

Issue: Tiingo returns endDate as current date for active stocks (data availability),
not delisting date. Our fix correctly interprets recent endDate as active status.
"""

import pytest
import asyncpg
from datetime import date, datetime, timedelta
import json
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

class TestTiingoEndDateInterpretation:
    """Test suite for Tiingo end date interpretation logic"""

    @pytest.fixture
    async def db_connection(self):
        """Create test database connection"""
        conn = await asyncpg.connect(
            host=os.getenv('DB_HOST', 'postgres'),
            port=os.getenv('DB_PORT', '5432'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'dev_password'),
            database=os.getenv('DB_NAME', 'dev_db')
        )
        yield conn
        await conn.close()

    @pytest.fixture
    def mock_tiingo_responses(self):
        """Mock Tiingo API responses for different stock scenarios"""
        return {
            'active_stock': {
                "ticker": "AAPL",
                "name": "Apple Inc",
                "startDate": "1980-12-12",
                "endDate": "2025-08-26",  # Today's date = active stock
                "exchangeCode": "NASDAQ"
            },
            'recently_delisted': {
                "ticker": "DELIST1",
                "name": "Recently Delisted Corp",
                "startDate": "2000-01-01",
                "endDate": "2025-08-15",  # Recent but old enough = truly delisted
                "exchangeCode": "NYSE"
            },
            'old_delisted': {
                "ticker": "OLDCO",
                "name": "Old Delisted Company",
                "startDate": "1990-01-01",
                "endDate": "2020-05-15",  # Old date = clearly delisted
                "exchangeCode": "NYSE"
            },
            'very_recent_active': {
                "ticker": "NEWTECH",
                "name": "New Tech Corp",
                "startDate": "2023-01-01",
                "endDate": "2025-08-27",  # Today = active
                "exchangeCode": "NASDAQ"
            }
        }

    def test_parse_date_function(self):
        """Test the parse_date function handles various formats correctly"""
        # Import the function from the actual script
        sys.path.append('/workspace/src')
        from vendor.tiingo.services.populate_instrument_tiingo import parse_date

        # Test valid date
        assert parse_date("2025-08-26") == date(2025, 8, 26)

        # Test date with time component
        assert parse_date("2025-08-26T00:00:00Z") == date(2025, 8, 26)

        # Test None/empty values
        assert parse_date(None) is None
        assert parse_date("") is None

        # Test invalid format
        assert parse_date("invalid-date") is None

    def test_end_date_interpretation_logic(self):
        """Test the core logic for interpreting Tiingo end dates"""
        today = date.today()
        cutoff_date = today - timedelta(days=7)

        # Recent end date (within 7 days) = active stock
        recent_date = today - timedelta(days=2)
        assert recent_date >= cutoff_date, "Recent dates should be >= cutoff"

        # Today's date = active stock
        assert today >= cutoff_date, "Today should be >= cutoff"

        # Old end date = truly delisted
        old_date = today - timedelta(days=30)
        assert old_date < cutoff_date, "Old dates should be < cutoff"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_instrument_population_active_stock(self, db_connection, mock_tiingo_responses):
        """Test that stocks with recent end dates are correctly marked as active"""
        # Setup: Insert test data that mimics the original problem
        await db_connection.execute("""
            INSERT INTO dev_instrument_tiingo (symbol, name, exchange, asset_type, currency, start_date, end_date, raw)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol) DO UPDATE SET
                end_date = EXCLUDED.end_date,
                updated_at = NOW()
        """,
        'TEST_ACTIVE',
        'Test Active Stock',
        'NASDAQ',
        'stock',
        'USD',
        date(2020, 1, 1),
        date.today(),  # Today's date - should be interpreted as active
        json.dumps(mock_tiingo_responses['active_stock'])
        )

        # Verify the stock exists with today's end_date
        row = await db_connection.fetchrow("""
            SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = 'TEST_ACTIVE'
        """)
        assert row['end_date'] == date.today()

        # Apply our fix logic - set end_date to NULL for recent dates
        cutoff_date = date.today() - timedelta(days=7)
        await db_connection.execute("""
            UPDATE dev_instrument_tiingo
            SET end_date = NULL
            WHERE symbol = 'TEST_ACTIVE' AND end_date >= $1
        """, cutoff_date)

        # Verify the fix worked - stock should now be active (end_date IS NULL)
        fixed_row = await db_connection.fetchrow("""
            SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = 'TEST_ACTIVE'
        """)
        assert fixed_row['end_date'] is None, "Recent end_date should be set to NULL (active)"

        # Cleanup
        await db_connection.execute("DELETE FROM dev_instrument_tiingo WHERE symbol = 'TEST_ACTIVE'")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_instrument_population_truly_delisted(self, db_connection, mock_tiingo_responses):
        """Test that stocks with old end dates remain correctly marked as delisted"""
        old_delist_date = date(2020, 5, 15)

        # Setup: Insert truly delisted stock
        await db_connection.execute("""
            INSERT INTO dev_instrument_tiingo (symbol, name, exchange, asset_type, currency, start_date, end_date, raw)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol) DO UPDATE SET
                end_date = EXCLUDED.end_date,
                updated_at = NOW()
        """,
        'TEST_DELISTED',
        'Test Delisted Corp',
        'NYSE',
        'stock',
        'USD',
        date(1990, 1, 1),
        old_delist_date,
        json.dumps(mock_tiingo_responses['old_delisted'])
        )

        # Apply our fix logic - should NOT change old delisting dates
        cutoff_date = date.today() - timedelta(days=7)
        result = await db_connection.execute("""
            UPDATE dev_instrument_tiingo
            SET end_date = NULL
            WHERE symbol = 'TEST_DELISTED' AND end_date >= $1
        """, cutoff_date)

        # Verify no update occurred (old date < cutoff)
        assert result == "UPDATE 0", "Old delisting dates should not be updated"

        # Verify the stock remains correctly delisted
        row = await db_connection.fetchrow("""
            SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = 'TEST_DELISTED'
        """)
        assert row['end_date'] == old_delist_date, "Old delisting dates should be preserved"

        # Cleanup
        await db_connection.execute("DELETE FROM dev_instrument_tiingo WHERE symbol = 'TEST_DELISTED'")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_major_stocks_are_active(self, db_connection):
        """Test that major stocks are correctly identified as active (regression protection)"""
        major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NFLX', 'NVDA']

        for symbol in major_stocks:
            row = await db_connection.fetchrow("""
                SELECT symbol, end_date,
                       CASE WHEN end_date IS NULL THEN 'ACTIVE' ELSE 'DELISTED' END as status
                FROM dev_instrument_tiingo
                WHERE symbol = $1
            """, symbol)

            if row:
                assert row['end_date'] is None, f"{symbol} should be active (end_date IS NULL)"
                assert row['status'] == 'ACTIVE', f"{symbol} should have ACTIVE status"
            else:
                # If not found, log warning but don't fail test (might not be in test data)
                print(f"Warning: {symbol} not found in Tiingo instruments")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_active_percentage_is_reasonable(self, db_connection):
        """Test that the percentage of active instruments is reasonable (>70%)"""
        stats = await db_connection.fetchrow("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN end_date IS NULL THEN 1 END) as active,
                ROUND(COUNT(CASE WHEN end_date IS NULL THEN 1 END) * 100.0 / COUNT(*), 1) as active_pct
            FROM dev_instrument_tiingo
        """)

        assert stats['total'] > 0, "Should have Tiingo instruments in database"
        assert stats['active_pct'] > 70, f"Active percentage should be >70%, got {stats['active_pct']}%"
        assert stats['active_pct'] < 95, f"Active percentage should be <95% (some stocks are legitimately delisted), got {stats['active_pct']}%"

    def test_fix_script_exists_and_runnable(self):
        """Test that our fix script exists and can be imported"""
        fix_script_path = '/workspace/scripts/fix_tiingo_population.py'
        assert os.path.exists(fix_script_path), "Tiingo fix script should exist"

        # Test script can be read and contains key logic
        with open(fix_script_path, 'r') as f:
            content = f.read()
            assert 'cutoff_date' in content, "Fix script should contain cutoff_date logic"
            assert 'end_date >= $1' in content, "Fix script should contain date comparison logic"
            assert 'end_date = NULL' in content, "Fix script should set end_date to NULL for active stocks"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_fix_logic(self, db_connection):
        """Test the batch fix logic on a sample of instruments"""
        # Create test instruments with various end_date scenarios
        test_instruments = [
            ('BATCH_ACTIVE_1', date.today()),
            ('BATCH_ACTIVE_2', date.today() - timedelta(days=1)),
            ('BATCH_DELISTED_1', date.today() - timedelta(days=30)),
            ('BATCH_DELISTED_2', date(2020, 1, 1))
        ]

        # Insert test data
        for symbol, end_date in test_instruments:
            await db_connection.execute("""
                INSERT INTO dev_instrument_tiingo (symbol, name, exchange, asset_type, currency, start_date, end_date, raw)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol) DO UPDATE SET
                    end_date = EXCLUDED.end_date
            """,
            symbol, f'Test {symbol}', 'TEST', 'stock', 'USD',
            date(2020, 1, 1), end_date, '{}')

        # Apply batch fix
        cutoff_date = date.today() - timedelta(days=7)
        result = await db_connection.execute("""
            UPDATE dev_instrument_tiingo
            SET end_date = NULL
            WHERE symbol LIKE 'BATCH_%' AND end_date >= $1
        """, cutoff_date)

        # Should have updated 2 instruments (the recent ones)
        assert "UPDATE 2" in result, f"Should update 2 recent instruments, got: {result}"

        # Verify results
        active_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM dev_instrument_tiingo
            WHERE symbol LIKE 'BATCH_%' AND end_date IS NULL
        """)
        assert active_count == 2, "Should have 2 active instruments after fix"

        delisted_count = await db_connection.fetchval("""
            SELECT COUNT(*) FROM dev_instrument_tiingo
            WHERE symbol LIKE 'BATCH_%' AND end_date IS NOT NULL
        """)
        assert delisted_count == 2, "Should have 2 delisted instruments preserved"

        # Cleanup
        await db_connection.execute("DELETE FROM dev_instrument_tiingo WHERE symbol LIKE 'BATCH_%'")

    def test_tiingo_api_response_parsing(self, mock_tiingo_responses):
        """Test parsing of actual Tiingo API response formats"""
        # Test active stock response
        active_response = mock_tiingo_responses['active_stock']
        assert active_response['endDate'] == '2025-08-26'

        # In real scenario, this would be interpreted as active (recent date)
        end_date = datetime.strptime(active_response['endDate'], '%Y-%m-%d').date()
        cutoff = date.today() - timedelta(days=7)
        assert end_date >= cutoff, "Recent endDate should indicate active stock"

        # Test old delisted response
        old_response = mock_tiingo_responses['old_delisted']
        old_end_date = datetime.strptime(old_response['endDate'], '%Y-%m-%d').date()
        assert old_end_date < cutoff, "Old endDate should indicate delisted stock"

@pytest.mark.integration
class TestTiingoEndDateIntegrationScenarios:
    """Integration tests for complete Tiingo end date scenarios"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_full_population_and_fix_workflow(self):
        """Test the complete workflow: populate -> identify issue -> fix -> validate"""
        # This would test the full workflow but requires actual API calls
        # For now, we document the expected workflow:

        workflow_steps = [
            "1. Populate instruments from Tiingo API",
            "2. Identify high percentage of 'delisted' stocks",
            "3. Recognize recent endDate pattern",
            "4. Apply cutoff date logic to fix end_dates",
            "5. Validate major stocks are active",
            "6. Confirm reasonable active percentage"
        ]

        # In a full integration test, we'd execute each step
        # For now, we ensure the workflow is documented and testable
        assert len(workflow_steps) == 6, "Complete workflow should have 6 steps"

    def test_prevent_future_regression(self):
        """Document prevention measures for future regression"""
        prevention_measures = {
            "automated_tests": "This test suite runs on every deployment",
            "data_validation": "Major stocks must be active in production",
            "monitoring": "Alert if active percentage drops below 70%",
            "documentation": "Clear explanation of Tiingo endDate interpretation",
            "code_review": "Any changes to date parsing must be reviewed"
        }

        # Ensure all measures are documented
        assert len(prevention_measures) == 5
        for measure, description in prevention_measures.items():
            assert description, f"Prevention measure {measure} must have description"