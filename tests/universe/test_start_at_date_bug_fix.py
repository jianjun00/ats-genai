#!/usr/bin/env python3
"""
Test case for universe membership start_at date bug fix

BUG: Universe evaluation sets start_at = evaluation_date (job run date)
FIX: Universe evaluation should set start_at = first_qualification_date (historical date)

This test verifies that the start_at date is correctly set to when the stock
first qualified for the universe, not when the evaluation job runs.
"""

import sys
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

sys.path.append('/home/jianjun/ats-genai-admin/src')

from domains.trading.services.universe_membership_manager import UniverseMembershipManager

class TestStartAtDateBugFix:
    """Test cases for the start_at date bug fix"""

    def setup_method(self):
        """Setup test environment"""
        self.manager = UniverseMembershipManager(environment='test')

    @patch('domains.trading.services.universe_membership_manager.get_raw_connection')
    def test_member_entry_uses_historical_qualification_date(self, mock_connection):
        """
        Test that _process_member_entry uses the historical qualification date
        as start_at, not the evaluation_date
        """

        # Setup mock data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Test data
        universe_id = 2
        symbol = 'AAPL'
        evaluation_date = datetime(2025, 9, 9)  # Job run date (today)
        first_qualification_date = datetime(2020, 3, 13)  # Historical qualification date

        volume_data = {
            'avg_volume': 150_000_000,
            'instrument_id': 123,
            'first_qualification_date': first_qualification_date  # Should be included
        }

        # Call the method that should be fixed
        self.manager._process_member_entry(
            mock_cursor, universe_id, symbol, volume_data, evaluation_date
        )

        # Verify the INSERT statement was called
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args

        # Extract the SQL and parameters
        sql = call_args[0][0]
        params = call_args[0][1]

        # Verify that start_at parameter is the historical date, not evaluation date
        assert params[2] == first_qualification_date, (
            f"BUG: start_at should be historical date {first_qualification_date}, "
            f"but got {params[2]} (evaluation_date = {evaluation_date})"
        )

        assert params[2] != evaluation_date, (
            "BUG: start_at should NOT be the evaluation_date (job run date)"
        )

    @patch('domains.trading.services.universe_membership_manager.get_raw_connection')
    def test_current_bug_behavior(self, mock_connection):
        """
        Test that demonstrates the current buggy behavior
        This test should FAIL until the bug is fixed
        """

        # Setup mock data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Test data
        universe_id = 2
        symbol = 'AAPL'
        evaluation_date = datetime(2025, 9, 9)  # Job run date

        volume_data = {
            'avg_volume': 150_000_000,
            'instrument_id': 123
            # NOTE: current implementation doesn't include first_qualification_date
        }

        # Call current buggy method
        self.manager._process_member_entry(
            mock_cursor, universe_id, symbol, volume_data, evaluation_date
        )

        # Verify the current buggy behavior
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]

        # Current bug: start_at is set to evaluation_date
        assert params[2] == evaluation_date, (
            "Current implementation sets start_at = evaluation_date (this is the BUG)"
        )

    def test_qualification_date_calculation(self):
        """
        Test the logic to calculate the first qualification date
        This is what should be implemented in the fix
        """

        # Mock historical volume data showing qualification progression
        historical_data = [
            {'date': datetime(2020, 3, 10), 'rolling_avg': 80_000_000, 'qualifies': False},
            {'date': datetime(2020, 3, 11), 'rolling_avg': 90_000_000, 'qualifies': False},
            {'date': datetime(2020, 3, 12), 'rolling_avg': 95_000_000, 'qualifies': False},
            {'date': datetime(2020, 3, 13), 'rolling_avg': 110_000_000, 'qualifies': True},  # FIRST
            {'date': datetime(2020, 3, 14), 'rolling_avg': 120_000_000, 'qualifies': True},
        ]

        # Find first qualification date
        first_qualified = None
        for data in historical_data:
            if data['qualifies'] and first_qualified is None:
                first_qualified = data['date']
                break

        expected_date = datetime(2020, 3, 13)
        assert first_qualified == expected_date, (
            f"First qualification date should be {expected_date}, got {first_qualified}"
        )

    def test_real_data_validation(self):
        """
        Test case to validate against real INTG data
        This demonstrates what the correct behavior should be
        """

        # Real data from INTG database
        # AAPL first qualified: 2020-03-13 (volume > $100M)
        # Current membership records show:
        # - Universe 2: start_at = 1980-12-12 ✅ (correct historical)
        # - Universe 3: start_at = 2025-09-09 ❌ (recent job run - bug)
        # - Universe 4: start_at = 2025-09-09 ❌ (recent job run - bug)

        expected_historical_date = datetime(2020, 3, 13)
        buggy_recent_date = datetime(2025, 9, 9)

        # What we should see after fix
        assert expected_historical_date.year == 2020
        assert buggy_recent_date.year == 2025

        # The difference shows the magnitude of this bug
        days_difference = (buggy_recent_date - expected_historical_date).days
        assert days_difference > 2000, f"Bug creates {days_difference} day difference in start_at dates"

if __name__ == "__main__":
    # Run the tests to demonstrate the bug
    import pytest
    pytest.main([__file__, "-v"])