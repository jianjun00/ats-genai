# moved from project root
import unittest
import pandas as pd
from unittest.mock import MagicMock
from calendars.market_calendar_utils import get_market_calendar, get_last_open_close, get_next_open_close

def make_mock_calendar():
    # Create a mock calendar with a known schedule including weekends and holidays
    # 2025-07-18: Friday (open)
    # 2025-07-19: Saturday (closed)
    # 2025-07-20: Sunday (closed)
    # 2025-07-21: Monday (open)
    # 2025-12-24: Christmas Eve (open)
    # 2025-12-25: Christmas (closed)
    # 2025-12-26: Boxing Day (closed)
    # 2025-12-29: Next open after Christmas
    cal = MagicMock()
    cal.schedule = pd.DataFrame({
        'market_open': [
            pd.Timestamp('2025-07-18 08:00'), # Friday
            pd.Timestamp('2025-07-21 08:00'), # Monday (long weekend)
            pd.Timestamp('2025-12-24 08:00'), # Christmas Eve
            pd.Timestamp('2025-12-29 08:00'), # Next open after Christmas
        ],
        'market_close': [
            pd.Timestamp('2025-07-18 16:30'), # Friday
            pd.Timestamp('2025-07-21 16:30'), # Monday
            pd.Timestamp('2025-12-24 12:30'), # Christmas Eve (early close)
            pd.Timestamp('2025-12-29 16:30'), # Next open after Christmas
        ]
    }, index=[
        pd.Timestamp('2025-07-18'),
        pd.Timestamp('2025-07-21'),
        pd.Timestamp('2025-12-24'),
        pd.Timestamp('2025-12-29'),
    ])
    return cal

class TestMarketCalendarUtils(unittest.TestCase):
    def setUp(self):
        self.cal = make_mock_calendar()

    def test_get_last_open_close(self):
        # Test before first open
        dt = pd.Timestamp('2025-07-17 07:00')
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertIsNone(last_open)
        self.assertIsNone(last_close)
        # Test during first session
        dt = pd.Timestamp('2025-07-18 12:00')
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertEqual(last_open, pd.Timestamp('2025-07-18 08:00'))
        self.assertEqual(last_close, pd.Timestamp('2025-07-18 16:30'))
        # Test after first close, before second open
        dt = pd.Timestamp('2025-07-18 20:00')
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertEqual(last_open, pd.Timestamp('2025-07-18 08:00'))
        self.assertEqual(last_close, pd.Timestamp('2025-07-18 16:30'))
        # Test during weekend (Saturday), should return last open/close as Friday
        dt = pd.Timestamp('2025-07-19 09:00')
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertEqual(last_open, pd.Timestamp('2025-07-18 08:00'))
        self.assertEqual(last_close, pd.Timestamp('2025-07-18 16:30'))

    def test_get_next_open_close(self):
        # Test before first open
        dt = pd.Timestamp('2025-07-17 07:00')
        next_open, next_close = get_next_open_close(self.cal, dt)
        self.assertEqual(next_open, pd.Timestamp('2025-07-18 08:00'))
        self.assertEqual(next_close, pd.Timestamp('2025-07-18 16:30'))
        # Test after first close
        dt = pd.Timestamp('2025-07-18 20:00')
        next_open, next_close = get_next_open_close(self.cal, dt)
        self.assertEqual(next_open, pd.Timestamp('2025-07-21 08:00'))
        self.assertEqual(next_close, pd.Timestamp('2025-07-21 16:30'))
        # Test after last close before Monday open (Sunday night), should return Monday session
        dt = pd.Timestamp('2025-07-20 20:00')
        next_open, next_close = get_next_open_close(self.cal, dt)
        self.assertEqual(next_open, pd.Timestamp('2025-07-21 08:00'))
        self.assertEqual(next_close, pd.Timestamp('2025-07-21 16:30'))

    def test_weekend(self):
        # Friday after close, before Monday open
        dt = pd.Timestamp('2025-07-19 12:00') # Saturday
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertEqual(last_open, pd.Timestamp('2025-07-18 08:00'))
        self.assertEqual(last_close, pd.Timestamp('2025-07-18 16:30'))
        next_open, next_close = get_next_open_close(self.cal, dt)
        self.assertEqual(next_open, pd.Timestamp('2025-07-21 08:00'))
        self.assertEqual(next_close, pd.Timestamp('2025-07-21 16:30'))

    def test_long_weekend(self):
        # Friday after close, before Monday open (long weekend)
        dt = pd.Timestamp('2025-07-20 12:00') # Sunday
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertEqual(last_open, pd.Timestamp('2025-07-18 08:00'))
        self.assertEqual(last_close, pd.Timestamp('2025-07-18 16:30'))
        next_open, next_close = get_next_open_close(self.cal, dt)
        self.assertEqual(next_open, pd.Timestamp('2025-07-21 08:00'))
        self.assertEqual(next_close, pd.Timestamp('2025-07-21 16:30'))

    def test_christmas_holiday(self):
        # On Christmas (closed)
        dt = pd.Timestamp('2025-12-25 10:00')
        last_open, last_close = get_last_open_close(self.cal, dt)
        self.assertEqual(last_open, pd.Timestamp('2025-12-24 08:00'))
        self.assertEqual(last_close, pd.Timestamp('2025-12-24 12:30'))
        next_open, next_close = get_next_open_close(self.cal, dt)
        self.assertEqual(next_open, pd.Timestamp('2025-12-29 08:00'))
        self.assertEqual(next_close, pd.Timestamp('2025-12-29 16:30'))

if __name__ == '__main__':
    unittest.main()
