import unittest
from src.market_data.signals import extract_all_signals
from datetime import datetime

class TestExtractAllSignals(unittest.TestCase):
    def setUp(self):
        self.tick_data = {
            'symbol': 'AAPL',
            'bid': 150.1,
            'ask': 150.3,
            'last': 150.2,
            'time': datetime(2025, 7, 18, 10, 15),
            'volume': 1000,
            'hour_of_day': 10,
            'day_of_week': 4,
            'week_of_month': 3,
            'lse_last_open': datetime(2025, 7, 18, 8, 0),
            'lse_last_close': datetime(2025, 7, 18, 16, 30),
            'interval_signals': {
                '5m': {
                    'high': 150.2,
                    'low': 149.5,
                    'close': 150.2,
                    'vwap': 150.0,
                    'true_range': 0.7
                },
                '1d': {
                    'high': 151.0,
                    'low': 148.0,
                    'close': 150.2,
                    'vwap': 149.8,
                    'true_range': 3.0
                }
            }
        }

    def test_flatten_all_signals(self):
        signals = extract_all_signals(self.tick_data)
        # Check tick-level
        self.assertEqual(signals['symbol'], 'AAPL')
        self.assertEqual(signals['bid'], 150.1)
        self.assertEqual(signals['hour_of_day'], 10)
        self.assertIn('lse_last_open', signals)
        # Check interval signals
        self.assertEqual(signals['5m_high'], 150.2)
        self.assertEqual(signals['5m_true_range'], 0.7)
        self.assertEqual(signals['1d_vwap'], 149.8)
        self.assertEqual(signals['1d_close'], 150.2)
        self.assertNotIn('15m_high', signals)  # Not present in input

    def test_empty_interval_signals(self):
        tick_data = dict(self.tick_data)
        tick_data['interval_signals'] = {}
        signals = extract_all_signals(tick_data)
        self.assertNotIn('5m_high', signals)
        self.assertNotIn('1d_vwap', signals)

    def test_missing_optional_fields(self):
        tick_data = {'symbol': 'AAPL', 'interval_signals': {}}
        signals = extract_all_signals(tick_data)
        self.assertEqual(signals['symbol'], 'AAPL')
        self.assertNotIn('bid', signals)
        self.assertNotIn('hour_of_day', signals)

    def test_none_and_empty_fields(self):
        tick_data = {
            'symbol': None,
            'bid': None,
            'interval_signals': {'5m': {'high': None, 'low': None}}
        }
        signals = extract_all_signals(tick_data)
        self.assertIsNone(signals['symbol'])
        self.assertIsNone(signals['bid'])
        self.assertIn('5m_high', signals)
        self.assertIsNone(signals['5m_high'])

    def test_extra_unexpected_fields(self):
        tick_data = {
            'symbol': 'AAPL',
            'foo': 'bar',
            'interval_signals': {'5m': {'high': 1, 'unexpected': 2}}
        }
        signals = extract_all_signals(tick_data)
        self.assertEqual(signals['symbol'], 'AAPL')
        self.assertIn('5m_high', signals)
        self.assertIn('5m_unexpected', signals)
        self.assertEqual(signals['5m_unexpected'], 2)
        self.assertNotIn('foo', signals)  # Only expected top-level keys

    def test_multiple_intervals(self):
        tick_data = {
            'symbol': 'AAPL',
            'interval_signals': {
                '5m': {'high': 1, 'low': 2},
                '1h': {'high': 3, 'low': 4}
            }
        }
        signals = extract_all_signals(tick_data)
        self.assertEqual(signals['5m_high'], 1)
        self.assertEqual(signals['1h_low'], 4)

    def test_non_string_keys(self):
        tick_data = {
            'symbol': 'AAPL',
            'interval_signals': {
                5: {'high': 1},
                None: {'low': 2}
            }
        }
        signals = extract_all_signals(tick_data)
        self.assertIn('5_high', signals)
        self.assertIn('None_low', signals)

    def test_nested_interval_dict(self):
        tick_data = {
            'symbol': 'AAPL',
            'interval_signals': {
                '5m': {'high': 1, 'nested': {'foo': 2}}
            }
        }
        signals = extract_all_signals(tick_data)
        self.assertEqual(signals['5m_high'], 1)
        self.assertIsInstance(signals['5m_nested'], dict)
        self.assertEqual(signals['5m_nested']['foo'], 2)

    def test_type_robustness(self):
        # Should not raise on completely unexpected types
        tick_data = 12345
        try:
            signals = extract_all_signals(tick_data)
        except Exception as e:
            self.fail(f"extract_all_signals raised {e} on unexpected input type")

        tick_data = {'interval_signals': 'notadict'}
        try:
            signals = extract_all_signals(tick_data)
        except Exception as e:
            self.fail(f"extract_all_signals raised {e} on string interval_signals")

if __name__ == '__main__':
    unittest.main()
