# moved from project root
import unittest
from datetime import datetime
from market_data_simulator import simulate_market_data
from signals import extract_all_signals

class TestMarketDataSimulatorWithSignals(unittest.TestCase):
    def test_simulator_and_signal_generation(self):
        symbol = 'AAPL'
        start_time = datetime(2025, 7, 18, 9, 30)
        ticks = simulate_market_data(symbol, start_time, num_ticks=20, interval_seconds=60)
        # Add fake time-based and interval signals to each tick for demonstration
        for i, tick in enumerate(ticks):
            tick['hour_of_day'] = tick['time'].hour
            tick['day_of_week'] = tick['time'].weekday()
            tick['week_of_month'] = 1 + (tick['time'].day - 1) // 7
            tick['lse_last_open'] = start_time
            tick['lse_last_close'] = start_time.replace(hour=16, minute=30)
            tick['interval_signals'] = {
                '5m': {
                    'high': tick['last'] + 1,
                    'low': tick['last'] - 1,
                    'close': tick['last'],
                    'vwap': tick['last'],
                    'true_range': 2
                }
            }
        # Extract signals from each tick and check structure
        for tick in ticks:
            signals = extract_all_signals(tick)
            self.assertEqual(signals['symbol'], symbol)
            self.assertIn('5m_high', signals)
            self.assertIn('hour_of_day', signals)
            self.assertIn('lse_last_open', signals)
            self.assertIn('5m_true_range', signals)
            self.assertIsInstance(signals['5m_true_range'], (int, float))

if __name__ == '__main__':
    unittest.main()
