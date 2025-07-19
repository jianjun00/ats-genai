# moved from project root
import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
import pandas as pd

from market_data import MarketDataStreamer

class DummyTick:
    def __init__(self, symbol, price, volume, time, bid=0, ask=0):
        self.contract = MagicMock()
        self.contract.symbol = symbol
        self.last = price
        self.lastSize = volume
        self.time = time
        self.bid = bid
        self.ask = ask

class TestMarketDataStreamer(unittest.TestCase):
    def setUp(self):
        self.symbols = ['AAPL']
        self.ib = MagicMock()
        self.streamer = MarketDataStreamer(self.ib, self.symbols)
        # Patch LSE calendar to avoid real calendar dependency
        self.streamer.lse_cal = MagicMock()
        self.streamer.lse_cal.schedule = pd.DataFrame({
            'market_open': [pd.Timestamp('2025-07-18 08:00')],
            'market_close': [pd.Timestamp('2025-07-18 16:30')]
        }, index=[pd.Timestamp('2025-07-18')])

    def test_time_signals(self):
        tick_time = datetime(2025, 7, 18, 10, 15)
        tick = DummyTick('AAPL', 150, 100, tick_time)
        self.streamer.on_tick(tick)
        data = self.streamer._queue.get_nowait()
        self.assertEqual(data['hour_of_day'], 10)
        self.assertEqual(data['day_of_week'], 4)  # Friday
        self.assertEqual(data['week_of_month'], 3)

    def test_lse_signals(self):
        tick_time = datetime(2025, 7, 18, 10, 15)
        tick = DummyTick('AAPL', 150, 100, tick_time)
        self.streamer.lse_cal.schedule.loc[:tick_time] = self.streamer.lse_cal.schedule
        self.streamer.on_tick(tick)
        data = self.streamer._queue.get_nowait()
        self.assertIsNotNone(data['lse_last_open'])
        self.assertIsNotNone(data['lse_last_close'])

    def test_interval_signals_basic(self):
        base_time = datetime(2025, 7, 18, 10, 0)
        # Simulate 10 ticks, 1 min apart, price increasing
        for i in range(10):
            tick_time = base_time + timedelta(minutes=i)
            tick = DummyTick('AAPL', 100 + i, 10 + i, tick_time)
            self.streamer.on_tick(tick)
        # Get last tick's signals
        data = self.streamer._queue.get_nowait()
        signals_5m = data['interval_signals']['5m']
        self.assertTrue('high' in signals_5m)
        self.assertTrue('low' in signals_5m)
        self.assertTrue('close' in signals_5m)
        self.assertTrue('vwap' in signals_5m)
        self.assertTrue('true_range' in signals_5m)
        # Check high/low/close correctness
        self.assertEqual(signals_5m['high'], 109)
        self.assertEqual(signals_5m['low'], 100)
        self.assertEqual(signals_5m['close'], 109)

    def test_true_range(self):
        base_time = datetime(2025, 7, 18, 10, 0)
        # First tick (no prev close)
        tick1 = DummyTick('AAPL', 100, 10, base_time)
        self.streamer.on_tick(tick1)
        # Second tick
        tick2 = DummyTick('AAPL', 110, 10, base_time + timedelta(minutes=1))
        self.streamer.on_tick(tick2)
        data = self.streamer._queue.get_nowait()
        tr_5m = data['interval_signals']['5m']['true_range']
        # Should be max(110-100, abs(110-100), abs(100-100)) = 10
        self.assertEqual(tr_5m, 10)

    def test_multi_interval(self):
        base_time = datetime(2025, 7, 18, 10, 0)
        # Simulate ticks over 2 hours
        for i in range(120):
            tick_time = base_time + timedelta(minutes=i)
            tick = DummyTick('AAPL', 100 + (i % 10), 10, tick_time)
            self.streamer.on_tick(tick)
        data = self.streamer._queue.get_nowait()
        # All intervals should be present
        for interval in self.streamer.intervals:
            self.assertIn(interval, data['interval_signals'])
            self.assertIn('high', data['interval_signals'][interval])
            self.assertIn('vwap', data['interval_signals'][interval])

if __name__ == '__main__':
    unittest.main()
