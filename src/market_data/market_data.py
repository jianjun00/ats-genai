import os
import asyncio
import logging
from ib_insync import IB, util, Contract, MarketOrder
from typing import List, AsyncGenerator, Callable, Any
import asyncpg
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from collections import deque, defaultdict
from calendars.market_calendar_utils import get_market_calendar, get_last_open_close
from datetime import timedelta
import pandas as pd

class MarketDataStreamer:
    def __init__(self, ib: IB, symbols: List[str], vwap_window: int = 100):
        self.ib = ib
        self.symbols = symbols
        self.contracts = [Contract(symbol=s, secType='STK', exchange='SMART', currency='USD') for s in symbols]
        self._queue = asyncio.Queue()
        # Multi-interval support
        self.intervals = {
            '5m': pd.Timedelta(minutes=5),
            '15m': pd.Timedelta(minutes=15),
            '1h': pd.Timedelta(hours=1),
            '4h': pd.Timedelta(hours=4),
            '1d': pd.Timedelta(days=1),
            '1w': pd.Timedelta(weeks=1),
            '1mo': pd.DateOffset(months=1),
            '1q': pd.DateOffset(months=3),
            '1y': pd.DateOffset(years=1)
        }
        # Store bars per symbol and interval: {symbol: {interval: deque of (time, price, volume)}}
        self.interval_bars = {symbol: {k: deque() for k in self.intervals} for symbol in symbols}
        self.previous_closes = {symbol: {k: None for k in self.intervals} for symbol in symbols}
        # Initialize LSE calendar
        self.lse_cal = get_market_calendar('LSE')

    def on_tick(self, tick):
        # Called on every tick update
        symbol = tick.contract.symbol
        price = tick.last
        volume = getattr(tick, 'lastSize', None)
        tick_time = tick.time

        # Ensure tick_time is a datetime object
        if not isinstance(tick_time, datetime):
            try:
                tick_time = datetime.fromisoformat(str(tick_time))
            except Exception:
                tick_time = datetime.utcnow()

        # Calculate time-based features
        hour_of_day = tick_time.hour
        day_of_week = tick_time.weekday()
        # Week of month calculation
        first_day = tick_time.replace(day=1)
        dom = tick_time.day
        adjusted_dom = dom + first_day.weekday()
        week_of_month = int((adjusted_dom - 1) / 7) + 1

        # Get last LSE open and close times using utility
        lse_last_open, lse_last_close = get_last_open_close(self.lse_cal, tick_time)
        if lse_last_open is not None:
            lse_last_open = lse_last_open.to_pydatetime()
        if lse_last_close is not None:
            lse_last_close = lse_last_close.to_pydatetime()

        # Multi-interval signal computation
        interval_signals = {}
        for interval, delta in self.intervals.items():
            # Maintain bar deque for this symbol/interval
            bar_deque = self.interval_bars[symbol][interval]
            # Remove old bars
            cutoff = tick_time - (delta if isinstance(delta, pd.Timedelta) else pd.Timedelta(days=365))
            while bar_deque and bar_deque[0][0] < cutoff:
                bar_deque.popleft()
            # Append current tick
            if price is not None and volume is not None:
                bar_deque.append((tick_time, price, volume))
            # Extract prices and volumes
            prices = [b[1] for b in bar_deque]
            volumes = [b[2] for b in bar_deque]
            high = max(prices) if prices else None
            low = min(prices) if prices else None
            close = prices[-1] if prices else None
            # VWAP
            total_pv = sum(p * v for _, p, v in bar_deque)
            total_vol = sum(volumes)
            vwap = total_pv / total_vol if total_vol > 0 else None
            # True Range
            prev_close = self.previous_closes[symbol][interval]
            if high is not None and low is not None and prev_close is not None:
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
            else:
                tr = None
            interval_signals[interval] = {
                'high': high, 'low': low, 'close': close, 'vwap': vwap, 'true_range': tr
            }
            # Store for next tick
            self.previous_closes[symbol][interval] = close

        # Compose tick dict
        tick_data = {
            'symbol': symbol,
            'bid': tick.bid if hasattr(tick, 'bid') else None,
            'ask': tick.ask if hasattr(tick, 'ask') else None,
            'last': price,
            'time': tick_time,
            'volume': volume,
            'hour_of_day': hour_of_day,
            'day_of_week': day_of_week,
            'week_of_month': week_of_month,
            'lse_last_open': lse_last_open,
            'lse_last_close': lse_last_close,
            'interval_signals': interval_signals
        }
        # Put on queue
        self._queue.put_nowait(tick_data)

    async def stream_ticks(self, on_tick: Callable[[dict], Any] = None) -> AsyncGenerator[dict, None]:
        """
        Asynchronously yields tick dicts as they arrive.
        Optionally calls on_tick callback.
        """
        while True:
            tick_data = await self._queue.get()
            if on_tick:
                on_tick(tick_data)
            yield tick_data

