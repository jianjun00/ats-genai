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
from market_calendar_utils import get_market_calendar, get_last_open_close
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
            total_pv = sum(p * v for p, v in bar_deque)
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
            # Store signals
            interval_signals[interval] = {
                'high': high,
                'low': low,
                'close': close,
                'vwap': vwap,
                'true_range': tr
            }
            # Update previous close
            if close is not None:
                self.previous_closes[symbol][interval] = close
        data = {
            'symbol': symbol,
            'bid': tick.bid,
            'ask': tick.ask,
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
        logger.debug(f"Tick: {data}")
        asyncio.create_task(self._queue.put(data))

    async def stream_ticks(self) -> AsyncGenerator[dict, None]:
        # Subscribe to real-time market data
        for contract in self.contracts:
            self.ib.reqMktData(contract, '', False, False)
        self.ib.pendingTickersEvent += self.on_tick
        while True:
            tick = await self._queue.get()
            yield tick

class StrategyEngine:
    def __init__(self, strategy_fn: Callable[[dict], Any]):
        self.strategy_fn = strategy_fn

    def on_market_data(self, data: dict):
        # Process new market data and generate signals
        return self.strategy_fn(data)

class OrderManager:
    def __init__(self, ib: IB):
        self.ib = ib
        self.orders = []

    def submit_order(self, contract: Contract, action: str, quantity: int):
        order = MarketOrder(action, quantity)
        trade = self.ib.placeOrder(contract, order)
        self.orders.append(trade)
        logger.info(f"Order submitted: {action} {quantity} {contract.symbol}")
        return trade

class RiskManager:
    def __init__(self, max_position: int = 100):
        self.max_position = max_position

    def check(self, portfolio: dict, signal: dict) -> bool:
        # Example: simple position limit check
        symbol = signal.get('symbol')
        desired_qty = signal.get('quantity', 0)
        current_qty = portfolio.get(symbol, 0)
        if abs(current_qty + desired_qty) > self.max_position:
            logger.warning(f"Risk limit exceeded for {symbol}")
            return False
        return True

# TimescaleDB integration for time series storage
class TimescaleDBClient:
    def __init__(self):
        # Set your TimescaleDB/Postgres connection string as an environment variable TSDB_URL
        # Example: postgresql://user:password@localhost:5432/yourdb
        self.db_url = os.getenv('TSDB_URL', 'postgresql://user:password@localhost:5432/yourdb')
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.db_url)

    async def save_market_data(self, data: dict):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO market_data (time, symbol, bid, ask, last)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (time, symbol) DO NOTHING
                """,
                data.get('time', datetime.utcnow()), data['symbol'], data.get('bid'), data.get('ask'), data.get('last')
            )

    async def save_signal(self, time: datetime, symbol: str, signal: dict):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO signals (time, symbol, signal)
                VALUES ($1, $2, $3)
                ON CONFLICT (time, symbol) DO NOTHING
                """,
                time, symbol, signal
            )

# Example persistence (placeholder)
def save_trade_to_db(trade):
    # Implement DB save logic here
    pass

# Example usage
async def main():
    ib = IB()
    ib_host = os.getenv('IB_HOST', '127.0.0.1')
    ib_port = int(os.getenv('IB_PORT', '7497'))
    ib_client_id = int(os.getenv('IB_CLIENT_ID', '1'))
    await util.runAsync(ib.connect, ib_host, ib_port, clientId=ib_client_id)

    symbols = ['AAPL', 'MSFT']
    streamer = MarketDataStreamer(ib, symbols)

    # Initialize TimescaleDB client
    db = TimescaleDBClient()
    await db.connect()

    def simple_strategy(data):
        # Example: always buy 1 share (for demo only!)
        return {'action': 'BUY', 'symbol': data['symbol'], 'quantity': 1}

    strategy = StrategyEngine(simple_strategy)
    order_manager = OrderManager(ib)
    risk_manager = RiskManager(max_position=10)
    portfolio = {s: 0 for s in symbols}  # Example portfolio state

    async for tick in streamer.stream_ticks():
        # Save tick data to TimescaleDB
        await db.save_market_data(tick)

        signal = strategy.on_market_data(tick)
        if signal and risk_manager.check(portfolio, signal):
            contract = Contract(symbol=signal['symbol'], secType='STK', exchange='SMART', currency='USD')
            trade = order_manager.submit_order(contract, signal['action'], signal['quantity'])
            portfolio[signal['symbol']] += signal['quantity']
            # Save signal to TimescaleDB
            await db.save_signal(tick.get('time', datetime.utcnow()), tick['symbol'], signal)
            save_trade_to_db(trade)

    ib.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
