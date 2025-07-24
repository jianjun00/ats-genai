from typing import List, Dict, Optional
from datetime import datetime, date, time, timedelta

from market_data.market_data_manager import MarketDataManager
from market_data.eod.daily_prices_dao import DailyPricesDAO
from config.environment import get_environment
from calendars.exchange_calendar import ExchangeCalendar
from state.instrument_interval import InstrumentInterval

class DailyPriceMarketDataManager(MarketDataManager):
    def __init__(self, db=None, env=None, exchange="NYSE", start_date: Optional[date]=None):
        super().__init__(db)
        self.env = env or get_environment()
        self.exchange = exchange
        self.calendar = ExchangeCalendar(self.exchange)
        self.dao = DailyPricesDAO(self.env)
        self._intervals: Dict[int, InstrumentInterval] = {}
        self._last_prices: Dict[int, Dict[str, float]] = {}
        self._start_date = start_date
        if start_date:
            # Load last price before start_date for all instruments
            self._load_last_prices_before_start()

    def _load_last_prices_before_start(self):
        # This method should load the last daily price before self._start_date for all instruments
        # For simplicity, we assume a method to get all symbols exists
        # In production, this may require a symbol universe or input list
        symbols = self._get_all_symbols()
        prev_date = self.calendar.prior_trading_date(self._start_date)
        if prev_date is None:
            return
        # Load last price for each symbol
        import asyncio
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.dao.list_prices_for_symbols_and_date(symbols, prev_date))
        for row in results:
            instrument_id = self._symbol_to_id(row['symbol'])
            self._last_prices[instrument_id] = {
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'traded_dollar': row['volume'] * row['close'] if row['volume'] is not None and row['close'] is not None else 0.0
            }

    def update_for_sod(self, cur_date: date):
        # Load daily_prices for cur_date and store as InstrumentInterval
        import asyncio
        symbols = self._get_all_symbols()
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.dao.list_prices_for_symbols_and_date(symbols, cur_date))
        open_time, close_time = self._get_exchange_open_close(cur_date)
        for row in results:
            instrument_id = self._symbol_to_id(row['symbol'])
            interval = InstrumentInterval(
                instrument_id=instrument_id,
                start_date_time=open_time,
                end_date_time=close_time,
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                traded_volume=row['volume'],
                traded_dollar=row['volume'] * row['close'] if row['volume'] is not None and row['close'] is not None else 0.0,
                status='ok'
            )
            self._intervals[instrument_id] = interval

    def update_for_eod(self, cur_date: date):
        # Can be used to flush, persist, or clear intervals if needed
        self._intervals.clear()

    def get_ohlc(self, instrument_id: int, start: datetime, end: datetime) -> Optional[Dict[str, float]]:
        # Return the ohlc for the interval if present
        interval = self._intervals.get(instrument_id)
        if interval and interval.start_date_time == start and interval.end_date_time == end:
            return {
                'open': interval.open,
                'high': interval.high,
                'low': interval.low,
                'close': interval.close,
                'volume': interval.traded_volume,
                'traded_dollar': interval.traded_dollar
            }
        # Optionally, fallback to last_prices
        return self._last_prices.get(instrument_id)

    def _get_exchange_open_close(self, cur_date: date):
        # For NYSE, open=9:30, close=16:00
        open_dt = datetime.combine(cur_date, time(9, 30))
        close_dt = datetime.combine(cur_date, time(16, 0))
        return open_dt, close_dt

    def _get_all_symbols(self) -> List[str]:
        # Placeholder: in production, this should return all symbols in the universe
        # For now, returns an empty list
        return []

    def _symbol_to_id(self, symbol: str) -> int:
        # Placeholder: in production, map symbol to instrument_id
        # For now, use hash
        return hash(symbol)
