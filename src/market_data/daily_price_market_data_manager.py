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
        # Note: _load_last_prices_before_start should be called by user after construction if needed, as it is now async.

    async def _load_last_prices_before_start(self):
        # This method should load the last daily price before self._start_date for all instruments
        symbols = self._get_all_symbols()
        prev_date = self.calendar.prior_trading_date(self._start_date)
        if prev_date is None:
            return
        # Load last price for each symbol
        results = await self.dao.list_prices_for_symbols_and_date(symbols, prev_date)
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

    async def update_for_sod(self, runner, event_time: datetime):
        cur_date = event_time.date()
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"update_for_sod: cur_date={cur_date}")
        # Load daily_prices for cur_date and store as InstrumentInterval
        symbols = self._get_all_symbols()
        logger.debug(f"update_for_sod: fetched symbols: {symbols}")
        results = await self.dao.list_prices_for_symbols_and_date(symbols, cur_date)
        logger.debug(f"update_for_sod: got {len(results)} price records from DB for date {cur_date}")
        open_time, close_time = self._get_exchange_open_close(cur_date)
        logger.debug(f"update_for_sod: open_time={open_time}, close_time={close_time}")
        for row in results:
            instrument_id = self._symbol_to_id(row['symbol'])
            logger.debug(f"update_for_sod: Creating interval for instrument_id={instrument_id}, symbol={row['symbol']} with row={row}")
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
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"get_ohlc: instrument_id={instrument_id}, start={start}, end={end}")
        interval = self._intervals.get(instrument_id)
        if interval:
            logger.debug(f"get_ohlc: found interval for {instrument_id}: start={interval.start_date_time}, end={interval.end_date_time}")
            if interval.start_date_time == start and interval.end_date_time == end:
                result = {
                    'open': interval.open,
                    'high': interval.high,
                    'low': interval.low,
                    'close': interval.close,
                    'volume': interval.traded_volume,
                    'traded_dollar': interval.traded_dollar
                }
                logger.debug(f"get_ohlc: returning interval data for {instrument_id}: {result}")
                return result
            else:
                logger.debug(f"get_ohlc: interval found but start/end mismatch for {instrument_id}")
        else:
            logger.debug(f"get_ohlc: no interval found for {instrument_id}")
        # Optionally, fallback to last_prices
        fallback = self._last_prices.get(instrument_id)
        logger.debug(f"get_ohlc: returning fallback last_prices for {instrument_id}: {fallback}")
        return fallback

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
