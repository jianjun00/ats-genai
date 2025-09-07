from typing import List, Dict, Optional
from datetime import datetime, date, time

from domains.market_data.services.market_data_manager import MarketDataManager
from domains.market_data.repositories.daily_prices_dao import DailyPricesDAO
from core.calendars.exchange_calendar import ExchangeCalendar
from domains.trading.services.state.instrument_interval import InstrumentInterval

class DailyPriceMarketDataManager(MarketDataManager):
    def __init__(self, env, exchange="NYSE", start_date: Optional[date]=None):
        from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
        self.env = env
        self.exchange = exchange
        self.calendar = ExchangeCalendar(self.exchange)
        self.dao = DailyPricesDAO(self.env)
        self.instrument_xref_dao = InstrumentXrefsDAO(self.env)
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
        # Load last price for each instrument
        instrument_ids = []
        for symbol in symbols:
            instrument_id = await self.instrument_xref_dao.resolve_instrument_id(symbol)
            if instrument_id is not None:
                instrument_ids.append(instrument_id)
        results = await self.dao.list_prices_for_instruments_and_date(instrument_ids, prev_date)
        for row in results:
            instrument_id = row['instrument_id']
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
        instrument_ids = []
        for symbol in symbols:
            instrument_id = await self.instrument_xref_dao.resolve_instrument_id(symbol)
            if instrument_id is not None:
                instrument_ids.append(instrument_id)
        results = await self.dao.list_prices_for_instruments_and_date(instrument_ids, cur_date)
        print(f"DEBUG update_for_sod: loaded rows for date={cur_date}, instrument_ids={instrument_ids}: {results}")
        logger.debug(f"update_for_sod: got {len(results)} price records from DB for date {cur_date}")
        open_time, close_time = self._get_exchange_open_close(cur_date)
        logger.debug(f"update_for_sod: open_time={open_time}, close_time={close_time}")
        for row in results:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"update_for_sod: row keys={list(row.keys())}, row values={list(row.values())}")
            instrument_id = row['instrument_id']
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

    async def update_for_eod(self, runner=None, current_time=None):
        # Compatible with both legacy and new signatures
        # If called from Runner, runner and current_time are provided
        # If called directly, cur_date may be passed as runner
        if current_time is not None:
            current_time.date()
        elif runner is not None:
            # runner is actually cur_date in legacy calls
            pass
        else:
            from datetime import date as _date
            _date.today()
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
        # No data found - fail explicitly
        raise ValueError(f"No OHLC data found for instrument_id {instrument_id} between {start} and {end}")
        return None

    def _get_exchange_open_close(self, cur_date: date):
        # For NYSE, open=9:30, close=16:00
        open_dt = datetime.combine(cur_date, time(9, 30))
        close_dt = datetime.combine(cur_date, time(16, 0))
        return open_dt, close_dt

    def _get_all_symbols(self) -> List[str]:
        # Placeholder: in production, this should return all symbols in the universe
        # For now, returns an empty list
        return []
        
    async def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime) -> Dict[int, Optional[Dict[str, float]]]:
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"get_ohlc_batch: instrument_ids={instrument_ids}, start={start}, end={end}")
        result = {}
        for iid in instrument_ids:
            result[iid] = self.get_ohlc(iid, start, end)
        logger.debug(f"get_ohlc_batch: returning {len(result)} results")
        return result

