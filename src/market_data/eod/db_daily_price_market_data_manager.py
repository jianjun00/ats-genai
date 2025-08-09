import logging
from datetime import datetime, date, time
from typing import List, Dict, Optional, Any
from .base_daily_price_market_data_manager import BaseDailyPriceMarketDataManager
from state.instrument_interval import InstrumentInterval
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from dao.daily_prices_dao import DailyPricesDAO
from config.environment import Environment

class DBDailyPriceMarketDataManager(BaseDailyPriceMarketDataManager):
    """
    Loads daily prices from the database using DAOs and reconciles as InstrumentInterval objects.
    Accepts an environment and an optional list of symbols.
    Example usage:
        mgr = await DBDailyPriceMarketDataManager.create_async(env, symbols=["AAPL", "TSLA"])
    """
    def __init__(self, env: Environment, symbols: Optional[List[str]] = None):
        super().__init__(symbols)
        self.env = env
        self.xrefs_dao = InstrumentXrefsDAO(self.env)
        self.prices_dao = DailyPricesDAO(self.env)

    @classmethod
    async def create_async(cls, env: Environment, symbols: Optional[List[str]] = None):
        self = cls.__new__(cls)
        cls.__init__(self, env, symbols)
        await self._load_symbol_mappings()
        return self

    async def _load_symbol_mappings(self):
        if self.symbols is None:
            # Infer all symbols from DB (universe membership preferred if available)
            from dao.universe_membership_dao import UniverseMembershipDAO
            um_dao = UniverseMembershipDAO(self.env)
            memberships = await um_dao.list_all_members()
            found = set(m['symbol'].upper() for m in memberships)
            self.symbols = sorted(found)
        self._symbol_to_id = {}
        self._id_to_symbol = {}
        for s in self.symbols:
            instrument_id = await self.xrefs_dao.resolve_instrument_id(s)
            if instrument_id is not None:
                self._symbol_to_id[s] = instrument_id
                self._id_to_symbol[instrument_id] = s
            else:
                logging.warning(f"[DBDailyPriceMarketDataManager] Could not resolve instrument_id for symbol {s}")


    async def get_ohlc(self, instrument_id: int, start: datetime, end: datetime, current_date: Optional[date] = None) -> Optional[Dict[str, float]]:
        symbol = self.resolve_symbol(instrument_id)
        if not symbol:
            logging.warning(f"[DBDailyPriceMarketDataManager] No symbol for instrument_id={instrument_id}")
            return None
        # Fetch daily price from DB for the date range (typically only one row per day)
        results = await self.prices_dao.list_prices_for_instruments_and_date([instrument_id], start.date())
        if not results:
            logging.info(f"[DBDailyPriceMarketDataManager] No price data for instrument_id={instrument_id} ({symbol}) at {start.date()}")
            return None
        row = results[0]
        close = row['close']
        volume = row['volume']
        traded_dollar = close * volume if close is not None and volume is not None else None
        return {
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': close,
            'traded_volume': volume,
            'traded_dollar': traded_dollar,
        }

    async def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime, current_date: Optional[date] = None) -> Dict[int, Optional[Dict[str, float]]]:
        batch = {}
        # Fetch all prices in one DB call
        results = await self.prices_dao.list_prices_for_instruments_and_date(instrument_ids, start.date())
        price_map = {row['instrument_id']: row for row in results}
        for iid in instrument_ids:
            row = price_map.get(iid)
            if row:
                close = row['close']
                volume = row['volume']
                traded_dollar = close * volume if close is not None and volume is not None else None
                batch[iid] = {
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': close,
                    'traded_volume': volume,
                    'traded_dollar': traded_dollar,
                }
            else:
                batch[iid] = None
        return batch
