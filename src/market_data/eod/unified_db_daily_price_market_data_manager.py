from datetime import datetime, date
from typing import List, Dict, Optional
from .base_daily_price_market_data_manager import BaseDailyPriceMarketDataManager
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from config.environment import Environment
from .unify_daily_prices import DatabaseDailyPricesUnifier

class UnifiedDBDailyPriceMarketDataManager(BaseDailyPriceMarketDataManager):
    """
    Loads raw daily prices from both daily_prices_tiingo and daily_prices_polygon tables,
    then uses unify_daily_prices logic to produce unified daily prices for each symbol/date.
    """
    def __init__(self, env: Environment, symbols: Optional[List[str]] = None):
        super().__init__(symbols)
        self.env = env
        self.xrefs_dao = InstrumentXrefsDAO(self.env)
        self.tiingo_dao = DailyPricesTiingoDAO(self.env)
        self.polygon_dao = DailyPricesPolygonDAO(self.env)
        self.unifier = DatabaseDailyPricesUnifier(self.env)

    @classmethod
    async def create_async(cls, env: Environment, symbols: Optional[List[str]] = None):
        print(f"[DEBUG_UDDM_CREATE_ASYNC] Creating UnifiedDBDailyPriceMarketDataManager with env: {env}, symbols: {symbols}")
        self = cls.__new__(cls)
        cls.__init__(self, env, symbols)
        await self._load_symbol_mappings()
        return self

    async def _load_symbol_mappings(self):
        if self.symbols is None:
            from dao.universe_membership_dao import UniverseMembershipDAO
            um_dao = UniverseMembershipDAO(self.env)
            memberships = await um_dao.list_all_members()
            found = set(m['symbol'].upper() for m in memberships)
            self.symbols = sorted(found)
        self._symbol_to_id = {}
        self._id_to_symbol = {}
        for s in self.symbols:
            instrument_id = await self.xrefs_dao.resolve_instrument_id_by_symbol(s)
            if instrument_id is not None:
                self._symbol_to_id[s] = instrument_id
                self._id_to_symbol[instrument_id] = s

    async def get_ohlc(self, instrument_id: int, start: datetime, end: datetime, current_date: Optional[date] = None) -> Optional[Dict[str, float]]:
        print(f"[DEBUG][get_ohlc] Called with instrument_id={instrument_id}, start={start}, end={end}, current_date={current_date}")
        symbol = await self.resolve_symbol(instrument_id)
        print(f"[DEBUG][get_ohlc] Resolved symbol={symbol} for instrument_id={instrument_id}")
        if not symbol:
            print(f"[DEBUG][get_ohlc] Could not resolve symbol for instrument_id={instrument_id}")
            return None
        # Use unify_daily_prices to get unified price for the date
        # Pass a tuple of (start_date, end_date) as asof parameter
        print(f"[DEBUG][get_ohlc] Calling unify_daily_prices with symbol={symbol}, asof=({start.date()}, {end.date()})")
        result = await self.unifier.unify_daily_prices(symbol, (start.date(), end.date()), current_date)
        print(f"[DEBUG][get_ohlc] unify_daily_prices returned {len(result)} rows: {result}")
        # unify_daily_prices returns a list of dicts, one per date
        for row in result:
            print(f"[DEBUG][get_ohlc] Checking row with date={row['date']}, start.date()={start.date()}, match={row['date'] == start.date()}")
            if row['date'] == start.date():
                price_dict = {
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'traded_volume': row['volume'],
                    'source': row.get('source'),
                    'status': row.get('status'),
                    'note': row.get('note'),
                }
                print(f"[DEBUG][get_ohlc] Returning price_dict: {price_dict}")
                return price_dict
        print(f"[DEBUG][get_ohlc] No matching row found for date={start.date()}, returning None")
        return None

    async def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime, current_date: Optional[date] = None) -> Dict[int, Optional[Dict[str, float]]]:
        batch = {}
        for iid in instrument_ids:
            batch[iid] = await self.get_ohlc(iid, start, end, current_date)
        return batch
