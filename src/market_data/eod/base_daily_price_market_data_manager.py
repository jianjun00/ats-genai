from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import List, Dict, Optional, Any
from market_data.market_data_manager import MarketDataManager
from state.instrument_interval import InstrumentInterval

class BaseDailyPriceMarketDataManager(MarketDataManager, ABC):
    """
    Abstract base class for daily price market data managers, providing symbol/id mapping,
    SOD date management, and batch OHLC fetch interface. Subclasses must implement
    data source-specific logic.
    """
    def __init__(self, symbols: Optional[List[str]] = None):
        self.symbols = symbols
        self._intervals: Dict[int, InstrumentInterval] = {}
        self._last_prices: Dict[int, Dict[str, float]] = {}
        self._symbol_to_id: Dict[str, int] = {}
        self._id_to_symbol: Dict[int, str] = {}
        self._last_sod_date: Optional[date] = None

    @abstractmethod
    async def _load_symbol_mappings(self):
        pass

    def set_last_sod_date(self, sod_date: date):
        self._last_sod_date = sod_date

    def get_last_sod_date(self) -> Optional[date]:
        return self._last_sod_date

    def resolve_instrument_id(self, symbol: str) -> Optional[int]:
        return self._symbol_to_id.get(symbol.upper())

    def resolve_symbol(self, instrument_id: int) -> Optional[str]:
        return self._id_to_symbol.get(instrument_id)

    @abstractmethod
    async def get_ohlc(self, instrument_id: int, start: datetime, end: datetime, current_date: Optional[date] = None) -> Optional[Dict[str, float]]:
        pass

    @abstractmethod
    async def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime, current_date: Optional[date] = None) -> Dict[int, Optional[Dict[str, float]]]:
        pass
