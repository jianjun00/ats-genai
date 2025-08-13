from typing import List, Dict, Tuple, Optional
from datetime import datetime

class MarketDataManager:
    """
    Provides open, high, low, close for a given instrument and time range.
    This is an interface; actual implementation should connect to your DB or data provider.
    """

    def get_ohlc(self, instrument_id: int, start: datetime, end: datetime) -> Optional[Dict[str, float]]:
        """
        Returns a dict with keys: 'open', 'high', 'low', 'close' for the instrument in the given time range.
        Returns None if no data is available.
        """
        # Example stub, replace with real DB/query logic
        # Example: return {'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 104.0}
        return None

    async def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime) -> Dict[int, Optional[Dict[str, float]]]:
        """
        Returns a dict mapping instrument_id to its ohlc dict for the given time range.
        """
        result = {}
        for iid in instrument_ids:
            result[iid] = self.get_ohlc(iid, start, end)
        return result
