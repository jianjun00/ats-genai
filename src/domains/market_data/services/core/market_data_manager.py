from typing import List, Dict, Optional
from datetime import datetime

class MarketDataManager:
    """
    Pure OHLC Data Interface - Provides raw OHLC market data only.
    
    SINGLE RESPONSIBILITY:
    - Fetch raw OHLC data (open, high, low, close) for instruments and time ranges
    
    STRICTLY ONLY:
    - Returns Dict[str, float] with keys: 'open', 'high', 'low', 'close'  
    - Handles individual instrument queries: get_ohlc()
    - Handles batch instrument queries: get_ohlc_batch()
    - Abstract interface - implementations connect to specific data sources
    
    DOES NOT:
    - Compute any indicators or technical signals
    - Manage any state or rolling windows  
    - Handle any business logic or transformations
    - Persist or cache any computed results
    - Format data beyond basic OHLC dict structure
    - Aggregate timeframes or perform any data processing
    
    INTERACTIONS:
    - Used BY: UniverseStateBuilder (primary consumer)
    - Returns: Pure OHLC data as simple dictionaries
    - That's it - no other responsibilities
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

    def get_ohlcv_data(self, instrument_id: int, reference_datetime: datetime, periods: int, time_interval: str, direction: str = 'backward') -> 'pd.DataFrame':
        """
        Get OHLCV data for a specific instrument over multiple periods.
        
        Args:
            instrument_id: The instrument ID to retrieve data for
            reference_datetime: Reference datetime point (direction determines if we go back or forward from here)
            periods: Number of periods to retrieve
            time_interval: Time interval ('1m', '5m', '15m', '1h', '1d', '1w')
            direction: 'backward' for historical data, 'forward' for future data
            
        Returns:
            DataFrame with columns ['open', 'high', 'low', 'close', 'volume'] and datetime index
        """
        raise NotImplementedError("get_ohlcv_data must be implemented by concrete MarketDataManager classes")
