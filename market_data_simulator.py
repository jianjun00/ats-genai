import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

def simulate_market_data(symbol: str, start_time: datetime, num_ticks: int, interval_seconds: int = 60) -> List[Dict[str, Any]]:
    """
    Simulates a stream of market data ticks for a given symbol.
    Each tick includes price, volume, and time.
    """
    ticks = []
    price = 100.0
    volume = 1000
    for i in range(num_ticks):
        # Random walk for price
        price += random.uniform(-1, 1)
        price = max(1, price)  # Prevent negative price
        # Random volume
        volume = max(1, int(volume + random.uniform(-100, 100)))
        tick_time = start_time + timedelta(seconds=i * interval_seconds)
        ticks.append({
            'symbol': symbol,
            'bid': price - 0.05,
            'ask': price + 0.05,
            'last': price,
            'time': tick_time,
            'volume': volume
        })
    return ticks
