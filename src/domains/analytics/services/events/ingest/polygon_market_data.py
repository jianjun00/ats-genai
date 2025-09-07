import os
import httpx
from datetime import datetime
from events.schemas import EventIn

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

def fetch_polygon_market_data(symbol: str, start: str, end: str, timespan: str = 'minute'):
    if not POLYGON_API_KEY:
        raise RuntimeError("POLYGON_API_KEY not set in environment.")
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/{timespan}/{start}/{end}?apiKey={POLYGON_API_KEY}"
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data.get("results", []):
        event_time = datetime.fromtimestamp(item["t"] / 1000)
        yield EventIn(
            event_type="market_data",
            symbol=symbol,
            event_time=event_time,
            reported_time=None,
            source="polygon",
            data=item,
        )
# Example usage:
# for event in fetch_polygon_market_data('AAPL', '2024-01-01', '2024-01-31'):
#     print(event)
