import os
import httpx
from datetime import datetime
from events.schemas import EventIn

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Polygon.io earnings API
# Docs: https://polygon.io/docs/stocks/get_vx_reference_earnings__symbol

def fetch_polygon_earnings(symbol: str, start: str, end: str):
    if not POLYGON_API_KEY:
        raise RuntimeError("POLYGON_API_KEY not set in environment.")
    url = f"https://api.polygon.io/vX/reference/earnings/{symbol}?apiKey={POLYGON_API_KEY}"
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data.get("results", []):
        # Polygon returns 'reportDate' as 'YYYY-MM-DD'
        event_time = datetime.strptime(item["reportDate"], "%Y-%m-%d")
        yield EventIn(
            event_type="earnings",
            symbol=symbol,
            event_time=event_time,
            reported_time=None,
            source="polygon",
            data=item,
        )
# Example usage:
# for event in fetch_polygon_earnings('AAPL', '2024-01-01', '2025-01-01'):
#     print(event)
