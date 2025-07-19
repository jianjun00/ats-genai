import os
import httpx
from datetime import datetime
from events.schemas import EventIn

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

def fetch_polygon_corporate_actions(symbol: str, start: str = None, end: str = None):
    if not POLYGON_API_KEY:
        raise RuntimeError("POLYGON_API_KEY not set in environment.")
    url = f"https://api.polygon.io/v3/reference/corporate-actions?ticker={symbol}&apiKey={POLYGON_API_KEY}"
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data.get("results", []):
        event_time = datetime.fromisoformat(item["declaration_date"])
        yield EventIn(
            event_type="corporate_action",
            symbol=symbol,
            event_time=event_time,
            reported_time=None,
            source="polygon",
            data=item,
        )
# Example usage:
# for event in fetch_polygon_corporate_actions('AAPL'):
#     print(event)
