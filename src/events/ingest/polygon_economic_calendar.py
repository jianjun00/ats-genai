import os
import httpx
from datetime import datetime
from events.schemas import EventIn

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

def fetch_polygon_economic_calendar(start: str = None, end: str = None):
    if not POLYGON_API_KEY:
        raise RuntimeError("POLYGON_API_KEY not set in environment.")
    url = f"https://api.polygon.io/v3/reference/economic-events?apiKey={POLYGON_API_KEY}"
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data.get("results", []):
        event_time = datetime.fromisoformat(item["date"])
        yield EventIn(
            event_type="economic_event",
            symbol=None,
            event_time=event_time,
            reported_time=None,
            source="polygon",
            data=item,
        )
# Example usage:
# for event in fetch_polygon_economic_calendar():
#     print(event)
