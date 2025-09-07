import os
import httpx
from datetime import datetime
from events.schemas import EventIn

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

def fetch_finnhub_earnings(symbol: str, start: str, end: str):
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY not set in environment.")
    url = (
        f"https://finnhub.io/api/v1/calendar/earnings"
        f"?symbol={symbol}&from={start}&to={end}&token={FINNHUB_API_KEY}"
    )
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data.get("earningsCalendar", []):
        event_time = datetime.strptime(item["date"], "%Y-%m-%d")
        yield EventIn(
            event_type="earnings",
            symbol=symbol,
            event_time=event_time,
            reported_time=None,
            source="finnhub",
            data=item,
        )
# Example usage:
# for event in fetch_finnhub_earnings('AAPL', '2024-01-01', '2025-01-01'):
#     print(event)
