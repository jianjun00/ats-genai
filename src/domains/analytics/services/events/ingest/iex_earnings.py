import os
import httpx
from datetime import datetime
from events.schemas import EventIn

IEX_API_KEY = os.getenv("IEX_API_KEY")

# IEX Cloud API docs: https://iexcloud.io/docs/api/
def fetch_iex_earnings(symbol: str, start: str, end: str):
    if not IEX_API_KEY:
        raise RuntimeError("IEX_API_KEY not set in environment.")
    url = f"https://cloud.iexapis.com/stable/stock/{symbol}/earnings?token={IEX_API_KEY}&period=quarter"
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data.get("earnings", []):
        event_time = datetime.strptime(item["EPSReportDate"], "%Y-%m-%d")
        yield EventIn(
            event_type="earnings",
            symbol=symbol,
            event_time=event_time,
            reported_time=None,
            source="iex",
            data=item,
        )
# Example usage:
# for event in fetch_iex_earnings('AAPL', '2024-01-01', '2025-01-01'):
#     print(event)
