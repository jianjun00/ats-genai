import os
import httpx
from datetime import datetime
from events.schemas import EventIn

FMP_API_KEY = os.getenv("FMP_API_KEY")

# Financial Modeling Prep earnings API docs:
# https://site.financialmodelingprep.com/developer/docs/earnings-calendar-free-api/
def fetch_fmp_earnings(symbol: str, start: str, end: str):
    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY not set in environment.")
    url = f"https://financialmodelingprep.com/api/v3/earning_calendar?symbol={symbol}&from={start}&to={end}&apikey={FMP_API_KEY}"
    r = httpx.get(url)
    r.raise_for_status()
    data = r.json()
    for item in data:
        # FMP returns 'date' as 'YYYY-MM-DD'
        event_time = datetime.strptime(item["date"], "%Y-%m-%d")
        yield EventIn(
            event_type="earnings",
            symbol=symbol,
            event_time=event_time,
            reported_time=None,
            source="fmp",
            data=item,
        )
# Example usage:
# for event in fetch_fmp_earnings('AAPL', '2024-01-01', '2025-01-01'):
#     print(event)
