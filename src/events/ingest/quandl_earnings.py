import os
import httpx
from datetime import datetime
from events.schemas import EventIn

QUANDL_API_KEY = os.getenv("QUANDL_API_KEY")

# Quandl's Zacks Earnings data is paid, but here's a stub for the pattern
# Docs: https://docs.data.nasdaq.com/docs/income-statement

def fetch_quandl_earnings(symbol: str, start: str, end: str):
    if not QUANDL_API_KEY:
        raise RuntimeError("QUANDL_API_KEY not set in environment.")
    # Example endpoint (replace with real one for your dataset)
    # url = f"https://data.nasdaq.com/api/v3/datatables/ZACKS/EE.json?ticker={symbol}&date.gte={start}&date.lte={end}&api_key={QUANDL_API_KEY}"
    # r = httpx.get(url)
    # r.raise_for_status()
    # data = r.json()
    # for item in data.get('datatable', {}).get('data', []):
    #     event_time = datetime.strptime(item[1], "%Y-%m-%d")
    #     yield EventIn(
    #         event_type="earnings",
    #         symbol=symbol,
    #         event_time=event_time,
    #         reported_time=None,
    #         source="quandl",
    #         data=item,
    #     )
    return
    yield  # This is a stub
# Example usage:
# for event in fetch_quandl_earnings('AAPL', '2024-01-01', '2025-01-01'):
#     print(event)
