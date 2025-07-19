import asyncio
from typing import List, Dict
from datetime import datetime
from events.schemas import EventIn
from events.db import insert_event
from events.ingest.yahoo_earnings import fetch_yahoo_earnings
from events.ingest.finnhub_earnings import fetch_finnhub_earnings
from events.ingest.iex_earnings import fetch_iex_earnings
from events.ingest.investing_earnings import fetch_investing_earnings
from events.ingest.quandl_earnings import fetch_quandl_earnings
from events.ingest.fmp_earnings import fetch_fmp_earnings
from events.ingest.polygon_earnings import fetch_polygon_earnings
from events.ingest.polygon_news import fetch_polygon_news
from events.ingest.polygon_corporate_actions import fetch_polygon_corporate_actions
from events.ingest.polygon_economic_calendar import fetch_polygon_economic_calendar
from events.ingest.polygon_market_data import fetch_polygon_market_data

# --- Reconciliation Logic ---
def group_events_by_key(events: List[EventIn]):
    grouped: Dict[str, List[EventIn]] = {}
    for event in events:
        key = f"{event.symbol or ''}|{event.event_type}|{event.event_time.isoformat()}"
        grouped.setdefault(key, []).append(event)
    return grouped

def reconcile_events(event_list: List[EventIn]) -> EventIn:
    unified = {}
    sources = []
    raw = {}
    for event in event_list:
        src = event.source or 'unknown'
        sources.append(src)
        raw[src] = event.data
        for k, v in event.data.items():
            if k not in unified or unified[k] is None:
                unified[k] = v
    base = event_list[0]
    return EventIn(
        event_type=base.event_type,
        symbol=base.symbol,
        event_time=base.event_time,
        reported_time=base.reported_time,
        source="unified",
        data=unified,
        sources=sources,
        raw=raw,
    )

def test_reconcile_events():
    # Simple test for reconciliation logic
    e1 = EventIn(event_type='earnings', symbol='AAPL', event_time=datetime(2025,1,1), data={'eps': 2.5}, source='yahoo')
    e2 = EventIn(event_type='earnings', symbol='AAPL', event_time=datetime(2025,1,1), data={'eps': 2.6, 'revenue': 100}, source='finnhub')
    unified = reconcile_events([e1, e2])
    assert 'eps' in unified.data and 'revenue' in unified.data
    assert 'yahoo' in unified.sources and 'finnhub' in unified.sources
    print("Reconciliation test passed.")

async def fetch_all_events(symbol: str, start: str, end: str) -> List[EventIn]:
    events = []
    # Yahoo
    try:
        events.extend(fetch_yahoo_earnings(symbol, start, end))
    except Exception as e:
        print(f"Yahoo fetch error: {e}")
    # Finnhub
    try:
        events.extend(fetch_finnhub_earnings(symbol, start, end))
    except Exception as e:
        print(f"Finnhub fetch error: {e}")
    # IEX
    try:
        events.extend(fetch_iex_earnings(symbol, start, end))
    except Exception as e:
        print(f"IEX fetch error: {e}")
    # Investing.com
    try:
        events.extend(fetch_investing_earnings(symbol, start, end))
    except Exception as e:
        print(f"Investing fetch error: {e}")
    # Quandl
    try:
        events.extend(fetch_quandl_earnings(symbol, start, end))
    except Exception as e:
        print(f"Quandl fetch error: {e}")
    # Financial Modeling Prep
    try:
        events.extend(fetch_fmp_earnings(symbol, start, end))
    except Exception as e:
        print(f"FMP fetch error: {e}")
    # Financial Modeling Prep
    try:
        events.extend(fetch_fmp_earnings(symbol, start, end))
    except Exception as e:
        print(f"FMP fetch error: {e}")
    # Polygon.io Earnings
    try:
        events.extend(fetch_polygon_earnings(symbol, start, end))
    except Exception as e:
        print(f"Polygon earnings fetch error: {e}")
    # Polygon.io News
    try:
        events.extend(fetch_polygon_news(symbol, start, end))
    except Exception as e:
        print(f"Polygon news fetch error: {e}")
    # Polygon.io Corporate Actions
    try:
        events.extend(fetch_polygon_corporate_actions(symbol, start, end))
    except Exception as e:
        print(f"Polygon corp actions fetch error: {e}")
    return events

async def ingest_unified_events(symbol: str, start: str, end: str):
    events = await fetch_all_events(symbol, start, end)
    grouped = group_events_by_key(events)
    for event_list in grouped.values():
        unified = reconcile_events(event_list)
        await insert_event(unified)
    print(f"Inserted {len(grouped)} unified events for {symbol}.")

async def ingest_for_multiple_symbols(symbols: List[str], start: str, end: str):
    for symbol in symbols:
        await ingest_unified_events(symbol, start, end)

# Example usage for batch ingestion:
# asyncio.run(ingest_for_multiple_symbols(['AAPL', 'MSFT', 'GOOG'], '2024-01-01', '2025-01-01'))

def test_polygon_fetchers():
    # Test Polygon.io earnings
    try:
        earnings = list(fetch_polygon_earnings('AAPL', '2024-01-01', '2025-01-01'))
        print(f"Polygon earnings fetched: {len(earnings)}")
    except Exception as e:
        print(f"Polygon earnings test error: {e}")
    # Test Polygon.io news
    try:
        news = list(fetch_polygon_news('AAPL'))
        print(f"Polygon news fetched: {len(news)}")
    except Exception as e:
        print(f"Polygon news test error: {e}")
    # Test Polygon.io corporate actions
    try:
        actions = list(fetch_polygon_corporate_actions('AAPL'))
        print(f"Polygon corp actions fetched: {len(actions)}")
    except Exception as e:
        print(f"Polygon corp actions test error: {e}")
    # Test Polygon.io economic calendar
    try:
        econ = list(fetch_polygon_economic_calendar())
        print(f"Polygon economic events fetched: {len(econ)}")
    except Exception as e:
        print(f"Polygon economic calendar test error: {e}")
    # Test Polygon.io market data
    try:
        bars = list(fetch_polygon_market_data('AAPL', '2024-01-01', '2024-01-03'))
        print(f"Polygon market data bars fetched: {len(bars)}")
    except Exception as e:
        print(f"Polygon market data test error: {e}")

def batch_ingest_polygon_news(symbols, start, end):
    for symbol in symbols:
        for event in fetch_polygon_news(symbol, start, end):
            print(event)
            # Optionally: await insert_event(event)

def batch_ingest_polygon_economic_events():
    for event in fetch_polygon_economic_calendar():
        print(event)
        # Optionally: await insert_event(event)

def batch_ingest_polyaxon_news(symbols, start, end):
    """Alias for Polygon.io news batch ingestion (for user typo or alternate naming)."""
    batch_ingest_polygon_news(symbols, start, end)

def batch_ingest_polyaxon_economic_events():
    """Alias for Polygon.io economic events batch ingestion (for user typo or alternate naming)."""
    batch_ingest_polygon_economic_events()

if __name__ == "__main__":
    test_reconcile_events()
    test_polygon_fetchers()
    # Example usage for batch ingest:
    # batch_ingest_polygon_news(['AAPL', 'MSFT'], '2024-01-01', '2025-01-01')
    # batch_ingest_polygon_economic_events()
