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

if __name__ == "__main__":
    test_reconcile_events()
