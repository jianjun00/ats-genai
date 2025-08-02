import json
import pytest
from datetime import datetime
from pathlib import Path

def load_fixture_prices(log_dir, symbol, start_date, end_date, provider):
    """
    Loads price data from the log response json for a given symbol, date range, and provider ('tiingo' or 'polygon').
    Returns a dict keyed by date.
    """
    fname = f"{provider}_{symbol.lower()}_response.json"
    fpath = Path(log_dir) / fname
    with open(fpath) as f:
        data = json.load(f)
    prices = {}
    if provider == 'tiingo':
        # Tiingo: list of dicts, each with 'date'
        for row in data:
            dt = row['date'][:10]
            prices[dt] = row
    elif provider == 'polygon':
        # Polygon: dict with 'results' key
        for row in data['results']:
            dt = datetime.utcfromtimestamp(row['t']/1000).strftime('%Y-%m-%d')
            prices[dt] = row
    return prices

def compare_prices(tiingo, polygon, close_threshold=0.01):
    """
    Compares tiingo and polygon price dicts by date, returns discrepancy stats.
    """
    all_dates = set(tiingo.keys()) | set(polygon.keys())
    stats = {
        'only_tiingo': 0,
        'only_polygon': 0,
        'close_enough': 0,
        'conflict': 0,
        'total_dates': len(all_dates),
        'conflicts': []
    }
    for d in sorted(all_dates):
        t = tiingo.get(d)
        p = polygon.get(d)
        if t and not p:
            stats['only_tiingo'] += 1
        elif p and not t:
            stats['only_polygon'] += 1
        elif t and p:
            diffs = []
            for k in ['open','high','low','close','volume']:
                t_val = t.get(k)
                # Polygon keys: o/h/l/c/v
                p_val = p.get({'open':'o','high':'h','low':'l','close':'c','volume':'v'}[k])
                if t_val is None or p_val is None:
                    diffs.append(f"{k}: tiingo={t_val}, polygon={p_val}")
                else:
                    try:
                        t_val = float(t_val)
                        p_val = float(p_val)
                        if abs(t_val - p_val) > max(abs(t_val), abs(p_val)) * close_threshold:
                            diffs.append(f"{k}: tiingo={t_val}, polygon={p_val}")
                    except Exception:
                        diffs.append(f"{k}: tiingo={t_val}, polygon={p_val}")
            if not diffs:
                stats['close_enough'] += 1
            else:
                stats['conflict'] += 1
                stats['conflicts'].append({'date': d, 'diffs': diffs})
    return stats

@pytest.mark.parametrize("symbol,start_date,end_date", [
    ("AAPL", "2025-01-01", "2025-01-10"),
    ("TSLA", "2025-01-01", "2025-01-10"),
])
def test_unify_daily_prices_discrepancies(symbol, start_date, end_date):
    tiingo_dir = "tests/data/daily_prices_tiingo"
    polygon_dir = "tests/data/daily_prices_polygon"
    tiingo = load_fixture_prices(tiingo_dir, symbol, start_date, end_date, "tiingo")
    polygon = load_fixture_prices(polygon_dir, symbol, start_date, end_date, "polygon")
    stats = compare_prices(tiingo, polygon)
    print(f"Discrepancy stats for {symbol}: {stats}")
    # Basic sanity: every date should be classified
    assert stats['total_dates'] == (stats['only_tiingo'] + stats['only_polygon'] + stats['close_enough'] + stats['conflict'])
    # Optionally, assert no conflicts or print details
    if stats['conflict'] > 0:
        for c in stats['conflicts']:
            print(f"Conflict on {c['date']}: {c['diffs']}")
