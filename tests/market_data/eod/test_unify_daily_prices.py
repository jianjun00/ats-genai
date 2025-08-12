import json
import pytest
from datetime import datetime
from pathlib import Path
from src.market_data.eod.unify_daily_prices import FileDailyPricesUnifier, DailyPricesUnifierBase

def load_fixture_prices(log_dir, symbol, provider):
    """
    Loads price data from the log response json for a given symbol and provider ('tiingo' or 'polygon').
    Returns a dict keyed by date.
    """
    fname = f"{provider}_{symbol.lower()}_response.json"
    fpath = Path(log_dir) / fname
    with open(fpath) as f:
        data = json.load(f)
    prices = {}
    if provider == 'tiingo':
        for row in data:
            dt = row['date'][:10]
            prices[dt] = row
    elif provider == 'polygon':
        for row in data['results']:
            dt = datetime.utcfromtimestamp(row['t']/1000).strftime('%Y-%m-%d')
            prices[dt] = {'open': row['o'], 'high': row['h'], 'low': row['l'], 'close': row['c'], 'volume': row['v']}
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
    ("AAPL", "2024-01-01", "2025-07-31"),
    ("TSLA", "2024-01-01", "2025-07-31"),
])
def test_unify_daily_prices_discrepancies(symbol, start_date, end_date):
    tiingo_dir = "tests/data/daily_prices_tiingo"
    polygon_dir = "tests/data/daily_prices_polygon"
    tiingo = load_fixture_prices(tiingo_dir, symbol, "tiingo")
    polygon = load_fixture_prices(polygon_dir, symbol, "polygon")
    # Build data dicts for FileDailyPricesUnifier
    tiingo_data = {symbol: tiingo}
    polygon_data = {symbol: polygon}
    unifier = FileDailyPricesUnifier(environment=None, tiingo_data=tiingo_data, polygon_data=polygon_data)
    # Run unification for the date range
    import asyncio
    from datetime import datetime, date
    # Convert string dates to date objects
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    current_date = end_date_obj  # Use end_date as current_date for testing
    results = asyncio.run(unifier.unify_daily_prices(symbol, (start_date_obj, end_date_obj), current_date))
    # Analyze results
    import pandas as pd
    from pandas.tseries.holiday import USFederalHolidayCalendar
    stats = {
        'total_dates': 0, 'only_tiingo': 0, 'only_polygon': 0, 'close_enough': 0,
        'conflict': 0, 'invalid': 0, 'invalid_holiday': 0, 'invalid_weekend': 0, 'invalid_weekday': 0, 'conflicts': [], 'invalid_notes': []
    }
    cal = USFederalHolidayCalendar()
    # NYSE holidays are more than USFederalHolidayCalendar, but this is a close proxy for test
    for row in results:
        stats['total_dates'] += 1
        classified = False
        if row['status'] == 'valid':
            if row['source'] == 'tiingo':
                stats['only_tiingo'] += 1
                classified = True
            elif row['source'] == 'polygon':
                stats['only_polygon'] += 1
                classified = True
            elif row['source'] == 'both':
                stats['close_enough'] += 1
                classified = True
        elif row['status'] == 'conflict':
            stats['conflict'] += 1
            stats['conflicts'].append({'date': row['date'], 'note': row['note']})
            classified = True
        elif row['status'] == 'invalid':
            stats['invalid'] += 1
            # Classify as holiday, weekend, or weekday
            dt = pd.to_datetime(row['date'])
            weekday = dt.weekday()
            us_holidays = cal.holidays(start=dt, end=dt)
            if weekday >= 5:
                stats['invalid_weekend'] += 1
            elif dt in us_holidays:
                stats['invalid_holiday'] += 1
            else:
                stats['invalid_weekday'] += 1
            # Save a sample of notes for debug
            if len(stats['invalid_notes']) < 10:
                stats['invalid_notes'].append({'date': row['date'], 'note': row['note']})
            classified = True
        if not classified:
            print(f"Unclassified row: date={row['date']} status={row['status']} source={row['source']}")
    print(f"Discrepancy stats for {symbol}: {stats}")
    if stats['invalid_notes']:
        print("Sample invalid notes:")
        for n in stats['invalid_notes']:
            print(f"Invalid on {n['date']}: {n['note']}")
    # Add 'missing' count to the assertion to account for dates with no vendor data
    stats['missing'] = len([r for r in results if r['status'] == 'missing'])
    assert stats['total_dates'] == (stats['only_tiingo'] + stats['only_polygon'] + stats['close_enough'] + stats['conflict'] + stats['invalid'] + stats['missing'])
    if stats['conflict'] > 0:
        for c in stats['conflicts']:
            print(f"Conflict on {c['date']}: {c['note']}")
