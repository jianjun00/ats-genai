import asyncio
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.state.universe_state_manager import UniverseStateManager


def test_get_lag_prices_from_in_memory_cache_single_day():
    usm = UniverseStateManager()
    iid = 1
    start = date(2024, 1, 1)
    days = 12
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        rows.append({
            'instrument_id': iid,
            'date': d,
            'open': 100 + i,
            'high': 101 + i,
            'low': 99 + i,
            'close': 100.5 + i,
        })
    df = pd.DataFrame(rows)
    usm._instrument_history = {iid: df}

    asof = start + timedelta(days=10)
    out_df = usm.get_lag_prices(iid, asof, lag_days=5)

    # Expect a DataFrame with available feature columns, at least OHLC
    for col in ['open','high','low','close']:
        assert col in out_df.columns
    assert len(out_df) == 5
    assert not out_df[['open','high','low','close']].isna().any().any()
    # Last row should be the day before asof
    last = out_df[['open','high','low','close']].iloc[-1].to_numpy()
    np.testing.assert_allclose(last, np.array([100+9, 101+9, 99+9, 100.5+9]))


def test_get_lag_prices_window_coverage():
    usm = UniverseStateManager()
    iid = 2
    start = date(2024, 2, 1)
    rows = []
    for i in range(30):
        d = start + timedelta(days=i)
        rows.append({
            'instrument_id': iid,
            'date': d,
            'open': float(i),
            'high': float(i)+0.1,
            'low': float(i)-0.1,
            'close': float(i)+0.05,
        })
    df = pd.DataFrame(rows)
    usm._instrument_history = {iid: df}

    asof = start + timedelta(days=20)
    out_df = usm.get_lag_prices(iid, asof, lag_days=10)

    # Ensure 10 rows and no NaNs in OHLC
    assert len(out_df) == 10
    assert not out_df[['open','high','low','close']].isna().any().any()
    # Check the first lag aligns to asof-10 days
    first = out_df[['open','high','low','close']].iloc[0].to_numpy()
    np.testing.assert_allclose(first, np.array([10.0, 10.1, 9.9, 10.05]))


def test_get_lag_prices_accepts_asof_datetime_and_normalizes_date():
    usm = UniverseStateManager()
    iid = 3
    start = date(2024, 3, 1)
    rows = []
    for i in range(5):
        d = start + timedelta(days=i)
        rows.append({
            'instrument_id': iid,
            'date': d,
            'open': 10 + i,
            'high': 11 + i,
            'low': 9 + i,
            'close': 10.5 + i,
        })
    df = pd.DataFrame(rows)
    usm._instrument_history = {iid: df}

    # Pass a datetime with non-midnight time; method should normalize to date
    asof_dt = datetime.combine(start + timedelta(days=4), datetime.min.time()).replace(hour=15, minute=30)
    out_df = usm.get_lag_prices(iid, asof_dt, lag_days=3)

    assert len(out_df) == 3
    assert not out_df[['open','high','low','close']].isna().any().any()
    last = out_df[['open','high','low','close']].iloc[-1].to_numpy()
    # Expect values for the day before asof (i=3)
    np.testing.assert_allclose(last, np.array([13, 14, 12, 13.5]))


def test_get_lag_prices_skips_when_insufficient_history():
    usm = UniverseStateManager()
    iid = 4
    start = date(2024, 4, 1)
    # Only 2 days of data
    df = pd.DataFrame([
        {'instrument_id': iid, 'date': start, 'open': 1, 'high': 2, 'low': 0.5, 'close': 1.5},
        {'instrument_id': iid, 'date': start + timedelta(days=1), 'open': 2, 'high': 3, 'low': 1.5, 'close': 2.5},
    ])
    usm._instrument_history = {iid: df}

    asof = start + timedelta(days=2)
    out_df = usm.get_lag_prices(iid, asof, lag_days=5)

    # Should return fewer than requested lags or an empty DataFrame depending on implementation.
    # We assert that it does not produce NaNs in OHLC if rows exist.
    assert out_df.empty or not out_df[['open','high','low','close']].isna().any().any()
