import datetime as dt

import pandas as pd
import pytest

from state.universe_state_manager import UniverseStateManager


class DummyDuration:
    def get_duration_string(self):
        return "1d"


class DummyUniverseState:
    def __init__(self, universe_id, start_dt, end_dt, df: pd.DataFrame):
        self.universe_id = universe_id
        self.start_date_time = start_dt
        self.end_date_time = end_dt
        self._df = df
        # UniverseStateManager.addUniverseState expects this
        self.instrument_intervals = {}
        self.instrument_indicator_intervals = {}

    def to_dataframe(self) -> pd.DataFrame:
        return self._df


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_in_memory_history_enables_single_day_lag(tmp_path):
    # Create manager that writes to a temp base path to avoid polluting repo
    mgr = UniverseStateManager(env=None, base_path=str(tmp_path))

    # Stub out persistence which requires env/DAO
    async def _noop_save(*args, **kwargs):
        return "noop"
    # assign directly on instance
    setattr(mgr, 'save_universe_state', _noop_save)

    # Build day1 data for instrument 1
    day1 = dt.date(2024, 2, 7)
    df1 = pd.DataFrame([
        {
            "instrument_id": 1,
            "date": day1,
            "open": 100.0,
            "high": 110.0,
            "low": 95.0,
            "close": 105.0,
            "etop": 107.0,
            "ebot": 98.0,
            "pldot": 102.0,
        }
    ])

    dur = DummyDuration()
    us1 = DummyUniverseState(universe_id=1, start_dt=dt.datetime.combine(day1, dt.time()), end_dt=dt.datetime.combine(day1, dt.time(23,59,59)), df=df1)

    # Add first day state
    await mgr.addUniverseState({dur: us1}, current_time=dt.datetime.combine(day1, dt.time()))

    # Now query lag for next day; should pull from in-memory cache (single row)
    cur_date = dt.date(2024, 2, 8)
    lag = mgr.get_lag_prices(instrument_id=1, cur_date=cur_date, lag_days=1)

    assert not lag.empty, "Expected one lag row from in-memory history"
    assert len(lag) == 1
    # Validate values preserved
    row = lag.iloc[0]
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 95.0
    assert row["close"] == 105.0
    assert row["etop"] == 107.0
    assert row["ebot"] == 98.0
    assert row["pldot"] == 102.0


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_in_memory_history_window_tail(tmp_path):
    mgr = UniverseStateManager(env=None, base_path=str(tmp_path))

    async def _noop_save(*args, **kwargs):
        return "noop"
    setattr(mgr, 'save_universe_state', _noop_save)

    dur = DummyDuration()

    # Day1
    day1 = dt.date(2024, 2, 7)
    df1 = pd.DataFrame([
        {"instrument_id": 1, "date": day1, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "etop": 10.7, "ebot": 9.8, "pldot": 10.2}
    ])
    us1 = DummyUniverseState(1, dt.datetime.combine(day1, dt.time()), dt.datetime.combine(day1, dt.time(23,59,59)), df1)
    await mgr.addUniverseState({dur: us1}, current_time=dt.datetime.combine(day1, dt.time()))

    # Day2
    day2 = dt.date(2024, 2, 8)
    df2 = pd.DataFrame([
        {"instrument_id": 1, "date": day2, "open": 11.0, "high": 12.0, "low": 10.0, "close": 11.5, "etop": 11.7, "ebot": 10.2, "pldot": 11.1}
    ])
    us2 = DummyUniverseState(1, dt.datetime.combine(day2, dt.time()), dt.datetime.combine(day2, dt.time(23,59,59)), df2)
    await mgr.addUniverseState({dur: us2}, current_time=dt.datetime.combine(day2, dt.time()))

    # Day3
    day3 = dt.date(2024, 2, 9)
    df3 = pd.DataFrame([
        {"instrument_id": 1, "date": day3, "open": 12.0, "high": 13.0, "low": 11.0, "close": 12.5, "etop": 12.7, "ebot": 11.2, "pldot": 12.1}
    ])
    us3 = DummyUniverseState(1, dt.datetime.combine(day3, dt.time()), dt.datetime.combine(day3, dt.time(23,59,59)), df3)
    await mgr.addUniverseState({dur: us3}, current_time=dt.datetime.combine(day3, dt.time()))

    # Query on day4 with lag_days=2 should return day2 and day3 rows (tail of window)
    cur_date = dt.date(2024, 2, 12)
    lag = mgr.get_lag_prices(instrument_id=1, cur_date=cur_date, lag_days=2)

    assert len(lag) == 2
    # Expect the last two days' close values: 11.5 (day2), 12.5 (day3)
    assert list(lag["close"]) == [11.5, 12.5]
    assert list(lag["open"]) == [11.0, 12.0]


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_history_date_normalization_from_asof_datetime(tmp_path):
    mgr = UniverseStateManager(env=None, base_path=str(tmp_path))

    async def _noop_save(*args, **kwargs):
        return "noop"
    setattr(mgr, 'save_universe_state', _noop_save)

    dur = DummyDuration()

    # Provide only as_of_datetime, no 'date' column
    day1_dt = dt.datetime(2024, 2, 7, 15, 30)
    df1 = pd.DataFrame([
        {"instrument_id": 1, "as_of_datetime": day1_dt.isoformat(), "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "etop": 10.7, "ebot": 9.8, "pldot": 10.2}
    ])
    us1 = DummyUniverseState(1, day1_dt.replace(hour=0, minute=0, second=0, microsecond=0), day1_dt.replace(hour=23, minute=59, second=59), df1)
    await mgr.addUniverseState({dur: us1}, current_time=day1_dt)

    # Query lag on next day; should normalize date from as_of_datetime
    cur_date = dt.date(2024, 2, 8)
    lag = mgr.get_lag_prices(instrument_id=1, cur_date=cur_date, lag_days=1)
    assert len(lag) == 1
    assert lag.iloc[0]["close"] == 10.5


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_history_dedup_keeps_latest_row(tmp_path):
    mgr = UniverseStateManager(env=None, base_path=str(tmp_path))

    async def _noop_save(*args, **kwargs):
        return "noop"
    setattr(mgr, 'save_universe_state', _noop_save)

    dur = DummyDuration()

    day1 = dt.date(2024, 2, 7)
    # First submission for day1
    df_first = pd.DataFrame([
        {"instrument_id": 1, "date": day1, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "etop": 10.7, "ebot": 9.8, "pldot": 10.2}
    ])
    us_first = DummyUniverseState(1, dt.datetime.combine(day1, dt.time()), dt.datetime.combine(day1, dt.time(23,59,59)), df_first)
    await mgr.addUniverseState({dur: us_first}, current_time=dt.datetime.combine(day1, dt.time()))

    # Second submission for same date with updated close
    df_second = pd.DataFrame([
        {"instrument_id": 1, "date": day1, "open": 10.1, "high": 11.1, "low": 9.1, "close": 10.9, "etop": 10.8, "ebot": 9.9, "pldot": 10.3}
    ])
    us_second = DummyUniverseState(1, dt.datetime.combine(day1, dt.time()), dt.datetime.combine(day1, dt.time(23,59,59)), df_second)
    await mgr.addUniverseState({dur: us_second}, current_time=dt.datetime.combine(day1, dt.time()))

    # Query next day; expect the latest duplicate to be retained (close 10.9)
    cur_date = dt.date(2024, 2, 8)
    lag = mgr.get_lag_prices(instrument_id=1, cur_date=cur_date, lag_days=1)
    assert len(lag) == 1
    assert lag.iloc[0]["close"] == 10.9
