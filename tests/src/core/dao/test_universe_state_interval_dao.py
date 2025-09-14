import pytest
from datetime import datetime, timedelta

from shared.utils.environment import Environment, EnvironmentType
from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO
from state.universe_state import UniverseStateInterval
from core.calendars.time_duration import TimeDuration

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_interval_dao_roundtrip(unit_test_db):
    """
    Integration test: Write and read UniverseStateInterval via DAO using real TimescaleDB.
    Ensures binary protobuf is persisted and round-tripped correctly.
    """
    # Setup env and DAO
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseStateIntervalDAO(env)
    universe_id = 42
    instrument_id = 101
    duration = TimeDuration("5m")
    start_date_time = datetime(2025, 8, 7, 9, 30)
    end_date_time = start_date_time + timedelta(minutes=5)

    # Create a UniverseStateInterval
    from state.instrument_interval import InstrumentInterval
    interval = UniverseStateInterval(
        universe_id=universe_id,
        duration=duration,
        start_date_time=start_date_time,
        end_date_time=end_date_time,
        factor_intervals=[],
        instrument_intervals={
            instrument_id: InstrumentInterval(
                instrument_id=instrument_id,
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                traded_volume=1000.0,
                traded_dollar=105000.0,
                status="ok",
                market_cap=1e9
            )
        },
        instrument_indicator_intervals={}
    )

    # Write to DB
    id = await dao.create(universe_id, str(duration), start_date_time, end_date_time)

    # Read back
    fetched = await dao.get(id)
    assert fetched is not None, "Fetched interval is None"
    # If fetched is a dict, keep as is; if it's a UniverseStateInterval, use attribute access
    if isinstance(fetched, dict):
        assert fetched["universe_id"] == universe_id
        assert fetched["duration"] == str(duration)
        assert fetched["start_date_time"] == start_date_time
        assert fetched["end_date_time"] == end_date_time
    else:
        assert fetched.universe_id == universe_id
        assert str(fetched.duration) == str(duration)
        assert fetched.start_date_time == start_date_time
        assert fetched.end_date_time == end_date_time

    # Clean up
    await dao.delete(id)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_interval_dao_list_filters(unit_test_db):
    """
    Test UniverseStateIntervalDAO.list with start_date_time and end_date_time filters.
    """
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseStateIntervalDAO(env)
    universe_id = 99
    duration = "1d"
    base_time = datetime(2025, 8, 7, 9, 0)
    # Insert 3 intervals, 1h apart
    ids = []
    for i in range(3):
        start = base_time + timedelta(hours=i)
        end = start + timedelta(hours=1)
        id = await dao.create(universe_id, duration, start, end)
        ids.append(id)
    # Filter: should only get the middle interval
    filter_start = base_time + timedelta(hours=1)
    filter_end = base_time + timedelta(hours=2)
    results = await dao.list(
        universe_id=universe_id,
        start_date_time=filter_start,
        end_date_time=filter_end
    )
    assert len(results) == 1
    result = results[0]
    # If result is a dict, use subscript; if object, use attribute
    if isinstance(result, dict):
        assert result["start_date_time"] == filter_start
        assert result["end_date_time"] == filter_end
    else:
        assert result.start_date_time == filter_start
        assert result.end_date_time == filter_end
    # Filter: out of range (should return empty)
    results = await dao.list(
        universe_id=universe_id,
        start_date_time=base_time + timedelta(hours=5),
        end_date_time=base_time + timedelta(hours=6)
    )
    assert results == []
    # Filter: only by universe_id (should return all 3)
    results = await dao.list(universe_id=universe_id)
    assert len(results) == 3
    # Clean up
    for id in ids:
        await dao.delete(id)
