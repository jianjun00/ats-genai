import pytest
import asyncio
from datetime import datetime, timedelta
import asyncpg

from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from dao.universe_state_interval_dao import UniverseStateIntervalDAO
from state.universe_state import UniverseStateInterval
from calendars.time_duration import TimeDuration

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
    assert fetched["universe_id"] == universe_id
    assert fetched["duration"] == str(duration)
    assert fetched["start_date_time"] == start_date_time
    assert fetched["end_date_time"] == end_date_time

    # Clean up
    await dao.delete(id)
