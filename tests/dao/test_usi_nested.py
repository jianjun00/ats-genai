import pytest
from datetime import datetime, timedelta
from shared.utils.environment import Environment, EnvironmentType
from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO
from domains.instruments.repositories.instrument_interval_dao import InstrumentIntervalDAO
from state.universe_state import UniverseStateInterval
from state.instrument_interval import InstrumentInterval
from core.calendars.time_duration import TimeDuration

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_interval_nested_loading(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    usi_dao = UniverseStateIntervalDAO(env)
    instr_dao = InstrumentIntervalDAO(env)
    universe_id = 77
    instrument_id = 888
    duration = TimeDuration("1d")
    start_date_time = datetime(2025, 8, 8, 9, 30)
    end_date_time = start_date_time + timedelta(days=1)

    # Insert parent UniverseStateInterval
    usi_id = await usi_core.dao.create(universe_id, duration.get_duration_string(), start_date_time, end_date_time)

    # Insert nested InstrumentInterval
    await instr_core.dao.create(
        usi_id,
        instrument_id,
        100.0, 110.0, 90.0, 105.0, 1000.0, 105000.0, "ok", 1e9
    )

    # Load via DAO with nested loading
    loaded = await usi_core.dao.async_load_row_to_interval({
        'id': usi_id,
        'universe_id': universe_id,
        'duration': duration.get_duration_string(),
        'start_date_time': start_date_time,
        'end_date_time': end_date_time
    })
    assert isinstance(loaded, UniverseStateInterval)
    assert loaded.universe_id == universe_id
    assert loaded.duration.get_duration_string() == duration.get_duration_string()
    assert loaded.start_date_time == start_date_time
    assert loaded.end_date_time == end_date_time
    assert instrument_id in loaded.instrument_intervals
    instr = loaded.instrument_intervals[instrument_id]
    assert isinstance(instr, InstrumentInterval)
    assert instr.open == 100.0
    assert instr.high == 110.0
    assert instr.low == 90.0
    assert instr.close == 105.0
    assert instr.traded_volume == 1000.0
    assert instr.traded_dollar == 105000.0
    assert instr.status == "ok"
    assert instr.market_cap == 1e9
