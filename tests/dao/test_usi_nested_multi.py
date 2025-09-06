import pytest
import asyncio
from datetime import datetime, timedelta
import asyncpg
from shared.utils.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO
from domains.instruments.repositories.instrument_interval_dao import InstrumentIntervalDAO
from domains.trading.repositories.factor_interval_dao import FactorIntervalDAO
from domains.instruments.repositories.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from state.universe_state import UniverseStateInterval
from state.instrument_interval import InstrumentInterval
from state.factor_interval import FactorInterval
from state.indicator_interval import IndicatorInterval
from core.calendars.time_duration import TimeDuration

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_interval_nested_multi(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    usi_dao = UniverseStateIntervalDAO(env)
    instr_dao = InstrumentIntervalDAO(env)
    factor_dao = FactorIntervalDAO(env)
    indicator_dao = InstrumentIndicatorIntervalDAO(env)
    universe_id = 77
    instrument_ids = [888, 999]
    factor_id = 555
    indicator_id = 444
    duration = TimeDuration("1d")
    start_date_time = datetime(2025, 8, 8, 9, 30)
    end_date_time = start_date_time + timedelta(days=1)

    # Insert parent UniverseStateInterval
    usi_id = await usi_core.dao.create(universe_id, duration.get_duration_string(), start_date_time, end_date_time)

    # Insert two InstrumentIntervals
    for iid in instrument_ids:
        await instr_core.dao.create(
            usi_id,
            iid,
            100.0 + iid, 110.0 + iid, 90.0 + iid, 105.0 + iid, 1000.0 + iid, 105000.0 + iid, "ok", 1e9 + iid
        )

    # Insert FactorInterval
    await factor_core.dao.create(
        usi_id,
        str(factor_id),
        123.45
    )

    # Insert InstrumentIndicatorInterval for one instrument
    # Find the instrument_interval_id for instrument_ids[0]
    instr_intervals = await instr_core.dao.list(usi_id)
    instr_interval_id = None
    for row in instr_intervals:
        if row['instrument_id'] == instrument_ids[0]:
            instr_interval_id = row['id']
            break
    assert instr_interval_id is not None, "InstrumentInterval not found for instrument_ids[0]"
    await indicator_core.dao.create(
        instr_interval_id,
        str(indicator_id),
        7.89,
        "ok"
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
    assert set(loaded.instrument_intervals.keys()) == set(instrument_ids)
    for iid in instrument_ids:
        instr = loaded.instrument_intervals[iid]
        assert isinstance(instr, InstrumentInterval)
        assert instr.open == 100.0 + iid
    # Factor interval
    assert len(loaded.factor_intervals) == 1
    f = loaded.factor_intervals[0]
    assert isinstance(f, FactorInterval)
    # FactorInterval does not have a .value attribute; only start/end/instrument_intervals
    # Optionally, check type and times
    assert f.start_date_time == start_date_time
    assert f.end_date_time == end_date_time
    # Instrument indicator interval
    # instrument_indicator_intervals is keyed by indicator_id (str), then instrument_id
    assert str(indicator_id) in loaded.instrument_indicator_intervals
    assert instrument_ids[0] in loaded.instrument_indicator_intervals[str(indicator_id)]
    ind = loaded.instrument_indicator_intervals[str(indicator_id)][instrument_ids[0]]
    assert isinstance(ind, IndicatorInterval)
    assert ind.indicators[str(indicator_id)]['value'] == 7.89
