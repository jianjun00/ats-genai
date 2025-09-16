from datetime import datetime
from core.calendars.time_duration import TimeDuration
from state.universe_state import UniverseStateInterval
from state.factor_interval import FactorInterval
from state.instrument_interval import InstrumentInterval
from state.indicator_interval import IndicatorInterval

def make_sample_universe_state():
    # Minimal, but covers all nested fields for roundtrip
    duration = TimeDuration("5m")
    start = datetime(2023, 1, 1, 9, 30)
    end = datetime(2023, 1, 1, 9, 35)
    instr_int = InstrumentInterval(
        instrument_id=1,
        start_date_time=start,
        end_date_time=end,
        open=100.0,
        high=105.0,
        low=99.0,
        close=104.0,
        traded_volume=10000.0,
        traded_dollar=1040000.0,
        status="ok",
        market_cap=1e9
    )
    factor_int = FactorInterval(
        start_date_time=start,
        end_date_time=end,
        instrument_intervals={1: instr_int}
    )
    indicator_int = IndicatorInterval(
        instrument_id=1,
        start_date_time=start,
        end_date_time=end,
        indicators={"PL": {"value": 1.23, "status": "ok", "update_at": start}}
    )
    return UniverseStateInterval(
        duration=duration,
        start_date_time=start,
        end_date_time=end,
        factor_intervals=[factor_int],
        instrument_intervals={1: instr_int},
        instrument_indicator_intervals={"PL": {1: indicator_int}}
    )

def test_universe_state_interval_proto_roundtrip():
    orig = make_sample_universe_state()
    proto_msg = orig.to_proto()
    roundtrip = UniverseStateInterval.from_proto(proto_msg)
    assert roundtrip.duration.get_duration_string() == orig.duration.get_duration_string()
    assert roundtrip.start_date_time == orig.start_date_time
    assert roundtrip.end_date_time == orig.end_date_time
    assert len(roundtrip.factor_intervals) == 1
    assert roundtrip.factor_intervals[0].start_date_time == orig.factor_intervals[0].start_date_time
    assert roundtrip.instrument_intervals[1].open == orig.instrument_intervals[1].open
    assert list(roundtrip.instrument_indicator_intervals.keys()) == list(orig.instrument_indicator_intervals.keys())
    # Check indicator value
    assert roundtrip.instrument_indicator_intervals["PL"][1].indicators["PL"]["value"] == "1.23"
