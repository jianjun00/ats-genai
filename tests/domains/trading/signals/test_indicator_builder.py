from datetime import datetime, timedelta
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.indicator_interval import IndicatorInterval
from domains.trading.services.indicators_builder import IndicatorBuilder
# PL indicator does not exist, OneOneHigh, OneOneLow, OneOneDot, ETop, EBot

class DummyIndicatorConfig:
    def __init__(self):
        self.indicators = {
            'PL': PL,
            'OneOneHigh': OneOneHigh,
            'OneOneLow': OneOneLow,
            'OneOneDot': OneOneDot,
        }

def make_intervals(inst_id, base_time, n, valid=True, nan_field=None):
    intervals = []
    for i in range(n):
        dt = base_time + timedelta(days=i)
        interval = InstrumentInterval(
            instrument_id=inst_id,
            start_date_time=dt,
            end_date_time=dt + timedelta(hours=1),
            open=1.0 + i,
            high=2.0 + i,
            low=0.5 + i,
            close=1.5 + i,
            traded_volume=1000 + i,
            traded_dollar=(1.5 + i) * (1000 + i),
            status='ok' if valid else 'invalid',
        )
        if nan_field:
            setattr(interval, nan_field, None)
        intervals.append(interval)
    return intervals

def test_indicator_builder_all_valid():
    config = DummyIndicatorConfig()
    builder = IndicatorBuilder(config)
    base_time = datetime(2023, 1, 1)
    rolling_cache = {
        1: make_intervals(1, base_time, 3),
        2: make_intervals(2, base_time, 3),
    }
    res = builder.build_indicator_intervals(rolling_cache, base_time, base_time + timedelta(hours=1))
    assert set(res.keys()) == {1, 2}
    for iid, interval in res.items():
        assert isinstance(interval, IndicatorInterval)
        assert interval.instrument_id == iid
        for name in config.indicators:
            assert interval.has_indicator(name)
            status = interval.get_indicator_status(name)
            assert status == 'ok', f"Indicator {name} for iid={iid} should be ok"
            val = interval.get_indicator_value(name)
            assert val is not None

def test_indicator_builder_missing_ohlc():
    config = DummyIndicatorConfig()
    builder = IndicatorBuilder(config)
    base_time = datetime(2023, 1, 1)
    # Make one interval with missing close
    rolling_cache = {
        1: make_intervals(1, base_time, 3, nan_field='close'),
    }
    res = builder.build_indicator_intervals(rolling_cache, base_time, base_time + timedelta(hours=1))
    interval = res[1]
    for name in config.indicators:
        assert interval.has_indicator(name)
        status = interval.get_indicator_status(name)
        assert status == 'invalid', f"Indicator {name} should be invalid if OHLC missing"
        val = interval.get_indicator_value(name)
        assert val is None

def test_indicator_builder_invalid_status():
    config = DummyIndicatorConfig()
    builder = IndicatorBuilder(config)
    base_time = datetime(2023, 1, 1)
    # All intervals have status 'invalid'
    rolling_cache = {
        1: make_intervals(1, base_time, 3, valid=False),
    }
    res = builder.build_indicator_intervals(rolling_cache, base_time, base_time + timedelta(hours=1))
    interval = res[1]
    for name in config.indicators:
        assert interval.has_indicator(name)
        status = interval.get_indicator_status(name)
        assert status == 'invalid', f"Indicator {name} should be invalid if interval status is not ok"
        val = interval.get_indicator_value(name)
        assert val is None

def test_indicator_builder_multiple_instruments_and_edge_cases():
    config = DummyIndicatorConfig()
    builder = IndicatorBuilder(config)
    base_time = datetime(2023, 1, 1)
    # Instrument 1: valid, Instrument 2: missing high, Instrument 3: all invalid
    rolling_cache = {
        1: make_intervals(1, base_time, 3),
        2: make_intervals(2, base_time, 3, nan_field='high'),
        3: make_intervals(3, base_time, 3, valid=False),
    }
    res = builder.build_indicator_intervals(rolling_cache, base_time, base_time + timedelta(hours=1))
    assert set(res.keys()) == {1, 2, 3}
    # Instrument 1: all ok
    for name in config.indicators:
        assert res[1].get_indicator_status(name) == 'ok'
        assert res[1].get_indicator_value(name) is not None
    # Instrument 2: all invalid due to missing high
    for name in config.indicators:
        assert res[2].get_indicator_status(name) == 'invalid'
        assert res[2].get_indicator_value(name) is None
    # Instrument 3: all invalid
    for name in config.indicators:
        assert res[3].get_indicator_status(name) == 'invalid'
        assert res[3].get_indicator_value(name) is None

def test_indicator_builder_7_day_print():
    # Setup
    config = DummyIndicatorConfig()
    # Add ETop, EBot, and OneOneDot for printout
    config.indicators['ETop'] = ETop
    config.indicators['EBot'] = EBot
    config.indicators['PLDot'] = OneOneDot
    builder = IndicatorBuilder(config)
    base_time = datetime(2025, 7, 20)
    n_days = 8  # 2025-07-20 to 2025-07-27 inclusive
    rolling_cache = {
        1: make_intervals(1, base_time, n_days),
        2: make_intervals(2, base_time, n_days),
    }
    # Build indicator intervals (simulate a 1-day window at a time)
    for i in range(n_days):
        window_start = base_time + timedelta(days=i)
        window_end = window_start + timedelta(hours=1)
        res = builder.build_indicator_intervals({k: v[:i+1] for k,v in rolling_cache.items()}, window_start, window_end)
        for iid in [1,2]:
            # Get InstrumentInterval for this instrument/date
            instr_interval = rolling_cache[iid][i]
            indicator_interval = res[iid]
            print(f"Instrument {iid} | Date {window_start.date()} | ", end='')
            print(f"Open={instr_interval.open} High={instr_interval.high} Low={instr_interval.low} Close={instr_interval.close}", end=' ')
            etop = indicator_interval.get_indicator_value('ETop')
            ebot = indicator_interval.get_indicator_value('EBot')
            pldot = indicator_interval.get_indicator_value('PLDot')
            print(f"ETop={etop} EBot={ebot} PLDot={pldot}")
