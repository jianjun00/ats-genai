import pytest
from signals.indicator import PL, OneOneHigh
from state.instrument_interval import InstrumentInterval
from datetime import datetime, timedelta

@pytest.fixture
def three_ok_intervals():
    base = datetime(2023, 1, 1)
    return [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'ok'),
    ]

def test_pl_ok(three_ok_intervals):
    pl = PL()
    pl.update(three_ok_intervals)
    assert pl.status == 'ok'
    # Manually compute expected PL
    vals = [ (i.high+i.low+i.close)/3.0 for i in three_ok_intervals ]
    expected = sum(vals)/3.0
    assert abs(pl.get_value() - expected) < 1e-8

def test_pl_invalid():
    base = datetime(2023, 1, 1)
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'invalid'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'ok'),
    ]
    pl = PL()
    pl.update(intervals)
    assert pl.status == 'invalid'
    assert pl.get_value() is None

def test_oneonehigh_ok(three_ok_intervals):
    indicator = OneOneHigh()
    indicator.update(three_ok_intervals)
    assert indicator.status == 'ok'
    # Compute expected: OneOneHigh = 2*OneOneDot - last low (current interval only)
    current = three_ok_intervals[-1]
    oneonedot = (current.high + current.low + current.close) / 3.0
    expected = 2 * oneonedot - current.low
    assert abs(indicator.get_value() - expected) < 1e-8

def test_oneonehigh_invalid():
    base = datetime(2023, 1, 1)
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'invalid'),
    ]
    indicator = OneOneHigh()
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None

def test_oneonelow_ok(three_ok_intervals):
    from signals.indicator import OneOneLow
    indicator = OneOneLow()
    indicator.update(three_ok_intervals)
    assert indicator.status == 'ok'
    # Compute expected: OneOneLow = 2*OneOneDot - last high (current interval only)
    current = three_ok_intervals[-1]
    oneonedot = (current.high + current.low + current.close) / 3.0
    expected = 2 * oneonedot - current.high
    assert abs(indicator.get_value() - expected) < 1e-8

def test_oneonedot_ok():
    from signals.indicator import OneOneDot
    base = datetime(2023, 1, 1)
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
    ]
    dot = OneOneDot()
    dot.update(intervals)
    expected = (intervals[-1].high + intervals[-1].low + intervals[-1].close) / 3.0
    assert dot.status == 'ok'
    assert abs(dot.get_value() - expected) < 1e-8

def test_oneonedot_invalid_status():
    from signals.indicator import OneOneDot
    base = datetime(2023, 1, 1)
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'invalid'),
    ]
    dot = OneOneDot()
    dot.update(intervals)
    assert dot.status == 'invalid'
    assert dot.get_value() is None

def test_oneonedot_invalid_empty():
    from signals.indicator import OneOneDot
    dot = OneOneDot()
    dot.update([])
    assert dot.status == 'invalid'
    assert dot.get_value() is None

def test_oneonelow_invalid():
    from signals.indicator import OneOneLow
    base = datetime(2023, 1, 1)
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'invalid'),
    ]
    indicator = OneOneLow()
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None

import pytest
from signals.indicator import PL, OneOneHigh, OneOneLow
from state.instrument_interval import InstrumentInterval
from datetime import datetime, timedelta

@pytest.mark.parametrize("indicator_cls, attr, values, expected, status_comb, expect_status, expect_val", [
    # Fewer than 3 intervals (PL still needs 3, but OneOneHigh/OneOneLow only need 1)
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200)], None, ['ok','ok'], 'invalid', None),
    (OneOneHigh, 'get_value', [(10,15,9,14,100,1000)], lambda vals: 2*((vals[0][1]+vals[0][2]+vals[0][3])/3.0) - vals[0][2], ['ok'], 'ok', 'compute'),
    (OneOneLow, 'get_value', [(10,15,9,14,100,1000)], lambda vals: 2*((vals[0][1]+vals[0][2]+vals[0][3])/3.0) - vals[0][1], ['ok'], 'ok', 'compute'),
    # All intervals invalid (only last interval matters for OneOneHigh/OneOneLow)
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], None, ['invalid','invalid','invalid'], 'invalid', None),
    (OneOneHigh, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], None, ['invalid','invalid','invalid'], 'invalid', None),
    (OneOneLow, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], None, ['invalid','invalid','invalid'], 'invalid', None),
    # Mixed valid/invalid (only last interval matters for OneOneHigh/OneOneLow)
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], None, ['ok','invalid','ok'], 'invalid', None),
    (OneOneHigh, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], lambda vals: 2*((vals[-1][1]+vals[-1][2]+vals[-1][3])/3.0) - vals[-1][2], ['ok','invalid','ok'], 'ok', 'compute'),
    (OneOneLow, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], lambda vals: 2*((vals[-1][1]+vals[-1][2]+vals[-1][3])/3.0) - vals[-1][1], ['ok','invalid','ok'], 'ok', 'compute'),
    # All valid
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], lambda vals: sum([(v[1]+v[2]+v[3])/3.0 for v in vals])/3.0, ['ok','ok','ok'], 'ok', 'compute'),
    (OneOneHigh, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], lambda vals: 2*((vals[-1][1]+vals[-1][2]+vals[-1][3])/3.0) - vals[-1][2], ['ok','ok','ok'], 'ok', 'compute'),
    (OneOneLow, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,120,1200), (13,17,12,16,130,1300)], lambda vals: 2*((vals[-1][1]+vals[-1][2]+vals[-1][3])/3.0) - vals[-1][1], ['ok','ok','ok'], 'ok', 'compute'),
    # Edge: status not exactly 'ok'
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,100,1000), (13,17,12,16,100,1000)], None, [None,'ok','ok'], 'invalid', None),
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,100,1000), (13,17,12,16,100,1000)], None, ['OK','ok','ok'], 'invalid', None),
    (PL, 'get_value', [(10,15,9,14,100,1000), (12,16,11,15,100,1000), (13,17,12,16,100,1000)], None, ['ok','Ok','ok'], 'invalid', None),
])
def test_indicator_parametrized(indicator_cls, attr, values, expected, status_comb, expect_status, expect_val):
    base = datetime(2023, 1, 1)
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), v[0], v[1], v[2], v[3], v[4], v[5], status_comb[i]) for i,v in enumerate(values)]
    indicator = indicator_cls()
    indicator.update(intervals)
    assert indicator.status == expect_status
    result = getattr(indicator, attr)()
    if expect_val == 'compute':
        assert abs(result - expected(values)) < 1e-8
    else:
        assert result == expect_val

# Rolling window propagation test
@pytest.mark.parametrize("indicator_cls, attr", [
    (PL, 'get_value'),
    (OneOneHigh, 'get_value'),
    (OneOneLow, 'get_value'),
])
def test_indicator_rolling_window(indicator_cls, attr):
    base = datetime(2023, 1, 1)
    vals = [
        (10,15,9,14,100,1000,'ok'),
        (12,16,11,15,120,1200,'ok'),
        (13,17,12,16,130,1300,'ok'),
        (14,18,13,17,140,1400,'ok'),
    ]
    # Different indicators have different minimum interval requirements
    min_intervals = 3 if indicator_cls.__name__ == 'PL' else 1
    
    # Test with insufficient intervals
    for i in range(min_intervals):
        intervals = [InstrumentInterval(1, base+timedelta(days=j), base+timedelta(days=j), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for j,v in enumerate(vals[:i+1])]
        indicator = indicator_cls()
        indicator.update(intervals)
        if i + 1 < min_intervals:
            assert indicator.status == 'invalid'
            assert getattr(indicator, attr)() is None
        else:
            assert indicator.status == 'ok'
            assert getattr(indicator, attr)() is not None
    # Third: valid
    intervals = [InstrumentInterval(1, base+timedelta(days=j), base+timedelta(days=j), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for j,v in enumerate(vals[:3])]
    indicator = indicator_cls()
    indicator.update(intervals)
    assert indicator.status == 'ok'
    assert getattr(indicator, attr)() is not None
    # Fourth: valid rolling
    intervals = [InstrumentInterval(1, base+timedelta(days=j), base+timedelta(days=j), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for j,v in enumerate(vals)]
    indicator = indicator_cls()
    indicator.update(intervals[-3:])
    assert indicator.status == 'ok'
    assert getattr(indicator, attr)() is not None

def test_etop_too_few():
    from signals.indicator import ETop
    base = datetime(2023, 1, 1)
    # Test with 1 interval (too few)
    intervals = [InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok')]
    etop = ETop()
    etop.update(intervals)
    assert etop.status == 'invalid'
    assert etop.get_value() is None
    # Test with 2 intervals (still too few)
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), 10+i, 15+i, 9+i, 14+i, 100, 1000, 'ok') for i in range(2)]
    etop.update(intervals)
    assert etop.status == 'invalid'
    assert etop.get_value() is None

def test_etop_all_ok():
    from signals.indicator import ETop
    base = datetime(2023, 1, 1)
    vals = [
        (10,15,9,14,100,1000,'ok'),
        (12,16,11,15,120,1200,'ok'),
        (13,17,12,16,130,1300,'ok'),
        (14,18,13,17,140,1400,'ok'),
        (15,19,14,18,150,1500,'ok'),
    ]
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for i,v in enumerate(vals)]
    etop = ETop()
    etop.update(intervals)
    # Compute expected: for last 3 intervals, OneOneHigh = 2*OneOneDot - last low
    oneonehighs = []
    for i in range(3):
        current = intervals[-3 + i]
        oneonedot = (current.high + current.low + current.close) / 3.0
        oneonehigh = 2 * oneonedot - current.low
        oneonehighs.append(oneonehigh)
    expected = sum(oneonehighs) / 3.0
    assert etop.status == 'ok'
    assert abs(etop.get_value() - expected) < 1e-8

def test_etop_invalid_in_window():
    from signals.indicator import ETop
    base = datetime(2023, 1, 1)
    vals = [
        (10,15,9,14,100,1000,'ok'),
        (12,16,11,15,120,1200,'ok'),
        (13,17,12,16,130,1300,'ok'),
        (14,18,13,17,140,1400,'ok'),
        (15,19,14,18,150,1500,'invalid'),
    ]
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for i,v in enumerate(vals)]
    etop = ETop()
    etop.update(intervals)
    assert etop.status == 'invalid'
    assert etop.get_value() is None

def test_ebot_too_few():
    from signals.indicator import EBot
    base = datetime(2023, 1, 1)
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), 10+i, 15+i, 9+i, 14+i, 100, 1000, 'ok') for i in range(4)]
    ebot = EBot()
    ebot.update(intervals[:1])
    assert ebot.status == 'invalid'
    assert ebot.get_value() is None
    ebot.update(intervals[:2])
    assert ebot.status == 'invalid'
    assert ebot.get_value() is None
    ebot.update(intervals[:3])
    assert ebot.status == 'invalid'
    assert ebot.get_value() is None

def test_ebot_all_ok():
    from signals.indicator import EBot
    base = datetime(2023, 1, 1)
    vals = [
        (10,15,9,14,100,1000,'ok'),
        (12,16,11,15,120,1200,'ok'),
        (13,17,12,16,130,1300,'ok'),
        (14,18,13,17,140,1400,'ok'),
        (15,19,14,18,150,1500,'ok'),
    ]
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for i,v in enumerate(vals)]
    ebot = EBot()
    ebot.update(intervals)
    # Compute expected: for last 3 intervals, OneOneLow = 2*OneOneDot - last high
    oneonelows = []
    for i in range(3):
        current = intervals[-3 + i]
        oneonedot = (current.high + current.low + current.close) / 3.0
        oneonelow = 2 * oneonedot - current.high
        oneonelows.append(oneonelow)
    expected = sum(oneonelows) / 3.0
    assert ebot.status == 'ok'
    assert abs(ebot.get_value() - expected) < 1e-8

def test_ebot_invalid_in_window():
    from signals.indicator import EBot
    base = datetime(2023, 1, 1)
    vals = [
        (10,15,9,14,100,1000,'ok'),
        (12,16,11,15,120,1200,'ok'),
        (13,17,12,16,130,1300,'ok'),
        (14,18,13,17,140,1400,'ok'),
        (15,19,14,18,150,1500,'invalid'),
    ]
    intervals = [InstrumentInterval(1, base+timedelta(days=i), base+timedelta(days=i), v[0], v[1], v[2], v[3], v[4], v[5], v[6]) for i,v in enumerate(vals)]
    ebot = EBot()
    ebot.update(intervals)
    assert ebot.status == 'invalid'
    assert ebot.get_value() is None
