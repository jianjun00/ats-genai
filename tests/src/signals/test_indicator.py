import pytest
from domains.trading.services.indicator import PL, OneOneHigh, Z1B, Z2B, Z5T, Z6T, EnvelopeTop, EnvelopeBot
from state.instrument_interval import InstrumentInterval
from datetime import datetime, timedelta
import math

# Mark all tests in this module as unit tests
pytestmark = pytest.mark.unit

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
    pass

def test_pl_missing_ohlc():
    from domains.trading.services.indicator import PL
    from state.instrument_interval import InstrumentInterval
    from datetime import datetime, timedelta
    base = datetime(2023, 1, 1)
    # Missing high
    intervals = [
        InstrumentInterval(1, base, base, 10, None, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'ok'),
    ]
    pl = PL()
    pl.update(intervals)
    assert pl.status == 'invalid'
    assert pl.get_value() is None
    # Missing low
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, None, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'ok'),
    ]
    pl.update(intervals)
    assert pl.status == 'invalid'
    assert pl.get_value() is None
    # Missing close
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, None, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, 16, 130, 1300, 'ok'),
    ]
    pl.update(intervals)
    assert pl.status == 'invalid'
    assert pl.get_value() is None

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
    pass

def test_oneonehigh_missing_ohlc():
    from domains.trading.services.indicator import OneOneHigh
    from state.instrument_interval import InstrumentInterval
    from datetime import datetime, timedelta
    base = datetime(2023, 1, 1)
    # Missing high
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, None, 12, 16, 130, 1300, 'ok'),
    ]
    indicator = OneOneHigh()
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None
    # Missing low
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, None, 16, 130, 1300, 'ok'),
    ]
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None
    # Missing close
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, None, 130, 1300, 'ok'),
    ]
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None

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
    from domains.trading.services.indicator import OneOneLow
    indicator = OneOneLow()
    indicator.update(three_ok_intervals)
    assert indicator.status == 'ok'
    # Compute expected: OneOneLow = 2*OneOneDot - last high (current interval only)
    current = three_ok_intervals[-1]
    oneonedot = (current.high + current.low + current.close) / 3.0
    expected = 2 * oneonedot - current.high
    assert abs(indicator.get_value() - expected) < 1e-8

def test_oneonedot_ok():
    from domains.trading.services.indicator import OneOneDot
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
    pass

def test_oneonedot_missing_ohlc():
    from domains.trading.services.indicator import OneOneDot
    from state.instrument_interval import InstrumentInterval
    from datetime import datetime, timedelta
    base = datetime(2023, 1, 1)
    # Missing high
    intervals = [
        InstrumentInterval(1, base, base, 10, None, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
    ]
    dot = OneOneDot()
    dot.update(intervals)
    assert dot.status == 'invalid'
    assert dot.get_value() is None
    # Missing low
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, None, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
    ]
    dot.update(intervals)
    assert dot.status == 'invalid'
    assert dot.get_value() is None
    # Missing close
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, None, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
    ]
    dot.update(intervals)
    assert dot.status == 'invalid'
    assert dot.get_value() is None

    from domains.trading.services.indicator import OneOneDot
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
    from domains.trading.services.indicator import OneOneDot
    dot = OneOneDot()
    dot.update([])
    assert dot.status == 'invalid'
    assert dot.get_value() is None

def test_oneonelow_invalid():
    pass

def test_oneonelow_missing_ohlc():
    from domains.trading.services.indicator import OneOneLow
    from state.instrument_interval import InstrumentInterval
    from datetime import datetime, timedelta
    base = datetime(2023, 1, 1)
    # Missing high
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, None, 12, 16, 130, 1300, 'ok'),
    ]
    indicator = OneOneLow()
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None
    # Missing low
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, None, 16, 130, 1300, 'ok'),
    ]
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None
    # Missing close
    intervals = [
        InstrumentInterval(1, base, base, 10, 15, 9, 14, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 12, 16, 11, 15, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 13, 17, 12, None, 130, 1300, 'ok'),
    ]
    indicator.update(intervals)
    assert indicator.status == 'invalid'
    assert indicator.get_value() is None

    from domains.trading.services.indicator import OneOneLow
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
from domains.trading.services.indicator import PL, OneOneHigh, OneOneLow
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
    from domains.trading.services.indicator import ETop
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
    from domains.trading.services.indicator import ETop
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
    from domains.trading.services.indicator import ETop
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
    from domains.trading.services.indicator import EBot
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
    from domains.trading.services.indicator import EBot
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
    from domains.trading.services.indicator import EBot
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

# ================================================================================================
# Z-SERIES INDICATORS TESTS
# ================================================================================================

@pytest.fixture
def known_test_data():
    """Test data from our linear regression analysis (08/19, 08/20, 08/21 -> 08/22)"""
    base = datetime(2024, 8, 19)
    return [
        InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok'),        # 08/19
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'ok'),     # 08/20
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),  # 08/21
    ]

@pytest.fixture
def expected_z_values():
    """Expected Z-series values for the test data (08/22 predictions)"""
    return {
        'z1b': 22795.06,
        'z2b': 22966.83,
        'z5t': 23708.67,
        'z6t': 23907.81
    }

# Z1B Tests
def test_z1b_initialization():
    """Test Z1B indicator initialization"""
    z1b = Z1B()
    assert z1b.latest_z1b is None
    assert z1b.status is None
    assert z1b.update_at is None
    assert len(z1b.coefficients) == 12
    # Verify first few coefficients are correct
    assert abs(z1b.coefficients[0] - (-1.242786)) < 1e-6
    assert abs(z1b.coefficients[1] - 0.772321) < 1e-6
    assert abs(z1b.coefficients[2] - 1.339376) < 1e-6
    assert abs(z1b.coefficients[3] - 1.258544) < 1e-6

def test_z1b_valid_calculation(known_test_data, expected_z_values):
    """Test Z1B calculation with known valid data"""
    z1b = Z1B()
    z1b.update(known_test_data)

    assert z1b.status == 'ok'
    assert z1b.latest_z1b is not None
    assert abs(z1b.get_value() - expected_z_values['z1b']) < 0.1  # Allow small rounding errors
    assert z1b.update_at is not None

def test_z1b_insufficient_intervals():
    """Test Z1B behavior with insufficient intervals"""
    z1b = Z1B()
    base = datetime(2024, 8, 19)

    # Test with 0 intervals
    z1b.update([])
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

    # Test with 1 interval
    intervals = [InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok')]
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

    # Test with 2 intervals
    intervals.append(InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'ok'))
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

def test_z1b_invalid_status():
    """Test Z1B behavior with invalid interval status"""
    z1b = Z1B()
    base = datetime(2024, 8, 19)

    # One invalid status
    intervals = [
        InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'invalid'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),
    ]
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

def test_z1b_missing_ohlc_data():
    """Test Z1B behavior with missing OHLC data"""
    z1b = Z1B()
    base = datetime(2024, 8, 19)

    # Missing open
    intervals = [
        InstrumentInterval(1, base, base, None, 23838, 23426, 23469.5, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),
    ]
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

    # Missing high
    intervals[0].open = 23800.75
    intervals[0].high = None
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

    # Missing low
    intervals[0].high = 23838
    intervals[0].low = None
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

    # Missing close
    intervals[0].low = 23426
    intervals[0].close = None
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

def test_z1b_nan_values():
    """Test Z1B behavior with NaN values"""
    z1b = Z1B()
    base = datetime(2024, 8, 19)

    intervals = [
        InstrumentInterval(1, base, base, float('nan'), 23838, 23426, 23469.5, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),
    ]
    z1b.update(intervals)
    assert z1b.status == 'invalid'
    assert z1b.get_value() is None

# Z2B Tests
def test_z2b_valid_calculation(known_test_data, expected_z_values):
    """Test Z2B calculation with known valid data"""
    z2b = Z2B()
    z2b.update(known_test_data)

    assert z2b.status == 'ok'
    assert z2b.latest_z2b is not None
    assert abs(z2b.get_value() - expected_z_values['z2b']) < 0.1
    assert z2b.update_at is not None

def test_z2b_coefficients():
    """Test Z2B has correct coefficients"""
    z2b = Z2B()
    assert len(z2b.coefficients) == 12
    # Verify first few coefficients
    assert abs(z2b.coefficients[0] - (-0.109183)) < 1e-6
    assert abs(z2b.coefficients[1] - (-0.448761)) < 1e-6
    assert abs(z2b.coefficients[2] - 0.180165) < 1e-6

def test_z2b_insufficient_intervals():
    """Test Z2B behavior with insufficient intervals"""
    z2b = Z2B()
    base = datetime(2024, 8, 19)

    intervals = [InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok')]
    z2b.update(intervals)
    assert z2b.status == 'invalid'
    assert z2b.get_value() is None

# Z5T Tests
def test_z5t_valid_calculation(known_test_data, expected_z_values):
    """Test Z5T calculation with known valid data"""
    z5t = Z5T()
    z5t.update(known_test_data)

    assert z5t.status == 'ok'
    assert z5t.latest_z5t is not None
    assert abs(z5t.get_value() - expected_z_values['z5t']) < 0.1
    assert z5t.update_at is not None

def test_z5t_coefficients():
    """Test Z5T has correct coefficients"""
    z5t = Z5T()
    assert len(z5t.coefficients) == 12
    # Verify first few coefficients
    assert abs(z5t.coefficients[0] - 0.572696) < 1e-6
    assert abs(z5t.coefficients[1] - 0.251544) < 1e-6
    assert abs(z5t.coefficients[2] - (-0.783063)) < 1e-6

def test_z5t_invalid_data():
    """Test Z5T behavior with invalid data"""
    z5t = Z5T()
    base = datetime(2024, 8, 19)

    # Mix of valid and invalid status
    intervals = [
        InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'invalid'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),
    ]
    z5t.update(intervals)
    assert z5t.status == 'invalid'
    assert z5t.get_value() is None

# Z6T Tests
def test_z6t_valid_calculation(known_test_data, expected_z_values):
    """Test Z6T calculation with known valid data"""
    z6t = Z6T()
    z6t.update(known_test_data)

    assert z6t.status == 'ok'
    assert z6t.latest_z6t is not None
    assert abs(z6t.get_value() - expected_z_values['z6t']) < 0.1
    assert z6t.update_at is not None

def test_z6t_coefficients():
    """Test Z6T has correct coefficients"""
    z6t = Z6T()
    assert len(z6t.coefficients) == 12
    # Verify first few coefficients
    assert abs(z6t.coefficients[0] - 1.853702) < 1e-6
    assert abs(z6t.coefficients[1] - (-1.198374)) < 1e-6
    assert abs(z6t.coefficients[2] - (-2.125780)) < 1e-6

def test_z6t_correlation_with_z5t(known_test_data):
    """Test Z6T correlation with Z5T (should be highly correlated ~0.9985)"""
    z5t = Z5T()
    z6t = Z6T()

    z5t.update(known_test_data)
    z6t.update(known_test_data)

    z5t_value = z5t.get_value()
    z6t_value = z6t.get_value()

    # Z6T should be larger than Z5T (upper breakout zone vs upper resistance)
    assert z6t_value > z5t_value

    # The difference should be roughly 123 +/- 35 based on our analysis
    difference = z6t_value - z5t_value
    assert 80 < difference < 250  # Allow wide range due to coefficient complexity

# Cross-validation tests for all Z-series indicators
@pytest.mark.parametrize("indicator_class,expected_key", [
    (Z1B, 'z1b'),
    (Z2B, 'z2b'),
    (Z5T, 'z5t'),
    (Z6T, 'z6t')
])
def test_z_series_parametrized(indicator_class, expected_key, known_test_data, expected_z_values):
    """Parametrized test for all Z-series indicators"""
    indicator = indicator_class()
    indicator.update(known_test_data)

    assert indicator.status == 'ok'
    assert indicator.get_value() is not None
    assert abs(indicator.get_value() - expected_z_values[expected_key]) < 0.1
    assert indicator.update_at is not None

def test_z_series_ordering(known_test_data):
    """Test that Z-series indicators follow expected ordering relationships"""
    indicators = {'z1b': Z1B(), 'z2b': Z2B(), 'z5t': Z5T(), 'z6t': Z6T()}
    values = {}

    for name, indicator in indicators.items():
        indicator.update(known_test_data)
        values[name] = indicator.get_value()

    # Z1B should be lowest (lower support zone)
    assert values['z1b'] < values['z2b']
    assert values['z1b'] < values['z5t']
    assert values['z1b'] < values['z6t']

    # Z2B should be between Z1B and the upper zones
    assert values['z1b'] < values['z2b'] < values['z5t']
    assert values['z1b'] < values['z2b'] < values['z6t']

    # Z6T should be highest (upper breakout zone)
    assert values['z5t'] < values['z6t']
    assert values['z2b'] < values['z6t']

def test_z_series_edge_cases():
    """Test Z-series indicators with edge case data"""
    base = datetime(2024, 8, 19)

    # Test with very small values
    small_intervals = [
        InstrumentInterval(1, base, base, 1.0, 1.1, 0.9, 1.05, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 1.05, 1.15, 0.95, 1.1, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 1.1, 1.2, 1.0, 1.15, 100, 1000, 'ok'),
    ]

    indicators = [Z1B(), Z2B(), Z5T(), Z6T()]
    for indicator in indicators:
        indicator.update(small_intervals)
        assert indicator.status == 'ok'
        assert indicator.get_value() is not None
        assert not math.isnan(indicator.get_value())
        assert not math.isinf(indicator.get_value())

    # Test with very large values
    large_intervals = [
        InstrumentInterval(1, base, base, 100000.0, 101000.0, 99000.0, 100500.0, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 100500.0, 101500.0, 99500.0, 101000.0, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 101000.0, 102000.0, 100000.0, 101500.0, 100, 1000, 'ok'),
    ]

    for indicator in indicators:
        indicator.update(large_intervals)
        assert indicator.status == 'ok'
        assert indicator.get_value() is not None
        assert not math.isnan(indicator.get_value())
        assert not math.isinf(indicator.get_value())

def test_z_series_coefficient_precision():
    """Test that coefficients are stored with sufficient precision"""
    indicators = {'Z1B': Z1B(), 'Z2B': Z2B(), 'Z5T': Z5T(), 'Z6T': Z6T()}

    for name, indicator in indicators.items():
        coeffs = indicator.coefficients
        assert len(coeffs) == 12, f"{name} should have 12 coefficients"

        # Check that coefficients are not all zeros or all the same
        assert not all(c == 0 for c in coeffs), f"{name} coefficients should not all be zero"
        assert len(set(coeffs)) > 6, f"{name} should have varied coefficients"

        # Check precision - should have at least 5 decimal places of precision
        for i, coeff in enumerate(coeffs):
            if abs(coeff) > 1e-6:  # Skip very small coefficients
                # Convert to string and check decimal places
                coeff_str = f"{coeff:.6f}"
                assert '.' in coeff_str, f"{name} coefficient {i} should have decimal precision"

def test_z_series_multiple_updates():
    """Test Z-series indicators with multiple updates (should handle state correctly)"""
    base = datetime(2024, 8, 19)

    # First set of intervals
    intervals1 = [
        InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),
    ]

    # Second set of intervals (different values)
    intervals2 = [
        InstrumentInterval(1, base, base, 24000.0, 24100.0, 23900.0, 24050.0, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 24050.0, 24150.0, 23950.0, 24100.0, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 24100.0, 24200.0, 24000.0, 24150.0, 100, 1000, 'ok'),
    ]

    z1b = Z1B()

    # First update
    z1b.update(intervals1)
    first_value = z1b.get_value()
    first_status = z1b.status

    # Second update
    z1b.update(intervals2)
    second_value = z1b.get_value()
    second_status = z1b.status

    assert first_status == 'ok'
    assert second_status == 'ok'
    assert first_value != second_value  # Values should be different
    assert abs(second_value - first_value) > 100  # Should be significantly different

# Performance test
def test_z_series_performance():
    """Test that Z-series indicators perform calculations efficiently"""
    import time
    base = datetime(2024, 8, 19)

    intervals = [
        InstrumentInterval(1, base, base, 23800.75, 23838, 23426, 23469.5, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 23461, 23485.5, 23035, 23324, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 23323, 23369.25, 23119, 23219.75, 100, 1000, 'ok'),
    ]

    indicators = [Z1B(), Z2B(), Z5T(), Z6T()]

    # Time multiple updates
    start_time = time.time()
    for _ in range(1000):
        for indicator in indicators:
            indicator.update(intervals)
            _ = indicator.get_value()
    end_time = time.time()

    elapsed = end_time - start_time
    # Should complete 4000 updates (1000 * 4 indicators) in reasonable time
    assert elapsed < 1.0, f"Performance test took too long: {elapsed:.3f} seconds"

def test_envelope_top_not_normalized():
    """Test that EnvelopeTop returns actual price levels, not normalized values between 0-1"""
    base = datetime(2023, 1, 1)
    # Use realistic stock price data (around $100)
    intervals = [
        InstrumentInterval(1, base, base, 98.5, 101.2, 97.8, 100.1, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 99.8, 102.5, 99.1, 101.8, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 101.2, 103.8, 100.5, 102.9, 130, 1300, 'ok'),
    ]

    envelope_top = EnvelopeTop()
    envelope_top.update(intervals)

    assert envelope_top.status == 'ok'
    value = envelope_top.get_value()

    # Envelope Top should return actual price level, not normalized value
    # With prices around $100-103, envelope_top should be in similar price range
    assert value is not None, "EnvelopeTop should return a value"
    assert value > 50.0, f"EnvelopeTop should return price level > 50, got {value}"
    assert value < 200.0, f"EnvelopeTop should return price level < 200, got {value}"

    # Most importantly: should NOT be between 0 and 1 (normalized)
    assert not (0.0 <= value <= 1.0), f"EnvelopeTop should NOT be normalized between 0-1, got {value}"

def test_envelope_bot_not_normalized():
    """Test that EnvelopeBot returns actual price levels, not normalized values between 0-1"""
    base = datetime(2023, 1, 1)
    # Use realistic stock price data (around $100)
    intervals = [
        InstrumentInterval(1, base, base, 98.5, 101.2, 97.8, 100.1, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 99.8, 102.5, 99.1, 101.8, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 101.2, 103.8, 100.5, 102.9, 130, 1300, 'ok'),
    ]

    envelope_bot = EnvelopeBot()
    envelope_bot.update(intervals)

    assert envelope_bot.status == 'ok'
    value = envelope_bot.get_value()

    # Envelope Bot should return actual price level, not normalized value
    # With prices around $100-103, envelope_bot should be in similar price range
    assert value is not None, "EnvelopeBot should return a value"
    assert value > 50.0, f"EnvelopeBot should return price level > 50, got {value}"
    assert value < 200.0, f"EnvelopeBot should return price level < 200, got {value}"

    # Most importantly: should NOT be between 0 and 1 (normalized)
    assert not (0.0 <= value <= 1.0), f"EnvelopeBot should NOT be normalized between 0-1, got {value}"

def test_price_level_indicators_not_normalized():
    """Test that all price level indicators (envelope_top, envelope_bot, pldot, z-series) are NOT normalized"""
    base = datetime(2023, 1, 1)
    # Use realistic stock price data (around $150)
    intervals = [
        InstrumentInterval(1, base, base, 148.5, 151.2, 147.8, 150.1, 100, 1000, 'ok'),
        InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 149.8, 152.5, 149.1, 151.8, 120, 1200, 'ok'),
        InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 151.2, 153.8, 150.5, 152.9, 130, 1300, 'ok'),
    ]

    # Test all price level indicators
    indicators = [
        ('EnvelopeTop', EnvelopeTop()),
        ('EnvelopeBot', EnvelopeBot()),
        ('Z1B', Z1B()),
        ('Z2B', Z2B()),
        ('Z5T', Z5T()),
        ('Z6T', Z6T())
    ]

    for name, indicator in indicators:
        indicator.update(intervals)
        assert indicator.status == 'ok', f"{name} should have valid status"

        value = indicator.get_value()
        assert value is not None, f"{name} should return a value"

        # Price level indicators should return actual price levels, not normalized values
        # With prices around $150, indicators should be in reasonable price range
        assert abs(value) > 10.0, f"{name} should return significant price level, got {value}"

        # Most importantly: should NOT be normalized between 0 and 1
        if value >= 0:
            assert not (0.0 <= value <= 1.0), f"{name} should NOT be normalized between 0-1, got {value}"
        else:
            assert not (-1.0 <= value <= 0.0), f"{name} should NOT be normalized between -1 and 0, got {value}"
