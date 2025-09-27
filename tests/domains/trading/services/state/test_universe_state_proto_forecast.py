import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Ensure PYTHONPATH=src for imports
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
os.environ["PYTHONPATH"] = f"{SRC_DIR}:{os.environ.get('PYTHONPATH','')}" if os.environ.get('PYTHONPATH') else str(SRC_DIR)
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.business.calendars.time_duration import TimeDuration
from domains.trading.services.state.universe_state import UniverseStateInterval
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.forecast_interval import ForecastInterval

def test_universe_state_interval_proto_roundtrip_with_forecasts():
    duration = TimeDuration("5m")
    start = datetime(2025, 1, 1, 10, 0, 0)
    end = start + timedelta(minutes=5)

    ii = InstrumentInterval(1, start, end, 10.0, 11.0, 9.0, 10.5, 1000.0, 10500.0)

    usi = UniverseStateInterval(
        universe_id=7,
        duration=duration,
        start_date_time=start,
        end_date_time=end,
        factor_intervals=[],
        instrument_intervals={1: ii},
        instrument_indicator_intervals={},
    )
    usi.instrument_forecast_intervals = {
        1: ForecastInterval(instrument_id=1, start_date_time=start, end_date_time=end, forecasts=[0.1, -0.2, 0.3])
    }

    # Serialize
    proto = usi.to_proto()

    # Confirm forecasts are present under 'forecast'
    assert 'forecast' in proto.instrument_indicator_intervals
    fmap = proto.instrument_indicator_intervals['forecast']
    assert 1 in fmap.value
    pind = fmap.value[1]
    assert sorted(list(pind.indicators.keys())) == ["t+1", "t+2", "t+3"]

    # Deserialize
    usi2 = UniverseStateInterval.from_proto(proto)

    # Check forecasts are reconstructed
    assert usi2.instrument_forecast_intervals
    f2 = usi2.instrument_forecast_intervals[1]
    assert f2.instrument_id == 1
    assert f2.start_date_time == start
    assert f2.end_date_time == end
    assert f2.forecasts == [0.1, -0.2, 0.3]
