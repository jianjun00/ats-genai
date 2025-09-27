import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pytest

# Ensure PYTHONPATH=src for direct test execution context
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
os.environ["PYTHONPATH"] = f"{SRC_DIR}:{os.environ.get('PYTHONPATH','')}" if os.environ.get('PYTHONPATH') else str(SRC_DIR)
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.business.calendars.time_duration import TimeDuration
from domains.trading.services.state.universe_state import UniverseStateInterval
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.forecast_interval import ForecastInterval

def test_to_dataframe_includes_forecast_rows():
    duration = TimeDuration("5m")
    start = datetime(2025, 1, 1, 10, 0, 0)
    end = start + timedelta(minutes=5)

    # Minimal instrument intervals for two instruments
    ii1 = InstrumentInterval(1, start, end, 10.0, 11.0, 9.0, 10.5, 1000.0, 10500.0)
    ii2 = InstrumentInterval(2, start, end, 20.0, 21.0, 19.0, 20.5, 2000.0, 41000.0)

    usi = UniverseStateInterval(
        universe_id=1,
        duration=duration,
        start_date_time=start,
        end_date_time=end,
        factor_intervals=[],
        instrument_intervals={1: ii1, 2: ii2},
        instrument_indicator_intervals={},
    )

    # Add forecast intervals
    usi.instrument_forecast_intervals = {
        1: ForecastInterval(instrument_id=1, start_date_time=start, end_date_time=end, forecasts=[0.1, 0.2]),
        2: ForecastInterval(instrument_id=2, start_date_time=start, end_date_time=end, forecasts=[-0.05])
    }

    df = usi.to_dataframe()

    # There should be 2 OHLC rows + 3 forecast rows = 5
    assert len(df) == 5

    # Check presence of forecast indicator rows
    frows = df[df["indicator_name"].notna()].copy()
    names = set(frows["indicator_name"].tolist())
    assert names == {"forecast_t+1", "forecast_t+2"}

    # Validate values by instrument
    row_1_t1 = frows[(frows.instrument_id == 1) & (frows.indicator_name == "forecast_t+1")]
    assert pytest.approx(row_1_t1.iloc[0].indicator_value) == 0.1
    row_1_t2 = frows[(frows.instrument_id == 1) & (frows.indicator_name == "forecast_t+2")]
    assert pytest.approx(row_1_t2.iloc[0].indicator_value) == 0.2
    row_2_t1 = frows[(frows.instrument_id == 2) & (frows.indicator_name == "forecast_t+1")]
    assert pytest.approx(row_2_t1.iloc[0].indicator_value) == -0.05
