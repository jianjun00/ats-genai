import pandas as pd
import pytest
from datetime import datetime, timedelta
from state.universe_state_manager import UniverseStateManager

class DummyManager(UniverseStateManager):
    def __init__(self, df):
        super().__init__(env=None)
        self._cache = {'dummy': df}

@pytest.fixture
def sample_df():
    # 10 consecutive days for instrument_id=1
    base_date = datetime(2024, 1, 1)
    data = []
    for i in range(10):
        row = {
            'instrument_id': 1,
            'date': base_date + timedelta(days=i),
            'open': i + 1,
            'high': i + 2,
            'low': i,
            'close': i + 1.5,
            'etop': i * 10,
            'ebot': i * 20,
            'pldot': i * 30,
        }
        data.append(row)
    return pd.DataFrame(data)

def test_get_lag_prices(sample_df):
    mgr = DummyManager(sample_df)
    cur_date = datetime(2024, 1, 6)  # 5 days after base
    lag_days = 3
    lag_df = mgr.get_lag_prices(1, cur_date, lag_days)
    # Should be rows for Jan 3, 4, 5
    expected_dates = [datetime(2024, 1, 3), datetime(2024, 1, 4), datetime(2024, 1, 5)]
    assert list(lag_df.index) == [0, 1, 2]
    assert list(lag_df['open']) == [3, 4, 5]
    assert list(lag_df['etop']) == [20, 30, 40]
    assert lag_df.shape == (3, 7)

def test_get_lead_prices(sample_df):
    mgr = DummyManager(sample_df)
    cur_date = datetime(2024, 1, 6)
    lead_days = 2
    lead_df = mgr.get_lead_prices(1, cur_date, lead_days)
    # Should be rows for Jan 7, 8
    assert list(lead_df['high']) == [8, 9]
    assert list(lead_df['low']) == [6, 7]
    assert lead_df.shape == (2, 2)
