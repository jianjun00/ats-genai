import pandas as pd

class DummyBuilder:
    def __init__(self, universe, state_manager):
        pass
    def build_universe_state(self, date_str):
        # Always return a DataFrame with one row for testing
        return pd.DataFrame({
            'instrument_id': [1],
            'low': [5],
            'high': [10],
            'close': [8],
            'volume': [100],
            'adv': [110],
            'pldot': [0.1],
            'etop': [0.2],
            'ebot': [0.3]
        })
