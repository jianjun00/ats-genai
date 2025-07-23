
import pandas as pd
class DummyBuilder:
    def __init__(self, universe, state_manager): pass
    def build_universe_state(self, date_str):
        return pd.DataFrame({
            'instrument_id': [1],
            'low': [11],
            'high': [22],
            'close': [15],
            'volume': [100],
            'adv': [110],
            'pldot': [0.5],
            'oneonedot': [0.9],
            'etop': [0.7],
            'ebot': [0.2]
        })
