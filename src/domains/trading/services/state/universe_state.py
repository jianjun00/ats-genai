from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime
from src.core.business.calendars.time_duration import TimeDuration
from .factor_interval import FactorInterval
from .instrument_interval import InstrumentInterval
from .indicator_interval import IndicatorInterval
from .forecast_interval import ForecastInterval

@dataclass
class UniverseStateInterval:
    """
    Represents the complete state of the universe for a given interval.
    Contains:
      - duration: The TimeDuration object (e.g., representing '5m', '1h') for this universe state interval
      - start_date_time: Start datetime of the universe state interval
      - end_date_time: End datetime of the universe state interval
      - factor_intervals: The overall interval for the universe (time window & membership)
      - instrument_intervals: Dict mapping instrument_id to InstrumentInterval
      - instrument_indicator_intervals: Dict mapping indicator_type (str) to dict of instrument_id to IndicatorInterval
    """
    # Field definitions MUST come first for dataclass to work properly
    duration: TimeDuration
    start_date_time: datetime
    end_date_time: datetime
    factor_intervals: List[FactorInterval]
    instrument_intervals: Dict[int, InstrumentInterval] = field(default_factory=dict)
    instrument_indicator_intervals: Dict[str, Dict[int, IndicatorInterval]] = field(default_factory=dict)
    instrument_forecast_intervals: Dict[int, ForecastInterval] = field(default_factory=dict)
    universe_id: int = 0

    def to_dataframe(self):
        """
        Flatten all instrument_intervals and instrument_indicator_intervals into a pandas DataFrame.
        Returns columns: start_date_time, instrument_id, open, high, low, close, volume, indicator_name, indicator_value
        """
        import pandas as pd
        rows = []
        # Instrument intervals: OHLCV
        for inst_id, ii in self.instrument_intervals.items():
            rows.append({
                'start_date_time': self.start_date_time,
                'end_date_time': self.end_date_time,
                'instrument_id': inst_id,
                'open': ii.open,
                'high': ii.high,
                'low': ii.low,
                'close': ii.close,
                'volume': getattr(ii, 'traded_volume', 0),
                'indicator_name': None,
                'indicator_value': None
            })
        # Instrument indicator intervals
        for ind_type, ind_map in self.instrument_indicator_intervals.items():
            for inst_id, ind_int in ind_map.items():
                for name, val in ind_int.indicators.items():
                    rows.append({
                        'start_date_time': ind_int.start_date_time,
                        'end_date_time': ind_int.end_date_time,
                        'instrument_id': inst_id,
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'volume': None,
                        'indicator_name': name,
                        'indicator_value': val.get('value')
                    })
        # Forecast intervals (optional)
        if hasattr(self, 'instrument_forecast_intervals') and self.instrument_forecast_intervals:
            for inst_id, f_int in self.instrument_forecast_intervals.items():
                for idx, val in enumerate(f_int.forecasts):
                    rows.append({
                        'start_date_time': f_int.start_date_time,
                        'end_date_time': f_int.end_date_time,
                        'instrument_id': inst_id,
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'volume': None,
                        'indicator_name': f'forecast_t+{idx+1}',
                        'indicator_value': val
                    })
        return pd.DataFrame(rows)