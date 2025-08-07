from dataclasses import dataclass, field
from typing import Dict, Any
from datetime import datetime
from typing import List
from calendars.time_duration import TimeDuration
from .factor_interval import FactorInterval
from .instrument_interval import InstrumentInterval
from .indicator_interval import IndicatorInterval

from .proto import universe_state_interval_pb2
from .proto import factor_interval_pb2
from .proto import instrument_interval_pb2
from .proto import indicator_interval_pb2
from .proto import time_duration_pb2

@dataclass
class UniverseStateInterval:
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
        return pd.DataFrame(rows)

    # ... existing fields ...

    def to_proto(self):
        """
        Serialize this UniverseStateInterval to a protobuf message.
        """
        msg = universe_state_interval_pb2.UniverseStateInterval()
        msg.duration.value = self.duration.get_duration_string()
        msg.start_date_time = self.start_date_time.isoformat()
        msg.end_date_time = self.end_date_time.isoformat()
        for fi in self.factor_intervals:
            proto_fi = msg.factor_intervals.add()
            proto_fi.start_date_time = fi.start_date_time.isoformat()
            proto_fi.end_date_time = fi.end_date_time.isoformat()
            # Serialize instrument_intervals as map<int32, InstrumentInterval>
            for iid, ii in fi.instrument_intervals.items():
                proto_ii = proto_fi.instrument_intervals[iid]
                proto_ii.instrument_id = ii.instrument_id
                proto_ii.start_date_time = ii.start_date_time.isoformat()
                proto_ii.end_date_time = ii.end_date_time.isoformat()
                proto_ii.open = ii.open
                proto_ii.high = ii.high
                proto_ii.low = ii.low
                proto_ii.close = ii.close
                proto_ii.traded_volume = ii.traded_volume
                proto_ii.traded_dollar = ii.traded_dollar
                if ii.status:
                    proto_ii.status = ii.status
                if ii.market_cap is not None:
                    proto_ii.market_cap = ii.market_cap
        for iid, ii in self.instrument_intervals.items():
            proto_ii = msg.instrument_intervals[iid]
            proto_ii.instrument_id = ii.instrument_id
            proto_ii.start_date_time = ii.start_date_time.isoformat()
            proto_ii.end_date_time = ii.end_date_time.isoformat()
            proto_ii.open = ii.open
            proto_ii.high = ii.high
            proto_ii.low = ii.low
            proto_ii.close = ii.close
            proto_ii.traded_volume = ii.traded_volume
            proto_ii.traded_dollar = ii.traded_dollar
            if ii.status:
                proto_ii.status = ii.status
            if ii.market_cap is not None:
                proto_ii.market_cap = ii.market_cap
        for ind_type, ind_map in self.instrument_indicator_intervals.items():
            indicator_map = msg.instrument_indicator_intervals[ind_type]
            for iid, ind_int in ind_map.items():
                proto_ind = indicator_map.value[iid]
                proto_ind.instrument_id = ind_int.instrument_id
                proto_ind.start_date_time = ind_int.start_date_time.isoformat()
                proto_ind.end_date_time = ind_int.end_date_time.isoformat()
                for name, val in ind_int.indicators.items():
                    proto_ind.indicators[name] = str(val.get('value'))
        return msg

    @staticmethod
    def from_proto(msg):
        """
        Deserialize a UniverseStateInterval from a protobuf message.
        """
        from calendars.time_duration import TimeDuration
        from state.factor_interval import FactorInterval
        from state.instrument_interval import InstrumentInterval
        from state.indicator_interval import IndicatorInterval
        from datetime import datetime
        duration = TimeDuration(msg.duration.value)
        start_date_time = datetime.fromisoformat(msg.start_date_time)
        end_date_time = datetime.fromisoformat(msg.end_date_time)
        factor_intervals = []
        for proto_fi in msg.factor_intervals:
            fi_start = datetime.fromisoformat(proto_fi.start_date_time)
            fi_end = datetime.fromisoformat(proto_fi.end_date_time)
            instr_intervals = {}
            for iid_str, proto_ii in proto_fi.instrument_intervals.items():
                instr_intervals[int(iid_str)] = InstrumentInterval(
                    instrument_id=proto_ii.instrument_id,
                    start_date_time=datetime.fromisoformat(proto_ii.start_date_time),
                    end_date_time=datetime.fromisoformat(proto_ii.end_date_time),
                    open=proto_ii.open,
                    high=proto_ii.high,
                    low=proto_ii.low,
                    close=proto_ii.close,
                    traded_volume=proto_ii.traded_volume,
                    traded_dollar=proto_ii.traded_dollar,
                    status=getattr(proto_ii, 'status', None),
                    market_cap=getattr(proto_ii, 'market_cap', None)
                )
            factor_intervals.append(FactorInterval(
                start_date_time=fi_start,
                end_date_time=fi_end,
                instrument_intervals=instr_intervals
            ))
        instrument_intervals = {}
        for iid_str, proto_ii in msg.instrument_intervals.items():
            instrument_intervals[int(iid_str)] = InstrumentInterval(
                instrument_id=proto_ii.instrument_id,
                start_date_time=datetime.fromisoformat(proto_ii.start_date_time),
                end_date_time=datetime.fromisoformat(proto_ii.end_date_time),
                open=proto_ii.open,
                high=proto_ii.high,
                low=proto_ii.low,
                close=proto_ii.close,
                traded_volume=proto_ii.traded_volume,
                traded_dollar=proto_ii.traded_dollar,
                status=getattr(proto_ii, 'status', None),
                market_cap=getattr(proto_ii, 'market_cap', None)
            )
        instrument_indicator_intervals = {}
        for ind_type, indicator_map in msg.instrument_indicator_intervals.items():
            indicator_dict = {}
            for iid_str, proto_ind in indicator_map.value.items():
                indicators = {}
                for name, val in proto_ind.indicators.items():
                    indicators[name] = {'value': val, 'status': 'ok', 'update_at': None}  # status/ts best effort
                indicator_dict[int(iid_str)] = IndicatorInterval(
                    instrument_id=proto_ind.instrument_id,
                    start_date_time=datetime.fromisoformat(proto_ind.start_date_time),
                    end_date_time=datetime.fromisoformat(proto_ind.end_date_time),
                    indicators=indicators
                )
            instrument_indicator_intervals[ind_type] = indicator_dict
        return UniverseStateInterval(
            duration=duration,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            factor_intervals=factor_intervals,
            instrument_intervals=instrument_intervals,
            instrument_indicator_intervals=instrument_indicator_intervals
        )

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
    duration: TimeDuration
    start_date_time: datetime
    end_date_time: datetime
    factor_intervals: List[FactorInterval]
    instrument_intervals: Dict[int, InstrumentInterval] = field(default_factory=dict)
    instrument_indicator_intervals: Dict[str, Dict[int, IndicatorInterval]] = field(default_factory=dict)
