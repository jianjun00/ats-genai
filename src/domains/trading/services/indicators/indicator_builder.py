from typing import Dict, List
from datetime import datetime
from domains.trading.services.state.indicator_interval import IndicatorInterval
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.indicator_config import IndicatorConfig

class IndicatorBuilder:
    """
    Builds indicator intervals for a set of instruments using a rolling window of InstrumentIntervals.
    """
    def __init__(self, indicator_config: IndicatorConfig):
        self.indicator_config = indicator_config
        # Map indicator name to indicator class
        self.indicator_classes = indicator_config.indicators

    def build_indicator_intervals(self, instrument_rolling_cache: Dict[int, List[InstrumentInterval]],
                                 start_date_time: datetime, end_date_time: datetime) -> Dict[int, IndicatorInterval]:
        """
        For each instrument, build an IndicatorInterval using the rolling cache and configured indicators.
        Returns a dict of instrument_id to IndicatorInterval.
        """
        indicator_intervals: Dict[int, IndicatorInterval] = {}
        for inst_id, intervals in instrument_rolling_cache.items():
            indicator_interval = IndicatorInterval(
                instrument_id=inst_id,
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                indicators={}
            )
            for name, indicator_cls in self.indicator_classes.items():
                indicator = indicator_cls()
                indicator.update(intervals)
                indicator_interval.add_indicator(
                    name=name,
                    value=getattr(indicator, 'get_value', lambda: None)(),
                    status=getattr(indicator, 'status', None),
                    update_at=getattr(indicator, 'update_at', None)
                )
            indicator_intervals[inst_id] = indicator_interval
        return indicator_intervals
