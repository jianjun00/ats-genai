from typing import Dict, List
from datetime import datetime
from state.indicator_interval import IndicatorInterval
from state.instrument_interval import InstrumentInterval
from signals.indicator_config import IndicatorConfig

class IndicatorBuilder:
    """
    Pure Indicator Computation Engine - Only computes indicators from provided data.
    
    SINGLE RESPONSIBILITY:
    - Compute technical indicators from InstrumentInterval data that is provided to it
    
    STRICTLY ONLY:
    - Takes rolling cache of InstrumentInterval objects (provided by caller)
    - Applies indicator classes to compute indicator values
    - Returns IndicatorInterval objects with computed indicator results
    - Stateless computation - no side effects
    
    DOES NOT:
    - Fetch any data from any source
    - Manage any state or rolling windows
    - Persist any results anywhere
    - Handle any business logic or orchestration
    - Decide which indicators to compute (IndicatorConfig decides)
    - Manage rolling cache (caller provides it)
    
    INTERACTIONS:
    - Used BY: UniverseStateBuilder (provides rolling cache, gets back results)
    - Given: Dict[int, List[InstrumentInterval]] rolling cache from caller
    - Returns: Dict[int, IndicatorInterval] with computed indicators
    - That's it - pure computation only
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
