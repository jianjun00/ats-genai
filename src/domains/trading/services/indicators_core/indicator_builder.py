import gin
from typing import Dict, List, Optional
from datetime import datetime
from domains.trading.services.state_core.indicator_interval import IndicatorInterval
from domains.trading.services.state_core.instrument_interval import InstrumentInterval
from domains.trading.services.indicator_config import IndicatorConfig

@gin.configurable
class IndicatorBuilder:
    """
    Builds indicator intervals for a set of instruments using a rolling window of InstrumentIntervals.
    Can auto-create indicators based on gin-configured signal names.
    """
    def __init__(self, indicator_config: Optional[IndicatorConfig] = None):
        # If no config provided, create one based on gin-configured signal names
        if indicator_config is None:
            indicator_config = self._create_config_from_gin_signal_names()
        
        self.indicator_config = indicator_config
        # Map indicator name to indicator class
        self.indicator_classes = indicator_config.indicators
    
    def _create_config_from_gin_signal_names(self) -> IndicatorConfig:
        """Create IndicatorConfig automatically from gin-configured signal names."""
        try:
            # Try to get signal names from gin configuration
            # First check if gin has any configured parameters
            try:
                # Use gin.operative_config_str() to see what's configured
                operative_config = gin.operative_config_str()
                print(f"🔍 DEBUG: Gin operative config length: {len(operative_config)} chars")
                
                # Try multiple approaches to get signal_names
                signal_names = None
                
                # Approach 1: Try gin.query_parameter
                try:
                    signal_names = gin.query_parameter('TrainingDataConfig.signal_names')
                    print(f"🔍 DEBUG: Found signal_names via short name: {signal_names}")
                except:
                    try:
                        signal_names = gin.query_parameter('domains.services.training_data.timeseries_sequence_training_generator.TrainingDataConfig.signal_names')
                        print(f"🔍 DEBUG: Found signal_names via full name: {signal_names}")
                    except:
                        print("🔍 DEBUG: Could not find signal_names in gin config")
                
                if not signal_names:
                    print("🔍 DEBUG: No gin-configured signal_names found, using default config")
                    return IndicatorConfig.default_config()
                
            except Exception as e:
                print(f"🔍 DEBUG: Error querying gin config: {e}")
                return IndicatorConfig.default_config()
            
            # Create indicator config and factory
            indicator_config = IndicatorConfig()
            from domains.trading.services.indicators_core.indicator_factory import IndicatorFactory
            factory = IndicatorFactory()
            
            # Auto-create indicators for each signal name
            indicators_created = 0
            for signal_name in signal_names:
                try:
                    # Create appropriate indicator based on signal name
                    if signal_name == "sma_20":
                        indicator = factory._create_sma_indicator(period=20)
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created SMA_20 indicator")
                        indicators_created += 1
                    elif signal_name == "ema_12":
                        indicator = factory._create_ema_indicator(period=12)
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created EMA_12 indicator")
                        indicators_created += 1
                    elif signal_name == "rsi_14":
                        indicator = factory._create_rsi_indicator(period=14)
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created RSI_14 indicator")
                        indicators_created += 1
                    elif signal_name in ["macd_line", "macd_signal"]:
                        indicator = factory._create_macd_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created MACD indicator for {signal_name}")
                        indicators_created += 1
                    elif signal_name in ["bb_upper", "bb_lower", "bb_middle"]:
                        indicator = factory._create_bollinger_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created Bollinger Band indicator for {signal_name}")
                        indicators_created += 1
                    else:
                        print(f"🔍 DEBUG: Skipping unknown signal: {signal_name}")
                    
                except Exception as e:
                    print(f"Warning: Could not create indicator for {signal_name}: {e}")
                    continue
            
            print(f"🔍 DEBUG: Created indicator config with {indicators_created} technical indicators")
            return indicator_config
            
        except Exception as e:
            print(f"Warning: Could not create indicators from gin config: {e}")
            # Fallback to default config
            return IndicatorConfig.default_config()

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
