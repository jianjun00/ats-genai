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
    def __init__(self, 
                 indicator_config: Optional[IndicatorConfig] = None,
                 signal_names: Optional[List[str]] = None):
        # Store signal names directly in IndicatorBuilder
        self.signal_names = signal_names
        if self.signal_names is None:
            self.signal_names = [
                "etop", "ebot", "pldot",           # Envelope indicators
                "envelope_top", "envelope_bot",    # Alternative envelope names
                "z1b", "z2b", "z5t", "z6t",       # Z-level indicators
                "sma_20", "ema_12", "rsi_14",     # Traditional technical indicators
                "macd_line", "macd_signal",       # MACD indicators
                "bb_upper", "bb_lower", "bb_middle" # Bollinger bands
            ]
        
        # If no config provided, create one based on signal names
        if indicator_config is None:
            indicator_config = self._create_config_from_signal_names()
        
        self.indicator_config = indicator_config
        # Map indicator name to indicator class
        self.indicator_classes = indicator_config.indicators
    
    def _create_config_from_signal_names(self) -> IndicatorConfig:
        """Create IndicatorConfig automatically from signal names."""
        try:
            # Create indicator config and factory
            indicator_config = IndicatorConfig()
            from domains.trading.services.indicators_core.indicator_factory import IndicatorFactory
            factory = IndicatorFactory()
            
            # Auto-create indicators for each signal name
            indicators_created = 0
            for signal_name in self.signal_names:
                try:
                    # Import basic indicator classes for simple indicators
                    from domains.trading.services.indicators_core.indicator import (
                        PL, OneOneHigh, OneOneLow, OneOneDot, EnvelopeBot, EnvelopeTop
                    )
                    
                    # Create appropriate indicator based on signal name
                    if signal_name == "sma_20":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_sma_indicator(period=20), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        indicators_created += 1
                    elif signal_name == "ema_12":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_ema_indicator(period=12), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        indicators_created += 1
                    elif signal_name == "rsi_14":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_rsi_indicator(period=14), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        indicators_created += 1
                    elif signal_name in ["macd_line", "macd_signal"]:
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_macd_indicator(), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        indicators_created += 1
                    elif signal_name in ["bb_upper", "bb_lower", "bb_middle"]:
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_bollinger_indicator(), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        indicators_created += 1
                    elif signal_name == "stoch_k" or signal_name == "stoch_d":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_stochastic_indicator(), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        print(f"🔍 DEBUG: Created Stochastic indicator for {signal_name}")
                        indicators_created += 1
                    elif signal_name == "williams_r":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_williams_r_indicator(), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        print(f"🔍 DEBUG: Created Williams %R indicator")
                        indicators_created += 1
                    elif signal_name == "cci":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_cci_indicator(), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        print(f"🔍 DEBUG: Created CCI indicator")
                        indicators_created += 1
                    elif signal_name == "momentum" or signal_name == "roc":
                        indicator_cls = self._create_historical_indicator_class(
                            factory._create_momentum_indicator(), signal_name)
                        indicator_config.add_indicator(signal_name, indicator_cls)
                        print(f"🔍 DEBUG: Created Momentum indicator for {signal_name}")
                        indicators_created += 1
                    # Basic envelope indicators - map to existing classes with proper names
                    elif signal_name == "etop":
                        # Envelope top indicator - use real EnvelopeTop
                        from domains.trading.signals.indicator import EnvelopeTop as RealEnvelopeTop
                        indicator_config.add_indicator(signal_name, RealEnvelopeTop)
                        indicators_created += 1
                    elif signal_name == "ebot":
                        # Envelope bottom indicator - use real EnvelopeBot
                        from domains.trading.signals.indicator import EnvelopeBot as RealEnvelopeBot
                        indicator_config.add_indicator(signal_name, RealEnvelopeBot)
                        indicators_created += 1
                    elif signal_name == "pldot":
                        # Price level indicator - use real PL
                        from domains.trading.signals.indicator import PL as RealPL
                        indicator_config.add_indicator(signal_name, RealPL)
                        indicators_created += 1
                    elif signal_name == "envelope_top":
                        # Envelope top indicator - use real EnvelopeTop
                        from domains.trading.signals.indicator import EnvelopeTop as RealEnvelopeTop
                        indicator_config.add_indicator(signal_name, RealEnvelopeTop)
                        indicators_created += 1
                    elif signal_name == "envelope_bot":
                        # Envelope bottom indicator - use real EnvelopeBot
                        from domains.trading.signals.indicator import EnvelopeBot as RealEnvelopeBot
                        indicator_config.add_indicator(signal_name, RealEnvelopeBot)
                        indicators_created += 1
                    # Advanced indicators that need custom implementation
                    elif signal_name == "adx":
                        indicator = self._create_adx_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created ADX indicator")
                        indicators_created += 1
                    elif signal_name == "trix":
                        indicator = self._create_trix_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created TRIX indicator")
                        indicators_created += 1
                    elif signal_name == "dpo":
                        indicator = self._create_dpo_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created DPO indicator")
                        indicators_created += 1
                    elif signal_name == "kama":
                        indicator = self._create_kama_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created KAMA indicator")
                        indicators_created += 1
                    elif signal_name == "macd_histogram":
                        indicator = factory._create_macd_indicator()
                        indicator_config.add_indicator(signal_name, type(indicator))
                        print(f"🔍 DEBUG: Created MACD Histogram indicator")
                        indicators_created += 1
                    # Z-level indicators (real implementations)
                    elif signal_name == "z1b":
                        # Import real Z1B indicator
                        from domains.trading.signals.indicator import Z1B
                        indicator_config.add_indicator(signal_name, Z1B)
                        indicators_created += 1
                    elif signal_name == "z2b":
                        # Import real Z2B indicator
                        from domains.trading.signals.indicator import Z2B
                        indicator_config.add_indicator(signal_name, Z2B)
                        indicators_created += 1
                    elif signal_name == "z5t":
                        # Import real Z5T indicator
                        from domains.trading.signals.indicator import Z5T
                        indicator_config.add_indicator(signal_name, Z5T)
                        indicators_created += 1
                    elif signal_name == "z6t":
                        # Import real Z6T indicator
                        from domains.trading.signals.indicator import Z6T
                        indicator_config.add_indicator(signal_name, Z6T)
                        indicators_created += 1
                    else:
                        print(f"🔍 DEBUG: Skipping unknown signal: {signal_name}")
                    
                except Exception as e:
                    print(f"Warning: Could not create indicator for {signal_name}: {e}")
                    import traceback
                    print(f"Traceback: {traceback.format_exc()}")
                    continue
            
            if indicators_created > 0:
                print(f"🔧 SUCCESS: Returning custom indicator config with {indicators_created} indicators")
                return indicator_config
            else:
                print(f"🔧 FALLBACK: No indicators created, falling back to default config")
                return IndicatorConfig.default_config()
            
        except Exception as e:
            print(f"🔧 ERROR: Could not create indicators from signal names: {e}")
            import traceback
            print(f"🔧 ERROR: Traceback: {traceback.format_exc()}")
            # Fallback to default config
            return IndicatorConfig.default_config()
    
    def get_signal_names(self) -> List[str]:
        """Get list of configured signal names."""
        return self.signal_names.copy()
    
    def get_max_lookback_period(self) -> int:
        """Calculate maximum lookback period needed for configured indicators."""
        max_period = 20  # Default minimum for basic indicators
        
        for signal_name in self.signal_names:
            # Extract period from signal names like 'sma_20', 'ema_12', 'rsi_14'
            if '_' in signal_name:
                parts = signal_name.split('_')
                for part in parts:
                    if part.isdigit():
                        period = int(part)
                        max_period = max(max_period, period)
        
        # Add 50% buffer for calculation stability and ensure minimum viable amount
        lookback_periods = max(int(max_period * 1.5), 60)  # At least 60 periods
        return lookback_periods

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

    def _create_adx_indicator(self, period: int = 14):
        """Create Average Directional Index (ADX) indicator."""
        from .indicator import Indicator
        import pandas as pd
        import numpy as np
        
        class ADXIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def update(self, intervals: List) -> None:
                """Update indicator with historical interval data."""
                if len(intervals) < self.period * 2:
                    self.value = None
                    self.status = "insufficient_data"
                    return

                # Convert intervals to DataFrame
                data = self._intervals_to_dataframe(intervals)
                result = self.calculate(data)
                self.value = result.get('value')
                self.status = result.get('status', 'ok')
                if intervals:
                    self.update_at = getattr(intervals[-1], 'end_date_time', None)

            def _intervals_to_dataframe(self, intervals):
                """Convert list of InstrumentInterval to DataFrame."""
                data = []
                for interval in intervals:
                    data.append({
                        'high': getattr(interval, 'high', 0.0),
                        'low': getattr(interval, 'low', 0.0),
                        'close': getattr(interval, 'close', 0.0),
                        'volume': getattr(interval, 'traded_volume', 0.0)
                    })
                return pd.DataFrame(data)

            def calculate(self, data: pd.DataFrame) -> dict:
                if len(data) < self.period * 2:
                    return {'value': None, 'status': 'insufficient_data'}

                high = data['high']
                low = data['low']
                close = data['close']
                
                # Calculate True Range (TR)
                tr1 = high - low
                tr2 = abs(high - close.shift(1))
                tr3 = abs(low - close.shift(1))
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                
                # Calculate Directional Movement
                plus_dm = high.diff()
                minus_dm = -low.diff()
                plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
                minus_dm = minus_dm.where(minus_dm > plus_dm, 0)
                
                # Smooth the values
                atr = tr.rolling(window=self.period).mean()
                plus_di = 100 * (plus_dm.rolling(window=self.period).mean() / atr)
                minus_di = 100 * (minus_dm.rolling(window=self.period).mean() / atr)
                
                # Calculate ADX
                dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
                adx = dx.rolling(window=self.period).mean()
                
                return {'value': adx.iloc[-1], 'status': 'valid'}
        
        return ADXIndicator(period)

    def _create_trix_indicator(self, period: int = 14):
        """Create TRIX indicator."""
        from .indicator import Indicator
        import pandas as pd
        
        class TRIXIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> dict:
                if len(data) < self.period * 3:
                    return {'value': None, 'status': 'insufficient_data'}

                close = data['close']
                
                # Triple smoothed EMA
                ema1 = close.ewm(span=self.period).mean()
                ema2 = ema1.ewm(span=self.period).mean()
                ema3 = ema2.ewm(span=self.period).mean()
                
                # TRIX = percentage change of triple smoothed EMA
                trix = ema3.pct_change() * 10000  # Multiply by 10000 for readability
                
                return {'value': trix.iloc[-1], 'status': 'valid'}
        
        return TRIXIndicator(period)

    def _create_dpo_indicator(self, period: int = 20):
        """Create Detrended Price Oscillator (DPO)."""
        from .indicator import Indicator
        import pandas as pd
        
        class DPOIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> dict:
                if len(data) < self.period + self.period // 2:
                    return {'value': None, 'status': 'insufficient_data'}

                close = data['close']
                
                # Simple Moving Average
                sma = close.rolling(window=self.period).mean()
                
                # DPO = Close - SMA shifted back by (period/2 + 1) periods
                shift_periods = self.period // 2 + 1
                dpo = close - sma.shift(shift_periods)
                
                return {'value': dpo.iloc[-1], 'status': 'valid'}
        
        return DPOIndicator(period)

    def _create_kama_indicator(self, period: int = 10, fast_sc: int = 2, slow_sc: int = 30):
        """Create Kaufman's Adaptive Moving Average (KAMA)."""
        from .indicator import Indicator
        import pandas as pd
        import numpy as np
        
        class KAMAIndicator(Indicator):
            def __init__(self, period, fast_sc, slow_sc):
                super().__init__()
                self.period = period
                self.fast_sc = fast_sc
                self.slow_sc = slow_sc

            def calculate(self, data: pd.DataFrame) -> dict:
                if len(data) < self.period + 1:
                    return {'value': None, 'status': 'insufficient_data'}

                close = data['close']
                
                # Calculate Change and Volatility
                change = abs(close - close.shift(self.period))
                volatility = abs(close.diff()).rolling(window=self.period).sum()
                
                # Efficiency Ratio
                er = change / volatility
                
                # Smoothing Constants
                fastest_sc = 2.0 / (self.fast_sc + 1)
                slowest_sc = 2.0 / (self.slow_sc + 1)
                
                # Smoothing Constant
                sc = (er * (fastest_sc - slowest_sc) + slowest_sc) ** 2
                
                # Calculate KAMA
                kama = pd.Series(index=close.index, dtype=float)
                kama.iloc[0] = close.iloc[0]  # Initialize with first close price
                
                for i in range(1, len(close)):
                    if pd.notna(sc.iloc[i]):
                        kama.iloc[i] = kama.iloc[i-1] + sc.iloc[i] * (close.iloc[i] - kama.iloc[i-1])
                    else:
                        kama.iloc[i] = kama.iloc[i-1]
                
                return {'value': kama.iloc[-1], 'status': 'valid'}
        
        return KAMAIndicator(period, fast_sc, slow_sc)

    def _create_historical_indicator_class(self, factory_indicator, signal_name):
        """Create a wrapper class that adapts factory indicators to IndicatorBuilder interface."""
        from .indicator import Indicator
        import pandas as pd
        
        class HistoricalIndicatorWrapper(Indicator):
            def __init__(self):
                super().__init__()
                self.factory_indicator = factory_indicator
                self.signal_name = signal_name

            def calculate(self, interval) -> float:
                """Required abstract method - not used in this implementation."""
                return 0.0

            def update(self, intervals: List) -> None:
                """Update indicator with historical interval data."""
                # Convert intervals to DataFrame that factory indicators expect
                data = self._intervals_to_dataframe(intervals)
                
                # Call the factory indicator's calculate method
                result = self.factory_indicator.calculate(data)
                
                # Extract the appropriate value based on signal name
                if isinstance(result, dict):
                    if self.signal_name == "macd_line":
                        self.value = result.get('value')
                    elif self.signal_name == "macd_signal":
                        self.value = result.get('signal')
                    elif self.signal_name == "macd_histogram":
                        self.value = result.get('histogram')
                    elif self.signal_name == "bb_upper":
                        self.value = result.get('upper_band')
                    elif self.signal_name == "bb_lower":
                        self.value = result.get('lower_band')
                    elif self.signal_name == "bb_middle":
                        self.value = result.get('value')
                    elif self.signal_name == "stoch_k":
                        self.value = result.get('k_percent')
                    elif self.signal_name == "stoch_d":
                        self.value = result.get('d_percent')
                    else:
                        self.value = result.get('value')
                    
                    self.status = result.get('status', 'ok')
                else:
                    self.value = result
                    self.status = 'ok'
                
                if intervals:
                    self.update_at = getattr(intervals[-1], 'end_date_time', None)

            def _intervals_to_dataframe(self, intervals):
                """Convert list of InstrumentInterval to DataFrame."""
                data = []
                for interval in intervals:
                    data.append({
                        'high': getattr(interval, 'high', 0.0),
                        'low': getattr(interval, 'low', 0.0),
                        'close': getattr(interval, 'close', 0.0),
                        'open': getattr(interval, 'open', 0.0),
                        'volume': getattr(interval, 'traded_volume', 0.0)
                    })
                return pd.DataFrame(data)
        
        return HistoricalIndicatorWrapper
