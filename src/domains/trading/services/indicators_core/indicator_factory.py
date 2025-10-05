"""
Indicator Factory for configurable feature and label generation.

Creates and manages technical indicators, features, and labels based on
gin configuration. Provides a unified interface for indicator creation.
"""

import gin
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from .indicator import Indicator
from .analysis.enhanced_indicators import EMAIndicator, ATRIndicator
from .feature_registry import FeatureRegistry, FeatureConfig
from .label_registry import LabelRegistry, LabelConfig

@dataclass
class IndicatorFactoryConfig:
    """Configuration for the indicator factory."""
    feature_registry: Optional[FeatureRegistry] = None
    label_registry: Optional[LabelRegistry] = None
    enabled_features: List[str] = field(default_factory=list)
    enabled_labels: List[str] = field(default_factory=list)

@gin.configurable
class IndicatorFactory:
    """Factory for creating and managing indicators, features, and labels."""

    def __init__(self,
                 feature_registry: Optional[FeatureRegistry] = None,
                 label_registry: Optional[LabelRegistry] = None):
        self.feature_registry = feature_registry or FeatureRegistry()
        self.label_registry = label_registry or LabelRegistry()

        # Built-in indicator classes
        self.indicator_classes = {
            'ema': EMAIndicator,
            'atr': ATRIndicator,
            'sma': self._create_sma_indicator,
            'rsi': self._create_rsi_indicator,
            'macd': self._create_macd_indicator,
            'bollinger': self._create_bollinger_indicator,
            'stochastic': self._create_stochastic_indicator,
            'williams_r': self._create_williams_r_indicator,
            'cci': self._create_cci_indicator,
            'momentum': self._create_momentum_indicator
        }

        # Register default custom functions
        self._register_default_custom_functions()

    def create_indicator(self, indicator_type: str, **kwargs) -> Indicator:
        """Create an indicator instance."""
        if indicator_type not in self.indicator_classes:
            raise ValueError(f"Unknown indicator type: {indicator_type}")

        indicator_class_or_func = self.indicator_classes[indicator_type]

        if isinstance(indicator_class_or_func, type):
            # Class-based indicator
            return indicator_class_or_func(**kwargs)
        else:
            # Function-based indicator
            return indicator_class_or_func(**kwargs)

    def add_feature_from_config(self, name: str, feature_type: str, **parameters):
        """Add a feature configuration to the registry."""
        config = FeatureConfig(
            name=name,
            feature_type=feature_type,
            parameters=parameters
        )
        self.feature_registry.add_feature(config)

    def add_label_from_config(self, name: str, label_type: str, **parameters):
        """Add a label configuration to the registry."""
        config = LabelConfig(
            name=name,
            label_type=label_type,
            parameters=parameters
        )
        self.label_registry.add_label(config)

    def generate_features_and_labels(self, data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Generate both features and labels from input data."""
        features_df = self.feature_registry.generate_features(data)
        labels_df = self.label_registry.generate_labels(data)

        return {
            'features': features_df,
            'labels': labels_df
        }

    def get_feature_names(self) -> List[str]:
        """Get all configured feature names."""
        return self.feature_registry.get_feature_names()

    def get_label_names(self) -> List[str]:
        """Get all configured label names."""
        return self.label_registry.get_label_names()

    def register_custom_feature_function(self, name: str, func):
        """Register a custom feature function."""
        self.feature_registry.register_custom_function(name, func)

    def register_custom_label_function(self, name: str, func):
        """Register a custom label function."""
        self.label_registry.register_custom_function(name, func)

    def _register_default_custom_functions(self):
        """Register default custom feature and label functions."""

        # Custom feature functions
        def price_velocity(data: pd.DataFrame, window: int = 5, **kwargs) -> pd.Series:
            """Price velocity (rate of change)."""
            return data['close'].pct_change(periods=window)

        def volume_price_trend(data: pd.DataFrame, **kwargs) -> pd.Series:
            """Volume Price Trend indicator."""
            typical_price = (data['high'] + data['low'] + data['close']) / 3
            price_change = typical_price.pct_change()
            vpt = (price_change * data['volume']).cumsum()
            return vpt

        def accumulation_distribution(data: pd.DataFrame, **kwargs) -> pd.Series:
            """Accumulation/Distribution Line."""
            money_flow_multiplier = ((data['close'] - data['low']) - (data['high'] - data['close'])) / (data['high'] - data['low'])
            money_flow_volume = money_flow_multiplier * data['volume']
            ad_line = money_flow_volume.cumsum()
            return ad_line

        def price_channel_position(data: pd.DataFrame, window: int = 20, **kwargs) -> pd.Series:
            """Position within price channel (0-1)."""
            high_max = data['high'].rolling(window=window).max()
            low_min = data['low'].rolling(window=window).min()
            position = (data['close'] - low_min) / (high_max - low_min)
            return position

        # Custom label functions
        def multi_period_return(data: pd.DataFrame, lead_periods: int, periods_list: List[int] = None, **kwargs) -> pd.Series:
            """Multi-period maximum return."""
            if periods_list is None:
                periods_list = [1, 3, 5]

            max_return = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - max(periods_list)):
                returns = []
                for period in periods_list:
                    if i + period < len(data):
                        ret = (data['close'].iloc[i + period] - data['close'].iloc[i]) / data['close'].iloc[i]
                        returns.append(ret)
                max_return.iloc[i] = max(returns) if returns else np.nan

            return max_return

        def drawdown_risk(data: pd.DataFrame, lead_periods: int, window: int = 20, **kwargs) -> pd.Series:
            """Future maximum drawdown risk."""
            drawdown = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - lead_periods):
                future_prices = data['close'].iloc[i:i+lead_periods]
                peak = future_prices.expanding().max()
                dd = (future_prices - peak) / peak
                drawdown.iloc[i] = dd.min()

            return drawdown.abs()

        # Register functions
        self.register_custom_feature_function('price_velocity', price_velocity)
        self.register_custom_feature_function('volume_price_trend', volume_price_trend)
        self.register_custom_feature_function('accumulation_distribution', accumulation_distribution)
        self.register_custom_feature_function('price_channel_position', price_channel_position)

        self.register_custom_label_function('multi_period_return', multi_period_return)
        self.register_custom_label_function('drawdown_risk', drawdown_risk)

    # Indicator creation methods
    def _create_sma_indicator(self, period: int = 20, **kwargs):
        """Create Simple Moving Average indicator."""
        class SMAIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.period:
                    return {'value': None, 'status': 'insufficient_data'}
                sma = data['close'].rolling(window=self.period).mean().iloc[-1]
                return {'value': sma, 'status': 'valid'}

        return SMAIndicator(period)

    def _create_rsi_indicator(self, period: int = 14, **kwargs):
        """Create RSI indicator."""
        class RSIIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.period + 1:
                    return {'value': None, 'status': 'insufficient_data'}

                close = data['close']
                delta = close.diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=self.period).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=self.period).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                return {'value': rsi.iloc[-1], 'status': 'valid'}

        return RSIIndicator(period)

    def _create_macd_indicator(self, fast: int = 12, slow: int = 26, signal: int = 9, **kwargs):
        """Create MACD indicator."""
        class MACDIndicator(Indicator):
            def __init__(self, fast, slow, signal):
                super().__init__()
                self.fast = fast
                self.slow = slow
                self.signal = signal

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.slow:
                    return {'value': None, 'status': 'insufficient_data'}

                close = data['close']
                ema_fast = close.ewm(span=self.fast).mean()
                ema_slow = close.ewm(span=self.slow).mean()
                macd = ema_fast - ema_slow
                signal_line = macd.ewm(span=self.signal).mean()
                histogram = macd - signal_line

                return {
                    'value': macd.iloc[-1],
                    'signal': signal_line.iloc[-1],
                    'histogram': histogram.iloc[-1],
                    'status': 'valid'
                }

        return MACDIndicator(fast, slow, signal)

    def _create_bollinger_indicator(self, period: int = 20, std_dev: float = 2.0, **kwargs):
        """Create Bollinger Bands indicator."""
        class BollingerIndicator(Indicator):
            def __init__(self, period, std_dev):
                super().__init__()
                self.period = period
                self.std_dev = std_dev

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.period:
                    return {'value': None, 'status': 'insufficient_data'}

                close = data['close']
                sma = close.rolling(window=self.period).mean()
                std = close.rolling(window=self.period).std()
                upper_band = sma + (std * self.std_dev)
                lower_band = sma - (std * self.std_dev)

                return {
                    'value': sma.iloc[-1],
                    'upper_band': upper_band.iloc[-1],
                    'lower_band': lower_band.iloc[-1],
                    'bandwidth': ((upper_band - lower_band) / sma).iloc[-1],
                    'status': 'valid'
                }

        return BollingerIndicator(period, std_dev)

    def _create_stochastic_indicator(self, k_period: int = 14, d_period: int = 3, **kwargs):
        """Create Stochastic Oscillator."""
        class StochasticIndicator(Indicator):
            def __init__(self, k_period, d_period):
                super().__init__()
                self.k_period = k_period
                self.d_period = d_period

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.k_period:
                    return {'value': None, 'status': 'insufficient_data'}

                low_min = data['low'].rolling(window=self.k_period).min()
                high_max = data['high'].rolling(window=self.k_period).max()
                k_percent = 100 * (data['close'] - low_min) / (high_max - low_min)
                d_percent = k_percent.rolling(window=self.d_period).mean()

                return {
                    'value': k_percent.iloc[-1],
                    'k_percent': k_percent.iloc[-1],
                    'd_percent': d_percent.iloc[-1],
                    'status': 'valid'
                }

        return StochasticIndicator(k_period, d_period)

    def _create_williams_r_indicator(self, period: int = 14, **kwargs):
        """Create Williams %R indicator."""
        class WilliamsRIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.period:
                    return {'value': None, 'status': 'insufficient_data'}

                high_max = data['high'].rolling(window=self.period).max()
                low_min = data['low'].rolling(window=self.period).min()
                williams_r = -100 * (high_max - data['close']) / (high_max - low_min)

                return {'value': williams_r.iloc[-1], 'status': 'valid'}

        return WilliamsRIndicator(period)

    def _create_cci_indicator(self, period: int = 20, **kwargs):
        """Create Commodity Channel Index."""
        class CCIIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.period:
                    return {'value': None, 'status': 'insufficient_data'}

                typical_price = (data['high'] + data['low'] + data['close']) / 3
                sma = typical_price.rolling(window=self.period).mean()
                mean_deviation = typical_price.rolling(window=self.period).apply(
                    lambda x: np.mean(np.abs(x - x.mean()))
                )
                cci = (typical_price - sma) / (0.015 * mean_deviation)

                return {'value': cci.iloc[-1], 'status': 'valid'}

        return CCIIndicator(period)

    def _create_momentum_indicator(self, period: int = 10, **kwargs):
        """Create Momentum indicator."""
        class MomentumIndicator(Indicator):
            def __init__(self, period):
                super().__init__()
                self.period = period

            def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
                if len(data) < self.period + 1:
                    return {'value': None, 'status': 'insufficient_data'}

                momentum = data['close'].iloc[-1] - data['close'].iloc[-1-self.period]
                momentum_pct = momentum / data['close'].iloc[-1-self.period] * 100

                return {
                    'value': momentum,
                    'momentum_pct': momentum_pct,
                    'status': 'valid'
                }

        return MomentumIndicator(period)

# Gin configurable factory creation
@gin.configurable
def create_indicator_factory(feature_registry: Optional[FeatureRegistry] = None,
                           label_registry: Optional[LabelRegistry] = None) -> IndicatorFactory:
    """Create an indicator factory - gin configurable."""
    return IndicatorFactory(feature_registry=feature_registry, label_registry=label_registry)