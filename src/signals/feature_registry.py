"""
Feature Registry for configurable training data generation.

Provides a registry of feature generators that can create lagging indicators
and features from market data. Features are configurable via gin.
"""

import gin
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Callable
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from .enhanced_indicators import EMAIndicator, ATRIndicator

@dataclass
class FeatureConfig:
    """Configuration for a single feature."""
    name: str
    feature_type: str  # 'indicator', 'transform', 'custom'
    parameters: Dict[str, Any] = field(default_factory=dict)
    lag_periods: int = 0  # How many periods to lag this feature
    enabled: bool = True

class FeatureGenerator(ABC):
    """Abstract base class for feature generators."""
    
    @abstractmethod
    def generate(self, data: pd.DataFrame, config: FeatureConfig) -> pd.Series:
        """Generate feature values from input data."""
    
    @abstractmethod
    def get_feature_names(self, config: FeatureConfig) -> List[str]:
        """Get the names of features this generator produces."""

class IndicatorFeatureGenerator(FeatureGenerator):
    """Generates features from technical indicators."""
    
    def __init__(self):
        self.indicator_map = {
            'ema': EMAIndicator,
            'atr': ATRIndicator,
            'sma': self._sma_indicator,
            'rsi': self._rsi_indicator,
            'macd': self._macd_indicator,
            'bollinger': self._bollinger_indicator
        }
    
    def generate(self, data: pd.DataFrame, config: FeatureConfig) -> pd.Series:
        """Generate indicator-based feature."""
        indicator_type = config.parameters.get('indicator_type')
        if indicator_type not in self.indicator_map:
            raise ValueError(f"Unknown indicator type: {indicator_type}")
        
        # Create indicator instance
        if indicator_type in ['ema', 'atr']:
            # Remove indicator_type from parameters and pass remaining parameters
            params = {k: v for k, v in config.parameters.items() if k != 'indicator_type'}
            indicator = self.indicator_map[indicator_type](**params)
            result = indicator.calculate(data)
            values = result.get('value', np.nan)
            
            # Convert single value to series
            if not isinstance(values, pd.Series):
                values = pd.Series([values] * len(data), index=data.index)
        else:
            # Use custom indicator functions
            indicator_func = self.indicator_map[indicator_type]
            params = {k: v for k, v in config.parameters.items() if k != 'indicator_type'}
            values = indicator_func(data, **params)
        
        # Apply lag if specified
        if config.lag_periods > 0:
            if isinstance(values, pd.Series):
                values = values.shift(config.lag_periods)
            else:
                # Convert to series and apply lag
                values = pd.Series([values] * len(data), index=data.index).shift(config.lag_periods)
        
        return values
    
    def get_feature_names(self, config: FeatureConfig) -> List[str]:
        """Get feature names for this indicator."""
        base_name = f"{config.name}_{config.parameters.get('indicator_type')}"
        if 'period' in config.parameters:
            base_name += f"_{config.parameters['period']}"
        if config.lag_periods > 0:
            base_name += f"_lag{config.lag_periods}"
        return [base_name]
    
    def _sma_indicator(self, data: pd.DataFrame, period: int = 20, **kwargs) -> pd.Series:
        """Simple Moving Average."""
        return data['close'].rolling(window=period).mean()
    
    def _rsi_indicator(self, data: pd.DataFrame, period: int = 14, **kwargs) -> pd.Series:
        """Relative Strength Index."""
        close = data['close']
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _macd_indicator(self, data: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, **kwargs) -> pd.Series:
        """MACD indicator."""
        close = data['close']
        ema_fast = close.ewm(span=fast).mean()
        ema_slow = close.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        return macd
    
    def _bollinger_indicator(self, data: pd.DataFrame, period: int = 20, std_dev: float = 2.0, **kwargs) -> pd.Series:
        """Bollinger Bands middle line (SMA)."""
        return data['close'].rolling(window=period).mean()

class TransformFeatureGenerator(FeatureGenerator):
    """Generates features from data transformations."""
    
    def generate(self, data: pd.DataFrame, config: FeatureConfig) -> pd.Series:
        """Generate transform-based feature."""
        transform_type = config.parameters.get('transform_type')
        column = config.parameters.get('column', 'close')
        
        if transform_type == 'log_return':
            values = np.log(data[column] / data[column].shift(1))
        elif transform_type == 'pct_change':
            periods = config.parameters.get('periods', 1)
            values = data[column].pct_change(periods=periods)
        elif transform_type == 'volatility':
            window = config.parameters.get('window', 20)
            returns = data[column].pct_change()
            values = returns.rolling(window=window).std()
        elif transform_type == 'volume_ratio':
            window = config.parameters.get('window', 20)
            values = data['volume'] / data['volume'].rolling(window=window).mean()
        else:
            raise ValueError(f"Unknown transform type: {transform_type}")
        
        # Apply lag if specified
        if config.lag_periods > 0:
            values = values.shift(config.lag_periods)
        
        return values
    
    def get_feature_names(self, config: FeatureConfig) -> List[str]:
        """Get feature names for this transform."""
        base_name = f"{config.name}_{config.parameters.get('transform_type')}"
        if config.lag_periods > 0:
            base_name += f"_lag{config.lag_periods}"
        return [base_name]

class CustomFeatureGenerator(FeatureGenerator):
    """Generates custom features from user-defined functions."""
    
    def __init__(self):
        self.custom_functions = {}
    
    def register_function(self, name: str, func: Callable):
        """Register a custom feature function."""
        self.custom_functions[name] = func
    
    def generate(self, data: pd.DataFrame, config: FeatureConfig) -> pd.Series:
        """Generate custom feature."""
        func_name = config.parameters.get('function_name')
        if func_name not in self.custom_functions:
            raise ValueError(f"Unknown custom function: {func_name}")
        
        func = self.custom_functions[func_name]
        values = func(data, **config.parameters)
        
        # Apply lag if specified
        if config.lag_periods > 0:
            values = values.shift(config.lag_periods)
        
        return values
    
    def get_feature_names(self, config: FeatureConfig) -> List[str]:
        """Get feature names for this custom feature."""
        base_name = config.name
        if config.lag_periods > 0:
            base_name += f"_lag{config.lag_periods}"
        return [base_name]

class DateTimeFeatureGenerator(FeatureGenerator):
    """Generates datetime-based features."""
    
    def generate(self, data: pd.DataFrame, config: FeatureConfig) -> pd.Series:
        """Generate datetime-based feature from index."""
        # Extract datetime information from the index
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("Data must have a DatetimeIndex for datetime features")
        
        # Handle timezone conversion more robustly
        try:
            import pytz
            edt_tz = pytz.timezone('US/Eastern')
            datetime_index_edt = data.index.tz_convert(edt_tz) if data.index.tz else data.index.tz_localize('UTC').tz_convert(edt_tz)
        except Exception:
            # Fallback: assume data is already in EDT or use a simple offset
            # For testing purposes, assume UTC-4 for EDT during summer
            if data.index.tz:
                datetime_index_edt = data.index
            else:
                # Localize as UTC then convert to EDT (UTC-4)
                datetime_index_edt = data.index.tz_localize('UTC')
                # Apply EDT offset manually (UTC-4 hours)
                datetime_index_edt = datetime_index_edt - pd.Timedelta(hours=4)
        
        # Generate requested datetime feature
        feature_name = config.name
        if feature_name == 'datetime':
            # Format datetime string with EDT timezone info
            if datetime_index_edt.tz:
                datetime_strings = datetime_index_edt.strftime('%Y-%m-%d %H:%M:%S %Z')
            else:
                # If we used manual offset, add EDT label
                datetime_strings = datetime_index_edt.strftime('%Y-%m-%d %H:%M:%S EDT')
            return pd.Series(datetime_strings, index=data.index, name='datetime')
        elif feature_name == 'hour_of_day_edt':
            return pd.Series(datetime_index_edt.hour, index=data.index, name='hour_of_day_edt')
        elif feature_name == 'day_of_week':
            return pd.Series(datetime_index_edt.dayofweek, index=data.index, name='day_of_week')  # 0=Monday, 6=Sunday
        elif feature_name == 'week_of_month':
            # Calculate week of month (1-5)
            return pd.Series((datetime_index_edt.day - 1) // 7 + 1, index=data.index, name='week_of_month')
        elif feature_name == 'week_of_year':
            return pd.Series(datetime_index_edt.isocalendar().week, index=data.index, name='week_of_year')
        elif feature_name == 'year':
            return pd.Series(datetime_index_edt.year, index=data.index, name='year')
        else:
            raise ValueError(f"Unknown datetime feature: {feature_name}")
    
    def get_feature_names(self, config: FeatureConfig) -> List[str]:
        """Get the names of datetime features this generator produces."""
        base_name = config.name
        if config.lag_periods > 0:
            base_name += f"_lag{config.lag_periods}"
        return [base_name]

@gin.configurable
class FeatureRegistry:
    """Central registry for feature generators."""
    
    def __init__(self, features: List[FeatureConfig] = None):
        self.features = features or []
        self.generators = {
            'indicator': IndicatorFeatureGenerator(),
            'transform': TransformFeatureGenerator(),
            'custom': CustomFeatureGenerator(),
            'datetime': DateTimeFeatureGenerator()
        }
    
    def add_feature(self, config: FeatureConfig):
        """Add a feature configuration to the registry."""
        self.features.append(config)
    
    def remove_feature(self, name: str):
        """Remove a feature by name."""
        self.features = [f for f in self.features if f.name != name]
    
    def get_enabled_features(self) -> List[FeatureConfig]:
        """Get all enabled feature configurations."""
        return [f for f in self.features if f.enabled]
    
    def generate_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate all configured features from input data."""
        feature_data = {}
        
        for config in self.get_enabled_features():
            try:
                generator = self.generators.get(config.feature_type)
                if generator is None:
                    print(f"Warning: Unknown feature type {config.feature_type} for feature {config.name}")
                    # Add NaN series as placeholder for unknown feature types
                    feature_data[config.name] = pd.Series([np.nan] * len(data), index=data.index)
                    continue
                
                values = generator.generate(data, config)
                feature_names = generator.get_feature_names(config)
                
                if len(feature_names) == 1:
                    feature_data[feature_names[0]] = values
                else:
                    # Multiple features from one generator
                    if isinstance(values, pd.DataFrame):
                        for i, name in enumerate(feature_names):
                            feature_data[name] = values.iloc[:, i] if i < values.shape[1] else np.nan
                    else:
                        # Single series for first feature name
                        feature_data[feature_names[0]] = values
                        
            except Exception as e:
                print(f"Error generating feature {config.name}: {e}")
                # Add NaN series as placeholder
                feature_data[config.name] = pd.Series([np.nan] * len(data), index=data.index)
        
        return pd.DataFrame(feature_data, index=data.index)
    
    def get_feature_names(self) -> List[str]:
        """Get all feature names that will be generated."""
        names = []
        for config in self.get_enabled_features():
            generator = self.generators.get(config.feature_type)
            if generator:
                names.extend(generator.get_feature_names(config))
        return names
    
    def register_custom_function(self, name: str, func: Callable):
        """Register a custom feature function."""
        custom_gen = self.generators['custom']
        custom_gen.register_function(name, func)

# Helper functions for gin configuration
@gin.configurable
def create_feature_config(name: str, 
                         feature_type: str,
                         parameters: Dict[str, Any] = None,
                         lag_periods: int = 0,
                         enabled: bool = True) -> FeatureConfig:
    """Create a feature configuration - gin configurable."""
    return FeatureConfig(
        name=name,
        feature_type=feature_type,
        parameters=parameters or {},
        lag_periods=lag_periods,
        enabled=enabled
    )

@gin.configurable  
def create_feature_registry(feature_configs: List[FeatureConfig] = None) -> FeatureRegistry:
    """Create a feature registry - gin configurable."""
    return FeatureRegistry(features=feature_configs or [])