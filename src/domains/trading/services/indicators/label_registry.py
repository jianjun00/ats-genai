"""
Label Registry for configurable training data generation.

Provides a registry of label generators that can create leading targets/labels
from market data. Labels are configurable via gin.
"""

import gin
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Callable
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

@dataclass
class LabelConfig:
    """Configuration for a single label."""
    name: str
    label_type: str  # 'price', 'return', 'classification', 'custom'
    parameters: Dict[str, Any] = field(default_factory=dict)
    lead_periods: int = 1  # How many periods to look ahead for this label
    enabled: bool = True

class LabelGenerator(ABC):
    """Abstract base class for label generators."""
    
    @abstractmethod
    def generate(self, data: pd.DataFrame, config: LabelConfig) -> pd.Series:
        """Generate label values from input data."""
    
    @abstractmethod
    def get_label_names(self, config: LabelConfig) -> List[str]:
        """Get the names of labels this generator produces."""

class PriceLabelGenerator(LabelGenerator):
    """Generates price-based labels (future prices, price changes)."""
    
    def generate(self, data: pd.DataFrame, config: LabelConfig) -> pd.Series:
        """Generate price-based label."""
        label_type = config.parameters.get('price_type', 'close')
        column = config.parameters.get('column', 'close')
        
        if label_type == 'future_price':
            # Future price at lead_periods ahead
            values = data[column].shift(-config.lead_periods)
        elif label_type == 'price_change':
            # Absolute price change
            values = data[column].shift(-config.lead_periods) - data[column]
        elif label_type == 'price_ratio':
            # Price ratio (future/current)
            future_price = data[column].shift(-config.lead_periods)
            values = future_price / data[column]
        elif label_type == 'high_low_range':
            # Future high-low range
            future_high = data['high'].shift(-config.lead_periods)
            future_low = data['low'].shift(-config.lead_periods)
            values = future_high - future_low
        else:
            raise ValueError(f"Unknown price label type: {label_type}")
        
        return values
    
    def get_label_names(self, config: LabelConfig) -> List[str]:
        """Get label names for price labels."""
        base_name = f"{config.name}_{config.parameters.get('price_type', 'close')}"
        base_name += f"_lead{config.lead_periods}"
        return [base_name]

class ReturnLabelGenerator(LabelGenerator):
    """Generates return-based labels (future returns, volatility)."""
    
    def generate(self, data: pd.DataFrame, config: LabelConfig) -> pd.Series:
        """Generate return-based label."""
        return_type = config.parameters.get('return_type', 'simple')
        column = config.parameters.get('column', 'close')
        
        if return_type == 'simple':
            # Simple return over lead_periods
            current_price = data[column]
            future_price = data[column].shift(-config.lead_periods)
            values = (future_price - current_price) / current_price
        elif return_type == 'log':
            # Log return over lead_periods
            current_price = data[column]
            future_price = data[column].shift(-config.lead_periods)
            values = np.log(future_price / current_price)
        elif return_type == 'cumulative':
            # Cumulative return over next lead_periods
            returns = data[column].pct_change()
            values = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - config.lead_periods):
                cum_return = (1 + returns.iloc[i+1:i+1+config.lead_periods]).prod() - 1
                values.iloc[i] = cum_return
        elif return_type == 'volatility':
            # Future volatility over lead_periods
            returns = data[column].pct_change()
            values = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - config.lead_periods):
                vol = returns.iloc[i+1:i+1+config.lead_periods].std()
                values.iloc[i] = vol
        elif return_type == 'max_return':
            # Maximum return over next lead_periods
            values = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - config.lead_periods):
                future_prices = data[column].iloc[i+1:i+1+config.lead_periods]
                max_price = future_prices.max()
                current_price = data[column].iloc[i]
                values.iloc[i] = (max_price - current_price) / current_price
        elif return_type == 'min_return':
            # Minimum return over next lead_periods
            values = pd.Series(index=data.index, dtype=float)
            for i in range(len(data) - config.lead_periods):
                future_prices = data[column].iloc[i+1:i+1+config.lead_periods]
                min_price = future_prices.min()
                current_price = data[column].iloc[i]
                values.iloc[i] = (min_price - current_price) / current_price
        else:
            raise ValueError(f"Unknown return label type: {return_type}")
        
        return values
    
    def get_label_names(self, config: LabelConfig) -> List[str]:
        """Get label names for return labels."""
        base_name = f"{config.name}_{config.parameters.get('return_type', 'simple')}"
        base_name += f"_lead{config.lead_periods}"
        return [base_name]

class ClassificationLabelGenerator(LabelGenerator):
    """Generates classification labels (direction, regime, quantile)."""
    
    def generate(self, data: pd.DataFrame, config: LabelConfig) -> pd.Series:
        """Generate classification label."""
        class_type = config.parameters.get('class_type', 'direction')
        column = config.parameters.get('column', 'close')
        
        if class_type == 'direction':
            # Future price direction (0=down, 1=up)
            current_price = data[column]
            future_price = data[column].shift(-config.lead_periods)
            values = (future_price > current_price).astype(int)
        elif class_type == 'direction_threshold':
            # Direction with threshold
            threshold = config.parameters.get('threshold', 0.01)  # 1% threshold
            current_price = data[column]
            future_price = data[column].shift(-config.lead_periods)
            return_pct = (future_price - current_price) / current_price
            values = pd.Series(index=data.index, dtype=int)
            values[return_pct > threshold] = 1  # Up
            values[return_pct < -threshold] = -1  # Down
            values[abs(return_pct) <= threshold] = 0  # Neutral
        elif class_type == 'quantile':
            # Quantile-based classification
            n_quantiles = config.parameters.get('n_quantiles', 5)
            window = config.parameters.get('window', 100)
            current_price = data[column]
            future_price = data[column].shift(-config.lead_periods)
            returns = (future_price - current_price) / current_price
            values = pd.Series(index=data.index, dtype=int)
            for i in range(window, len(data)):
                hist_returns = returns.iloc[i-window:i]
                current_return = returns.iloc[i]
                quantile = pd.qcut(hist_returns, n_quantiles, labels=False, duplicates='drop')
                # Find which quantile current return falls into
                quantile_edges = np.quantile(hist_returns.dropna(), np.linspace(0, 1, n_quantiles + 1))
                values.iloc[i] = np.digitize(current_return, quantile_edges) - 1
        elif class_type == 'volatility_regime':
            # Volatility regime classification (0=low, 1=high)
            window = config.parameters.get('window', 20)
            returns = data[column].pct_change()
            volatility = returns.rolling(window=window).std()
            future_vol = volatility.shift(-config.lead_periods)
            vol_median = volatility.rolling(window=100).median()
            values = (future_vol > vol_median).astype(int)
        else:
            raise ValueError(f"Unknown classification label type: {class_type}")
        
        return values
    
    def get_label_names(self, config: LabelConfig) -> List[str]:
        """Get label names for classification labels."""
        base_name = f"{config.name}_{config.parameters.get('class_type', 'direction')}"
        base_name += f"_lead{config.lead_periods}"
        return [base_name]

class CustomLabelGenerator(LabelGenerator):
    """Generates custom labels from user-defined functions."""
    
    def __init__(self):
        self.custom_functions = {}
    
    def register_function(self, name: str, func: Callable):
        """Register a custom label function."""
        self.custom_functions[name] = func
    
    def generate(self, data: pd.DataFrame, config: LabelConfig) -> pd.Series:
        """Generate custom label."""
        func_name = config.parameters.get('function_name')
        if func_name not in self.custom_functions:
            raise ValueError(f"Unknown custom function: {func_name}")
        
        func = self.custom_functions[func_name]
        values = func(data, config.lead_periods, **config.parameters)
        
        return values
    
    def get_label_names(self, config: LabelConfig) -> List[str]:
        """Get label names for custom labels."""
        base_name = config.name
        base_name += f"_lead{config.lead_periods}"
        return [base_name]

@gin.configurable
class LabelRegistry:
    """Central registry for label generators."""
    
    def __init__(self, labels: List[LabelConfig] = None):
        self.labels = labels or []
        self.generators = {
            'price': PriceLabelGenerator(),
            'return': ReturnLabelGenerator(),
            'classification': ClassificationLabelGenerator(),
            'custom': CustomLabelGenerator()
        }
    
    def add_label(self, config: LabelConfig):
        """Add a label configuration to the registry."""
        self.labels.append(config)
    
    def remove_label(self, name: str):
        """Remove a label by name."""
        self.labels = [l for l in self.labels if l.name != name]
    
    def get_enabled_labels(self) -> List[LabelConfig]:
        """Get all enabled label configurations."""
        return [l for l in self.labels if l.enabled]
    
    def generate_labels(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate all configured labels from input data."""
        label_data = {}
        
        for config in self.get_enabled_labels():
            try:
                generator = self.generators.get(config.label_type)
                if generator is None:
                    print(f"Warning: Unknown label type {config.label_type} for label {config.name}")
                    continue
                
                values = generator.generate(data, config)
                label_names = generator.get_label_names(config)
                
                if len(label_names) == 1:
                    label_data[label_names[0]] = values
                else:
                    # Multiple labels from one generator
                    if isinstance(values, pd.DataFrame):
                        for i, name in enumerate(label_names):
                            label_data[name] = values.iloc[:, i] if i < values.shape[1] else np.nan
                    else:
                        # Single series for first label name
                        label_data[label_names[0]] = values
                        
            except Exception as e:
                print(f"Error generating label {config.name}: {e}")
                # Add NaN series as placeholder
                label_data[config.name] = pd.Series([np.nan] * len(data), index=data.index)
        
        return pd.DataFrame(label_data, index=data.index)
    
    def get_label_names(self) -> List[str]:
        """Get all label names that will be generated."""
        names = []
        for config in self.get_enabled_labels():
            generator = self.generators.get(config.label_type)
            if generator:
                names.extend(generator.get_label_names(config))
        return names
    
    def register_custom_function(self, name: str, func: Callable):
        """Register a custom label function."""
        custom_gen = self.generators['custom']
        custom_gen.register_function(name, func)

# Helper functions for gin configuration
@gin.configurable
def create_label_config(name: str, 
                       label_type: str,
                       parameters: Dict[str, Any] = None,
                       lead_periods: int = 1,
                       enabled: bool = True) -> LabelConfig:
    """Create a label configuration - gin configurable."""
    return LabelConfig(
        name=name,
        label_type=label_type,
        parameters=parameters or {},
        lead_periods=lead_periods,
        enabled=enabled
    )

@gin.configurable  
def create_label_registry(label_configs: List[LabelConfig] = None) -> LabelRegistry:
    """Create a label registry - gin configurable."""
    return LabelRegistry(labels=label_configs or [])