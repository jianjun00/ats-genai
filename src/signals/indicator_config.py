from dataclasses import dataclass, field
from typing import Dict, List, Type
from .indicator import Indicator, PL, OneOneHigh, OneOneLow, OneOneDot, EnvelopeBot, EnvelopeTop
from .standard_technical_indicators import SMA, EMA, RSI, VWAP, BollingerBands, MACD, StochasticOscillator


@dataclass
class IndicatorConfig:
    """
    Configuration for which indicators to compute in UniverseStateBuilder.
    Maps indicator names to their corresponding classes.
    """
    indicators: Dict[str, Type[Indicator]] = field(default_factory=dict)
    
    def __post_init__(self):
        # If no indicators specified, use empty dict
        if not self.indicators:
            self.indicators = {}
    
    def add_indicator(self, name: str, indicator_class: Type[Indicator]):
        """Add an indicator to the configuration."""
        self.indicators[name] = indicator_class
    
    def remove_indicator(self, name: str):
        """Remove an indicator from the configuration."""
        if name in self.indicators:
            del self.indicators[name]
    
    def has_indicator(self, name: str) -> bool:
        """Check if configuration includes a specific indicator."""
        return name in self.indicators
    
    def get_indicator_names(self) -> List[str]:
        """Get list of all configured indicator names."""
        return list(self.indicators.keys())
    
    def create_indicator_instances(self) -> Dict[str, Indicator]:
        """Create instances of all configured indicators."""
        return {name: indicator_class() for name, indicator_class in self.indicators.items()}
    
    @classmethod
    def default_config(cls) -> 'IndicatorConfig':
        """Create a default configuration with commonly used indicators."""
        config = cls()
        config.add_indicator('PL', PL)
        config.add_indicator('OneOneHigh', OneOneHigh)
        config.add_indicator('OneOneLow', OneOneLow)
        config.add_indicator('OneOneDot', OneOneDot)
        config.add_indicator('EnvelopeBot', EnvelopeBot)
        config.add_indicator('EnvelopeTop', EnvelopeTop)
        return config
    
    @classmethod
    def standard_technical_config(cls) -> 'IndicatorConfig':
        """Create configuration with standard technical indicators."""
        config = cls()
        # Standard technical indicators with common parameters
        config.add_indicator('SMA_20', lambda: SMA(20))
        config.add_indicator('EMA_20', lambda: EMA(20))
        config.add_indicator('RSI_14', lambda: RSI(14))
        config.add_indicator('VWAP', VWAP)
        config.add_indicator('BB_20', lambda: BollingerBands(20, 2.0))
        config.add_indicator('MACD', lambda: MACD(12, 26, 9))
        config.add_indicator('Stoch_14', lambda: StochasticOscillator(14, 3))
        return config
    
    @classmethod
    def multi_timeframe_config(cls) -> 'IndicatorConfig':
        """Create configuration optimized for multi-timeframe analysis."""
        config = cls()
        # Multi-timeframe technical indicators
        config.add_indicator('SMA_20', lambda: SMA(20))
        config.add_indicator('EMA_20', lambda: EMA(20))
        config.add_indicator('RSI_14', lambda: RSI(14))
        config.add_indicator('ETOP', EnvelopeTop)
        config.add_indicator('EBOT', EnvelopeBot)
        config.add_indicator('PLDOT', PL)
        return config
    
    @classmethod
    def basic_config(cls) -> 'IndicatorConfig':
        """Create a basic configuration with essential indicators."""
        config = cls()
        config.add_indicator('OneOneDot', OneOneDot)
        config.add_indicator('OneOneHigh', OneOneHigh)
        config.add_indicator('OneOneLow', OneOneLow)
        return config
    
    @classmethod
    def empty_config(cls) -> 'IndicatorConfig':
        """Create an empty configuration with no indicators."""
        return cls()
    
    def __len__(self) -> int:
        return len(self.indicators)
    
    def __contains__(self, name: str) -> bool:
        return name in self.indicators
    
    def __iter__(self):
        return iter(self.indicators.items())
