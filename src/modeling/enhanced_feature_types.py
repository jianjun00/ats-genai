"""
Enhanced Feature Type System for Multi-Timeframe Training Data

This module provides a comprehensive type system for training features including:
- OHLC interval matrices
- Technical indicator sequences  
- Cross-timeframe aligned features
- Rich metadata and visualization hints
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any, Union
import json
import logging
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)


class FeatureType(Enum):
    """Enhanced feature types for multi-timeframe analysis."""
    
    # OHLC Data Types
    OHLC_INTERVALS = "ohlc_intervals"           # [time_steps, 4] OHLC matrices
    OHLC_SEQUENCES = "ohlc_sequences"           # Variable length OHLC sequences
    
    # Technical Indicator Types  
    PRICE_INDICATOR_INTERVALS = "price_indicator_intervals"  # [time_steps, 1] indicator arrays
    VOLUME_INDICATOR_INTERVALS = "volume_indicator_intervals"  # [time_steps, 1] volume arrays
    
    # Cross-Timeframe Types
    CROSS_TIMEFRAME_INDICATORS = "cross_timeframe_indicators"  # Aligned multi-timeframe data
    TIMEFRAME_ALIGNED_FEATURES = "timeframe_aligned_features"   # Synchronized features
    
    # Traditional Types (backward compatibility)
    SCALAR_FEATURES = "scalar_features"         # Single values
    SEQUENCE_FEATURES = "sequence_features"     # 1D sequences
    CATEGORICAL_FEATURES = "categorical_features"  # Category labels


class TimeframeSpec(Enum):
    """Supported timeframe specifications with minute multipliers."""
    MINUTE_5 = ("5min", 1)
    MINUTE_15 = ("15min", 3)
    HOUR_1 = ("1hour", 12)
    DAILY = ("daily", 288)      # 5min * 288 = 24 hours
    WEEKLY = ("weekly", 2016)   # 5min * 2016 = 7 days  
    MONTHLY = ("monthly", 8640) # 5min * 8640 = 30 days
    
    def __init__(self, label: str, multiplier: int):
        self.label = label
        self.multiplier = multiplier  # Multiplier from 5-minute base


class TechnicalIndicator(Enum):
    """Supported technical indicators."""
    ETOP = ("etop", "Envelope Top", "#FF5722", "resistance")
    EBOT = ("ebot", "Envelope Bottom", "#4CAF50", "support")  
    PLDOT = ("pldot", "Pivot Line Dot", "#2196F3", "pivot")
    EMA = ("ema", "Exponential Moving Average", "#9C27B0", "trend")
    RSI = ("rsi", "Relative Strength Index", "#FF9800", "momentum")
    MACD = ("macd", "MACD", "#607D8B", "momentum")
    
    def __init__(self, code: str, display_name: str, color: str, category: str):
        self.code = code
        self.display_name = display_name
        self.color = color
        self.category = category


@dataclass
class VisualizationMetadata:
    """Metadata for feature visualization."""
    type: str  # candlestick_sequence, line_overlay, scatter_overlay, etc.
    color: Optional[str] = None
    opacity: float = 1.0
    line_width: int = 2
    marker_size: int = 4
    show_volume: bool = False
    y_axis: str = "price"  # price, volume, indicator
    layer: int = 1  # Rendering layer (higher = on top)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "type": self.type,
            "color": self.color,
            "opacity": self.opacity,
            "line_width": self.line_width,
            "marker_size": self.marker_size,
            "show_volume": self.show_volume,
            "y_axis": self.y_axis,
            "layer": self.layer
        }


@dataclass
class FeatureSpecification:
    """Complete specification for a typed feature."""
    name: str
    feature_type: FeatureType
    timeframe: TimeframeSpec
    intervals: int  # Number of time steps
    dimensions: Tuple[int, ...]  # Feature shape
    
    # Optional fields
    indicator_type: Optional[TechnicalIndicator] = None
    source_timeframe: Optional[TimeframeSpec] = None  # For cross-timeframe features
    description: str = ""
    visualization: Optional[VisualizationMetadata] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set default visualization based on feature type."""
        if self.visualization is None:
            self.visualization = self._create_default_visualization()
    
    def _create_default_visualization(self) -> VisualizationMetadata:
        """Create default visualization metadata based on feature type."""
        if self.feature_type == FeatureType.OHLC_INTERVALS:
            return VisualizationMetadata(
                type="candlestick_sequence",
                show_volume=True,
                y_axis="price",
                layer=1
            )
        elif self.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS:
            color = self.indicator_type.color if self.indicator_type else "#2196F3"
            return VisualizationMetadata(
                type="line_overlay",
                color=color,
                opacity=0.8,
                line_width=2,
                y_axis="price",
                layer=2
            )
        elif self.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS:
            color = self.indicator_type.color if self.indicator_type else "#FF5722"
            return VisualizationMetadata(
                type="cross_timeframe_overlay",
                color=color,
                opacity=0.6,
                line_width=3,
                y_axis="price",
                layer=3
            )
        else:
            return VisualizationMetadata(type="default")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "feature_type": self.feature_type.value,
            "timeframe": self.timeframe.label,
            "intervals": self.intervals,
            "dimensions": list(self.dimensions),
            "indicator_type": self.indicator_type.code if self.indicator_type else None,
            "source_timeframe": self.source_timeframe.label if self.source_timeframe else None,
            "description": self.description,
            "visualization": self.visualization.to_dict() if self.visualization else None,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FeatureSpecification':
        """Create from dictionary."""
        # Convert enum values back
        feature_type = FeatureType(data["feature_type"])
        timeframe = next(tf for tf in TimeframeSpec if tf.label == data["timeframe"])
        
        indicator_type = None
        if data.get("indicator_type"):
            indicator_type = next(
                ind for ind in TechnicalIndicator 
                if ind.code == data["indicator_type"]
            )
        
        source_timeframe = None
        if data.get("source_timeframe"):
            source_timeframe = next(
                tf for tf in TimeframeSpec 
                if tf.label == data["source_timeframe"]
            )
        
        visualization = None
        if data.get("visualization"):
            visualization = VisualizationMetadata(**data["visualization"])
        
        return cls(
            name=data["name"],
            feature_type=feature_type,
            timeframe=timeframe,
            intervals=data["intervals"],
            dimensions=tuple(data["dimensions"]),
            indicator_type=indicator_type,
            source_timeframe=source_timeframe,
            description=data.get("description", ""),
            visualization=visualization,
            metadata=data.get("metadata", {})
        )


class EnhancedFeatureRegistry:
    """Registry for all supported feature types and configurations."""
    
    def __init__(self):
        self.feature_specs: Dict[str, FeatureSpecification] = {}
        self.timeframe_configs: Dict[str, Dict] = {}
        self._register_default_features()
        
        logger.info(f"Initialized feature registry with {len(self.feature_specs)} features")
    
    def _register_default_features(self):
        """Register standard feature configurations."""
        
        # OHLC Interval Features
        logger.info("Registering OHLC interval features...")
        ohlc_count = 0
        for timeframe in [TimeframeSpec.MINUTE_5, TimeframeSpec.MINUTE_15, 
                         TimeframeSpec.HOUR_1, TimeframeSpec.DAILY]:
            for intervals in [8, 16, 32]:
                feature_name = f"ohlc_{timeframe.label}_{intervals}"
                spec = FeatureSpecification(
                    name=feature_name,
                    feature_type=FeatureType.OHLC_INTERVALS,
                    timeframe=timeframe,
                    intervals=intervals,
                    dimensions=(intervals, 4),
                    description=f"{intervals} intervals of {timeframe.label} OHLC data",
                    metadata={
                        "columns": ["open", "high", "low", "close"],
                        "data_source": "market_data"
                    }
                )
                self.register_feature(spec)
                ohlc_count += 1
        
        logger.info(f"Registered {ohlc_count} OHLC features")
        
        # Technical Indicator Features
        logger.info("Registering technical indicator features...")
        indicator_count = 0
        for indicator in [TechnicalIndicator.ETOP, TechnicalIndicator.EBOT, 
                         TechnicalIndicator.PLDOT, TechnicalIndicator.EMA]:
            for timeframe in [TimeframeSpec.MINUTE_5, TimeframeSpec.MINUTE_15, 
                             TimeframeSpec.HOUR_1]:
                for intervals in [8, 16, 32]:
                    feature_name = f"{indicator.code}_{timeframe.label}_{intervals}"
                    spec = FeatureSpecification(
                        name=feature_name,
                        feature_type=FeatureType.PRICE_INDICATOR_INTERVALS,
                        timeframe=timeframe,
                        intervals=intervals,
                        dimensions=(intervals, 1),
                        indicator_type=indicator,
                        description=f"{intervals} intervals of {timeframe.label} {indicator.display_name}",
                        metadata={
                            "indicator_category": indicator.category,
                            "calculation_params": self._get_indicator_params(indicator)
                        }
                    )
                    self.register_feature(spec)
                    indicator_count += 1
        
        logger.info(f"Registered {indicator_count} technical indicator features")
        
        # Cross-Timeframe Features
        logger.info("Registering cross-timeframe features...")
        cross_count = 0
        cross_mappings = [
            (TimeframeSpec.HOUR_1, TimeframeSpec.MINUTE_5),
            (TimeframeSpec.DAILY, TimeframeSpec.MINUTE_15),
            (TimeframeSpec.WEEKLY, TimeframeSpec.DAILY)
        ]
        
        for source_tf, target_tf in cross_mappings:
            for indicator in [TechnicalIndicator.ETOP, TechnicalIndicator.EBOT, 
                             TechnicalIndicator.PLDOT]:
                feature_name = f"{indicator.code}_{source_tf.label}_on_{target_tf.label}"
                spec = FeatureSpecification(
                    name=feature_name,
                    feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
                    timeframe=target_tf,
                    intervals=16,  # Standard for cross-timeframe
                    dimensions=(16, 1),
                    indicator_type=indicator,
                    source_timeframe=source_tf,
                    description=f"{source_tf.label} {indicator.display_name} aligned to {target_tf.label} intervals",
                    metadata={
                        "alignment_method": "repeat_and_interpolate",
                        "source_multiplier": source_tf.multiplier / target_tf.multiplier
                    }
                )
                self.register_feature(spec)
                cross_count += 1
        
        logger.info(f"Registered {cross_count} cross-timeframe features")
        logger.info(f"Total features registered: {len(self.feature_specs)}")
    
    def _get_indicator_params(self, indicator: TechnicalIndicator) -> Dict[str, Any]:
        """Get default parameters for technical indicators."""
        params = {
            TechnicalIndicator.ETOP: {"envelope_pct": 2.0, "period": 20},
            TechnicalIndicator.EBOT: {"envelope_pct": 2.0, "period": 20},
            TechnicalIndicator.PLDOT: {"pivot_period": 5, "threshold": 0.1},
            TechnicalIndicator.EMA: {"period": 20, "alpha": 0.1},
            TechnicalIndicator.RSI: {"period": 14, "overbought": 70, "oversold": 30},
            TechnicalIndicator.MACD: {"fast": 12, "slow": 26, "signal": 9}
        }
        return params.get(indicator, {})
    
    def register_feature(self, spec: FeatureSpecification):
        """Register a new feature specification."""
        self.feature_specs[spec.name] = spec
        logger.debug(f"Registered feature: {spec.name} ({spec.feature_type.value})")
    
    def get_feature_spec(self, name: str) -> Optional[FeatureSpecification]:
        """Get feature specification by name."""
        return self.feature_specs.get(name)
    
    def list_features_by_type(self, feature_type: FeatureType) -> List[FeatureSpecification]:
        """List all features of a specific type."""
        return [spec for spec in self.feature_specs.values() 
                if spec.feature_type == feature_type]
    
    def list_features_by_timeframe(self, timeframe: TimeframeSpec) -> List[FeatureSpecification]:
        """List all features for a specific timeframe."""
        return [spec for spec in self.feature_specs.values() 
                if spec.timeframe == timeframe]
    
    def list_indicator_features(self, indicator: TechnicalIndicator) -> List[FeatureSpecification]:
        """List all features for a specific technical indicator."""
        return [spec for spec in self.feature_specs.values() 
                if spec.indicator_type == indicator]
    
    def get_cross_timeframe_features(self) -> List[FeatureSpecification]:
        """Get all cross-timeframe features."""
        return self.list_features_by_type(FeatureType.CROSS_TIMEFRAME_INDICATORS)
    
    def export_registry(self, file_path: str):
        """Export feature registry to JSON file."""
        export_data = {
            "features": {name: spec.to_dict() for name, spec in self.feature_specs.items()},
            "metadata": {
                "total_features": len(self.feature_specs),
                "feature_types": list(set(spec.feature_type.value for spec in self.feature_specs.values())),
                "timeframes": list(set(spec.timeframe.label for spec in self.feature_specs.values())),
                "indicators": list(set(spec.indicator_type.code for spec in self.feature_specs.values() 
                                     if spec.indicator_type))
            }
        }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"Exported feature registry to {file_path}")
    
    def load_registry(self, file_path: str):
        """Load feature registry from JSON file."""
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        self.feature_specs.clear()
        for name, spec_data in data["features"].items():
            spec = FeatureSpecification.from_dict(spec_data)
            self.feature_specs[name] = spec
        
        logger.info(f"Loaded {len(self.feature_specs)} features from {file_path}")
    
    def get_visualization_config(self, feature_names: List[str]) -> Dict[str, Any]:
        """Get visualization configuration for multiple features."""
        config = {
            "timeframes": {},
            "features": {},
            "overlays": []
        }
        
        for name in feature_names:
            spec = self.get_feature_spec(name)
            if not spec:
                continue
                
            # Add timeframe info
            tf_label = spec.timeframe.label
            if tf_label not in config["timeframes"]:
                config["timeframes"][tf_label] = {
                    "label": tf_label,
                    "multiplier": spec.timeframe.multiplier,
                    "features": []
                }
            config["timeframes"][tf_label]["features"].append(name)
            
            # Add feature visualization config
            config["features"][name] = {
                "spec": spec.to_dict(),
                "visualization": spec.visualization.to_dict() if spec.visualization else None
            }
            
            # Add cross-timeframe overlays
            if spec.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS:
                config["overlays"].append({
                    "feature": name,
                    "source_timeframe": spec.source_timeframe.label,
                    "target_timeframe": spec.timeframe.label,
                    "indicator": spec.indicator_type.code if spec.indicator_type else None
                })
        
        return config


# Global feature registry instance
feature_registry = EnhancedFeatureRegistry()


def get_feature_registry() -> EnhancedFeatureRegistry:
    """Get the global feature registry instance."""
    return feature_registry


if __name__ == "__main__":
    # Demo and testing
    logging.basicConfig(level=logging.INFO)
    
    registry = EnhancedFeatureRegistry()
    
    # Print summary
    print(f"\n=== Enhanced Feature Registry Summary ===")
    print(f"Total Features: {len(registry.feature_specs)}")
    
    for feature_type in FeatureType:
        features = registry.list_features_by_type(feature_type)
        if features:
            print(f"\n{feature_type.value}: {len(features)} features")
            for spec in features[:3]:  # Show first 3
                print(f"  - {spec.name}: {spec.dimensions}")
    
    # Test specific feature
    ohlc_feature = registry.get_feature_spec("ohlc_5min_8")
    if ohlc_feature:
        print(f"\n=== Sample Feature: {ohlc_feature.name} ===")
        print(f"Type: {ohlc_feature.feature_type.value}")
        print(f"Dimensions: {ohlc_feature.dimensions}")
        print(f"Timeframe: {ohlc_feature.timeframe.label}")
        print(f"Description: {ohlc_feature.description}")
        if ohlc_feature.visualization:
            print(f"Visualization: {ohlc_feature.visualization.to_dict()}")
    
    # Export registry
    export_path = "/tmp/feature_registry_test.json"
    registry.export_registry(export_path)
    print(f"\nExported registry to: {export_path}")