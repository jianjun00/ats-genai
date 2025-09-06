"""
Training Data Metadata System

Provides comprehensive metadata tracking for generated training data including
feature types, dimensions, primary keys, and visualization hints.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
import json
from pathlib import Path
import numpy as np
from datetime import datetime, date


class FeatureType(Enum):
    """Feature data types for training data visualization."""
    INT = "int"
    FLOAT = "float"
    OHLC = "ohlc"  # Open, High, Low, Close data
    PRICE_INDICATOR = "price_indicator"  # Price-based technical indicators
    VOLUME_INDICATOR = "volume_indicator"  # Volume-based indicators
    RETURN = "return"  # Return calculations
    CLASSIFICATION = "classification"  # Categorical/class labels
    BINARY = "binary"  # Binary 0/1 features
    NORMALIZED = "normalized"  # Normalized/scaled features


class VisualizationType(Enum):
    """Visualization types for different feature categories."""
    HISTOGRAM = "histogram"
    TIME_SERIES = "time_series"
    CANDLESTICK = "candlestick"
    LINE_CHART = "line_chart"
    BAR_CHART = "bar_chart"
    SCATTER_PLOT = "scatter_plot"
    CORRELATION_MATRIX = "correlation_matrix"
    DISTRIBUTION = "distribution"


@dataclass
class FeatureMetadata:
    """Metadata for a single feature in training data."""
    name: str
    feature_type: FeatureType
    data_type: str  # numpy dtype (float64, int32, etc.)
    dimension: int  # Feature dimension (1 for scalar, >1 for vector)
    description: str
    source_column: Optional[str] = None  # Original data column if applicable
    lag_periods: Optional[int] = None  # For lagged features
    lead_periods: Optional[int] = None  # For future/label features
    window_size: Optional[int] = None  # For windowed features
    parameters: Dict[str, Any] = field(default_factory=dict)
    visualization_type: VisualizationType = VisualizationType.HISTOGRAM
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None
    null_count: int = 0
    is_primary_key: bool = False
    
    # Enhanced metadata fields
    shape: List[int] = field(default_factory=list)  # Feature shape [sequence_length, feature_dim]
    outlier_count: int = 0
    visualization_hints: Dict[str, Any] = field(default_factory=dict)
    technical_indicator_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LabelMetadata:
    """Metadata for label/target variables."""
    name: str
    label_type: str  # return, classification, price, etc.
    data_type: str
    dimension: int
    description: str
    lead_periods: int
    parameters: Dict[str, Any] = field(default_factory=dict)
    visualization_type: VisualizationType = VisualizationType.HISTOGRAM
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unique_values: Optional[List] = None  # For classification labels
    class_distribution: Optional[Dict] = None  # For classification
    
    # Enhanced metadata fields
    shape: List[int] = field(default_factory=list)  # Label shape [prediction_horizon]
    outlier_count: int = 0


@dataclass
class TrainingDataMetadata:
    """Complete metadata for a training dataset."""
    dataset_name: str
    creation_timestamp: str
    total_sequences: int
    sequence_length: int
    prediction_horizon: int
    feature_count: int
    label_count: int
    
    # Feature and label metadata
    features: List[FeatureMetadata] = field(default_factory=list)
    labels: List[LabelMetadata] = field(default_factory=list)
    
    # Data source information
    symbols: List[str] = field(default_factory=list)
    date_range: Dict[str, str] = field(default_factory=dict)
    data_sources: List[str] = field(default_factory=list)
    
    # File paths and identifiers
    data_file_path: Optional[str] = None
    feature_file_path: Optional[str] = None
    label_file_path: Optional[str] = None
    sample_ids: List[str] = field(default_factory=list)
    
    # Primary key information
    primary_key_feature: Optional[str] = None
    primary_key_type: Optional[str] = None
    
    # Configuration used to generate data
    gin_config_path: Optional[str] = None
    generation_parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Data quality metrics
    data_quality_metrics: Dict[str, float] = field(default_factory=dict)
    outlier_count: int = 0
    missing_data_ratio: float = 0.0


class TrainingDataMetadataManager:
    """Manager for creating and handling training data metadata."""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def create_feature_metadata(
        self,
        name: str,
        feature_type: FeatureType,
        data: np.ndarray,
        config: Optional[Dict] = None
    ) -> FeatureMetadata:
        """Create metadata for a feature from its data and configuration."""
        
        # Determine data type and statistics
        data_flat = data.flatten() if data.ndim > 1 else data
        valid_data = data_flat[~np.isnan(data_flat)]
        
        # Calculate enhanced statistics
        statistics = self._calculate_enhanced_statistics(valid_data)
        visualization_hints = self._generate_visualization_hints(name, feature_type, statistics)
        
        # Determine visualization type based on feature type
        viz_type_map = {
            FeatureType.OHLC: VisualizationType.CANDLESTICK,
            FeatureType.PRICE_INDICATOR: VisualizationType.LINE_CHART,
            FeatureType.VOLUME_INDICATOR: VisualizationType.LINE_CHART,
            FeatureType.RETURN: VisualizationType.DISTRIBUTION,
            FeatureType.CLASSIFICATION: VisualizationType.BAR_CHART,
            FeatureType.BINARY: VisualizationType.BAR_CHART,
        }
        viz_type = viz_type_map.get(feature_type, VisualizationType.HISTOGRAM)
        
        # Generate description based on feature type and config
        description = self._generate_feature_description(name, feature_type, config or {})
        
        # Extract technical indicator parameters if present
        tech_params = self._extract_technical_indicator_params(name, config or {})
        
        return FeatureMetadata(
            name=name,
            feature_type=feature_type,
            data_type=str(data.dtype),
            dimension=data.shape[-1] if data.ndim > 1 else 1,
            description=description,
            source_column=config.get('source_column') if config else None,
            lag_periods=config.get('lag_periods') if config else None,
            window_size=config.get('window_size') if config else None,
            parameters=config or {},
            visualization_type=viz_type,
            min_value=statistics.get('min_value'),
            max_value=statistics.get('max_value'),
            mean_value=statistics.get('mean_value'),
            std_value=statistics.get('std_value'),
            null_count=int(np.isnan(data_flat).sum()),
            shape=list(data.shape),
            outlier_count=statistics.get('outlier_count', 0),
            visualization_hints=visualization_hints,
            technical_indicator_params=tech_params
        )
    
    def create_label_metadata(
        self,
        name: str,
        label_type: str,
        data: np.ndarray,
        config: Optional[Dict] = None
    ) -> LabelMetadata:
        """Create metadata for a label from its data and configuration."""
        
        data_flat = data.flatten() if data.ndim > 1 else data
        valid_data = data_flat[~np.isnan(data_flat)]
        
        # Calculate enhanced statistics
        statistics = self._calculate_enhanced_statistics(valid_data)
        
        # For classification labels, get unique values and distribution
        unique_values = None
        class_distribution = None
        if 'classification' in label_type.lower():
            unique_values = np.unique(valid_data).tolist()
            if len(unique_values) < 20:  # Only for reasonable number of classes
                unique, counts = np.unique(valid_data, return_counts=True)
                class_distribution = dict(zip(unique.tolist(), counts.tolist()))
        
        viz_type = VisualizationType.BAR_CHART if 'classification' in label_type.lower() else VisualizationType.DISTRIBUTION
        
        description = self._generate_label_description(name, label_type, config or {})
        
        return LabelMetadata(
            name=name,
            label_type=label_type,
            data_type=str(data.dtype),
            dimension=data.shape[-1] if data.ndim > 1 else 1,
            description=description,
            lead_periods=config.get('lead_periods', 1) if config else 1,
            parameters=config or {},
            visualization_type=viz_type,
            min_value=statistics.get('min_value'),
            max_value=statistics.get('max_value'),
            unique_values=unique_values,
            class_distribution=class_distribution,
            shape=list(data.shape),
            outlier_count=statistics.get('outlier_count', 0)
        )
    
    def create_training_metadata(
        self,
        dataset_name: str,
        features_data: np.ndarray,
        labels_data: np.ndarray,
        feature_names: List[str],
        label_names: List[str],
        feature_configs: List[Dict],
        label_configs: List[Dict],
        symbols: List[str],
        date_range: Dict[str, str],
        **kwargs
    ) -> TrainingDataMetadata:
        """Create complete training data metadata."""
        
        from datetime import datetime
        
        # Create feature metadata
        features_meta = []
        for i, (name, config) in enumerate(zip(feature_names, feature_configs)):
            feature_type = self._infer_feature_type(name, config)
            feature_data = features_data[:, :, i] if features_data.ndim == 3 else features_data[:, i]
            meta = self.create_feature_metadata(name, feature_type, feature_data, config)
            features_meta.append(meta)
        
        # Create label metadata
        labels_meta = []
        for i, (name, config) in enumerate(zip(label_names, label_configs)):
            label_data = labels_data[:, :, i] if labels_data.ndim == 3 else labels_data[:, i]
            meta = self.create_label_metadata(name, config.get('label_type', 'unknown'), label_data, config)
            labels_meta.append(meta)
        
        # Determine primary key feature (prefer symbol or datetime features)
        primary_key_feature = self._determine_primary_key_feature(feature_names, feature_configs)
        
        # Calculate data quality metrics
        data_quality_metrics = self._calculate_data_quality_metrics(features_data, labels_data)
        
        return TrainingDataMetadata(
            dataset_name=dataset_name,
            creation_timestamp=datetime.now().isoformat(),
            total_sequences=features_data.shape[0],
            sequence_length=features_data.shape[1] if features_data.ndim > 1 else 1,
            prediction_horizon=labels_data.shape[1] if labels_data.ndim > 1 else 1,
            feature_count=len(feature_names),
            label_count=len(label_names),
            features=features_meta,
            labels=labels_meta,
            symbols=symbols,
            date_range=date_range,
            primary_key_feature=primary_key_feature,
            data_quality_metrics=data_quality_metrics,
            **kwargs
        )
    
    def save_metadata(self, metadata: TrainingDataMetadata, filename: str = "metadata.json") -> str:
        """Save metadata to JSON file."""
        filepath = self.output_dir / filename
        
        # Convert to dictionary and handle non-serializable types
        metadata_dict = self._metadata_to_dict(metadata)
        
        with open(filepath, 'w') as f:
            json.dump(metadata_dict, f, indent=2, default=self._json_serializer)
        
        return str(filepath)
    
    def load_metadata(self, filepath: str) -> TrainingDataMetadata:
        """Load metadata from JSON file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        return self._dict_to_metadata(data)
    
    def _infer_feature_type(self, name: str, config: Dict) -> FeatureType:
        """Infer feature type from name and configuration."""
        name_lower = name.lower()
        
        if any(x in name_lower for x in ['open', 'high', 'low', 'close']):
            return FeatureType.OHLC
        elif any(x in name_lower for x in ['sma', 'ema', 'price', 'bollinger', 'rsi']) and 'normalized' not in name_lower:
            return FeatureType.PRICE_INDICATOR
        elif any(x in name_lower for x in ['volume', 'vwap']):
            return FeatureType.VOLUME_INDICATOR
        elif any(x in name_lower for x in ['return', 'pct_change', 'log_return']):
            return FeatureType.RETURN
        elif 'classification' in config.get('feature_type', '').lower():
            return FeatureType.CLASSIFICATION
        elif any(x in name_lower for x in ['binary', 'flag', 'indicator']):
            return FeatureType.BINARY
        elif 'normalized' in name_lower or 'scaled' in name_lower:
            return FeatureType.NORMALIZED
        elif config.get('data_type') == 'int':
            return FeatureType.INT
        else:
            return FeatureType.FLOAT
    
    def _generate_feature_description(self, name: str, feature_type: FeatureType, config: Dict) -> str:
        """Generate human-readable description for feature."""
        descriptions = {
            FeatureType.OHLC: f"OHLC price data",
            FeatureType.PRICE_INDICATOR: f"Price-based technical indicator",
            FeatureType.VOLUME_INDICATOR: f"Volume-based indicator", 
            FeatureType.RETURN: f"Return calculation",
            FeatureType.CLASSIFICATION: f"Classification feature",
            FeatureType.BINARY: f"Binary indicator",
            FeatureType.NORMALIZED: f"Normalized feature",
            FeatureType.INT: f"Integer feature",
            FeatureType.FLOAT: f"Floating point feature"
        }
        
        base_desc = descriptions.get(feature_type, "Feature")
        
        # Add configuration details
        if config.get('lag_periods'):
            base_desc += f" with {config['lag_periods']} period lag"
        if config.get('window_size'):
            base_desc += f" using {config['window_size']} period window"
        
        return base_desc
    
    def _generate_label_description(self, name: str, label_type: str, config: Dict) -> str:
        """Generate human-readable description for label."""
        base_desc = f"{label_type.capitalize()} label"
        
        if config.get('lead_periods'):
            base_desc += f" predicting {config['lead_periods']} periods ahead"
        
        return base_desc
    
    def _determine_primary_key_feature(self, feature_names: List[str], feature_configs: List[Dict]) -> Optional[str]:
        """Determine which feature can serve as primary key."""
        # Look for symbol, datetime, or ID features
        for name, config in zip(feature_names, feature_configs):
            name_lower = name.lower()
            if any(x in name_lower for x in ['symbol', 'ticker', 'id', 'datetime', 'timestamp']):
                return name
        
        # Fallback to first feature
        return feature_names[0] if feature_names else None
    
    def _calculate_data_quality_metrics(self, features_data: np.ndarray, labels_data: np.ndarray) -> Dict[str, float]:
        """Calculate data quality metrics."""
        total_features = features_data.size
        total_labels = labels_data.size
        
        feature_missing = np.isnan(features_data).sum()
        label_missing = np.isnan(labels_data).sum()
        
        return {
            'feature_missing_ratio': float(feature_missing / total_features),
            'label_missing_ratio': float(label_missing / total_labels),
            'overall_missing_ratio': float((feature_missing + label_missing) / (total_features + total_labels)),
            'feature_completeness': float(1.0 - feature_missing / total_features),
            'label_completeness': float(1.0 - label_missing / total_labels)
        }
    
    def _metadata_to_dict(self, metadata: TrainingDataMetadata) -> Dict:
        """Convert metadata to dictionary for JSON serialization."""
        result = {}
        for field_name, field_value in metadata.__dict__.items():
            if isinstance(field_value, list):
                if field_value and hasattr(field_value[0], '__dict__'):
                    # List of dataclass objects
                    result[field_name] = [item.__dict__ for item in field_value]
                else:
                    result[field_name] = field_value
            elif hasattr(field_value, '__dict__'):
                # Dataclass object
                result[field_name] = field_value.__dict__
            elif isinstance(field_value, Enum):
                result[field_name] = field_value.value
            else:
                result[field_name] = field_value
        return result
    
    def _dict_to_metadata(self, data: Dict) -> TrainingDataMetadata:
        """Convert dictionary back to metadata object."""
        # This is a simplified version - full implementation would need proper deserialization
        return TrainingDataMetadata(**data)
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for non-standard types."""
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def _calculate_enhanced_statistics(self, valid_data: np.ndarray) -> Dict[str, float]:
        """Calculate enhanced statistics including outlier detection."""
        if len(valid_data) == 0:
            return {'min_value': None, 'max_value': None, 'mean_value': None, 'std_value': None, 'outlier_count': 0}
        
        mean_val = float(valid_data.mean())
        std_val = float(valid_data.std())
        
        # Detect outliers using 3-sigma rule
        outlier_threshold = 3 * std_val
        outliers = np.abs(valid_data - mean_val) > outlier_threshold
        outlier_count = int(outliers.sum())
        
        return {
            'min_value': float(valid_data.min()),
            'max_value': float(valid_data.max()),
            'mean_value': mean_val,
            'std_value': std_val,
            'outlier_count': outlier_count
        }
    
    def _generate_visualization_hints(self, name: str, feature_type: FeatureType, statistics: Dict[str, float]) -> Dict[str, Any]:
        """Generate visualization hints for feature based on type and statistics."""
        hints = {
            'color_scheme': 'blue',
            'scale_type': 'linear',
            'is_primary_indicator': False
        }
        
        # Set color scheme based on feature type
        if feature_type == FeatureType.OHLC:
            hints['color_scheme'] = 'green_red'
            hints['is_primary_indicator'] = True
        elif feature_type == FeatureType.PRICE_INDICATOR:
            hints['color_scheme'] = 'blue'
            hints['is_primary_indicator'] = 'price' in name.lower()
        elif feature_type == FeatureType.VOLUME_INDICATOR:
            hints['color_scheme'] = 'orange'
        elif feature_type == FeatureType.RETURN:
            hints['color_scheme'] = 'green_red'
            hints['scale_type'] = 'symmetric'
        
        # Determine if log scale is appropriate
        min_val = statistics.get('min_value')
        max_val = statistics.get('max_value')
        if min_val is not None and max_val is not None and min_val > 0 and max_val / min_val >= 100:
            hints['scale_type'] = 'log'
        
        return hints
    
    def _extract_technical_indicator_params(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract technical indicator parameters from feature name and config."""
        params = {}
        
        # Import regex at the beginning
        import re
        
        # Common technical indicator patterns
        if 'sma' in name.lower() or 'ema' in name.lower():
            # Extract window size from name (e.g., SMA_20, EMA_14)
            match = re.search(r'(\d+)', name)
            if match:
                params['window_size'] = int(match.group(1))
        
        elif 'rsi' in name.lower():
            params['indicator_type'] = 'relative_strength_index'
            match = re.search(r'(\d+)', name)
            if match:
                params['window_size'] = int(match.group(1))
        
        elif 'bollinger' in name.lower():
            params['indicator_type'] = 'bollinger_bands'
            params['std_dev_multiplier'] = config.get('std_dev_multiplier', 2.0)
        
        elif 'envelope' in name.lower():
            params['indicator_type'] = 'price_envelope'
            params['percentage'] = config.get('percentage', 2.5)
        
        elif any(x in name.lower() for x in ['bx', 'trender']):
            params['indicator_type'] = 'bx_trender'
            if 'basic' in name.lower():
                params['variant'] = 'basic'
            elif 'directional' in name.lower():
                params['variant'] = 'directional'
            elif 'volume' in name.lower():
                params['variant'] = 'volume_weighted'
        
        # Add any additional parameters from config
        params.update(config.get('indicator_params', {}))
        
        return params