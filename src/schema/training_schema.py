"""
Training Dataset Schema Management System

Provides comprehensive schema definitions, validation, and management 
for ATS training datasets with financial-specific feature types.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Dict, Any, Optional, Union, Tuple
import json
import hashlib
import numpy as np
from datetime import datetime, date
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataType(Enum):
    """Core data types supported in training datasets."""
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INT32 = "int32"
    INT64 = "int64"
    BOOL = "bool"
    STRING = "string"
    UINT32 = "uint32"
    UINT64 = "uint64"

class FeatureType(Enum):
    """Financial-specific feature types for ATS training data."""
    # Basic price data
    OHLC_INTERVALS = "ohlc_intervals"              # [time_steps, 4] OHLC price matrices
    PRICE_SERIES = "price_series"                  # [time_steps, 1] single price series  
    VOLUME_SERIES = "volume_series"                # [time_steps, 1] volume series
    
    # Technical indicators
    TECHNICAL_INDICATOR = "technical_indicator"     # Single technical indicator values
    PRICE_INDICATOR_INTERVALS = "price_indicator_intervals"   # Price-based indicator time series
    VOLUME_INDICATOR_INTERVALS = "volume_indicator_intervals" # Volume-based indicator time series
    CROSS_TIMEFRAME_INDICATORS = "cross_timeframe_indicators" # Multi-timeframe combinations
    
    # Derived features
    RETURN_SERIES = "return_series"                # Price return calculations
    VOLATILITY_SERIES = "volatility_series"       # Volatility measurements
    MOMENTUM_SERIES = "momentum_series"            # Momentum indicators
    
    # Market context
    MARKET_REGIME_INDICATORS = "market_regime_indicators"       # Bull/bear market indicators
    RELATIVE_STRENGTH_INDICATORS = "relative_strength_indicators" # RSI, relative performance
    SEASONAL_INDICATORS = "seasonal_indicators"    # Day/month/quarter seasonality
    
    # Label types
    CLASSIFICATION_LABEL = "classification_label"  # Categorical labels (up/down/sideways)
    REGRESSION_LABEL = "regression_label"          # Continuous target values
    MULTI_HORIZON_LABEL = "multi_horizon_label"    # Multiple prediction horizons

class VisualizationType(Enum):
    """Visualization recommendations for EDA dashboard."""
    TIME_SERIES_LINE = "time_series_line"          # Line chart over time
    HISTOGRAM = "histogram"                        # Distribution histogram
    CANDLESTICK = "candlestick"                   # OHLC candlestick chart
    HEATMAP = "heatmap"                           # Matrix/correlation heatmap
    SCATTER_PLOT = "scatter_plot"                  # Scatter plot for relationships
    BOX_PLOT = "box_plot"                         # Box plot for distributions
    DUAL_AXIS = "dual_axis"                       # Dual-axis chart (price + indicator)
    MULTI_TIMEFRAME = "multi_timeframe"           # Multiple timeframe comparison

@dataclass
class FeatureConstraints:
    """Statistical constraints and validation rules."""
    min_value: Optional[float] = None              # Minimum allowed value
    max_value: Optional[float] = None              # Maximum allowed value
    min_fraction: Optional[float] = None           # Minimum fraction of non-null values
    allowed_values: List[str] = field(default_factory=list)  # Allowed categorical values
    required: bool = False                         # Whether feature is required
    expected_mean: Optional[float] = None          # Expected statistical mean
    expected_std: Optional[float] = None           # Expected standard deviation
    max_categories: Optional[int] = None           # Maximum unique categories
    monotonic_increasing: bool = False             # Values should be monotonic increasing
    monotonic_decreasing: bool = False             # Values should be monotonic decreasing

@dataclass
class TimeframeSpec:
    """Timeframe specification for multi-timeframe features."""
    base_timeframe: str                            # Base timeframe (1min, 5min, 1hour, 1day)
    additional_timeframes: List[str] = field(default_factory=list) # Additional timeframes
    lookback_periods: int = 0                      # Number of periods to look back
    forecast_periods: int = 0                      # Number of periods to forecast

@dataclass
class FeatureSchema:
    """Individual feature definition."""
    name: str                                      # Feature name (e.g., "close_price", "rsi_14")
    type: FeatureType                              # Feature type classification
    data_type: DataType                            # Underlying data type
    shape: List[int]                               # Shape dimensions [time_steps, features]
    description: str = ""                          # Human-readable description
    constraints: FeatureConstraints = field(default_factory=FeatureConstraints)
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional metadata
    visualization_type: VisualizationType = VisualizationType.TIME_SERIES_LINE
    timeframe: Optional[TimeframeSpec] = None      # Timeframe information
    source_column: Optional[str] = None            # Original data source column
    dependencies: List[str] = field(default_factory=list)  # Dependent features

@dataclass
class LabelSchema:
    """Label (target) schema definition."""
    name: str                                      # Label name (e.g., "future_return_5d")
    type: FeatureType                              # Label type (CLASSIFICATION_LABEL, REGRESSION_LABEL)
    data_type: DataType                            # Data type
    shape: List[int]                               # Shape dimensions
    description: str = ""                          # Description
    constraints: FeatureConstraints = field(default_factory=FeatureConstraints)
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional metadata
    forecast_horizon: int = 1                      # Prediction horizon in time steps
    target_metric: str = ""                        # Target metric (return, volatility, direction)

@dataclass
class TFDVStatistics:
    """TensorFlow Data Validation integration."""
    serialized_stats: bytes = b""                 # Serialized TFDV statistics
    stats_version: str = ""                        # TFDV version used
    generation_timestamp: str = ""                 # When stats were generated
    feature_stats_paths: Dict[str, str] = field(default_factory=dict)  # Paths to individual feature stats

@dataclass
class DatasetMetadata:
    """Dataset-level metadata."""
    symbol: str = ""                               # Primary symbol (e.g., "AAPL")
    additional_symbols: List[str] = field(default_factory=list) # Additional symbols
    start_date: str = ""                           # Data start date (YYYY-MM-DD)
    end_date: str = ""                             # Data end date (YYYY-MM-DD)
    base_timeframe: str = ""                       # Base data timeframe
    total_samples: int = 0                         # Total number of samples
    total_features: int = 0                        # Total number of features
    sequence_length: int = 0                       # Time sequence length
    data_quality_score: float = 0.0               # Overall data quality score (0-1)
    generation_params: Dict[str, Any] = field(default_factory=dict) # Generation parameters
    data_sources: List[str] = field(default_factory=list)          # Data source identifiers

@dataclass
class SchemaCompatibility:
    """Schema compatibility tracking."""
    compatible_versions: List[str] = field(default_factory=list)   # Compatible schema versions
    breaking_changes: List[str] = field(default_factory=list)     # Description of breaking changes
    migration_guide: str = ""                      # Migration instructions
    backward_compatible: bool = True               # Whether backward compatible
    forward_compatible: bool = True                # Whether forward compatible

@dataclass
class ValidationError:
    """Individual validation error."""
    feature_name: str                              # Feature that failed validation
    error_type: str                                # Type of error (constraint, type, shape)
    error_message: str                             # Detailed error message
    expected_value: str = ""                       # Expected value/constraint
    actual_value: str = ""                         # Actual value found
    sample_index: int = -1                         # Sample index where error occurred

@dataclass
class ValidationWarning:
    """Individual validation warning."""
    feature_name: str                              # Feature with warning
    warning_type: str                              # Type of warning
    warning_message: str                           # Detailed warning message
    recommendation: str = ""                       # Recommended action

@dataclass
class ValidationResult:
    """Schema validation result."""
    is_valid: bool                                 # Overall validation result
    errors: List[ValidationError] = field(default_factory=list)     # List of validation errors
    warnings: List[ValidationWarning] = field(default_factory=list) # List of validation warnings
    confidence_score: float = 1.0                 # Confidence in validation (0-1)
    validation_timestamp: str = ""                 # When validation was performed

@dataclass
class TrainingDatasetSchema:
    """Complete dataset schema definition."""
    schema_version: str                            # Schema version (e.g., "1.0.0")
    dataset_name: str                              # Dataset identifier
    dataset_description: str = ""                  # Dataset description
    features: List[FeatureSchema] = field(default_factory=list)    # All feature definitions
    labels: List[LabelSchema] = field(default_factory=list)       # All label definitions
    metadata: DatasetMetadata = field(default_factory=DatasetMetadata) # Dataset-level metadata
    tfdv_stats: TFDVStatistics = field(default_factory=TFDVStatistics) # TFDV statistics
    created_at: str = ""                           # Creation timestamp
    created_by: str = ""                           # Creator information
    compatibility: SchemaCompatibility = field(default_factory=SchemaCompatibility)

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary format."""
        def _convert_dataclass(obj):
            if hasattr(obj, '__dataclass_fields__'):
                return {k: _convert_dataclass(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, Enum):
                return obj.value
            elif isinstance(obj, list):
                return [_convert_dataclass(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: _convert_dataclass(v) for k, v in obj.items()}
            elif isinstance(obj, bytes):
                return obj.hex() if obj else ""
            else:
                return obj
        
        return _convert_dataclass(self)
    
    def to_json(self) -> str:
        """Convert schema to JSON string."""
        return json.dumps(self.to_dict(), indent=2, default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TrainingDatasetSchema':
        """Create schema from dictionary."""
        # This would need more complex conversion logic
        # For now, return a basic implementation
        return cls(
            schema_version=data.get('schema_version', '1.0.0'),
            dataset_name=data.get('dataset_name', ''),
            dataset_description=data.get('dataset_description', ''),
        )
    
    def get_schema_hash(self) -> str:
        """Generate unique hash for schema."""
        schema_str = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

@dataclass
class SchemaRegistryEntry:
    """Schema registry entry."""
    schema_name: str                               # Schema name
    schema_version: str                            # Schema version
    schema: TrainingDatasetSchema                  # The actual schema
    created_at: str                                # Creation timestamp
    created_by: str                                # Creator
    tags: List[str] = field(default_factory=list) # Searchable tags
    status: str = "active"                         # Status (draft, active, deprecated)


class TrainingSchemaManager:
    """Manages training data schemas with validation and registry."""
    
    def __init__(self, registry_path: Optional[str] = None):
        self.registry_path = Path(registry_path) if registry_path else Path("schema_registry.json")
        self.schemas: Dict[str, SchemaRegistryEntry] = {}
        self.load_registry()
    
    def create_financial_schema(
        self,
        dataset_name: str,
        symbol: str,
        ohlc_features: List[str],
        technical_indicators: List[Dict[str, Any]],
        labels: List[Dict[str, Any]],
        sequence_length: int = 60,
        **kwargs
    ) -> TrainingDatasetSchema:
        """Create schema optimized for financial training data."""
        
        features = []
        
        # OHLC features - [time_steps, 4] shape  
        for feature_name in ohlc_features:
            features.append(FeatureSchema(
                name=feature_name,
                type=FeatureType.OHLC_INTERVALS,
                data_type=DataType.FLOAT32,
                shape=[sequence_length, 4],
                description=f"OHLC price data for {feature_name}",
                constraints=FeatureConstraints(min_value=0.0, required=True),
                visualization_type=VisualizationType.CANDLESTICK,
                source_column=feature_name
            ))
        
        # Technical indicators
        for indicator in technical_indicators:
            features.append(FeatureSchema(
                name=indicator['name'],
                type=FeatureType.TECHNICAL_INDICATOR,
                data_type=DataType.FLOAT32,
                shape=[sequence_length, 1],
                description=f"Technical indicator: {indicator.get('description', indicator['name'])}",
                constraints=FeatureConstraints(required=False),
                visualization_type=VisualizationType.DUAL_AXIS,
                metadata={
                    'indicator_type': indicator.get('type', ''),
                    'params': indicator.get('params', {}),
                    'calculation_method': indicator.get('method', '')
                }
            ))
        
        # Labels
        label_schemas = []
        for label_def in labels:
            label_schemas.append(LabelSchema(
                name=label_def['name'],
                type=FeatureType.REGRESSION_LABEL if label_def.get('type') == 'regression' else FeatureType.CLASSIFICATION_LABEL,
                data_type=DataType.FLOAT32,
                shape=[label_def.get('horizon', 1)],
                description=label_def.get('description', f"Prediction target: {label_def['name']}"),
                forecast_horizon=label_def.get('horizon', 1),
                target_metric=label_def.get('metric', 'return')
            ))
        
        # Dataset metadata
        metadata = DatasetMetadata(
            symbol=symbol,
            sequence_length=sequence_length,
            total_features=len(features),
            base_timeframe=kwargs.get('base_timeframe', '1day'),
            data_quality_score=kwargs.get('data_quality_score', 1.0),
            generation_params=kwargs.get('generation_params', {})
        )
        
        schema = TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name=dataset_name,
            dataset_description=f"Financial training dataset for {symbol}",
            features=features,
            labels=label_schemas,
            metadata=metadata,
            created_at=datetime.now().isoformat(),
            created_by=kwargs.get('created_by', 'ATS Training System')
        )
        
        return schema
    
    def validate_training_data(
        self,
        schema: TrainingDatasetSchema,
        features: np.ndarray,
        labels: Optional[np.ndarray] = None
    ) -> ValidationResult:
        """Validate actual training data against schema definition."""
        
        errors = []
        warnings = []
        
        try:
            # Shape validation
            expected_features = len(schema.features)
            if features.ndim == 3 and features.shape[-1] != expected_features:
                errors.append(ValidationError(
                    feature_name="all_features",
                    error_type="shape_mismatch",
                    error_message=f"Feature count mismatch: expected {expected_features}, got {features.shape[-1]}",
                    expected_value=str(expected_features),
                    actual_value=str(features.shape[-1])
                ))
            
            # Individual feature validation
            for i, feature_spec in enumerate(schema.features):
                if i >= features.shape[-1]:
                    continue
                    
                # Extract feature data
                if features.ndim == 3:
                    feature_data = features[:, :, i]
                elif features.ndim == 2:
                    feature_data = features[:, i]
                else:
                    feature_data = features
                
                # Type validation
                if feature_spec.data_type == DataType.FLOAT32:
                    if not np.issubdtype(feature_data.dtype, np.floating):
                        warnings.append(ValidationWarning(
                            feature_name=feature_spec.name,
                            warning_type="type_mismatch",
                            warning_message=f"Expected float32, got {feature_data.dtype}",
                            recommendation="Consider converting to float32 for consistency"
                        ))
                
                # Constraint validation
                constraints = feature_spec.constraints
                if constraints.min_value is not None:
                    if np.any(feature_data < constraints.min_value):
                        violation_count = np.sum(feature_data < constraints.min_value)
                        errors.append(ValidationError(
                            feature_name=feature_spec.name,
                            error_type="min_constraint_violation",
                            error_message=f"Found {violation_count} values below minimum {constraints.min_value}",
                            expected_value=f">= {constraints.min_value}",
                            actual_value=f"min: {np.min(feature_data)}"
                        ))
                
                if constraints.max_value is not None:
                    if np.any(feature_data > constraints.max_value):
                        violation_count = np.sum(feature_data > constraints.max_value)
                        errors.append(ValidationError(
                            feature_name=feature_spec.name,
                            error_type="max_constraint_violation",
                            error_message=f"Found {violation_count} values above maximum {constraints.max_value}",
                            expected_value=f"<= {constraints.max_value}",
                            actual_value=f"max: {np.max(feature_data)}"
                        ))
                
                # NaN validation
                nan_count = np.sum(np.isnan(feature_data))
                if nan_count > 0:
                    if constraints.required:
                        errors.append(ValidationError(
                            feature_name=feature_spec.name,
                            error_type="missing_required_data",
                            error_message=f"Required feature has {nan_count} NaN values",
                            expected_value="no NaN values",
                            actual_value=f"{nan_count} NaN values"
                        ))
                    else:
                        warnings.append(ValidationWarning(
                            feature_name=feature_spec.name,
                            warning_type="missing_data",
                            warning_message=f"Feature has {nan_count} NaN values",
                            recommendation="Consider imputation or data cleaning"
                        ))
            
            # Label validation
            if labels is not None and schema.labels:
                expected_labels = len(schema.labels)
                if labels.ndim >= 2 and labels.shape[-1] != expected_labels:
                    errors.append(ValidationError(
                        feature_name="all_labels",
                        error_type="shape_mismatch",
                        error_message=f"Label count mismatch: expected {expected_labels}, got {labels.shape[-1]}",
                        expected_value=str(expected_labels),
                        actual_value=str(labels.shape[-1])
                    ))
            
        except Exception as e:
            errors.append(ValidationError(
                feature_name="validation_system",
                error_type="validation_error",
                error_message=f"Validation system error: {str(e)}",
            ))
        
        # Calculate confidence score
        total_checks = len(schema.features) * 3  # Shape, type, constraint checks
        failed_checks = len([e for e in errors if e.error_type != "validation_error"])
        confidence_score = max(0.0, 1.0 - (failed_checks / total_checks)) if total_checks > 0 else 1.0
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            confidence_score=confidence_score,
            validation_timestamp=datetime.now().isoformat()
        )
    
    def register_schema(self, schema: TrainingDatasetSchema) -> str:
        """Register schema in the registry."""
        entry = SchemaRegistryEntry(
            schema_name=schema.dataset_name,
            schema_version=schema.schema_version,
            schema=schema,
            created_at=datetime.now().isoformat(),
            created_by=schema.created_by,
            tags=[schema.metadata.symbol, schema.metadata.base_timeframe]
        )
        
        registry_key = f"{schema.dataset_name}:{schema.schema_version}"
        self.schemas[registry_key] = entry
        self.save_registry()
        
        return schema.get_schema_hash()
    
    def get_schema(self, schema_name: str, version: str = "latest") -> Optional[TrainingDatasetSchema]:
        """Retrieve schema from registry."""
        if version == "latest":
            # Find latest version
            matching_schemas = [
                (k, v) for k, v in self.schemas.items() 
                if k.startswith(f"{schema_name}:")
            ]
            if not matching_schemas:
                return None
            # Sort by version and take latest
            latest_key = sorted(matching_schemas, key=lambda x: x[0])[-1][0]
            return self.schemas[latest_key].schema
        else:
            registry_key = f"{schema_name}:{version}"
            entry = self.schemas.get(registry_key)
            return entry.schema if entry else None
    
    def save_registry(self):
        """Save schema registry to file."""
        try:
            registry_data = {
                key: {
                    'schema_name': entry.schema_name,
                    'schema_version': entry.schema_version,
                    'created_at': entry.created_at,
                    'created_by': entry.created_by,
                    'tags': entry.tags,
                    'status': entry.status,
                    'schema': entry.schema.to_dict()
                }
                for key, entry in self.schemas.items()
            }
            
            self.registry_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.registry_path, 'w') as f:
                json.dump(registry_data, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save schema registry: {e}")
    
    def load_registry(self):
        """Load schema registry from file."""
        if not self.registry_path.exists():
            return
        
        try:
            with open(self.registry_path, 'r') as f:
                registry_data = json.load(f)
            
            for key, entry_data in registry_data.items():
                # This would need proper deserialization
                # For now, store basic info
                self.schemas[key] = entry_data
                
        except Exception as e:
            logger.error(f"Failed to load schema registry: {e}")


# Convenience functions for common use cases
def create_ohlcv_schema(
    dataset_name: str,
    symbol: str,
    sequence_length: int = 60,
    include_volume: bool = True,
    technical_indicators: Optional[List[str]] = None
) -> TrainingDatasetSchema:
    """Create a standard OHLCV schema with optional technical indicators."""
    
    manager = TrainingSchemaManager()
    
    ohlc_features = ["open", "high", "low", "close"]
    if include_volume:
        ohlc_features.append("volume")
    
    indicators = []
    if technical_indicators:
        for indicator in technical_indicators:
            indicators.append({
                'name': indicator,
                'type': 'technical_indicator',
                'description': f"{indicator} technical indicator"
            })
    
    labels = [{
        'name': 'future_return_1d',
        'type': 'regression',
        'description': '1-day future return',
        'horizon': 1,
        'metric': 'return'
    }]
    
    return manager.create_financial_schema(
        dataset_name=dataset_name,
        symbol=symbol,
        ohlc_features=ohlc_features,
        technical_indicators=indicators,
        labels=labels,
        sequence_length=sequence_length
    )


def create_multi_horizon_schema(
    dataset_name: str,
    symbol: str,
    horizons: List[int] = [1, 3, 5, 10],
    sequence_length: int = 60
) -> TrainingDatasetSchema:
    """Create schema for multi-horizon predictions."""
    
    manager = TrainingSchemaManager()
    
    ohlc_features = ["open", "high", "low", "close", "volume"]
    
    # Common technical indicators
    indicators = [
        {'name': 'sma_10', 'type': 'moving_average', 'description': '10-period simple moving average'},
        {'name': 'sma_20', 'type': 'moving_average', 'description': '20-period simple moving average'},
        {'name': 'rsi_14', 'type': 'momentum', 'description': '14-period RSI'},
        {'name': 'macd', 'type': 'momentum', 'description': 'MACD indicator'},
    ]
    
    # Multi-horizon labels
    labels = []
    for horizon in horizons:
        labels.append({
            'name': f'future_return_{horizon}d',
            'type': 'regression',
            'description': f'{horizon}-day future return',
            'horizon': horizon,
            'metric': 'return'
        })
    
    return manager.create_financial_schema(
        dataset_name=dataset_name,
        symbol=symbol,
        ohlc_features=ohlc_features,
        technical_indicators=indicators,
        labels=labels,
        sequence_length=sequence_length
    )