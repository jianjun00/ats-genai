# ATS Training Dataset Schema Management

## Overview

The ATS Training Dataset Schema Management System provides comprehensive schema definitions, validation, and lifecycle management for machine learning training datasets. This system ensures data quality, enables schema evolution, and facilitates seamless integration with EDA visualization and model training pipelines.

## Quick Start

```python
from src.schema.training_schema import create_ohlcv_schema
from src.dao.training_schema_dao import TrainingSchemaDAO
from src.modeling.training_data_generator import generate_residual_return_training_data

# 1. Create schema-aware training data
result = await generate_residual_return_training_data(
    connection_pool=pool, env=environment, universe_state_manager=manager,
    start_date=datetime(2023, 1, 1), end_date=datetime(2023, 12, 31),
    instrument_ids=[1, 2, 3], include_schema=True,
    output_path="/data/training/aapl_2023"
)

# 2. Access schema-aware results
features = result.features_array      # NumPy array
schema = result.schema               # TrainingDatasetSchema
validation = result.validation_result # ValidationResult

print(f"Generated {features.shape[0]} samples with {features.shape[1]} features")
print(f"Data quality score: {validation.confidence_score:.3f}")
```

## Architecture

```
┌─────────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│ Training Generator  │ -> │ Schema Creation      │ -> │ Validation          │
└─────────────────────┘    └──────────────────────┘    └─────────────────────┘
                                       |                           |
┌─────────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
│ EDA Integration     │ <- │ Schema Registry      │ <- │ Database Storage    │
└─────────────────────┘    └──────────────────────┘    └─────────────────────┘
```

## Core Components

### 1. Schema Definitions (`training_schema.py`)

- **TrainingDatasetSchema**: Complete dataset schema with features, labels, and metadata
- **FeatureSchema**: Individual feature definitions with financial ML types
- **LabelSchema**: Target/label specifications for supervised learning
- **DatasetMetadata**: Dataset-level information (symbol, timeframe, etc.)

### 2. Feature Types

Financial-specific feature classifications:

- `OHLC_INTERVALS`: OHLC price matrices
- `TECHNICAL_INDICATOR`: Technical analysis indicators (SMA, RSI, MACD)
- `RETURN_SERIES`: Price return calculations
- `VOLUME_SERIES`: Volume-based features
- `VOLATILITY_SERIES`: Volatility measurements
- `MARKET_REGIME_INDICATORS`: Market context features
- `SEASONAL_INDICATORS`: Time-based features

### 3. Database Integration (`training_schema_dao.py`)

- **TrainingSchemaDAO**: CRUD operations for schema registry
- **Schema versioning**: Semantic versioning with compatibility tracking
- **JSONB storage**: Efficient PostgreSQL storage with querying
- **Usage analytics**: Track schema popularity and usage patterns

### 4. Training Integration (`training_data_generator.py`)

Enhanced training data generator with:
- Automatic feature type inference
- Schema-aware data generation
- Comprehensive validation
- EDA preparation
- Backwards compatibility

## Key Features

### Schema-Aware Training Data Generation

```python
# Enhanced generator automatically creates schemas
generator = ResidualReturnTrainingDataGenerator(pool, env, universe_manager)

result = await generator.generate_training_dataset(
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 12, 31), 
    instrument_ids=[1, 2, 3],
    include_schema=True  # Enable schema management
)

# Automatic feature classification
for feature in result.schema.features:
    print(f"{feature.name}: {feature.type.value}")
    # Output: sma_20: technical_indicator
    #         return_1d: return_series
    #         volume_avg: volume_series
```

### Data Validation

```python
# Comprehensive validation with financial ML best practices
validation = result.validation_result

if not validation.is_valid:
    print("❌ Validation errors:")
    for error in validation.errors:
        print(f"  - {error}")

print(f"📊 Confidence score: {validation.confidence_score:.3f}")
print(f"⚠️  Warnings: {len(validation.warnings)}")
```

### Schema Registry

```python
# Register schemas for reuse and version control
dao = TrainingSchemaDAO(environment)

schema_hash = await dao.register_schema(
    schema=result.schema,
    created_by="ML Team",
    tags=["AAPL", "daily", "technical_analysis"],
    description="AAPL daily returns with technical indicators"
)

# Find compatible schemas
compatible = await dao.find_compatible_schemas(
    feature_count=25, 
    sequence_length=60, 
    symbol="AAPL"
)
```

### EDA Integration

```python
# Schema enables intelligent EDA visualization
def create_eda_config(schema):
    feature_groups = {}
    for feature in schema.features:
        feature_type = feature.type.value
        if feature_type not in feature_groups:
            feature_groups[feature_type] = []
        feature_groups[feature_type].append(feature.name)
    
    # Automatic visualization recommendations
    viz_recommendations = {
        'technical_indicator': ['line_chart', 'overlay_plot'],
        'return_series': ['histogram', 'qq_plot'], 
        'volume_series': ['bar_chart', 'volume_profile']
    }
    
    return feature_groups, viz_recommendations
```

## Factory Functions

### OHLCV Schema

```python
from src.schema.training_schema import create_ohlcv_schema

schema = create_ohlcv_schema(
    dataset_name="aapl_ohlcv_comprehensive",
    symbol="AAPL",
    sequence_length=60,
    include_volume=True,
    technical_indicators=["sma_10", "sma_20", "rsi_14", "macd", "bb_upper"]
)

# Automatically includes:
# - OHLC price features
# - Volume feature (if enabled)  
# - Specified technical indicators
# - Appropriate feature type classifications
```

### Multi-Horizon Schema

```python
from src.schema.training_schema import create_multi_horizon_schema

schema = create_multi_horizon_schema(
    dataset_name="multi_horizon_prediction",
    symbol="AAPL",
    horizons=[1, 3, 5, 10],  # 1, 3, 5, 10-day predictions
    sequence_length=60
)

# Creates labels for each prediction horizon
# - return_1d, return_3d, return_5d, return_10d
```

## Migration Guide

### From Legacy Training Data

```python
# Before (Legacy DataFrame approach)
training_df = await generator.generate_training_dataset(
    start_date, end_date, instrument_ids
)

# After (Schema-aware approach)  
result = await generator.generate_training_dataset(
    start_date, end_date, instrument_ids,
    include_schema=True,  # Enable schema features
    output_path="/data/training/run_001"
)

# Access legacy DataFrame if needed
training_df = result.metadata.get('dataframe')
```

### Backwards Compatibility

The system maintains 100% backwards compatibility:

```python
# Existing code continues to work unchanged
result = await generator.generate_training_dataset(
    start_date, end_date, instrument_ids,
    include_schema=False  # Disable schema features
)

# Returns TrainingDatasetResult with DataFrame in metadata  
legacy_dataframe = result.metadata['dataframe']
```

## Database Schema

### Enhanced Training Datasets Table

```sql
ALTER TABLE dev_training_datasets 
ADD COLUMN schema_hash VARCHAR(64),
ADD COLUMN schema_version VARCHAR(50) DEFAULT '1.0.0',
ADD COLUMN schema_json JSONB DEFAULT '{}',
ADD COLUMN feature_schema JSONB DEFAULT '{}',
ADD COLUMN label_schema JSONB DEFAULT '{}',
ADD COLUMN validation_results JSONB DEFAULT '{}';
```

### Schema Registry Table

```sql
CREATE TABLE dev_training_schema_registry (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(255) NOT NULL,
    schema_version VARCHAR(50) NOT NULL,
    schema_hash VARCHAR(64) NOT NULL,
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(255),
    tags TEXT[],
    description TEXT,
    status VARCHAR(50) DEFAULT 'active',
    usage_count INTEGER DEFAULT 0,
    last_used_at TIMESTAMP,
    UNIQUE(schema_name, schema_version)
);
```

## Performance Optimization

### Efficient Schema Storage

- **JSONB columns**: Fast querying with GIN indexes
- **Schema hashing**: Duplicate detection and caching
- **Batch operations**: Bulk schema registration
- **Connection pooling**: Optimized database connections

### Memory Management

- **NumPy arrays**: Efficient feature/label storage
- **Lazy loading**: Load schema metadata on demand
- **Streaming validation**: Process large datasets incrementally

## Testing

```bash
# Run schema component tests
PYTHONPATH=src python3 -m pytest tests/unit/test_training_schema_components.py -v

# Run integration tests
PYTHONPATH=src python3 -m pytest tests/integration/test_schema_aware_training_generation.py -v

# Run EDA integration tests  
PYTHONPATH=src python3 -m pytest tests/integration/test_schema_eda_integration.py -v

# Manual verification
python3 scripts/run_dev.py run --script test_schema_implementation.py
```

## Best Practices

### 1. Schema Design

- Use semantic versioning (MAJOR.MINOR.PATCH)
- Provide descriptive feature names
- Set appropriate validation constraints
- Include comprehensive metadata

### 2. Feature Classification

- Use specific feature types for better ML integration
- Include visualization hints for EDA
- Add dependency tracking for complex features
- Document feature engineering steps

### 3. Validation

- Set quality score thresholds (>0.85 for production)
- Review warnings before model training
- Track validation trends over time
- Use confidence scores for data quality monitoring

### 4. Performance

- Batch schema operations when possible
- Use schema hashing for duplicate detection  
- Index frequently queried JSONB fields
- Cache schemas for repeated use

## Troubleshooting

### Common Issues

**Schema Hash Mismatch**: Ensure consistent serialization order
**Feature Type Inference**: Check column name patterns
**Database Connection**: Verify environment configuration
**Slow Validation**: Use sampling for large datasets during development

### Getting Help

- API Documentation: See method docstrings
- Integration Examples: Check tests/ directory
- Architecture Overview: See docs/TRAINING_DATASET_SCHEMA_MANAGEMENT.md
- Issues: Contact ATS development team

## Example Workflows

### Complete Training Pipeline

```python
async def complete_training_pipeline():
    # 1. Generate schema-aware training data
    result = await generate_residual_return_training_data(
        pool, env, universe_manager,
        datetime(2023, 1, 1), datetime(2023, 12, 31),
        [1, 2, 3], include_schema=True
    )
    
    # 2. Validate data quality
    if not result.validation_result.is_valid:
        raise ValueError("Data quality issues detected")
    
    # 3. Prepare for ML training
    X = result.features_array
    y = result.labels_array
    
    # 4. Register schema for future use
    dao = TrainingSchemaDAO(env)
    await dao.register_schema(result.schema, "Training Pipeline")
    
    return X, y, result.schema
```

This schema management system provides a robust foundation for financial ML workflows with comprehensive data quality assurance and seamless integration across the ATS platform.