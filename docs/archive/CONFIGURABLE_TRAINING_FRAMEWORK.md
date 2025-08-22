# Configurable Training Data Generation Framework

This framework provides a flexible, gin-configurable system for generating machine learning training data from financial market data. Users can define custom features (lagging indicators) and labels (leading targets) through configuration files, making it easy to experiment with different feature sets for ML models.

## 🎯 Key Features

- **Gin Configurable**: Define features and labels in gin configuration files
- **Feature Registry**: Supports technical indicators, data transforms, and custom functions
- **Label Registry**: Supports price-based, return-based, and classification labels
- **Indicator Factory**: Unified interface for creating technical indicators
- **Flexible Output**: PyTorch tensors, NumPy arrays, or Pandas DataFrames
- **Data Quality**: Built-in outlier detection, missing value handling, and scaling
- **Multi-Symbol Support**: Process multiple instruments simultaneously

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Feature        │    │  Label          │    │  Indicator      │
│  Registry       │    │  Registry       │    │  Factory        │
│                 │    │                 │    │                 │
│ • Technical     │    │ • Future Returns│    │ • EMA, RSI, ATR │
│   Indicators    │    │ • Price Changes │    │ • Custom        │
│ • Transforms    │    │ • Classification│    │   Indicators    │
│ • Custom Funcs  │    │ • Custom Labels │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  Configurable Training  │
                    │  Data Generator         │
                    │                         │
                    │ • Multi-timeframe       │
                    │ • Sequence generation   │
                    │ • Data validation       │
                    │ • Output formatting     │
                    └─────────────────────────┘
```

## 📦 Components

### 1. Feature Registry (`src/signals/feature_registry.py`)

Manages feature generation from market data. Supports three types:

- **Indicator Features**: Technical indicators (EMA, RSI, ATR, etc.)
- **Transform Features**: Data transformations (returns, volatility, etc.)
- **Custom Features**: User-defined functions

### 2. Label Registry (`src/signals/label_registry.py`)

Manages label/target generation for ML training. Supports four types:

- **Price Labels**: Future prices, price changes, ratios
- **Return Labels**: Simple/log returns, volatility, max/min returns
- **Classification Labels**: Direction, quantiles, regime classification
- **Custom Labels**: User-defined label functions

### 3. Indicator Factory (`src/signals/indicator_factory.py`)

Unified interface for creating and managing technical indicators. Includes:

- Built-in indicators (EMA, RSI, MACD, Bollinger Bands, etc.)
- Custom indicator registration
- Parameter management

### 4. Configurable Training Data Generator (`src/modeling/configurable_train_data_generator.py`)

Main pipeline that orchestrates feature/label generation and creates training sequences:

- Time series windowing
- Data quality validation
- Scaling and normalization
- Multi-symbol processing

## 🚀 Quick Start

### 1. Basic Usage

```python
import gin
from modeling.configurable_train_data_generator import (
    ConfigurableTrainingDataGenerator,
    create_configurable_training_data_config
)

# Configure with gin file
gin.parse_config_file('config/configurable_training_simple.gin')

# Create generator
config = create_configurable_training_data_config()
generator = ConfigurableTrainingDataGenerator(config)

# Generate training data
result = generator.generate_training_data(market_data, symbols=['AAPL', 'MSFT'])

print(f"Features shape: {result['features'].shape}")
print(f"Labels shape: {result['labels'].shape}")
print(f"Feature names: {result['feature_names']}")
print(f"Label names: {result['label_names']}")
```

### 2. Gin Configuration Example

```gin
# config/my_training.gin

# Define features
returns_1d/create_feature_config.name = "returns_1d"
returns_1d/create_feature_config.feature_type = "transform"
returns_1d/create_feature_config.parameters = {
    'transform_type': 'pct_change',
    'column': 'close',
    'periods': 1
}
returns_1d/create_feature_config.lag_periods = 1

rsi_14/create_feature_config.name = "rsi_14"
rsi_14/create_feature_config.feature_type = "indicator"
rsi_14/create_feature_config.parameters = {
    'indicator_type': 'rsi',
    'period': 14
}

# Define labels
future_return/create_label_config.name = "future_return_1d"
future_return/create_label_config.label_type = "return"
future_return/create_label_config.parameters = {
    'return_type': 'simple',
    'column': 'close'
}
future_return/create_label_config.lead_periods = 1

# Create registries
create_feature_registry.feature_configs = [
    @returns_1d/create_feature_config(),
    @rsi_14/create_feature_config()
]

create_label_registry.label_configs = [
    @future_return/create_label_config()
]

# Configure training data generation
create_configurable_training_data_config.sequence_length = 30
create_configurable_training_data_config.prediction_horizon = 5
create_configurable_training_data_config.feature_registry = @create_feature_registry()
create_configurable_training_data_config.label_registry = @create_label_registry()
```

### 3. Programmatic Configuration

```python
from signals.feature_registry import FeatureRegistry, FeatureConfig
from signals.label_registry import LabelRegistry, LabelConfig
from modeling.configurable_train_data_generator import ConfigurableTrainingDataConfig

# Create feature configurations
feature_configs = [
    FeatureConfig(
        name="ema_20",
        feature_type="indicator",
        parameters={'indicator_type': 'ema', 'period': 20}
    ),
    FeatureConfig(
        name="volatility",
        feature_type="transform",
        parameters={'transform_type': 'volatility', 'window': 20}
    )
]

# Create label configurations
label_configs = [
    LabelConfig(
        name="future_return",
        label_type="return",
        parameters={'return_type': 'simple', 'column': 'close'},
        lead_periods=5
    )
]

# Create registries
feature_registry = FeatureRegistry(features=feature_configs)
label_registry = LabelRegistry(labels=label_configs)

# Create configuration
config = ConfigurableTrainingDataConfig(
    sequence_length=60,
    prediction_horizon=10,
    feature_registry=feature_registry,
    label_registry=label_registry
)
```

## 📊 Available Features

### Technical Indicators
- **EMA**: Exponential Moving Average with trend analysis
- **SMA**: Simple Moving Average
- **RSI**: Relative Strength Index (14 period default)
- **ATR**: Average True Range for volatility
- **MACD**: Moving Average Convergence Divergence
- **Bollinger Bands**: Price bands with statistical analysis
- **Stochastic**: Momentum oscillator
- **Williams %R**: Momentum indicator
- **CCI**: Commodity Channel Index

### Data Transforms
- **Percentage Change**: Simple returns over various periods
- **Log Returns**: Natural logarithm of price ratios
- **Volatility**: Rolling standard deviation of returns
- **Volume Ratio**: Current volume vs. average volume
- **Price Velocity**: Rate of price change

### Custom Features
Register your own feature functions:

```python
def my_custom_feature(data: pd.DataFrame, window: int = 10, **kwargs) -> pd.Series:
    """Custom feature calculation."""
    return data['close'].rolling(window).apply(lambda x: x.max() - x.min())

# Register the function
generator.feature_registry.register_custom_function('price_range', my_custom_feature)
```

## 🎯 Available Labels

### Price Labels
- **Future Price**: Direct future price values
- **Price Change**: Absolute price differences
- **Price Ratio**: Future price / current price ratios
- **High-Low Range**: Future trading ranges

### Return Labels
- **Simple Returns**: (Future - Current) / Current
- **Log Returns**: ln(Future / Current)
- **Cumulative Returns**: Returns over multiple periods
- **Volatility**: Future volatility estimates
- **Max/Min Returns**: Best/worst returns over horizon

### Classification Labels
- **Direction**: Binary up/down classification
- **Direction with Threshold**: Up/Down/Neutral with percentage thresholds
- **Quantile Classification**: Return quantile buckets
- **Volatility Regime**: High/low volatility classification

## ⚙️ Configuration Options

### Training Data Config
```gin
create_configurable_training_data_config.sequence_length = 60        # Input sequence length
create_configurable_training_data_config.prediction_horizon = 10     # Prediction time steps
create_configurable_training_data_config.window_stride = 1           # Stride between windows
create_configurable_training_data_config.min_valid_ratio = 0.8       # Min non-NaN ratio
create_configurable_training_data_config.normalize_features = True   # Feature normalization
create_configurable_training_data_config.normalize_labels = False    # Label normalization
create_configurable_training_data_config.feature_scaling_method = 'robust'  # Scaling method
create_configurable_training_data_config.remove_outliers = True      # Outlier removal
create_configurable_training_data_config.outlier_threshold = 3.0     # Std dev threshold
create_configurable_training_data_config.output_format = 'pytorch'   # Output format
```

### Scaling Methods
- **`standard`**: StandardScaler (zero mean, unit variance)
- **`robust`**: RobustScaler (median and IQR)
- **`minmax`**: MinMaxScaler (0-1 range)
- **`none`**: No scaling

### Output Formats
- **`pytorch`**: PyTorch tensors
- **`numpy`**: NumPy arrays
- **`pandas`**: Pandas DataFrames

## 📁 Configuration Files

Three example configurations are provided:

1. **`configurable_training_simple.gin`**: Basic transforms only (most reliable)
2. **`configurable_training_basic.gin`**: Simple technical indicators
3. **`configurable_training_advanced.gin`**: Comprehensive feature set

## 🧪 Testing

Run the test suite to verify functionality:

```bash
# Basic framework test
PYTHONPATH=src python test_configurable_framework.py

# Simple configuration test (recommended for new users)
PYTHONPATH=src python test_simple_configurable.py

# Full examples
PYTHONPATH=src python examples/configurable_training_data_example.py
```

## 📊 Output Structure

The framework returns a dictionary with:

```python
{
    'features': torch.Tensor,           # Shape: [batch, sequence_length, n_features]
    'labels': torch.Tensor,             # Shape: [batch, prediction_horizon, n_labels]
    'feature_masks': torch.Tensor,      # Shape: [batch, sequence_length, n_features]
    'label_masks': torch.Tensor,        # Shape: [batch, prediction_horizon, n_labels]
    'feature_names': List[str],         # Feature column names
    'label_names': List[str],           # Label column names
    'config': ConfigurableTrainingDataConfig  # Configuration used
}
```

## 🔧 Advanced Usage

### Multi-Timeframe Features (Future Enhancement)
The framework is designed to support multiple timeframes:

```gin
create_configurable_training_data_config.base_timeframe = '1d'
create_configurable_training_data_config.additional_timeframes = ['1h', '4h']
```

### Custom Indicator Development
Create custom indicators by extending the base `Indicator` class:

```python
from signals.indicator import Indicator

class MyCustomIndicator(Indicator):
    def __init__(self, period: int = 20):
        super().__init__()
        self.period = period
    
    def calculate(self, data: pd.DataFrame) -> Dict[str, Any]:
        if len(data) < self.period:
            return {'value': None, 'status': 'insufficient_data'}
        
        # Your calculation logic here
        result_value = data['close'].rolling(self.period).mean().iloc[-1]
        
        return {
            'value': result_value,
            'status': 'valid'
        }
```

### Integration with Existing Training Pipelines

The framework integrates seamlessly with the existing `IndicatorRunner`:

```python
from modeling.configurable_train_data_generator import ConfigurableTrainDataCallback

# Create callback with your configuration
callback = ConfigurableTrainDataCallback(
    config=your_config,
    output_path="training_data.pt"
)

# Add to IndicatorRunner
runner = IndicatorRunner(
    start_date=start_date,
    end_date=end_date,
    environment=environment,
    callbacks=[callback]  # Add the configurable callback
)

await runner.run()
```

## 🚨 Important Notes

1. **Data Requirements**: Ensure your input data has OHLCV columns and proper date indexing
2. **Memory Usage**: Large sequence lengths and many features can consume significant memory
3. **NaN Handling**: The framework handles missing values but excessive NaNs may reduce training data quality
4. **Feature Engineering**: Consider the temporal relationships when designing lag/lead periods
5. **Performance**: Some technical indicators require significant historical data for calculation

## 🤝 Contributing

To add new features or indicators:

1. **Features**: Add new generator classes to `feature_registry.py`
2. **Labels**: Add new generator classes to `label_registry.py`
3. **Indicators**: Add new indicator classes to `indicator_factory.py`
4. **Tests**: Add corresponding tests to verify functionality

## 📚 Examples

See the `examples/` directory for complete working examples:

- `configurable_training_data_example.py`: Comprehensive demonstration
- `test_simple_configurable.py`: Simple working example

This framework makes it easy to experiment with different feature sets for your ML models. Start with the simple configuration and gradually add more sophisticated features as needed!