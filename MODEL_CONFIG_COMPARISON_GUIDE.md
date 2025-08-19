# Model Configuration Comparison Framework

A comprehensive system for comparing different model configurations using production-grade backtesting infrastructure.

## Overview

This framework allows you to:
- ✅ **Define baseline vs test configurations** easily
- 🚀 **Run comparative backtests** with real market data
- 📊 **Generate statistical significance analysis**
- 📈 **Get actionable recommendations** for model selection
- 💾 **Save/load configurations** for reuse
- 🔄 **Integrate with existing production infrastructure**

## Quick Start

### 1. Basic CLI Usage

Compare two predefined model templates:

```bash
# Simple comparison
PYTHONPATH=src python scripts/ml/compare_model_configs.py \
    --baseline sr_baseline \
    --test sr_enhanced \
    --start-date 2023-01-01 \
    --end-date 2024-06-30

# With custom universe
PYTHONPATH=src python scripts/ml/compare_model_configs.py \
    --baseline adaptive_baseline \
    --test adaptive_aggressive \
    --start-date 2023-01-01 \
    --end-date 2024-06-30 \
    --universe sp500_liquid \
    --universe-size 50
```

### 2. Creating Parameter Variants

Generate multiple test variants automatically:

```bash
# Learning rate sweep
PYTHONPATH=src python scripts/ml/compare_model_configs.py \
    --baseline sr_baseline \
    --create-variants \
    --lr-variants "0.0001,0.0005,0.001,0.002" \
    --start-date 2023-01-01 \
    --end-date 2024-03-31

# Architecture exploration
PYTHONPATH=src python scripts/ml/compare_model_configs.py \
    --baseline sr_baseline \
    --create-variants \
    --arch-variants "256,128;512,256,128;1024,512,256" \
    --start-date 2023-01-01 \
    --end-date 2024-03-31
```

### 3. Custom Configuration Files

Create and compare custom configurations:

```bash
# Compare using config files
PYTHONPATH=src python scripts/ml/compare_model_configs.py \
    --baseline-config configs/my_baseline.yaml \
    --test-config configs/my_experimental.yaml \
    --start-date 2023-01-01 \
    --end-date 2024-06-30 \
    --save-configs
```

## Available Model Templates

### Support/Resistance Models
- `sr_baseline` - Conservative baseline S/R model
- `sr_enhanced` - Deeper network with more capacity
- `sr_deep` - Very deep architecture for complex patterns
- `sr_wide` - Wide network for feature capture

### Adaptive Models  
- `adaptive_baseline` - Weekly retraining strategy
- `adaptive_aggressive` - Daily retraining with shorter memory
- `adaptive_conservative` - Monthly retraining with long memory

### Temporal Fusion Transformers
- `tft_baseline` - Standard TFT configuration
- `tft_large` - Large model with extended context
- `tft_sentiment` - Enhanced sentiment integration

## Creating Custom Configurations

### 1. Programmatically

```python
from ml.evaluation.model_config_comparison import ModelConfigDefinition, ModelType
from ml.models.support_resistance_model import SRModelConfig

# Define custom S/R model
custom_sr = ModelConfigDefinition(
    name="custom_sr_optimized",
    model_type=ModelType.SUPPORT_RESISTANCE,
    config=SRModelConfig(
        input_dim=75,
        hidden_dims=[512, 256, 128, 64],
        dropout_rate=0.4,
        epochs=120,
        batch_size=32,
        learning_rate=0.0008,
        max_support_levels=5,
        max_resistance_levels=5
    ),
    description="Optimized S/R model from hyperparameter search",
    tags=["custom", "optimized", "production_candidate"]
)
```

### 2. Configuration Files

Create `my_config.yaml`:

```yaml
name: my_experimental_model
model_type: support_resistance
description: Experimental model with novel architecture
tags: [experimental, research]

config:
  input_dim: 50
  hidden_dims: [1024, 512, 256, 128, 64]
  dropout_rate: 0.5
  epochs: 200
  batch_size: 16
  learning_rate: 0.0001
  max_support_levels: 7
  max_resistance_levels: 7
  activation: swish

metadata:
  created_by: research_team
  optimization_method: bayesian_optimization
  validation_score: 0.847
```

## Programmatic Usage

### Basic Comparison

```python
import asyncio
from datetime import date
from ml.evaluation.model_config_comparison import ModelConfigComparison

async def run_comparison():
    comparison = ModelConfigComparison()
    
    # Add configurations
    comparison.add_baseline_config("baseline", "sr_baseline")
    comparison.add_test_config("enhanced", "sr_enhanced")
    
    # Run comparison
    results = await comparison.run_comparative_backtest(
        start_date=date(2023, 1, 1),
        end_date=date(2024, 6, 30),
        universe=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    )
    
    # Print results
    for name, result in results.items():
        print(f"{name}: {result.summary}")

asyncio.run(run_comparison())
```

### Production-Grade Comparison

```python
from ml.evaluation.production_model_comparison import (
    ProductionModelComparison, 
    ProductionComparisonConfig
)

async def production_comparison():
    config = ProductionComparisonConfig(
        start_date=date(2023, 1, 1),
        end_date=date(2024, 6, 30),
        universe_name="sp500_liquid",
        initial_capital=1000000.0,
        use_real_trading_logic=True
    )
    
    comparison = ProductionModelComparison(config)
    
    # Load baseline and test configurations
    baseline_configs = {"baseline": manager.get_template("sr_baseline")}
    test_configs = {"enhanced": manager.get_template("sr_enhanced")}
    
    # Run production comparison
    results = await comparison.compare_model_configurations(
        baseline_configs, test_configs
    )
    
    return results
```

## Configuration Variants and Factories

### Learning Rate Variants

```python
from ml.evaluation.model_config_comparison import ConfigFactory

base_config = manager.get_template("sr_baseline")

# Create learning rate variants
lr_variants = ConfigFactory.create_learning_rate_variants(
    base_config, [0.0001, 0.0005, 0.001, 0.002, 0.005]
)

# Use in comparison
for variant in lr_variants:
    comparison.add_test_config(variant.name, variant)
```

### Architecture Variants

```python
# Different architectures
architectures = [
    [128, 64],           # Shallow
    [256, 128, 64],      # Medium (baseline)
    [512, 256, 128],     # Wide
    [256, 128, 64, 32],  # Deep
    [512, 256, 128, 64, 32]  # Wide + Deep
]

arch_variants = ConfigFactory.create_architecture_variants(
    base_config, architectures
)
```

### Custom Variants

```python
manager = ModelConfigManager()

# Create custom variant
enhanced_variant = manager.create_variant(
    base_config,
    modifications={
        'hidden_dims': [512, 256, 128, 64],
        'learning_rate': 0.0005,
        'epochs': 150,
        'dropout_rate': 0.4
    },
    new_name="sr_enhanced_custom",
    description="Custom enhanced S/R model"
)
```

## Understanding Results

### Performance Metrics

The system compares models on:
- **Total Return** - Overall portfolio performance
- **Sharpe Ratio** - Risk-adjusted returns
- **Maximum Drawdown** - Worst peak-to-trough decline
- **Win Rate** - Percentage of profitable trades
- **Volatility** - Portfolio return volatility
- **Profit Factor** - Ratio of gross profits to gross losses

### Statistical Significance

Results include:
- **T-test results** - Statistical significance of performance differences
- **Effect size (Cohen's d)** - Magnitude of performance difference
- **Confidence intervals** - Range of expected performance differences
- **P-values** - Probability that differences are due to chance

### Recommendations

Each comparison provides:
- **Decision**: `adopt_test`, `keep_baseline`, or `requires_further_testing`
- **Confidence**: `high`, `medium`, or `low`
- **Reasons**: Specific factors supporting the recommendation
- **Concerns**: Potential risks or issues
- **Next Steps**: Recommended actions

## Output Structure

### Console Output
```
📊 BASELINE_VS_TEST
────────────────────────────────────────────────────────────
BASELINE (sr_baseline):
  Total Return: 12.5%
  Sharpe Ratio: 1.23
  Max Drawdown: -8.2%
  Win Rate: 58.3%

TEST (sr_enhanced):
  Total Return: 15.7%
  Sharpe Ratio: 1.41
  Max Drawdown: -6.9%
  Win Rate: 62.1%

PERFORMANCE DIFFERENCES:
  Total Return: ↑ 25.6%
  Sharpe Ratio: ↑ 14.6%
  Max Drawdown: ↑ 15.9%
  Win Rate: ↑ 6.5%

RECOMMENDATIONS:
  ✅ Test configuration shows 25.6% better returns
  📈 Test configuration has superior risk-adjusted returns
  ✅ Performance difference is statistically significant
```

### File Outputs

```
comparison_results/
├── comparison_report.md           # Human-readable report
├── comparison_results.json        # Structured results
├── detailed_results.pkl          # Complete data for analysis
├── baseline_config.yaml          # Saved baseline configuration
└── test_config.yaml              # Saved test configuration
```

## Integration with Existing Infrastructure

### With Production Backtest Runner

The system integrates seamlessly with your existing `ProductionBacktestRunner`:

```python
# The production comparison extends your existing runner
from scripts.analytics.production_backtest_runner import ProductionBacktestRunner
from ml.evaluation.production_model_comparison import ProductionModelComparison

# Uses same database connections, trading logic, and market data
```

### With Kubernetes Jobs

Run comparisons as Kubernetes jobs:

```bash
# Generate K8s job for model comparison
python scripts/kubernetes/generate_comparison_job.py \
    --baseline-config configs/baseline.yaml \
    --test-config configs/test.yaml \
    --output k8s/model-comparison-job.yaml

# Apply job
kubectl apply -f k8s/model-comparison-job.yaml
```

### With Gin Configuration

Model configs work with your existing Gin framework:

```gin
# config/model_comparison.gin
ModelConfigComparison.output_dir = "production_comparisons"
ProductionComparisonConfig.initial_capital = 5000000.0
ProductionComparisonConfig.universe_name = "sp500_liquid"
```

## Best Practices

### 1. Configuration Management
- **Use descriptive names** for configurations
- **Tag configurations** appropriately (baseline, test, experimental)
- **Document changes** in the description field
- **Version control** configuration files

### 2. Backtesting
- **Use realistic time periods** (at least 6 months)
- **Test multiple market conditions** (bull, bear, sideways)
- **Ensure statistical significance** (minimum 50 trades)
- **Consider transaction costs** and slippage

### 3. Model Selection
- **Don't just optimize for returns** - consider risk-adjusted metrics
- **Test out-of-sample** performance extensively
- **Monitor for overfitting** to specific time periods
- **Consider operational complexity** for production deployment

### 4. Production Deployment
- **Validate configurations** in staging environment first
- **Monitor model performance** after deployment
- **Have rollback procedures** ready
- **Document deployment decisions**

## Examples

Run the comprehensive examples:

```bash
# Run all examples
PYTHONPATH=src python examples/model_config_comparison_examples.py

# Individual examples
PYTHONPATH=src python -c "
import asyncio
from examples.model_config_comparison_examples import example_1_basic_sr_comparison
asyncio.run(example_1_basic_sr_comparison())
"
```

## Troubleshooting

### Common Issues

1. **"Template not found"**
   - Check template name spelling
   - Use `manager.list_templates()` to see available templates

2. **"No market data available"**
   - Verify database connection
   - Check date range and universe
   - Ensure data exists for the period

3. **"Insufficient trades for significance testing"**
   - Extend backtest period
   - Increase universe size
   - Lower confidence thresholds

4. **Memory issues with large comparisons**
   - Reduce universe size
   - Shorter backtest periods
   - Use sampling for parameter sweeps

### Performance Tips

- **Limit universe size** to 20-50 symbols for development
- **Use shorter periods** for parameter exploration
- **Run comparisons in parallel** when possible
- **Cache market data** to avoid repeated fetches

## Contributing

To add new model types or features:

1. **Extend ModelType enum** in `model_config_comparison.py`
2. **Add configuration class** following existing patterns  
3. **Implement backtesting logic** in production comparison
4. **Add templates** to ModelConfigManager
5. **Create examples** demonstrating usage
6. **Update documentation**

---

## Summary

This framework provides a powerful, production-ready system for comparing model configurations. It integrates with your existing infrastructure while providing statistical rigor and actionable recommendations for model selection.

Key benefits:
- 🎯 **Objective model comparison** with statistical significance
- 🚀 **Production integration** with real market data and trading logic
- 📊 **Comprehensive analysis** including risk and performance metrics
- 🔧 **Flexible configuration** system with templates and variants
- 📈 **Actionable recommendations** for deployment decisions

Start with the CLI examples, then move to custom configurations as your needs evolve!