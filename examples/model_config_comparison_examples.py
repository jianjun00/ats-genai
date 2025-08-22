#!/usr/bin/env python3
"""
Model Configuration Comparison Examples

This script demonstrates various ways to use the model configuration comparison system
with practical examples that integrate with your existing ATS infrastructure.
"""

import sys
import asyncio
import logging
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ml.evaluation.model_config_comparison import (
    ModelConfigComparison, 
    ModelConfigManager, 
    ModelConfigDefinition,
    ModelType,
    ConfigFactory
)
from ml.models.support_resistance_model import SRModelConfig
from ml.dynamic_training.adaptive_sr_model import AdaptiveModelConfig


async def example_1_basic_sr_comparison():
    """Example 1: Basic Support/Resistance model comparison"""
    
    print("🔥 Example 1: Basic S/R Model Comparison")
    print("="*60)
    
    comparison = ModelConfigComparison(output_dir="examples/basic_sr_comparison")
    manager = ModelConfigManager()
    
    # Define baseline configuration
    baseline = ModelConfigDefinition(
        name="sr_baseline",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=50,
            hidden_dims=[256, 128, 64],
            dropout_rate=0.3,
            epochs=50,  # Reduced for faster testing
            batch_size=64,
            learning_rate=0.001
        ),
        description="Conservative baseline S/R model",
        tags=["baseline", "conservative"]
    )
    
    # Define test configuration - deeper network
    test_deep = ModelConfigDefinition(
        name="sr_deep",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=50,
            hidden_dims=[512, 256, 128, 64, 32],  # Deeper
            dropout_rate=0.4,  # Higher dropout for regularization
            epochs=75,  # More training
            batch_size=32,  # Smaller batches
            learning_rate=0.0005  # Lower learning rate
        ),
        description="Deeper S/R model with more capacity",
        tags=["test", "deep", "regularized"]
    )
    
    comparison.add_baseline_config("baseline", baseline)
    comparison.add_test_config("deep", test_deep)
    
    # Run comparison
    results = await comparison.run_comparative_backtest(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),  # Shorter period for demo
        universe=['AAPL', 'MSFT', 'GOOGL']  # Small universe for demo
    )
    
    print("✅ Basic S/R comparison completed!")
    return results


async def example_2_adaptive_retraining_comparison():
    """Example 2: Compare different adaptive retraining strategies"""
    
    print("\n🧠 Example 2: Adaptive Retraining Strategy Comparison")
    print("="*60)
    
    comparison = ModelConfigComparison(output_dir="examples/adaptive_comparison")
    
    # Conservative adaptive strategy (baseline)
    conservative_adaptive = ModelConfigDefinition(
        name="adaptive_conservative",
        model_type=ModelType.ADAPTIVE_SR,
        config=AdaptiveModelConfig(
            bootstrap_years=3,
            rolling_window_days=365,  # 1 year window
            retrain_frequency_days=7,  # Weekly retraining
            learning_rate_decay=0.95,
            min_accuracy_threshold=0.4,
            model_memory_weight=0.8  # Remember more of old model
        ),
        description="Conservative adaptive strategy with weekly retraining",
        tags=["baseline", "conservative", "weekly"]
    )
    
    # Aggressive adaptive strategy (test)
    aggressive_adaptive = ModelConfigDefinition(
        name="adaptive_aggressive",
        model_type=ModelType.ADAPTIVE_SR,
        config=AdaptiveModelConfig(
            bootstrap_years=2,  # Less historical data
            rolling_window_days=180,  # 6 month window
            retrain_frequency_days=1,  # Daily retraining
            learning_rate_decay=0.98,  # Slower decay
            min_accuracy_threshold=0.5,  # Higher threshold
            model_memory_weight=0.6  # Less weight to old model
        ),
        description="Aggressive adaptive strategy with daily retraining",
        tags=["test", "aggressive", "daily"]
    )
    
    comparison.add_baseline_config("conservative", conservative_adaptive)
    comparison.add_test_config("aggressive", aggressive_adaptive)
    
    results = await comparison.run_comparative_backtest(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
        universe=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    )
    
    print("✅ Adaptive retraining comparison completed!")
    return results


async def example_3_learning_rate_sweep():
    """Example 3: Learning rate parameter sweep"""
    
    print("\n📈 Example 3: Learning Rate Parameter Sweep")
    print("="*60)
    
    comparison = ModelConfigComparison(output_dir="examples/lr_sweep")
    manager = ModelConfigManager()
    
    # Base configuration
    base_config = ModelConfigDefinition(
        name="sr_base",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=50,
            hidden_dims=[256, 128, 64],
            epochs=30,  # Reduced for sweep
            batch_size=64
        ),
        description="Base S/R configuration for learning rate sweep"
    )
    
    # Create learning rate variants
    learning_rates = [0.0001, 0.0005, 0.001, 0.002, 0.005]
    lr_variants = ConfigFactory.create_learning_rate_variants(base_config, learning_rates)
    
    # Use first as baseline, rest as tests
    comparison.add_baseline_config("baseline", lr_variants[0])
    
    for variant in lr_variants[1:]:
        comparison.add_test_config(variant.name, variant)
    
    results = await comparison.run_comparative_backtest(
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 29),  # Short period for sweep
        universe=['AAPL', 'MSFT', 'GOOGL']
    )
    
    print("✅ Learning rate sweep completed!")
    return results


async def example_4_architecture_exploration():
    """Example 4: Neural network architecture exploration"""
    
    print("\n🏗️  Example 4: Architecture Exploration")
    print("="*60)
    
    comparison = ModelConfigComparison(output_dir="examples/architecture_exploration")
    manager = ModelConfigManager()
    
    # Base configuration
    base_config = ModelConfigDefinition(
        name="sr_base_arch",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=50,
            epochs=40,
            batch_size=64,
            learning_rate=0.001
        ),
        description="Base configuration for architecture exploration"
    )
    
    # Different architectures to test
    architectures = [
        [128, 64],           # Shallow
        [256, 128, 64],      # Baseline  
        [512, 256, 128],     # Wide
        [256, 128, 64, 32],  # Deep
        [512, 256, 128, 64, 32]  # Wide + Deep
    ]
    
    arch_variants = ConfigFactory.create_architecture_variants(base_config, architectures)
    
    # Use second as baseline (256, 128, 64), others as tests
    comparison.add_baseline_config("baseline", arch_variants[1])
    
    for i, variant in enumerate(arch_variants):
        if i != 1:  # Skip baseline
            comparison.add_test_config(variant.name, variant)
    
    results = await comparison.run_comparative_backtest(
        start_date=date(2024, 2, 1),
        end_date=date(2024, 2, 29),
        universe=['AAPL', 'MSFT']  # Small for arch exploration
    )
    
    print("✅ Architecture exploration completed!")
    return results


async def example_5_multi_factor_comparison():
    """Example 5: Multi-factor configuration comparison"""
    
    print("\n🎯 Example 5: Multi-Factor Configuration Comparison") 
    print("="*60)
    
    comparison = ModelConfigComparison(output_dir="examples/multi_factor_comparison")
    
    # Production-ready baseline
    production_baseline = ModelConfigDefinition(
        name="production_baseline",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=50,
            hidden_dims=[256, 128, 64],
            dropout_rate=0.3,
            epochs=100,
            batch_size=64,
            learning_rate=0.001,
            max_support_levels=3,
            max_resistance_levels=3
        ),
        description="Production baseline - balanced performance/stability",
        tags=["production", "baseline", "stable"]
    )
    
    # Research configuration - exploring limits
    research_config = ModelConfigDefinition(
        name="research_experimental",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=50,
            hidden_dims=[1024, 512, 256, 128, 64],  # Much larger
            dropout_rate=0.5,  # Heavy regularization
            epochs=200,  # Extensive training
            batch_size=16,  # Small batches
            learning_rate=0.0001,  # Very low LR
            max_support_levels=5,  # More levels
            max_resistance_levels=5,
            activation='swish'  # Different activation
        ),
        description="Experimental research configuration - pushing boundaries",
        tags=["research", "experimental", "large_capacity"]
    )
    
    comparison.add_baseline_config("production", production_baseline)
    comparison.add_test_config("research", research_config)
    
    results = await comparison.run_comparative_backtest(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 29),
        universe=['AAPL', 'MSFT', 'GOOGL', 'AMZN']
    )
    
    print("✅ Multi-factor comparison completed!")
    return results


async def example_6_save_and_load_configs():
    """Example 6: Save and load configurations for reuse"""
    
    print("\n💾 Example 6: Save and Load Configuration Examples")
    print("="*60)
    
    manager = ModelConfigManager()
    
    # Create a custom configuration
    custom_config = ModelConfigDefinition(
        name="custom_sr_model",
        model_type=ModelType.SUPPORT_RESISTANCE,
        config=SRModelConfig(
            input_dim=75,  # More features
            hidden_dims=[384, 192, 96, 48],
            dropout_rate=0.35,
            epochs=120,
            batch_size=48,
            learning_rate=0.0008,
            level_weight=1.2,
            confidence_weight=0.6,
            ranking_weight=0.4
        ),
        description="Custom S/R model with optimized hyperparameters",
        tags=["custom", "optimized", "production_candidate"],
        metadata={
            "created_by": "research_team",
            "optimization_method": "bayesian_optimization",
            "validation_score": 0.847
        }
    )
    
    # Save in different formats
    yaml_path = manager.save_config(custom_config, "yaml")
    json_path = manager.save_config(custom_config, "json")
    
    print(f"📁 Saved YAML config: {yaml_path}")
    print(f"📁 Saved JSON config: {json_path}")
    
    # Load and verify
    loaded_yaml = manager.load_config(yaml_path)
    loaded_json = manager.load_config(json_path)
    
    print(f"✅ Loaded configs match: {loaded_yaml.name == loaded_json.name == custom_config.name}")
    print(f"📋 Config description: {loaded_yaml.description}")
    print(f"🏷️  Tags: {loaded_yaml.tags}")
    print(f"📊 Metadata: {loaded_yaml.metadata}")
    
    return custom_config


async def main():
    """Run all examples"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 MODEL CONFIGURATION COMPARISON EXAMPLES")
    print("="*80)
    print("These examples demonstrate how to compare different model configurations")
    print("using your existing ATS backtesting infrastructure.")
    print("="*80)
    
    examples = [
        ("Basic S/R Comparison", example_1_basic_sr_comparison),
        ("Adaptive Retraining Comparison", example_2_adaptive_retraining_comparison),  
        ("Learning Rate Sweep", example_3_learning_rate_sweep),
        ("Architecture Exploration", example_4_architecture_exploration),
        ("Multi-Factor Comparison", example_5_multi_factor_comparison),
        ("Save/Load Configurations", example_6_save_and_load_configs)
    ]
    
    results = {}
    
    for name, example_func in examples:
        try:
            print(f"\n🔄 Running: {name}")
            result = await example_func()
            results[name] = result
            print(f"✅ Completed: {name}")
        except Exception as e:
            print(f"❌ Failed: {name} - {e}")
            logging.error(f"Example '{name}' failed: {e}", exc_info=True)
    
    print(f"\n🎉 Examples completed! Results saved in examples/ directory")
    print(f"📊 Successful examples: {len(results)}/{len(examples)}")
    
    print(f"\n📋 To use these configurations in production:")
    print(f"   1. Review the generated comparison reports")
    print(f"   2. Select best-performing configurations") 
    print(f"   3. Save selected configs using manager.save_config()")
    print(f"   4. Load in production with manager.load_config()")
    
    return results


if __name__ == "__main__":
    asyncio.run(main())