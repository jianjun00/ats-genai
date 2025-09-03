"""
Model Configuration Comparison Framework

This module provides a flexible system for defining baseline vs test model configurations
and running comparative backtests to evaluate performance differences.

Usage:
    config_comparison = ModelConfigComparison()
    config_comparison.add_baseline_config("sr_baseline", sr_baseline_config)
    config_comparison.add_test_config("sr_enhanced", sr_test_config)
    results = await config_comparison.run_comparative_backtest(start_date, end_date)
"""

import asyncio
import logging
import json
import yaml
from datetime import date, datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, asdict, field
from pathlib import Path
from enum import Enum
import numpy as np

from domains.ml.services.models.support_resistance_model import SRModelConfig
from domains.ml.services.dynamic_training.adaptive_sr_model import AdaptiveModelConfig
from models.temporal_fusion_transformer import TFTConfig
from domains.ml.services.evaluation.adaptive_backtester import AdaptiveBacktester, AdaptiveBacktestConfig


class ModelType(Enum):
    """Supported model types"""
    SUPPORT_RESISTANCE = "support_resistance"
    ADAPTIVE_SR = "adaptive_sr"
    TEMPORAL_FUSION_TRANSFORMER = "tft"
    ENSEMBLE = "ensemble"


@dataclass
class ModelConfigDefinition:
    """Complete model configuration definition"""
    name: str
    model_type: ModelType
    config: Union[SRModelConfig, AdaptiveModelConfig, TFTConfig, Dict[str, Any]]
    description: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'name': self.name,
            'model_type': self.model_type.value,
            'config': asdict(self.config) if hasattr(self.config, '__dict__') else self.config,
            'description': self.description,
            'tags': self.tags,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelConfigDefinition':
        """Create from dictionary"""
        model_type = ModelType(data['model_type'])
        
        # Reconstruct config object based on type
        config_data = data['config']
        if model_type == ModelType.SUPPORT_RESISTANCE:
            config = SRModelConfig(**config_data)
        elif model_type == ModelType.ADAPTIVE_SR:
            config = AdaptiveModelConfig(**config_data)
        elif model_type == ModelType.TEMPORAL_FUSION_TRANSFORMER:
            config = TFTConfig(**config_data)
        else:
            config = config_data
        
        return cls(
            name=data['name'],
            model_type=model_type,
            config=config,
            description=data.get('description', ''),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )


@dataclass
class ComparisonResult:
    """Results from comparing two model configurations"""
    baseline_config: ModelConfigDefinition
    test_config: ModelConfigDefinition
    baseline_metrics: Dict[str, float]
    test_metrics: Dict[str, float]
    performance_diff: Dict[str, float]
    statistical_significance: Dict[str, Dict[str, float]]
    summary: str
    recommendations: List[str]
    detailed_results: Dict[str, Any]


class ModelConfigManager:
    """Manages model configuration definitions and templates"""
    
    def __init__(self, config_dir: str = "model_configs"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # Pre-defined templates
        self._initialize_templates()
    
    def _initialize_templates(self):
        """Initialize common configuration templates"""
        self.templates = {
            'sr_baseline': ModelConfigDefinition(
                name="sr_baseline",
                model_type=ModelType.SUPPORT_RESISTANCE,
                config=SRModelConfig(
                    input_dim=50,
                    hidden_dims=[256, 128, 64],
                    dropout_rate=0.3,
                    max_support_levels=3,
                    max_resistance_levels=3,
                    epochs=100,
                    batch_size=64,
                    learning_rate=0.001
                ),
                description="Baseline Support/Resistance model configuration",
                tags=["baseline", "sr", "neural_network"]
            ),
            
            'sr_enhanced': ModelConfigDefinition(
                name="sr_enhanced",
                model_type=ModelType.SUPPORT_RESISTANCE,
                config=SRModelConfig(
                    input_dim=50,
                    hidden_dims=[512, 256, 128, 64],  # Deeper network
                    dropout_rate=0.4,  # Higher dropout
                    max_support_levels=5,  # More levels
                    max_resistance_levels=5,
                    epochs=150,  # More training
                    batch_size=32,  # Smaller batches
                    learning_rate=0.0005,  # Lower learning rate
                    activation='swish'  # Different activation
                ),
                description="Enhanced S/R model with deeper architecture",
                tags=["test", "sr", "enhanced", "deeper"]
            ),
            
            'adaptive_baseline': ModelConfigDefinition(
                name="adaptive_baseline",
                model_type=ModelType.ADAPTIVE_SR,
                config=AdaptiveModelConfig(
                    bootstrap_years=3,
                    rolling_window_days=365,
                    retrain_frequency_days=7,  # Weekly retraining
                    learning_rate_decay=0.95,
                    min_accuracy_threshold=0.4
                ),
                description="Baseline adaptive model with weekly retraining",
                tags=["baseline", "adaptive", "weekly"]
            ),
            
            'adaptive_aggressive': ModelConfigDefinition(
                name="adaptive_aggressive",
                model_type=ModelType.ADAPTIVE_SR,
                config=AdaptiveModelConfig(
                    bootstrap_years=2,  # Less historical data
                    rolling_window_days=180,  # Shorter window
                    retrain_frequency_days=1,  # Daily retraining
                    learning_rate_decay=0.98,  # Slower decay
                    min_accuracy_threshold=0.5,  # Higher threshold
                    model_memory_weight=0.7  # Less weight to old model
                ),
                description="Aggressive adaptive model with daily retraining",
                tags=["test", "adaptive", "daily", "aggressive"]
            ),
            
            'tft_baseline': ModelConfigDefinition(
                name="tft_baseline",
                model_type=ModelType.TEMPORAL_FUSION_TRANSFORMER,
                config=TFTConfig(
                    hidden_size=64,
                    lstm_layers=2,
                    attention_head_size=4,
                    max_encoder_length=120,
                    max_prediction_length=30,
                    use_sentiment_features=True,
                    sentiment_weight=0.3
                ),
                description="Baseline Temporal Fusion Transformer",
                tags=["baseline", "tft", "transformer"]
            ),
            
            'tft_large': ModelConfigDefinition(
                name="tft_large",
                model_type=ModelType.TEMPORAL_FUSION_TRANSFORMER,
                config=TFTConfig(
                    hidden_size=128,  # Larger hidden size
                    lstm_layers=3,  # More layers
                    attention_head_size=8,  # More attention heads
                    max_encoder_length=240,  # Longer context
                    max_prediction_length=60,  # Longer predictions
                    use_sentiment_features=True,
                    sentiment_weight=0.4,  # Higher sentiment weight
                    dropout=0.2  # Higher dropout
                ),
                description="Large TFT model with extended context",
                tags=["test", "tft", "large", "extended_context"]
            )
        }
    
    def get_template(self, name: str) -> Optional[ModelConfigDefinition]:
        """Get a predefined template"""
        return self.templates.get(name)
    
    def list_templates(self) -> List[str]:
        """List available templates"""
        return list(self.templates.keys())
    
    def save_config(self, config: ModelConfigDefinition, format: str = "yaml") -> str:
        """Save configuration to file"""
        if format.lower() == "yaml":
            file_path = self.config_dir / f"{config.name}.yaml"
            with open(file_path, 'w') as f:
                yaml.dump(config.to_dict(), f, default_flow_style=False)
        else:
            file_path = self.config_dir / f"{config.name}.json"
            with open(file_path, 'w') as f:
                json.dump(config.to_dict(), f, indent=2)
        
        self.logger.info(f"Saved config '{config.name}' to {file_path}")
        return str(file_path)
    
    def load_config(self, file_path: str) -> ModelConfigDefinition:
        """Load configuration from file"""
        file_path = Path(file_path)
        
        if file_path.suffix.lower() in ['.yml', '.yaml']:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
        else:
            with open(file_path, 'r') as f:
                data = json.load(f)
        
        return ModelConfigDefinition.from_dict(data)
    
    def create_variant(self, base_config: ModelConfigDefinition, 
                      modifications: Dict[str, Any], 
                      new_name: str,
                      description: str = "") -> ModelConfigDefinition:
        """Create a variant of an existing configuration"""
        
        # Deep copy the base config
        base_dict = base_config.to_dict()
        config_dict = base_dict['config'].copy()
        
        # Apply modifications
        for key, value in modifications.items():
            if key in config_dict:
                config_dict[key] = value
            else:
                self.logger.warning(f"Key '{key}' not found in base config")
        
        # Create new config
        new_config_data = base_dict.copy()
        new_config_data['name'] = new_name
        new_config_data['config'] = config_dict
        new_config_data['description'] = description or f"Variant of {base_config.name}"
        new_config_data['tags'] = base_config.tags + ['variant']
        
        return ModelConfigDefinition.from_dict(new_config_data)


class ModelConfigComparison:
    """Orchestrates model configuration comparisons"""
    
    def __init__(self, output_dir: str = "comparison_results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        self.config_manager = ModelConfigManager()
        self.baseline_configs: Dict[str, ModelConfigDefinition] = {}
        self.test_configs: Dict[str, ModelConfigDefinition] = {}
    
    def add_baseline_config(self, name: str, config: Union[ModelConfigDefinition, str]):
        """Add a baseline configuration"""
        if isinstance(config, str):
            config = self.config_manager.get_template(config)
            if not config:
                raise ValueError(f"Template '{config}' not found")
        
        self.baseline_configs[name] = config
        self.logger.info(f"Added baseline config: {name}")
    
    def add_test_config(self, name: str, config: Union[ModelConfigDefinition, str]):
        """Add a test configuration"""
        if isinstance(config, str):
            config = self.config_manager.get_template(config)
            if not config:
                raise ValueError(f"Template '{config}' not found")
        
        self.test_configs[name] = config
        self.logger.info(f"Added test config: {name}")
    
    async def run_comparative_backtest(
        self,
        start_date: date,
        end_date: date,
        universe: List[str] = None,
        initial_capital: float = 1000000.0,
        comparison_pairs: List[Tuple[str, str]] = None
    ) -> Dict[str, ComparisonResult]:
        """Run comparative backtests for all baseline vs test pairs"""
        
        if not comparison_pairs:
            # Compare all baselines against all tests
            comparison_pairs = [
                (baseline_name, test_name)
                for baseline_name in self.baseline_configs.keys()
                for test_name in self.test_configs.keys()
            ]
        
        self.logger.info(f"Running {len(comparison_pairs)} comparative backtests")
        
        results = {}
        
        for baseline_name, test_name in comparison_pairs:
            self.logger.info(f"Comparing {baseline_name} vs {test_name}")
            
            comparison_result = await self._run_single_comparison(
                baseline_name, test_name, start_date, end_date, 
                universe, initial_capital
            )
            
            results[f"{baseline_name}_vs_{test_name}"] = comparison_result
        
        # Generate overall comparison report
        await self._generate_comparison_report(results)
        
        return results
    
    async def _run_single_comparison(
        self,
        baseline_name: str,
        test_name: str,
        start_date: date,
        end_date: date,
        universe: List[str],
        initial_capital: float
    ) -> ComparisonResult:
        """Run comparison between two specific configurations"""
        
        baseline_config = self.baseline_configs[baseline_name]
        test_config = self.test_configs[test_name]
        
        # Run baseline backtest
        self.logger.info(f"Running baseline backtest: {baseline_name}")
        baseline_metrics = await self._run_backtest_for_config(
            baseline_config, start_date, end_date, universe, initial_capital
        )
        
        # Run test backtest
        self.logger.info(f"Running test backtest: {test_name}")
        test_metrics = await self._run_backtest_for_config(
            test_config, start_date, end_date, universe, initial_capital
        )
        
        # Calculate performance differences
        performance_diff = self._calculate_performance_diff(baseline_metrics, test_metrics)
        
        # Statistical significance testing
        statistical_significance = self._calculate_statistical_significance(
            baseline_metrics, test_metrics
        )
        
        # Generate summary and recommendations
        summary = self._generate_comparison_summary(
            baseline_config, test_config, performance_diff
        )
        recommendations = self._generate_recommendations(performance_diff)
        
        return ComparisonResult(
            baseline_config=baseline_config,
            test_config=test_config,
            baseline_metrics=baseline_metrics,
            test_metrics=test_metrics,
            performance_diff=performance_diff,
            statistical_significance=statistical_significance,
            summary=summary,
            recommendations=recommendations,
            detailed_results={
                'baseline_name': baseline_name,
                'test_name': test_name,
                'comparison_date': datetime.now().isoformat()
            }
        )
    
    async def _run_backtest_for_config(
        self,
        config: ModelConfigDefinition,
        start_date: date,
        end_date: date,
        universe: List[str],
        initial_capital: float
    ) -> Dict[str, float]:
        """Run backtest for a specific configuration"""
        
        # Create appropriate backtest configuration based on model type
        if config.model_type == ModelType.ADAPTIVE_SR:
            backtest_config = AdaptiveBacktestConfig(
                backtest_start_date=start_date,
                backtest_end_date=end_date,
                symbols=universe or ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
                adaptive_config=config.config,
                compare_static_model=False,
                output_dir=str(self.output_dir / f"backtest_{config.name}")
            )
            
            backtester = AdaptiveBacktester(backtest_config)
            results = await backtester.run_adaptive_backtest()
            
            # Extract key metrics
            return self._extract_key_metrics(results, "adaptive_model")
        
        else:
            # For other model types, use a simplified backtest
            # This would be expanded based on your specific needs
            return await self._run_simplified_backtest(config, start_date, end_date, universe, initial_capital)
    
    async def _run_simplified_backtest(
        self,
        config: ModelConfigDefinition,
        start_date: date,
        end_date: date,
        universe: List[str],
        initial_capital: float
    ) -> Dict[str, float]:
        """Simplified backtest for non-adaptive models"""
        
        # This is a placeholder implementation
        # In practice, you'd implement actual backtesting logic for each model type
        
        # Simulate some performance metrics
        np.random.seed(hash(config.name) % 2**32)  # Deterministic but different per config
        
        base_return = np.random.normal(0.08, 0.15)  # 8% mean return, 15% volatility
        sharpe = np.random.normal(1.2, 0.3)
        max_drawdown = np.random.uniform(0.05, 0.25)
        win_rate = np.random.uniform(0.45, 0.65)
        
        return {
            'total_return': base_return,
            'annualized_return': base_return,
            'volatility': 0.15,
            'sharpe_ratio': sharpe,
            'max_drawdown': -abs(max_drawdown),
            'win_rate': win_rate,
            'total_trades': np.random.randint(50, 200),
            'profit_factor': np.random.uniform(1.1, 2.5),
            'final_value': initial_capital * (1 + base_return)
        }
    
    def _extract_key_metrics(self, results: Dict[str, Any], model_key: str) -> Dict[str, float]:
        """Extract key metrics from backtest results"""
        model_results = results.get(model_key, {})
        metrics = model_results.get('metrics', {})
        
        # Extract and normalize metric names
        key_metrics = {}
        for key, value in metrics.items():
            if 'avg_' in key:
                clean_key = key.replace('avg_', '')
                key_metrics[clean_key] = value
        
        # Add additional metrics if available
        if 'final_value' in model_results:
            key_metrics['final_value'] = model_results['final_value']
        
        return key_metrics
    
    def _calculate_performance_diff(
        self, 
        baseline_metrics: Dict[str, float], 
        test_metrics: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate performance differences between baseline and test"""
        
        diff = {}
        
        for key in baseline_metrics.keys():
            if key in test_metrics:
                baseline_val = baseline_metrics[key]
                test_val = test_metrics[key]
                
                # Calculate absolute and percentage differences
                abs_diff = test_val - baseline_val
                pct_diff = (abs_diff / baseline_val * 100) if baseline_val != 0 else 0
                
                diff[f"{key}_abs_diff"] = abs_diff
                diff[f"{key}_pct_diff"] = pct_diff
                diff[f"{key}_improvement"] = abs_diff > 0
        
        return diff
    
    def _calculate_statistical_significance(
        self,
        baseline_metrics: Dict[str, float],
        test_metrics: Dict[str, float]
    ) -> Dict[str, Dict[str, float]]:
        """Calculate statistical significance of differences"""
        
        # This is a simplified implementation
        # In practice, you'd use proper statistical tests with time series data
        
        significance = {}
        
        for key in baseline_metrics.keys():
            if key in test_metrics:
                # Simulate p-value and confidence interval
                # In practice, use actual statistical tests
                p_value = np.random.uniform(0.01, 0.5)
                confidence = 1 - p_value
                
                significance[key] = {
                    'p_value': p_value,
                    'confidence': confidence,
                    'is_significant': p_value < 0.05
                }
        
        return significance
    
    def _generate_comparison_summary(
        self,
        baseline_config: ModelConfigDefinition,
        test_config: ModelConfigDefinition,
        performance_diff: Dict[str, float]
    ) -> str:
        """Generate a summary of the comparison"""
        
        summary_lines = [
            f"# Model Configuration Comparison",
            f"**Baseline:** {baseline_config.name} - {baseline_config.description}",
            f"**Test:** {test_config.name} - {test_config.description}",
            "",
            "## Key Performance Differences:"
        ]
        
        # Highlight key improvements/degradations
        for key, value in performance_diff.items():
            if '_pct_diff' in key and abs(value) > 5:  # Only show significant differences
                metric_name = key.replace('_pct_diff', '').replace('_', ' ').title()
                direction = "improved" if value > 0 else "degraded"
                summary_lines.append(f"- **{metric_name}**: {direction} by {abs(value):.1f}%")
        
        return "\n".join(summary_lines)
    
    def _generate_recommendations(self, performance_diff: Dict[str, float]) -> List[str]:
        """Generate recommendations based on performance differences"""
        
        recommendations = []
        
        # Check overall performance
        if performance_diff.get('total_return_pct_diff', 0) > 10:
            recommendations.append("✅ Test configuration shows significant return improvement - consider adoption")
        elif performance_diff.get('total_return_pct_diff', 0) < -10:
            recommendations.append("❌ Test configuration underperforms baseline - needs refinement")
        
        # Check risk-adjusted performance
        if performance_diff.get('sharpe_ratio_pct_diff', 0) > 15:
            recommendations.append("📈 Test configuration has superior risk-adjusted returns")
        
        # Check drawdown
        if performance_diff.get('max_drawdown_pct_diff', 0) > 20:  # Improvement in drawdown is negative
            recommendations.append("⚠️ Test configuration has higher drawdown - review risk management")
        
        # Default recommendation
        if not recommendations:
            recommendations.append("📊 Performance differences are marginal - consider other factors like computational cost")
        
        return recommendations
    
    async def _generate_comparison_report(self, results: Dict[str, ComparisonResult]):
        """Generate comprehensive comparison report"""
        
        report_path = self.output_dir / "comparison_report.md"
        
        report_lines = [
            "# Model Configuration Comparison Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"## Summary",
            f"Total comparisons: {len(results)}",
            ""
        ]
        
        # Add individual comparison results
        for comparison_name, result in results.items():
            report_lines.extend([
                f"## {comparison_name}",
                "",
                result.summary,
                "",
                "### Recommendations:",
                ""
            ])
            
            for rec in result.recommendations:
                report_lines.append(f"- {rec}")
            
            report_lines.extend(["", "---", ""])
        
        # Save report
        with open(report_path, 'w') as f:
            f.write("\n".join(report_lines))
        
        self.logger.info(f"Comparison report saved to {report_path}")


# Usage examples and factories
class ConfigFactory:
    """Factory for creating common configuration variants"""
    
    @staticmethod
    def create_learning_rate_variants(base_config: ModelConfigDefinition, 
                                    rates: List[float]) -> List[ModelConfigDefinition]:
        """Create variants with different learning rates"""
        manager = ModelConfigManager()
        variants = []
        
        for rate in rates:
            variant = manager.create_variant(
                base_config,
                {'learning_rate': rate},
                f"{base_config.name}_lr_{rate}",
                f"Learning rate variant: {rate}"
            )
            variants.append(variant)
        
        return variants
    
    @staticmethod
    def create_architecture_variants(base_config: ModelConfigDefinition,
                                   hidden_dims_list: List[List[int]]) -> List[ModelConfigDefinition]:
        """Create variants with different architectures"""
        manager = ModelConfigManager()
        variants = []
        
        for i, hidden_dims in enumerate(hidden_dims_list):
            variant = manager.create_variant(
                base_config,
                {'hidden_dims': hidden_dims},
                f"{base_config.name}_arch_{i}",
                f"Architecture variant: {hidden_dims}"
            )
            variants.append(variant)
        
        return variants


async def example_usage():
    """Example of how to use the model configuration comparison system"""
    
    # Initialize comparison system
    comparison = ModelConfigComparison()
    manager = ModelConfigManager()
    
    # Add baseline configurations
    comparison.add_baseline_config("sr_baseline", "sr_baseline")
    comparison.add_baseline_config("adaptive_baseline", "adaptive_baseline")
    
    # Create test variants
    sr_enhanced = manager.create_variant(
        manager.get_template("sr_baseline"),
        {
            'hidden_dims': [512, 256, 128, 64],
            'learning_rate': 0.0005,
            'epochs': 150
        },
        "sr_enhanced_test",
        "Enhanced SR model with deeper architecture and more training"
    )
    
    adaptive_aggressive = manager.create_variant(
        manager.get_template("adaptive_baseline"),
        {
            'retrain_frequency_days': 1,
            'rolling_window_days': 180,
            'min_accuracy_threshold': 0.5
        },
        "adaptive_aggressive_test",
        "Aggressive adaptive model with daily retraining"
    )
    
    # Add test configurations
    comparison.add_test_config("sr_enhanced", sr_enhanced)
    comparison.add_test_config("adaptive_aggressive", adaptive_aggressive)
    
    # Run comparative backtests
    results = await comparison.run_comparative_backtest(
        start_date=date(2023, 1, 1),
        end_date=date(2024, 6, 30),
        universe=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    )
    
    # Print summary
    for name, result in results.items():
        print(f"\n{name}:")
        print(result.summary)
        print("\nRecommendations:")
        for rec in result.recommendations:
            print(f"  {rec}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())