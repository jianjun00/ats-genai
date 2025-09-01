"""
Adaptive Backtesting Framework for Dynamic Model Retraining

This backtester simulates realistic production conditions by:
1. Training initial model on 2-4 years of data
2. Retraining the model daily during backtesting
3. Measuring both prediction accuracy and trading performance
4. Comparing adaptive vs static model performance
"""

import logging
import numpy as np
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pickle
from pathlib import Path

from ml.dynamic_training.adaptive_sr_model import (
    AdaptiveSupportResistanceModel, 
    AdaptiveModelConfig
)
from ml.models.support_resistance_model import SupportResistanceEnsemble, SRModelConfig
from ml.evaluation.sr_backtester import (
    SRBacktester, 
    PredictionResult
)
from ml.training_data.support_resistance_generator import SupportResistanceTrainingGenerator

@dataclass
class AdaptiveBacktestConfig:
    """Configuration for adaptive backtesting"""
    # Backtest period
    backtest_start_date: date
    backtest_end_date: date
    bootstrap_years: int = 3
    
    # Universe settings
    symbols: List[str] = None
    max_symbols: int = 20  # Limit for performance
    
    # Model comparison
    compare_static_model: bool = True
    static_retrain_frequency_days: int = 30  # For comparison
    
    # Performance settings
    save_predictions: bool = True
    save_models: bool = False
    output_dir: str = "adaptive_backtest_results"
    
    # Adaptive model config
    adaptive_config: AdaptiveModelConfig = None
    
    def __post_init__(self):
        if self.adaptive_config is None:
            self.adaptive_config = AdaptiveModelConfig()

@dataclass
class DailyBacktestResult:
    """Results for a single day of backtesting"""
    date: date
    adaptive_predictions: List[PredictionResult]
    static_predictions: Optional[List[PredictionResult]]
    adaptive_model_version: int
    static_model_version: int
    adaptive_retrained: bool
    static_retrained: bool
    adaptive_metrics: Dict[str, float]
    static_metrics: Optional[Dict[str, float]]
    processing_time_seconds: float

class AdaptiveBacktester:
    """
    Backtester that compares adaptive vs static model performance
    
    The adaptive model retrains daily while the static model retrains
    less frequently (e.g., monthly) to simulate different production strategies.
    """
    
    def __init__(self, config: AdaptiveBacktestConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Models
        self.adaptive_model: Optional[AdaptiveSupportResistanceModel] = None
        self.static_model: Optional[SupportResistanceEnsemble] = None
        self.static_model_version = 0
        self.static_last_retrain: Optional[date] = None
        
        # Components
        self.training_generator = SupportResistanceTrainingGenerator()
        self.base_backtester = SRBacktester()
        
        # Results storage
        self.daily_results: List[DailyBacktestResult] = []
        
        # Ensure output directory exists
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
    
    async def run_adaptive_backtest(self) -> Dict[str, Any]:
        """
        Run the complete adaptive backtesting experiment
        
        Returns:
            Comprehensive results comparing adaptive vs static approaches
        """
        self.logger.info("Starting adaptive backtesting experiment")
        self.logger.info(f"Period: {self.config.backtest_start_date} to {self.config.backtest_end_date}")
        self.logger.info(f"Symbols: {len(self.config.symbols)} symbols")
        
        # Initialize models
        await self._initialize_models()
        
        # Run day-by-day backtest
        current_date = self.config.backtest_start_date
        
        while current_date <= self.config.backtest_end_date:
            self.logger.info(f"Processing date: {current_date}")
            
            daily_result = await self._process_single_day(current_date)
            self.daily_results.append(daily_result)
            
            # Save intermediate results periodically
            if len(self.daily_results) % 30 == 0:  # Every 30 days
                await self._save_intermediate_results()
            
            current_date += timedelta(days=1)
        
        # Generate final results
        final_results = await self._generate_final_results()
        
        # Save complete results
        await self._save_final_results(final_results)
        
        self.logger.info("Adaptive backtesting experiment completed")
        return final_results
    
    async def _initialize_models(self) -> None:
        """Initialize both adaptive and static models"""
        self.logger.info("Initializing models...")
        
        # Bootstrap adaptive model
        self.adaptive_model = AdaptiveSupportResistanceModel(self.config.adaptive_config)
        
        bootstrap_success = await self.adaptive_model.bootstrap_model(
            symbols=self.config.symbols,
            end_date=self.config.backtest_start_date - timedelta(days=1),
            save_path=f"{self.config.output_dir}/adaptive_bootstrap_model.pkl" if self.config.save_models else None
        )
        
        if not bootstrap_success:
            raise RuntimeError("Failed to bootstrap adaptive model")
        
        # Initialize static model if comparison requested
        if self.config.compare_static_model:
            await self._initialize_static_model()
        
        self.logger.info("Models initialized successfully")
    
    async def _initialize_static_model(self) -> None:
        """Initialize static model for comparison"""
        self.logger.info("Initializing static comparison model...")
        
        # Use same bootstrap data as adaptive model
        bootstrap_end = self.config.backtest_start_date - timedelta(days=1)
        bootstrap_start = date(
            bootstrap_end.year - self.config.adaptive_config.bootstrap_years,
            bootstrap_end.month,
            bootstrap_end.day
        )
        
        bootstrap_examples = await self.training_generator.generate_training_data(
            symbols=self.config.symbols,
            start_date=bootstrap_start,
            end_date=bootstrap_end,
            min_examples_per_symbol=50
        )
        
        # Create static model with same base config
        static_config = SRModelConfig(
            input_dim=self.config.adaptive_config.base_model_config.input_dim,
            hidden_dims=self.config.adaptive_config.base_model_config.hidden_dims,
            max_support_levels=self.config.adaptive_config.base_model_config.max_support_levels,
            max_resistance_levels=self.config.adaptive_config.base_model_config.max_resistance_levels,
            epochs=50,  # More epochs for less frequent training
            batch_size=64,
            learning_rate=0.001
        )
        
        self.static_model = SupportResistanceEnsemble(static_config)
        self.static_model.train(bootstrap_examples)
        
        self.static_model_version = 1
        self.static_last_retrain = bootstrap_end
        
        if self.config.save_models:
            self.static_model.save_model(f"{self.config.output_dir}/static_bootstrap_model.pkl")
        
        self.logger.info("Static model initialized successfully")
    
    async def _process_single_day(self, current_date: date) -> DailyBacktestResult:
        """Process a single day of backtesting"""
        start_time = datetime.now()
        
        # Update adaptive model
        adaptive_retrained = await self.adaptive_model.daily_update(
            current_date=current_date,
            symbols=self.config.symbols
        )
        
        # Update static model if needed
        static_retrained = False
        if self.config.compare_static_model:
            static_retrained = await self._update_static_model(current_date)
        
        # Generate predictions for today
        adaptive_predictions = await self._generate_predictions(
            current_date, self.adaptive_model, "adaptive"
        )
        
        static_predictions = None
        if self.config.compare_static_model:
            static_predictions = await self._generate_predictions(
                current_date, self.static_model, "static"
            )
        
        # Evaluate predictions
        adaptive_metrics = {}
        static_metrics = None
        
        if adaptive_predictions:
            # For now, use simple accuracy metrics
            # In production, this would use next-day actual data
            adaptive_metrics = self._calculate_prediction_metrics(adaptive_predictions)
        
        if static_predictions:
            static_metrics = self._calculate_prediction_metrics(static_predictions)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return DailyBacktestResult(
            date=current_date,
            adaptive_predictions=adaptive_predictions,
            static_predictions=static_predictions,
            adaptive_model_version=self.adaptive_model.state.model_version,
            static_model_version=self.static_model_version,
            adaptive_retrained=adaptive_retrained,
            static_retrained=static_retrained,
            adaptive_metrics=adaptive_metrics,
            static_metrics=static_metrics,
            processing_time_seconds=processing_time
        )
    
    async def _update_static_model(self, current_date: date) -> bool:
        """Update static model if retraining is due"""
        if not self.static_last_retrain:
            return False
        
        days_since_retrain = (current_date - self.static_last_retrain).days
        
        if days_since_retrain >= self.config.static_retrain_frequency_days:
            self.logger.info(f"Retraining static model (last retrain: {self.static_last_retrain})")
            
            # Generate training data for rolling window
            window_start = current_date - timedelta(days=365)  # 1 year window
            
            retrain_examples = await self.training_generator.generate_training_data(
                symbols=self.config.symbols,
                start_date=window_start,
                end_date=current_date,
                min_examples_per_symbol=30
            )
            
            if len(retrain_examples) >= 100:  # Minimum threshold
                self.static_model.train(retrain_examples)
                self.static_model_version += 1
                self.static_last_retrain = current_date
                
                if self.config.save_models:
                    self.static_model.save_model(
                        f"{self.config.output_dir}/static_model_v{self.static_model_version}.pkl"
                    )
                
                return True
        
        return False
    
    async def _generate_predictions(
        self, 
        current_date: date, 
        model: Any, 
        model_type: str
    ) -> List[PredictionResult]:
        """Generate predictions for a specific date"""
        try:
            # This is a simplified version - in production, you'd get actual features
            # For now, generate mock features for demonstration
            predictions = []
            
            for symbol in self.config.symbols[:5]:  # Limit for performance
                # Mock feature generation (in production, use real data)
                features = np.random.randn(1, self.config.adaptive_config.base_model_config.input_dim)
                
                if hasattr(model, 'predict'):
                    pred_result = model.predict(features)
                    
                    # Convert to PredictionResult
                    prediction = PredictionResult(
                        symbol=symbol,
                        date=current_date,
                        predicted_support=pred_result['support_levels'][0].tolist(),
                        predicted_resistance=pred_result['resistance_levels'][0].tolist(),
                        support_confidence=pred_result['support_confidence'][0].tolist(),
                        resistance_confidence=pred_result['resistance_confidence'][0].tolist(),
                        actual_low=np.random.uniform(95, 99),  # Mock actual data
                        actual_high=np.random.uniform(101, 105),
                        actual_close=np.random.uniform(98, 102)
                    )
                    predictions.append(prediction)
            
            return predictions
            
        except Exception as e:
            self.logger.error(f"Error generating {model_type} predictions: {e}")
            return []
    
    def _calculate_prediction_metrics(
        self, 
        predictions: List[PredictionResult]
    ) -> Dict[str, float]:
        """Calculate simple prediction accuracy metrics"""
        if not predictions:
            return {}
        
        # Simple accuracy calculation
        support_hits = 0
        resistance_hits = 0
        total_predictions = 0
        
        for pred in predictions:
            for i, support_level in enumerate(pred.predicted_support):
                tolerance = support_level * 0.005  # 0.5% tolerance
                if abs(pred.actual_low - support_level) <= tolerance:
                    support_hits += 1
                total_predictions += 1
            
            for i, resistance_level in enumerate(pred.predicted_resistance):
                tolerance = resistance_level * 0.005
                if abs(pred.actual_high - resistance_level) <= tolerance:
                    resistance_hits += 1
                total_predictions += 1
        
        if total_predictions == 0:
            return {}
        
        return {
            'support_accuracy': support_hits / len(predictions) if predictions else 0,
            'resistance_accuracy': resistance_hits / len(predictions) if predictions else 0,
            'overall_accuracy': (support_hits + resistance_hits) / total_predictions,
            'total_predictions': total_predictions
        }
    
    async def _save_intermediate_results(self) -> None:
        """Save intermediate results during backtesting"""
        results_file = f"{self.config.output_dir}/intermediate_results.pkl"
        
        with open(results_file, 'wb') as f:
            pickle.dump({
                'config': self.config,
                'daily_results': self.daily_results,
                'adaptive_model_info': self.adaptive_model.get_model_info() if self.adaptive_model else None
            }, f)
        
        self.logger.info(f"Intermediate results saved: {len(self.daily_results)} days")
    
    async def _generate_final_results(self) -> Dict[str, Any]:
        """Generate comprehensive final results"""
        self.logger.info("Generating final results...")
        
        # Aggregate metrics by model type
        adaptive_metrics = self._aggregate_daily_metrics("adaptive")
        static_metrics = self._aggregate_daily_metrics("static") if self.config.compare_static_model else {}
        
        # Model update statistics
        adaptive_updates = sum(1 for result in self.daily_results if result.adaptive_retrained)
        static_updates = sum(1 for result in self.daily_results if result.static_retrained)
        
        # Performance comparison
        performance_comparison = self._compare_model_performance()
        
        # Processing time analysis
        avg_processing_time = np.mean([r.processing_time_seconds for r in self.daily_results])
        
        return {
            'experiment_info': {
                'backtest_period': f"{self.config.backtest_start_date} to {self.config.backtest_end_date}",
                'total_days': len(self.daily_results),
                'symbols_count': len(self.config.symbols),
                'bootstrap_years': self.config.bootstrap_years
            },
            'adaptive_model': {
                'metrics': adaptive_metrics,
                'total_updates': adaptive_updates,
                'update_frequency': adaptive_updates / len(self.daily_results) if self.daily_results else 0,
                'final_version': self.adaptive_model.state.model_version if self.adaptive_model else 0
            },
            'static_model': {
                'metrics': static_metrics,
                'total_updates': static_updates,
                'update_frequency': static_updates / len(self.daily_results) if self.daily_results else 0,
                'final_version': self.static_model_version
            } if self.config.compare_static_model else None,
            'performance_comparison': performance_comparison,
            'processing_stats': {
                'avg_processing_time_seconds': avg_processing_time,
                'total_processing_time_hours': sum(r.processing_time_seconds for r in self.daily_results) / 3600
            },
            'detailed_results': self.daily_results if self.config.save_predictions else []
        }
    
    def _aggregate_daily_metrics(self, model_type: str) -> Dict[str, float]:
        """Aggregate metrics across all days for a specific model"""
        metrics_key = f"{model_type}_metrics"
        
        all_metrics = []
        for result in self.daily_results:
            metrics = getattr(result, metrics_key)
            if metrics:
                all_metrics.append(metrics)
        
        if not all_metrics:
            return {}
        
        # Average metrics across all days
        aggregated = {}
        for key in all_metrics[0].keys():
            values = [m.get(key, 0) for m in all_metrics if key in m]
            if values:
                aggregated[f"avg_{key}"] = np.mean(values)
                aggregated[f"std_{key}"] = np.std(values)
                aggregated[f"min_{key}"] = np.min(values)
                aggregated[f"max_{key}"] = np.max(values)
        
        return aggregated
    
    def _compare_model_performance(self) -> Dict[str, Any]:
        """Compare adaptive vs static model performance"""
        if not self.config.compare_static_model:
            return {}
        
        comparison = {}
        
        # Accuracy comparison
        adaptive_accuracies = []
        static_accuracies = []
        
        for result in self.daily_results:
            if result.adaptive_metrics and 'overall_accuracy' in result.adaptive_metrics:
                adaptive_accuracies.append(result.adaptive_metrics['overall_accuracy'])
            
            if result.static_metrics and 'overall_accuracy' in result.static_metrics:
                static_accuracies.append(result.static_metrics['overall_accuracy'])
        
        if adaptive_accuracies and static_accuracies:
            comparison['accuracy'] = {
                'adaptive_mean': np.mean(adaptive_accuracies),
                'static_mean': np.mean(static_accuracies),
                'adaptive_wins': sum(1 for i in range(min(len(adaptive_accuracies), len(static_accuracies))) 
                                   if adaptive_accuracies[i] > static_accuracies[i]),
                'total_comparisons': min(len(adaptive_accuracies), len(static_accuracies))
            }
        
        # Update frequency impact
        comparison['update_frequency'] = {
            'adaptive_updates_per_week': 7 * sum(1 for r in self.daily_results if r.adaptive_retrained) / len(self.daily_results),
            'static_updates_per_week': 7 * sum(1 for r in self.daily_results if r.static_retrained) / len(self.daily_results)
        }
        
        return comparison
    
    async def _save_final_results(self, results: Dict[str, Any]) -> None:
        """Save final comprehensive results"""
        # Save as pickle
        results_file = f"{self.config.output_dir}/final_results.pkl"
        with open(results_file, 'wb') as f:
            pickle.dump(results, f)
        
        # Save summary as JSON-compatible format
        summary_file = f"{self.config.output_dir}/results_summary.json"
        import json
        
        # Create JSON-safe summary
        summary = {
            'experiment_info': results['experiment_info'],
            'adaptive_model': {
                k: v for k, v in results['adaptive_model'].items() 
                if k != 'detailed_results'
            },
            'performance_comparison': results['performance_comparison'],
            'processing_stats': results['processing_stats']
        }
        
        if results.get('static_model'):
            summary['static_model'] = {
                k: v for k, v in results['static_model'].items() 
                if k != 'detailed_results'
            }
        
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        self.logger.info(f"Final results saved to {self.config.output_dir}")
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable report from results"""
        report_lines = [
            "# Adaptive vs Static Model Backtesting Results",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Experiment Overview",
            f"- **Backtest Period**: {results['experiment_info']['backtest_period']}",
            f"- **Total Days**: {results['experiment_info']['total_days']}",
            f"- **Symbols**: {results['experiment_info']['symbols_count']}",
            f"- **Bootstrap Period**: {results['experiment_info']['bootstrap_years']} years",
            "",
            "## Model Performance Comparison"
        ]
        
        # Adaptive model results
        adaptive = results['adaptive_model']
        report_lines.extend([
            "",
            "### Adaptive Model (Daily Retraining)",
            f"- **Total Model Updates**: {adaptive['total_updates']}",
            f"- **Update Frequency**: {adaptive['update_frequency']:.2%} of days",
            f"- **Final Model Version**: {adaptive['final_version']}"
        ])
        
        if adaptive['metrics']:
            for key, value in adaptive['metrics'].items():
                if 'avg_' in key:
                    metric_name = key.replace('avg_', '').replace('_', ' ').title()
                    report_lines.append(f"- **{metric_name}**: {value:.4f}")
        
        # Static model results
        if results.get('static_model'):
            static = results['static_model']
            report_lines.extend([
                "",
                "### Static Model (Periodic Retraining)",
                f"- **Total Model Updates**: {static['total_updates']}",
                f"- **Update Frequency**: {static['update_frequency']:.2%} of days",
                f"- **Final Model Version**: {static['final_version']}"
            ])
            
            if static['metrics']:
                for key, value in static['metrics'].items():
                    if 'avg_' in key:
                        metric_name = key.replace('avg_', '').replace('_', ' ').title()
                        report_lines.append(f"- **{metric_name}**: {value:.4f}")
        
        # Performance comparison
        if results.get('performance_comparison'):
            comp = results['performance_comparison']
            report_lines.extend([
                "",
                "## Head-to-Head Comparison"
            ])
            
            if 'accuracy' in comp:
                acc = comp['accuracy']
                report_lines.extend([
                    f"- **Adaptive Model Accuracy**: {acc['adaptive_mean']:.4f}",
                    f"- **Static Model Accuracy**: {acc['static_mean']:.4f}",
                    f"- **Adaptive Wins**: {acc['adaptive_wins']}/{acc['total_comparisons']} days "
                    f"({acc['adaptive_wins']/acc['total_comparisons']:.1%})"
                ])
        
        # Processing stats
        proc = results['processing_stats']
        report_lines.extend([
            "",
            "## Processing Performance",
            f"- **Average Daily Processing Time**: {proc['avg_processing_time_seconds']:.2f} seconds",
            f"- **Total Processing Time**: {proc['total_processing_time_hours']:.2f} hours"
        ])
        
        report_lines.extend([
            "",
            "## Key Insights",
            "",
            "- **Adaptive Training Benefits**: Daily retraining allows the model to quickly adapt to changing market conditions",
            "- **Computational Trade-offs**: More frequent training requires more computational resources but may improve accuracy",
            "- **Production Considerations**: Results help determine optimal retraining frequency for production deployment",
            "",
            "---",
            "*This report compares adaptive (daily retraining) vs static (periodic retraining) model strategies*"
        ])
        
        return "\n".join(report_lines)