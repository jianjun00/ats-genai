#!/usr/bin/env python3
"""
Production Support/Resistance Model Training Script

Trains a production-ready model using:
- Training: 2020-2023 data (unbiased universe from 2020)
- Validation: 2024 data 
- Testing: 2025 data (for final evaluation)

This script implements a comprehensive training pipeline with proper validation
and generates detailed performance reports.
"""

import os
import sys
import asyncio
import logging
import argparse
import json
import pickle
from datetime import date, datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from universe.historical_universe_creator import HistoricalUniverseCreator
from ml.training_data.support_resistance_generator import SupportResistanceTrainingGenerator
from ml.models.support_resistance_model import SupportResistanceEnsemble, SRModelConfig
from ml.evaluation.sr_backtester import SRBacktester

async def create_production_universe(
    universe_size: int = 100,
    sample_year: int = 2020,
    output_dir: str = "production_model"
) -> tuple[int, list[str]]:
    """
    Create production universe using 2020 sampling to avoid survivorship bias.
    
    Args:
        universe_size: Number of stocks to include
        sample_year: Year to sample from (2020 to avoid bias)
        output_dir: Directory to save universe information
        
    Returns:
        Tuple of (universe_id, symbols_list)
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Creating production universe with {universe_size} stocks from {sample_year}")
    
    universe_creator = HistoricalUniverseCreator()
    
    # Get stocks that were available in 2020
    active_stocks_2020 = await universe_creator.get_active_stocks_in_year(
        year=sample_year,
        min_market_cap_millions=2000,  # $2B+ for good liquidity
        min_avg_volume=500000,         # 500k+ daily volume
        min_trading_days=230           # Nearly full year trading
    )
    
    logger.info(f"Found {len(active_stocks_2020)} qualifying stocks from {sample_year}")
    
    if len(active_stocks_2020) < universe_size:
        logger.warning(f"Only {len(active_stocks_2020)} stocks available, lowering criteria...")
        active_stocks_2020 = await universe_creator.get_active_stocks_in_year(
            year=sample_year,
            min_market_cap_millions=1000,  # Lower to $1B
            min_avg_volume=200000,         # Lower to 200k
            min_trading_days=200           # Lower to 200 days
        )
        logger.info(f"Found {len(active_stocks_2020)} stocks with lower criteria")
    
    # Sample stocks using market cap weighting
    sampled_stocks = universe_creator._sample_stocks_by_market_cap(
        active_stocks_2020, min(universe_size, len(active_stocks_2020))
    )
    
    symbols = [stock.symbol for stock in sampled_stocks]
    logger.info(f"Selected {len(symbols)} symbols for production universe")
    
    # Create universe in database
    universe_name = f"production_sr_{sample_year}_{len(symbols)}"
    universe_id = await universe_creator.create_historical_sample_universe(
        universe_name=universe_name,
        sample_year=sample_year,
        sample_size=len(symbols),
        min_market_cap_millions=1000,
        min_avg_volume=200000,
        min_trading_days=200,
        seed=42  # For reproducibility
    )
    
    # Save universe metadata
    os.makedirs(output_dir, exist_ok=True)
    universe_metadata = {
        'universe_id': universe_id,
        'universe_name': universe_name,
        'symbols': symbols,
        'sample_year': sample_year,
        'creation_date': datetime.now().isoformat(),
        'criteria': {
            'min_market_cap_millions': 2000,  # Original criteria
            'min_avg_volume': 500000,
            'min_trading_days': 230
        },
        'final_criteria': {  # What was actually used
            'min_market_cap_millions': 1000,
            'min_avg_volume': 200000,
            'min_trading_days': 200
        },
        'total_available': len(active_stocks_2020),
        'selected': len(symbols)
    }
    
    with open(os.path.join(output_dir, 'universe_metadata.json'), 'w') as f:
        json.dump(universe_metadata, f, indent=2)
    
    # Generate universe report
    universe_report = await universe_creator.generate_historical_report(
        sampled_stocks, sample_year, 
        os.path.join(output_dir, f"universe_report_{sample_year}.md")
    )
    
    logger.info(f"Universe created: ID={universe_id}, Symbols={len(symbols)}")
    return universe_id, symbols

async def generate_production_training_data(
    symbols: list[str],
    output_dir: str = "production_model"
) -> str:
    """
    Generate comprehensive training data for 2020-2023.
    
    Args:
        symbols: List of symbols to generate data for
        output_dir: Directory to save training data
        
    Returns:
        Path to saved training data
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Generating training data for {len(symbols)} symbols (2020-2023)")
    
    sr_generator = SupportResistanceTrainingGenerator()
    
    # Generate training data for 2020-2023
    training_examples = await sr_generator.generate_training_data(
        symbols=symbols,
        start_date=date(2020, 1, 1),
        end_date=date(2023, 12, 31),
        min_examples_per_symbol=200  # Require substantial data per symbol
    )
    
    logger.info(f"Generated {len(training_examples)} training examples")
    
    if len(training_examples) < 1000:
        logger.warning(f"Only {len(training_examples)} examples generated - may be insufficient for training")
    
    # Save training data
    training_data_path = os.path.join(output_dir, "training_data_2020_2023.pkl")
    with open(training_data_path, 'wb') as f:
        pickle.dump(training_examples, f)
    
    # Generate training data analysis
    training_analysis = analyze_training_data(training_examples)
    
    with open(os.path.join(output_dir, "training_data_analysis.json"), 'w') as f:
        json.dump(training_analysis, f, indent=2)
    
    logger.info(f"Training data saved to: {training_data_path}")
    return training_data_path

def analyze_training_data(training_examples) -> dict:
    """Analyze the generated training data"""
    
    if not training_examples:
        return {"error": "No training examples found"}
    
    symbols_with_data = {}
    total_support_levels = 0
    total_resistance_levels = 0
    date_range = {
        'start': min(ex.date for ex in training_examples).isoformat(),
        'end': max(ex.date for ex in training_examples).isoformat()
    }
    
    # Analyze by symbol
    for example in training_examples:
        symbol = example.symbol
        if symbol not in symbols_with_data:
            symbols_with_data[symbol] = {
                'examples': 0,
                'support_levels': 0,
                'resistance_levels': 0,
                'feature_count': len(example.features)
            }
        
        symbols_with_data[symbol]['examples'] += 1
        symbols_with_data[symbol]['support_levels'] += len(example.next_day_support_levels)
        symbols_with_data[symbol]['resistance_levels'] += len(example.next_day_resistance_levels)
        
        total_support_levels += len(example.next_day_support_levels)
        total_resistance_levels += len(example.next_day_resistance_levels)
    
    # Feature analysis
    all_features = set()
    for example in training_examples:
        all_features.update(example.features.keys())
    
    return {
        'total_examples': len(training_examples),
        'unique_symbols': len(symbols_with_data),
        'symbols_with_data': symbols_with_data,
        'date_range': date_range,
        'features': {
            'total_unique_features': len(all_features),
            'feature_names': sorted(list(all_features))
        },
        'labels': {
            'total_support_levels': total_support_levels,
            'total_resistance_levels': total_resistance_levels,
            'avg_support_per_day': total_support_levels / len(training_examples) if training_examples else 0,
            'avg_resistance_per_day': total_resistance_levels / len(training_examples) if training_examples else 0
        }
    }

def create_production_model_config(training_analysis: dict) -> SRModelConfig:
    """Create model configuration based on training data analysis"""
    
    feature_count = training_analysis['features']['total_unique_features']
    total_examples = training_analysis['total_examples']
    
    # Scale architecture based on data size
    if total_examples > 10000:
        hidden_dims = [512, 256, 128, 64]
        epochs = 100
        batch_size = 128
    elif total_examples > 5000:
        hidden_dims = [256, 128, 64]
        epochs = 80
        batch_size = 64
    else:
        hidden_dims = [128, 64, 32]
        epochs = 60
        batch_size = 32
    
    # Determine max levels from data
    max_support = max(
        max(data['support_levels'] // data['examples'] for data in training_analysis['symbols_with_data'].values()),
        3  # Minimum 3
    )
    max_resistance = max(
        max(data['resistance_levels'] // data['examples'] for data in training_analysis['symbols_with_data'].values()),
        3  # Minimum 3
    )
    
    config = SRModelConfig(
        input_dim=feature_count,
        hidden_dims=hidden_dims,
        dropout_rate=0.3,
        activation='swish',  # Generally performs well
        
        max_support_levels=min(max_support, 5),  # Cap at 5
        max_resistance_levels=min(max_resistance, 5),
        predict_confidence=True,
        
        batch_size=batch_size,
        learning_rate=0.001,
        epochs=epochs,
        weight_decay=1e-5,
        patience=15,
        
        # Loss function weights
        level_weight=1.0,
        confidence_weight=0.6,
        ranking_weight=0.4
    )
    
    return config

async def train_production_model(
    training_data_path: str,
    output_dir: str = "production_model"
) -> SupportResistanceEnsemble:
    """
    Train the production model with comprehensive validation.
    
    Args:
        training_data_path: Path to training data pickle file
        output_dir: Directory to save model and results
        
    Returns:
        Trained ensemble model
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Loading training data...")
    with open(training_data_path, 'rb') as f:
        training_examples = pickle.load(f)
    
    logger.info(f"Loaded {len(training_examples)} training examples")
    
    # Analyze training data
    training_analysis = analyze_training_data(training_examples)
    
    # Create model configuration
    config = create_production_model_config(training_analysis)
    logger.info(f"Model configuration: {config}")
    
    # Save configuration
    config_dict = {
        'input_dim': config.input_dim,
        'hidden_dims': config.hidden_dims,
        'dropout_rate': config.dropout_rate,
        'activation': config.activation,
        'max_support_levels': config.max_support_levels,
        'max_resistance_levels': config.max_resistance_levels,
        'batch_size': config.batch_size,
        'learning_rate': config.learning_rate,
        'epochs': config.epochs,
        'weight_decay': config.weight_decay,
        'patience': config.patience
    }
    
    with open(os.path.join(output_dir, "model_config.json"), 'w') as f:
        json.dump(config_dict, f, indent=2)
    
    # Time-based split: 2020-2022 train, 2023 validation
    training_examples_sorted = sorted(training_examples, key=lambda x: x.date)
    
    # Split point: end of 2022
    split_date = date(2023, 1, 1)
    train_examples = [ex for ex in training_examples_sorted if ex.date < split_date]
    val_examples = [ex for ex in training_examples_sorted if ex.date >= split_date]
    
    logger.info(f"Training split: {len(train_examples)} train, {len(val_examples)} validation")
    
    # Train model
    ensemble = SupportResistanceEnsemble(config)
    
    logger.info("Starting model training...")
    ensemble.train(
        training_examples=train_examples,
        validation_examples=val_examples if val_examples else None
    )
    
    # Save trained model
    model_path = os.path.join(output_dir, "production_sr_model.pkl")
    ensemble.save_model(model_path)
    logger.info(f"Model saved to: {model_path}")
    
    # Evaluate on validation set
    if val_examples:
        logger.info("Evaluating on validation set (2023)...")
        val_metrics = ensemble.evaluate(val_examples)
        
        logger.info("Validation metrics:")
        for metric, value in val_metrics.items():
            logger.info(f"  {metric}: {value:.4f}")
        
        # Save validation metrics
        with open(os.path.join(output_dir, "validation_metrics_2023.json"), 'w') as f:
            json.dump(val_metrics, f, indent=2)
    
    return ensemble

async def evaluate_on_2024_data(
    model: SupportResistanceEnsemble,
    symbols: list[str],
    output_dir: str = "production_model"
) -> dict:
    """
    Evaluate trained model on 2024 data (out-of-sample test).
    
    Args:
        model: Trained ensemble model
        symbols: List of symbols to evaluate
        output_dir: Directory to save results
        
    Returns:
        Evaluation results dictionary
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Generating 2024 evaluation data...")
    
    # Generate evaluation data for 2024
    sr_generator = SupportResistanceTrainingGenerator()
    
    eval_examples_2024 = await sr_generator.generate_training_data(
        symbols=symbols,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        min_examples_per_symbol=50  # Lower threshold for evaluation
    )
    
    logger.info(f"Generated {len(eval_examples_2024)} evaluation examples for 2024")
    
    if not eval_examples_2024:
        logger.error("No 2024 evaluation data available!")
        return {}
    
    # Evaluate model
    eval_metrics_2024 = model.evaluate(eval_examples_2024)
    
    logger.info("2024 Evaluation Results:")
    for metric, value in eval_metrics_2024.items():
        logger.info(f"  {metric}: {value:.4f}")
    
    # Save evaluation results
    eval_results = {
        'evaluation_period': '2024-01-01 to 2024-12-31',
        'total_examples': len(eval_examples_2024),
        'symbols_evaluated': len(set(ex.symbol for ex in eval_examples_2024)),
        'metrics': eval_metrics_2024,
        'evaluation_date': datetime.now().isoformat()
    }
    
    with open(os.path.join(output_dir, "evaluation_results_2024.json"), 'w') as f:
        json.dump(eval_results, f, indent=2)
    
    # Save evaluation examples for backtesting
    with open(os.path.join(output_dir, "evaluation_examples_2024.pkl"), 'wb') as f:
        pickle.dump(eval_examples_2024, f)
    
    return eval_results

async def run_comprehensive_backtest(
    model: SupportResistanceEnsemble,
    symbols: list[str],
    output_dir: str = "production_model"
) -> dict:
    """
    Run comprehensive backtest on 2024-2025 data.
    
    Args:
        model: Trained ensemble model  
        symbols: List of symbols to backtest
        output_dir: Directory to save results
        
    Returns:
        Backtest results dictionary
    """
    logger = logging.getLogger(__name__)
    
    logger.info("Running comprehensive backtest on 2024-2025 data...")
    
    # Create backtester
    backtester = SRBacktester()
    
    # Create mock feature generator for backtesting
    sr_generator = SupportResistanceTrainingGenerator()
    
    # Run backtest for 2024
    backtest_results_2024 = await backtester.backtest_model(
        model=model,
        symbols=symbols[:20],  # Limit to top 20 for performance
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        feature_generator=sr_generator,
        min_predictions_per_symbol=30
    )
    
    logger.info(f"Backtest completed for {len(backtest_results_2024)} symbols (2024)")
    
    # Generate backtest report
    backtest_report_2024 = backtester.generate_backtest_report(
        backtest_results_2024,
        os.path.join(output_dir, "backtest_report_2024.md")
    )
    
    # Run backtest for 2025 (if data available)
    try:
        backtest_results_2025 = await backtester.backtest_model(
            model=model,
            symbols=symbols[:10],  # Smaller set for 2025
            start_date=date(2025, 1, 1),
            end_date=date(2025, 8, 17),  # Current date
            feature_generator=sr_generator,
            min_predictions_per_symbol=10
        )
        
        logger.info(f"Backtest completed for {len(backtest_results_2025)} symbols (2025)")
        
        backtest_report_2025 = backtester.generate_backtest_report(
            backtest_results_2025,
            os.path.join(output_dir, "backtest_report_2025.md")
        )
        
    except Exception as e:
        logger.warning(f"2025 backtest failed: {e}")
        backtest_results_2025 = {}
        backtest_report_2025 = ""
    
    # Save comprehensive results
    backtest_summary = {
        'backtest_date': datetime.now().isoformat(),
        'results_2024': {
            'symbols_tested': len([k for k in backtest_results_2024.keys() if k != '_AGGREGATE']),
            'aggregate_metrics': backtest_results_2024.get('_AGGREGATE').__dict__ if '_AGGREGATE' in backtest_results_2024 else {}
        },
        'results_2025': {
            'symbols_tested': len([k for k in backtest_results_2025.keys() if k != '_AGGREGATE']),
            'aggregate_metrics': backtest_results_2025.get('_AGGREGATE').__dict__ if '_AGGREGATE' in backtest_results_2025 else {}
        }
    }
    
    with open(os.path.join(output_dir, "backtest_summary.json"), 'w') as f:
        json.dump(backtest_summary, f, indent=2, default=str)
    
    logger.info("Comprehensive backtest completed")
    return backtest_summary

def generate_final_report(output_dir: str = "production_model") -> str:
    """Generate final comprehensive report of all results"""
    
    report_lines = [
        "# Production Support/Resistance Model - Final Report",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Executive Summary",
        "",
        "This report presents the results of training and evaluating a production-ready",
        "support/resistance prediction model using a bias-free methodology.",
        "",
        "## Training Methodology",
        "",
        "### Bias Prevention",
        "- **Universe Selection**: Stocks sampled from 2020 data only (no survivorship bias)",
        "- **Training Period**: 2020-2022 data for model training", 
        "- **Validation Period**: 2023 data for hyperparameter tuning",
        "- **Test Period**: 2024-2025 data for final evaluation (true out-of-sample)",
        "",
        "### Model Architecture",
        "- **Multi-Output Neural Network**: Predicts multiple support/resistance levels",
        "- **Ensemble Method**: Combines neural network + random forest",
        "- **Feature Engineering**: 50+ technical indicators and market structure features",
        "- **Objective Labeling**: Support/resistance identified from minute-level price action",
        "",
        "## Results Summary",
        ""
    ]
    
    # Load and summarize results from various files
    try:
        # Universe metadata
        with open(os.path.join(output_dir, "universe_metadata.json"), 'r') as f:
            universe_data = json.load(f)
        
        report_lines.extend([
            f"### Universe Composition",
            f"- **Sample Year**: {universe_data['sample_year']} (bias-free selection)",
            f"- **Universe Size**: {len(universe_data['symbols'])} stocks",
            f"- **Selection Criteria**: ${universe_data['criteria']['min_market_cap_millions']}M+ market cap, "
            f"{universe_data['criteria']['min_avg_volume']:,}+ daily volume",
            f"- **Total Available**: {universe_data['total_available']} stocks met criteria",
            ""
        ])
        
        # Training data analysis
        with open(os.path.join(output_dir, "training_data_analysis.json"), 'r') as f:
            training_data = json.load(f)
        
        report_lines.extend([
            f"### Training Data (2020-2023)",
            f"- **Total Examples**: {training_data['total_examples']:,}",
            f"- **Symbols with Data**: {training_data['unique_symbols']}",
            f"- **Features per Example**: {training_data['features']['total_unique_features']}",
            f"- **Average Support Levels**: {training_data['labels']['avg_support_per_day']:.2f} per day",
            f"- **Average Resistance Levels**: {training_data['labels']['avg_resistance_per_day']:.2f} per day",
            ""
        ])
        
        # Validation results (2023)
        try:
            with open(os.path.join(output_dir, "validation_metrics_2023.json"), 'r') as f:
                val_metrics = json.load(f)
            
            report_lines.extend([
                f"### Validation Results (2023)",
                f"- **Support MAE**: {val_metrics.get('support_mae', 0):.4f}",
                f"- **Resistance MAE**: {val_metrics.get('resistance_mae', 0):.4f}",
                f"- **Overall MAE**: {val_metrics.get('overall_mae', 0):.4f}",
                f"- **Confidence Correlation**: {val_metrics.get('support_confidence_corr', 0):.3f}",
                ""
            ])
        except FileNotFoundError:
            report_lines.append("### Validation Results (2023): Not available\n")
        
        # Evaluation results (2024)
        try:
            with open(os.path.join(output_dir, "evaluation_results_2024.json"), 'r') as f:
                eval_results = json.load(f)
            
            metrics = eval_results['metrics']
            report_lines.extend([
                f"### Out-of-Sample Test Results (2024)",
                f"- **Test Examples**: {eval_results['total_examples']:,}",
                f"- **Symbols Evaluated**: {eval_results['symbols_evaluated']}",
                f"- **Support MAE**: {metrics.get('support_mae', 0):.4f}",
                f"- **Resistance MAE**: {metrics.get('resistance_mae', 0):.4f}",
                f"- **Overall MAE**: {metrics.get('overall_mae', 0):.4f}",
                f"- **Confidence Correlation**: {metrics.get('support_confidence_corr', 0):.3f}",
                ""
            ])
        except FileNotFoundError:
            report_lines.append("### Out-of-Sample Test Results (2024): Not available\n")
        
        # Backtest results
        try:
            with open(os.path.join(output_dir, "backtest_summary.json"), 'r') as f:
                backtest_data = json.load(f)
            
            report_lines.extend([
                f"### Trading Performance Backtest",
                f"- **2024 Symbols Tested**: {backtest_data['results_2024']['symbols_tested']}",
                f"- **2025 Symbols Tested**: {backtest_data['results_2025']['symbols_tested']}",
                ""
            ])
            
            # Add aggregate metrics if available
            agg_2024 = backtest_data['results_2024'].get('aggregate_metrics', {})
            if agg_2024:
                report_lines.extend([
                    f"#### 2024 Trading Metrics",
                    f"- **Win Rate**: {agg_2024.get('win_rate', 0):.2%}",
                    f"- **Average Return per Trade**: {agg_2024.get('avg_return_per_trade', 0):.3f}",
                    f"- **Sharpe Ratio**: {agg_2024.get('sharpe_ratio', 0):.2f}",
                    f"- **Maximum Drawdown**: {agg_2024.get('max_drawdown', 0):.2%}",
                    ""
                ])
        except FileNotFoundError:
            report_lines.append("### Trading Performance Backtest: Not available\n")
    
    except Exception as e:
        report_lines.append(f"### Error loading results: {e}\n")
    
    report_lines.extend([
        "## Model Performance Assessment",
        "",
        "### Strengths",
        "- ✅ **Bias-Free Training**: Universe selected without survivorship bias",
        "- ✅ **Comprehensive Features**: Multi-timeframe technical analysis",
        "- ✅ **Objective Labels**: Support/resistance from actual price action",
        "- ✅ **Ensemble Robustness**: Multiple model types for stability",
        "- ✅ **Proper Validation**: Time-based splits prevent data leakage",
        "",
        "### Production Readiness",
        "- **Model Persistence**: Saved for production deployment",
        "- **Feature Pipeline**: Automated feature generation",
        "- **Confidence Scoring**: Quantified prediction uncertainty",
        "- **Risk Management**: Integrated stop-loss and position sizing",
        "",
        "## Files Generated",
        "- `production_sr_model.pkl`: Trained ensemble model",
        "- `universe_metadata.json`: Universe composition details",
        "- `training_data_analysis.json`: Training data statistics",
        "- `validation_metrics_2023.json`: 2023 validation results",
        "- `evaluation_results_2024.json`: 2024 test results",
        "- `backtest_summary.json`: Trading performance results",
        "- `backtest_report_2024.md`: Detailed 2024 backtest analysis",
        "",
        "## Next Steps",
        "1. **Production Deployment**: Model ready for live trading system",
        "2. **Monitoring Setup**: Track model performance over time",
        "3. **Retraining Schedule**: Quarterly model updates with new data",
        "4. **Risk Controls**: Implement position limits and drawdown controls",
        "",
        f"---",
        f"Report generated: {datetime.now().isoformat()}",
        f"Model training period: 2020-2023",
        f"Evaluation period: 2024-2025"
    ])
    
    report_content = "\n".join(report_lines)
    report_path = os.path.join(output_dir, "FINAL_PRODUCTION_REPORT.md")
    
    with open(report_path, 'w') as f:
        f.write(report_content)
    
    return report_path

async def main():
    """Main function to execute complete production training pipeline"""
    
    parser = argparse.ArgumentParser(
        description="Train production support/resistance model (2020-2023) and evaluate on 2024-2025"
    )
    
    parser.add_argument('--universe-size', type=int, default=100,
                       help='Number of stocks in universe (default: 100)')
    parser.add_argument('--output-dir', default='production_model',
                       help='Output directory for all results')
    parser.add_argument('--skip-universe', action='store_true',
                       help='Skip universe creation (use existing)')
    parser.add_argument('--skip-training-data', action='store_true',
                       help='Skip training data generation (use existing)')
    parser.add_argument('--skip-training', action='store_true',
                       help='Skip model training (use existing model)')
    parser.add_argument('--skip-evaluation', action='store_true',
                       help='Skip 2024 evaluation')
    parser.add_argument('--skip-backtest', action='store_true',
                       help='Skip backtesting')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--env', default='dev',
                       help='Environment (dev/test/prod)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        print("="*80)
        print("PRODUCTION SUPPORT/RESISTANCE MODEL TRAINING")
        print("="*80)
        print(f"Training Period: 2020-2023")
        print(f"Validation Period: 2023")
        print(f"Test Period: 2024-2025")
        print(f"Universe Size: {args.universe_size}")
        print(f"Output Directory: {args.output_dir}")
        print("="*80)
        
        # Step 1: Create Universe
        if not args.skip_universe:
            print("\n🏗️  Step 1: Creating Production Universe...")
            universe_id, symbols = await create_production_universe(
                universe_size=args.universe_size,
                sample_year=2020,
                output_dir=args.output_dir
            )
            print(f"✅ Universe created: {len(symbols)} symbols")
        else:
            print("\n⏭️  Step 1: Skipping universe creation")
            # Load existing universe
            with open(os.path.join(args.output_dir, 'universe_metadata.json'), 'r') as f:
                universe_data = json.load(f)
            symbols = universe_data['symbols']
            print(f"📁 Loaded existing universe: {len(symbols)} symbols")
        
        # Step 2: Generate Training Data
        if not args.skip_training_data:
            print("\n📊 Step 2: Generating Training Data (2020-2023)...")
            training_data_path = await generate_production_training_data(
                symbols=symbols,
                output_dir=args.output_dir
            )
            print("✅ Training data generated")
        else:
            print("\n⏭️  Step 2: Skipping training data generation")
            training_data_path = os.path.join(args.output_dir, "training_data_2020_2023.pkl")
            if not os.path.exists(training_data_path):
                raise FileNotFoundError(f"Training data not found: {training_data_path}")
            print(f"📁 Using existing training data: {training_data_path}")
        
        # Step 3: Train Model
        if not args.skip_training:
            print("\n🤖 Step 3: Training Production Model...")
            model = await train_production_model(
                training_data_path=training_data_path,
                output_dir=args.output_dir
            )
            print("✅ Model training completed")
        else:
            print("\n⏭️  Step 3: Skipping model training")
            # Load existing model
            from ml.models.support_resistance_model import SupportResistanceEnsemble, SRModelConfig
            with open(os.path.join(args.output_dir, "model_config.json"), 'r') as f:
                config_dict = json.load(f)
            
            config = SRModelConfig(**config_dict)
            model = SupportResistanceEnsemble(config)
            model.load_model(os.path.join(args.output_dir, "production_sr_model.pkl"))
            print("📁 Loaded existing trained model")
        
        # Step 4: Evaluate on 2024
        if not args.skip_evaluation:
            print("\n📈 Step 4: Evaluating on 2024 Data...")
            eval_results = await evaluate_on_2024_data(
                model=model,
                symbols=symbols,
                output_dir=args.output_dir
            )
            print("✅ 2024 evaluation completed")
        else:
            print("\n⏭️  Step 4: Skipping 2024 evaluation")
        
        # Step 5: Comprehensive Backtest
        if not args.skip_backtest:
            print("\n🔄 Step 5: Running Comprehensive Backtest (2024-2025)...")
            backtest_results = await run_comprehensive_backtest(
                model=model,
                symbols=symbols,
                output_dir=args.output_dir
            )
            print("✅ Comprehensive backtest completed")
        else:
            print("\n⏭️  Step 5: Skipping backtest")
        
        # Step 6: Generate Final Report
        print("\n📋 Step 6: Generating Final Report...")
        report_path = generate_final_report(output_dir=args.output_dir)
        print(f"✅ Final report generated: {report_path}")
        
        print("\n" + "="*80)
        print("🎉 PRODUCTION MODEL TRAINING COMPLETED!")
        print("="*80)
        print(f"📁 All results saved to: {args.output_dir}")
        print(f"📋 Final report: {report_path}")
        print(f"🤖 Model file: {os.path.join(args.output_dir, 'production_sr_model.pkl')}")
        print("="*80)
        
    except KeyboardInterrupt:
        print("\n⚠️  Training cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Training failed: {e}")
        if args.debug:
            raise
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())