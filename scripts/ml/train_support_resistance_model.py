#!/usr/bin/env python3
"""
Train Support/Resistance Prediction Model

This script trains a multi-output neural network ensemble to predict next-day
support and resistance levels using unbiased training data.
"""

import os
import sys
import logging
import argparse
import pickle
import json
import pandas as pd
import numpy as np
from datetime import date, datetime
from pathlib import Path
from sklearn.model_selection import train_test_split
from typing import List, Dict, Tuple

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from ml.models.support_resistance_model import SupportResistanceEnsemble, SRModelConfig
from ml.training_data.support_resistance_generator import TrainingExample

def load_training_data(data_dir: str) -> List[TrainingExample]:
    """Load training data from pickle file"""
    logger = logging.getLogger(__name__)
    
    pickle_file = os.path.join(data_dir, "training_examples.pkl")
    
    if not os.path.exists(pickle_file):
        raise FileNotFoundError(f"Training data not found: {pickle_file}")
    
    logger.info(f"Loading training data from {pickle_file}")
    
    with open(pickle_file, 'rb') as f:
        training_examples = pickle.load(f)
    
    logger.info(f"Loaded {len(training_examples)} training examples")
    return training_examples

def create_time_based_split(training_examples: List[TrainingExample], 
                          train_ratio: float = 0.7,
                          val_ratio: float = 0.15) -> Tuple[List, List, List]:
    """
    Create time-based train/validation/test split to prevent look-ahead bias.
    
    Args:
        training_examples: List of training examples
        train_ratio: Proportion for training (default 0.7)
        val_ratio: Proportion for validation (default 0.15, rest goes to test)
    
    Returns:
        Tuple of (train_examples, val_examples, test_examples)
    """
    logger = logging.getLogger(__name__)
    
    # Sort by date to ensure time-based split
    sorted_examples = sorted(training_examples, key=lambda x: x.date)
    
    total_examples = len(sorted_examples)
    train_end = int(total_examples * train_ratio)
    val_end = int(total_examples * (train_ratio + val_ratio))
    
    train_examples = sorted_examples[:train_end]
    val_examples = sorted_examples[train_end:val_end]
    test_examples = sorted_examples[val_end:]
    
    logger.info(f"Time-based split:")
    logger.info(f"  Training: {len(train_examples)} examples "
               f"({min(ex.date for ex in train_examples)} to {max(ex.date for ex in train_examples)})")
    logger.info(f"  Validation: {len(val_examples)} examples "
               f"({min(ex.date for ex in val_examples) if val_examples else 'N/A'} to "
               f"{max(ex.date for ex in val_examples) if val_examples else 'N/A'})")
    logger.info(f"  Test: {len(test_examples)} examples "
               f"({min(ex.date for ex in test_examples) if test_examples else 'N/A'} to "
               f"{max(ex.date for ex in test_examples) if test_examples else 'N/A'})")
    
    return train_examples, val_examples, test_examples

def analyze_training_data(training_examples: List[TrainingExample]) -> Dict[str, any]:
    """Analyze training data to inform model configuration"""
    logger = logging.getLogger(__name__)
    
    logger.info("Analyzing training data characteristics...")
    
    # Feature analysis
    all_features = set()
    feature_completeness = {}
    
    for example in training_examples:
        all_features.update(example.features.keys())
    
    for feature in all_features:
        non_null_count = sum(1 for ex in training_examples if ex.features.get(feature) is not None)
        feature_completeness[feature] = non_null_count / len(training_examples)
    
    # Filter features with good completeness (>80%)
    good_features = [f for f, completeness in feature_completeness.items() if completeness > 0.8]
    
    # Support/resistance level statistics
    support_level_counts = [len(ex.next_day_support_levels) for ex in training_examples]
    resistance_level_counts = [len(ex.next_day_resistance_levels) for ex in training_examples]
    
    max_support_levels = max(support_level_counts) if support_level_counts else 0
    max_resistance_levels = max(resistance_level_counts) if resistance_level_counts else 0
    
    avg_support_levels = np.mean(support_level_counts) if support_level_counts else 0
    avg_resistance_levels = np.mean(resistance_level_counts) if resistance_level_counts else 0
    
    # Symbol distribution
    symbol_counts = {}
    for ex in training_examples:
        symbol_counts[ex.symbol] = symbol_counts.get(ex.symbol, 0) + 1
    
    analysis = {
        'total_examples': len(training_examples),
        'unique_symbols': len(symbol_counts),
        'total_features': len(all_features),
        'good_features': len(good_features),
        'feature_completeness_threshold': 0.8,
        'max_support_levels': max_support_levels,
        'max_resistance_levels': max_resistance_levels,
        'avg_support_levels': avg_support_levels,
        'avg_resistance_levels': avg_resistance_levels,
        'date_range': {
            'start': min(ex.date for ex in training_examples).isoformat(),
            'end': max(ex.date for ex in training_examples).isoformat()
        },
        'good_feature_names': good_features,
        'feature_completeness': feature_completeness
    }
    
    logger.info(f"Analysis results:")
    logger.info(f"  Total examples: {analysis['total_examples']:,}")
    logger.info(f"  Features: {analysis['good_features']}/{analysis['total_features']} with >80% completeness")
    logger.info(f"  Max S/R levels: {analysis['max_support_levels']}S, {analysis['max_resistance_levels']}R")
    logger.info(f"  Avg S/R levels: {analysis['avg_support_levels']:.2f}S, {analysis['avg_resistance_levels']:.2f}R")
    
    return analysis

def create_model_config(analysis: Dict[str, any], args) -> SRModelConfig:
    """Create model configuration based on data analysis and arguments"""
    
    config = SRModelConfig(
        # Architecture (from analysis)
        input_dim=analysis['good_features'],
        hidden_dims=[512, 256, 128, 64] if analysis['total_examples'] > 10000 else [256, 128, 64],
        dropout_rate=0.3,
        activation='swish',
        
        # Output configuration (from analysis)
        max_support_levels=min(analysis['max_support_levels'], 5),  # Cap at 5
        max_resistance_levels=min(analysis['max_resistance_levels'], 5),
        predict_confidence=True,
        
        # Training configuration (from args)
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        weight_decay=args.weight_decay,
        patience=args.patience,
        
        # Loss weights (can be tuned)
        level_weight=1.0,
        confidence_weight=0.5,
        ranking_weight=0.3
    )
    
    return config

def train_model(training_examples: List[TrainingExample], 
                config: SRModelConfig,
                output_dir: str,
                args) -> SupportResistanceEnsemble:
    """Train the support/resistance ensemble model"""
    logger = logging.getLogger(__name__)
    
    logger.info("Starting model training...")
    
    # Time-based split
    train_examples, val_examples, test_examples = create_time_based_split(
        training_examples, args.train_ratio, args.val_ratio
    )
    
    # Create and train model
    model = SupportResistanceEnsemble(config)
    
    # Train the model
    model.train(
        training_examples=train_examples,
        validation_examples=val_examples if val_examples else None
    )
    
    # Evaluate on test set
    if test_examples:
        logger.info("Evaluating on test set...")
        test_metrics = model.evaluate(test_examples)
        
        logger.info("Test set results:")
        for metric, value in test_metrics.items():
            logger.info(f"  {metric}: {value:.4f}")
        
        # Save test metrics
        with open(os.path.join(output_dir, "test_metrics.json"), 'w') as f:
            json.dump(test_metrics, f, indent=2)
    
    # Save model
    model_path = os.path.join(output_dir, "sr_ensemble_model.pkl")
    model.save_model(model_path)
    
    logger.info(f"Model saved to {model_path}")
    
    return model

def generate_training_report(training_examples: List[TrainingExample],
                           analysis: Dict[str, any],
                           config: SRModelConfig,
                           model: SupportResistanceEnsemble,
                           output_dir: str) -> str:
    """Generate comprehensive training report"""
    
    report_lines = [
        "# Support/Resistance Model Training Report",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Training Data Summary",
        f"- **Total Examples**: {analysis['total_examples']:,}",
        f"- **Date Range**: {analysis['date_range']['start']} to {analysis['date_range']['end']}",
        f"- **Unique Symbols**: {analysis['unique_symbols']}",
        f"- **Features Used**: {analysis['good_features']} (>{analysis['feature_completeness_threshold']*100:.0f}% complete)",
        "",
        "## Model Architecture",
        f"- **Model Type**: Multi-output Neural Network + Random Forest Ensemble",
        f"- **Input Dimension**: {config.input_dim}",
        f"- **Hidden Layers**: {config.hidden_dims}",
        f"- **Activation**: {config.activation}",
        f"- **Dropout Rate**: {config.dropout_rate}",
        f"- **Max Support Levels**: {config.max_support_levels}",
        f"- **Max Resistance Levels**: {config.max_resistance_levels}",
        "",
        "## Training Configuration",
        f"- **Batch Size**: {config.batch_size}",
        f"- **Learning Rate**: {config.learning_rate}",
        f"- **Epochs**: {config.epochs}",
        f"- **Weight Decay**: {config.weight_decay}",
        f"- **Early Stopping Patience**: {config.patience}",
        "",
        "## Loss Function Weights",
        f"- **Level Weight**: {config.level_weight}",
        f"- **Confidence Weight**: {config.confidence_weight}",
        f"- **Ranking Weight**: {config.ranking_weight}",
        "",
        "## Training History"
    ]
    
    # Add training history if available
    if hasattr(model, 'training_history') and model.training_history['train_loss']:
        history = model.training_history
        final_train_loss = history['train_loss'][-1]
        final_train_mae = history['train_mae'][-1]
        
        report_lines.extend([
            f"- **Final Training Loss**: {final_train_loss:.4f}",
            f"- **Final Training MAE**: {final_train_mae:.4f}",
        ])
        
        if history['val_loss']:
            final_val_loss = history['val_loss'][-1]
            final_val_mae = history['val_mae'][-1]
            best_val_loss = min(history['val_loss'])
            
            report_lines.extend([
                f"- **Final Validation Loss**: {final_val_loss:.4f}",
                f"- **Final Validation MAE**: {final_val_mae:.4f}",
                f"- **Best Validation Loss**: {best_val_loss:.4f}",
            ])
    
    # Add feature importance analysis
    report_lines.extend([
        "",
        "## Feature Categories",
        "| Category | Count | Examples |",
        "|----------|-------|----------|"
    ])
    
    # Group features by category
    feature_categories = {
        'Price Action': [],
        'Technical Indicators': [],
        'Volume': [],
        'Support/Resistance': [],
        'Market Structure': [],
        'Volatility': [],
        'Intraday': [],
        'Other': []
    }
    
    for feature in analysis['good_feature_names']:
        if any(x in feature for x in ['close', 'high', 'low', 'open', 'return', 'range']):
            feature_categories['Price Action'].append(feature)
        elif any(x in feature for x in ['ma_', 'rsi', 'bb_', 'macd']):
            feature_categories['Technical Indicators'].append(feature)
        elif 'volume' in feature:
            feature_categories['Volume'].append(feature)
        elif any(x in feature for x in ['support', 'resistance', 'distance']):
            feature_categories['Support/Resistance'].append(feature)
        elif any(x in feature for x in ['trend', 'higher', 'pivot']):
            feature_categories['Market Structure'].append(feature)
        elif any(x in feature for x in ['atr', 'volatility']):
            feature_categories['Volatility'].append(feature)
        elif any(x in feature for x in ['opening', 'morning', 'intraday']):
            feature_categories['Intraday'].append(feature)
        else:
            feature_categories['Other'].append(feature)
    
    for category, features in feature_categories.items():
        if features:
            examples = ', '.join(features[:3])
            if len(features) > 3:
                examples += f", +{len(features)-3} more"
            report_lines.append(f"| {category} | {len(features)} | {examples} |")
    
    report_lines.extend([
        "",
        "## Model Output",
        "The trained model predicts:",
        "1. **Support Levels**: Up to 3 price levels where stock may find support",
        "2. **Resistance Levels**: Up to 3 price levels where stock may face resistance", 
        "3. **Confidence Scores**: 0-1 probability that each level will hold",
        "4. **Level Rankings**: Importance ordering of predicted levels",
        "",
        "## Usage",
        "```python",
        "# Load trained model",
        "model = SupportResistanceEnsemble.load_model('sr_ensemble_model.pkl')",
        "",
        "# Make predictions",
        "predictions = model.predict(features)",
        "support_levels = predictions['support_levels']",
        "resistance_levels = predictions['resistance_levels']",
        "confidence = predictions['support_confidence']",
        "```",
        "",
        "## Files Generated",
        "- `sr_ensemble_model.pkl`: Trained ensemble model",
        "- `test_metrics.json`: Performance metrics on test set",
        "- `training_analysis.json`: Detailed data analysis",
        "- `model_config.json`: Model configuration used",
        "- `training_report.md`: This report",
        "",
        "## Next Steps",
        "1. **Backtesting**: Test model predictions on historical data",
        "2. **Paper Trading**: Validate with live market data",
        "3. **Model Monitoring**: Track performance drift over time",
        "4. **Feature Engineering**: Add new market regime indicators",
        "5. **Ensemble Improvement**: Experiment with additional model types"
    ])
    
    return "\n".join(report_lines)

def main():
    parser = argparse.ArgumentParser(
        description="Train support/resistance prediction model"
    )
    
    parser.add_argument('--data-dir', required=True,
                       help='Directory containing training data')
    parser.add_argument('--output-dir', default='trained_models',
                       help='Output directory for trained model')
    
    # Model configuration
    parser.add_argument('--batch-size', type=int, default=64,
                       help='Training batch size')
    parser.add_argument('--learning-rate', type=float, default=0.001,
                       help='Learning rate')
    parser.add_argument('--epochs', type=int, default=100,
                       help='Maximum training epochs')
    parser.add_argument('--weight-decay', type=float, default=1e-5,
                       help='Weight decay for regularization')
    parser.add_argument('--patience', type=int, default=15,
                       help='Early stopping patience')
    
    # Data split configuration
    parser.add_argument('--train-ratio', type=float, default=0.7,
                       help='Proportion of data for training')
    parser.add_argument('--val-ratio', type=float, default=0.15,
                       help='Proportion of data for validation')
    
    # Other options
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--no-gpu', action='store_true',
                       help='Force CPU training')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        # Load training data
        training_examples = load_training_data(args.data_dir)
        
        if len(training_examples) == 0:
            logger.error("No training examples found!")
            return
        
        # Analyze training data
        analysis = analyze_training_data(training_examples)
        
        # Save analysis
        with open(os.path.join(args.output_dir, "training_analysis.json"), 'w') as f:
            json.dump(analysis, f, indent=2)
        
        # Create model configuration
        config = create_model_config(analysis, args)
        
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
        
        with open(os.path.join(args.output_dir, "model_config.json"), 'w') as f:
            json.dump(config_dict, f, indent=2)
        
        logger.info(f"Model configuration: {config}")
        
        # Train model
        model = train_model(training_examples, config, args.output_dir, args)
        
        # Generate training report
        report = generate_training_report(training_examples, analysis, config, model, args.output_dir)
        
        report_path = os.path.join(args.output_dir, "training_report.md")
        with open(report_path, 'w') as f:
            f.write(report)
        
        logger.info(f"Training report saved to {report_path}")
        
        print("\n" + "="*60)
        print("MODEL TRAINING COMPLETED!")
        print("="*60)
        print(f"Output directory: {args.output_dir}")
        print(f"Training examples: {len(training_examples):,}")
        print(f"Model features: {config.input_dim}")
        print(f"Max S/R levels: {config.max_support_levels}S, {config.max_resistance_levels}R")
        print("="*60)
        print("\n✓ Ready for backtesting and deployment!")
        
    except KeyboardInterrupt:
        print("\nTraining cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Training failed: {e}")
        if args.debug:
            raise
        sys.exit(1)

if __name__ == "__main__":
    main()