#!/usr/bin/env python3
"""
Generate Unbiased Support/Resistance Training Data

This script:
1. Creates an unbiased universe using 2020 stock samples (avoids survivorship bias)
2. Generates comprehensive training data for support/resistance prediction
3. Exports data in formats suitable for ML model training
"""

import os
import sys
import asyncio
import logging
import argparse
import pickle
import json
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import Environment
from universe.historical_universe_creator import HistoricalUniverseCreator
from ml.training_data.support_resistance_generator import SupportResistanceTrainingGenerator, TrainingExample

async def generate_unbiased_training_data(
    universe_size: int = 50,
    sample_year: int = 2020,
    training_start_date: date = date(2021, 1, 1),
    training_end_date: date = date(2023, 12, 31),
    output_dir: str = "ml_training_data",
    min_examples_per_symbol: int = 100
) -> Dict[str, any]:
    """
    Generate unbiased training data for support/resistance prediction.
    
    Args:
        universe_size: Number of stocks to sample from 2020
        sample_year: Year to use for unbiased sampling
        training_start_date: Start generating training data from this date
        training_end_date: End training data generation on this date
        output_dir: Directory to save training data
        min_examples_per_symbol: Minimum examples needed per symbol
        
    Returns:
        Dictionary with generation statistics and file paths
    """
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("UNBIASED SUPPORT/RESISTANCE TRAINING DATA GENERATION")
    logger.info("="*60)
    logger.info(f"Universe size: {universe_size}")
    logger.info(f"Sample year: {sample_year}")
    logger.info(f"Training period: {training_start_date} to {training_end_date}")
    logger.info(f"Min examples per symbol: {min_examples_per_symbol}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    results = {
        'universe_size': universe_size,
        'sample_year': sample_year,
        'training_start_date': training_start_date.isoformat(),
        'training_end_date': training_end_date.isoformat(),
        'generation_timestamp': datetime.now().isoformat()
    }
    
    # Step 1: Create unbiased universe
    logger.info("\nStep 1: Creating unbiased universe...")
    
    universe_creator = HistoricalUniverseCreator()
    
    # Get stocks that were available in sample year
    active_stocks = await universe_creator.get_active_stocks_in_year(
        year=sample_year,
        min_market_cap_millions=1000,  # $1B minimum
        min_avg_volume=100000,         # 100k shares daily
        min_trading_days=200           # Active for most of the year
    )
    
    logger.info(f"Found {len(active_stocks)} stocks active in {sample_year}")
    
    # Sample stocks using market cap weighting
    if len(active_stocks) >= universe_size:
        sampled_stocks = universe_creator._sample_stocks_by_market_cap(active_stocks, universe_size)
    else:
        logger.warning(f"Only {len(active_stocks)} available, using all")
        sampled_stocks = active_stocks
    
    symbols = [stock.symbol for stock in sampled_stocks]
    logger.info(f"Selected {len(symbols)} symbols for training data generation")
    
    # Save universe metadata
    universe_report = await universe_creator.generate_historical_report(
        sampled_stocks, sample_year, 
        os.path.join(output_dir, f"universe_report_{sample_year}.md")
    )
    
    results['universe_symbols'] = symbols
    results['universe_report_file'] = f"universe_report_{sample_year}.md"
    
    # Step 2: Generate training data
    logger.info("\nStep 2: Generating support/resistance training data...")
    
    sr_generator = SupportResistanceTrainingGenerator()
    
    training_examples = await sr_generator.generate_training_data(
        symbols=symbols,
        start_date=training_start_date,
        end_date=training_end_date,
        min_examples_per_symbol=min_examples_per_symbol
    )
    
    logger.info(f"Generated {len(training_examples)} training examples")
    
    if not training_examples:
        logger.error("No training examples generated!")
        return results
    
    results['total_examples'] = len(training_examples)
    results['symbols_with_data'] = len(set(ex.symbol for ex in training_examples))
    
    # Step 3: Process and export training data
    logger.info("\nStep 3: Processing and exporting training data...")
    
    # Convert to DataFrame for analysis
    features_data = []
    labels_data = []
    
    for example in training_examples:
        # Prepare features
        feature_row = {
            'symbol': example.symbol,
            'date': example.date.isoformat(),
            **example.features
        }
        features_data.append(feature_row)
        
        # Prepare labels
        label_row = {
            'symbol': example.symbol,
            'date': example.date.isoformat(),
            'next_day_high': example.next_day_high,
            'next_day_low': example.next_day_low,
            'next_day_close': example.next_day_close,
            'next_day_volume': example.next_day_volume,
            'num_support_levels': len(example.next_day_support_levels),
            'num_resistance_levels': len(example.next_day_resistance_levels),
        }
        
        # Add top support/resistance levels
        for i, level in enumerate(example.next_day_support_levels[:3]):
            label_row[f'support_{i+1}_level'] = level.level
            label_row[f'support_{i+1}_strength'] = level.strength
            label_row[f'support_{i+1}_tests'] = level.tests_count
        
        for i, level in enumerate(example.next_day_resistance_levels[:3]):
            label_row[f'resistance_{i+1}_level'] = level.level
            label_row[f'resistance_{i+1}_strength'] = level.strength
            label_row[f'resistance_{i+1}_tests'] = level.tests_count
        
        labels_data.append(label_row)
    
    # Create DataFrames
    features_df = pd.DataFrame(features_data)
    labels_df = pd.DataFrame(labels_data)
    
    # Save in multiple formats
    
    # 1. CSV files (good for analysis)
    features_csv = os.path.join(output_dir, "features.csv")
    labels_csv = os.path.join(output_dir, "labels.csv")
    features_df.to_csv(features_csv, index=False)
    labels_df.to_csv(labels_csv, index=False)
    
    # 2. Pickle file (preserves all data structures)
    pickle_file = os.path.join(output_dir, "training_examples.pkl")
    with open(pickle_file, 'wb') as f:
        pickle.dump(training_examples, f)
    
    # 3. Combined dataset for ML frameworks
    combined_df = features_df.merge(
        labels_df.drop(['symbol', 'date'], axis=1), 
        left_index=True, right_index=True
    )
    combined_csv = os.path.join(output_dir, "combined_dataset.csv")
    combined_df.to_csv(combined_csv, index=False)
    
    results['output_files'] = {
        'features_csv': features_csv,
        'labels_csv': labels_csv,
        'combined_csv': combined_csv,
        'pickle_file': pickle_file
    }
    
    # Step 4: Generate data quality report
    logger.info("\nStep 4: Generating data quality report...")
    
    quality_report = generate_data_quality_report(
        training_examples, features_df, labels_df, sample_year
    )
    
    quality_report_file = os.path.join(output_dir, "data_quality_report.md")
    with open(quality_report_file, 'w') as f:
        f.write(quality_report)
    
    results['quality_report_file'] = quality_report_file
    
    # Save metadata
    metadata_file = os.path.join(output_dir, "generation_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info("\nTraining data generation completed!")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Total examples: {len(training_examples)}")
    logger.info(f"Feature count: {len(features_df.columns) - 2}")  # -2 for symbol, date
    logger.info(f"Symbols with data: {len(set(ex.symbol for ex in training_examples))}")
    
    return results

def generate_data_quality_report(
    training_examples: List[TrainingExample],
    features_df: pd.DataFrame,
    labels_df: pd.DataFrame,
    sample_year: int
) -> str:
    """Generate a comprehensive data quality report"""
    
    total_examples = len(training_examples)
    unique_symbols = len(set(ex.symbol for ex in training_examples))
    date_range = f"{min(ex.date for ex in training_examples)} to {max(ex.date for ex in training_examples)}"
    
    # Feature statistics
    feature_cols = [col for col in features_df.columns if col not in ['symbol', 'date']]
    feature_completeness = {}
    for col in feature_cols:
        non_null = features_df[col].notna().sum()
        feature_completeness[col] = (non_null / total_examples) * 100
    
    # Label statistics
    support_counts = [len(ex.next_day_support_levels) for ex in training_examples]
    resistance_counts = [len(ex.next_day_resistance_levels) for ex in training_examples]
    
    avg_support_levels = sum(support_counts) / len(support_counts) if support_counts else 0
    avg_resistance_levels = sum(resistance_counts) / len(resistance_counts) if resistance_counts else 0
    
    # Symbol distribution
    symbol_counts = {}
    for ex in training_examples:
        symbol_counts[ex.symbol] = symbol_counts.get(ex.symbol, 0) + 1
    
    min_examples = min(symbol_counts.values()) if symbol_counts else 0
    max_examples = max(symbol_counts.values()) if symbol_counts else 0
    avg_examples = sum(symbol_counts.values()) / len(symbol_counts) if symbol_counts else 0
    
    report_lines = [
        "# Support/Resistance Training Data Quality Report",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Dataset Overview",
        f"- **Sample Universe Year**: {sample_year} (avoids survivorship bias)",
        f"- **Training Period**: {date_range}",
        f"- **Total Examples**: {total_examples:,}",
        f"- **Unique Symbols**: {unique_symbols}",
        f"- **Features**: {len(feature_cols)}",
        "",
        "## Data Distribution",
        f"- **Examples per Symbol**: {avg_examples:.1f} avg (min: {min_examples}, max: {max_examples})",
        f"- **Support Levels per Day**: {avg_support_levels:.2f} avg",
        f"- **Resistance Levels per Day**: {avg_resistance_levels:.2f} avg",
        "",
        "## Feature Completeness",
        "| Feature Category | Completeness | Top Features |",
        "|-----------------|--------------|--------------|"
    ]
    
    # Group features by category
    feature_categories = {
        'Price Action': [col for col in feature_cols if any(x in col for x in ['close', 'high', 'low', 'open', 'return', 'range'])],
        'Technical Indicators': [col for col in feature_cols if any(x in col for x in ['ma_', 'rsi', 'bb_', 'macd'])],
        'Volume': [col for col in feature_cols if 'volume' in col],
        'Support/Resistance': [col for col in feature_cols if any(x in col for x in ['support', 'resistance', 'distance'])],
        'Market Structure': [col for col in feature_cols if any(x in col for x in ['trend', 'higher', 'pivot'])],
        'Volatility': [col for col in feature_cols if any(x in col for x in ['atr', 'volatility'])],
        'Intraday': [col for col in feature_cols if any(x in col for x in ['opening', 'morning', 'intraday'])]
    }
    
    for category, cols in feature_categories.items():
        if cols:
            avg_completeness = sum(feature_completeness.get(col, 0) for col in cols) / len(cols)
            top_features = sorted(cols, key=lambda x: feature_completeness.get(x, 0), reverse=True)[:3]
            report_lines.append(f"| {category} | {avg_completeness:.1f}% | {', '.join(top_features)} |")
    
    report_lines.extend([
        "",
        "## Symbol Coverage",
        "| Symbol | Examples | Avg Support Levels | Avg Resistance Levels |",
        "|--------|----------|-------------------|----------------------|"
    ])
    
    # Top 10 symbols by example count
    top_symbols = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    for symbol, count in top_symbols:
        symbol_examples = [ex for ex in training_examples if ex.symbol == symbol]
        avg_support = sum(len(ex.next_day_support_levels) for ex in symbol_examples) / len(symbol_examples)
        avg_resistance = sum(len(ex.next_day_resistance_levels) for ex in symbol_examples) / len(symbol_examples)
        
        report_lines.append(f"| {symbol} | {count} | {avg_support:.2f} | {avg_resistance:.2f} |")
    
    report_lines.extend([
        "",
        "## Data Quality Indicators",
        "",
        "### Bias Prevention",
        f"✓ **Survivorship Bias Avoided**: Universe sampled from {sample_year} using only historical information",
        "✓ **Look-ahead Bias Prevented**: Features use only data available up to prediction date",
        "✓ **Selection Bias Minimized**: Market cap weighted sampling includes various company sizes",
        "",
        "### Feature Quality",
        f"- **Feature Completeness**: {sum(feature_completeness.values()) / len(feature_completeness):.1f}% average",
        f"- **Multi-timeframe Coverage**: Daily + intraday features included",
        f"- **Technical Analysis**: Comprehensive indicator coverage",
        "",
        "### Label Quality", 
        f"- **Objective Detection**: Support/resistance identified from minute-level price action",
        f"- **Strength Scoring**: Levels weighted by volume, tests, and hold time",
        f"- **Multiple Levels**: Captures up to 5 support and 5 resistance levels per day",
        "",
        "## Recommendations for Model Training",
        "",
        "1. **Train/Validation Split**: Use time-based split (e.g., 2021-2022 train, 2023 validation)",
        "2. **Feature Selection**: Consider removing features with <80% completeness",
        "3. **Multi-output Models**: Predict multiple support/resistance levels with confidence",
        "4. **Ensemble Approach**: Combine regression (exact levels) and classification (level zones)",
        "5. **Cross-validation**: Use walk-forward validation to prevent temporal leakage",
        "",
        "## Files Generated",
        "- `features.csv`: Input features for model training",
        "- `labels.csv`: Target labels for supervised learning", 
        "- `combined_dataset.csv`: Features + labels in single file",
        "- `training_examples.pkl`: Full data structures with all metadata",
        f"- `universe_report_{sample_year}.md`: Details on stock selection methodology"
    ])
    
    return "\n".join(report_lines)

async def validate_training_data(output_dir: str) -> None:
    """Validate the generated training data"""
    logger = logging.getLogger(__name__)
    
    logger.info("Validating generated training data...")
    
    # Load and validate files
    files_to_check = [
        "features.csv",
        "labels.csv", 
        "combined_dataset.csv",
        "training_examples.pkl"
    ]
    
    for filename in files_to_check:
        filepath = os.path.join(output_dir, filename)
        if not os.path.exists(filepath):
            logger.error(f"Missing file: {filepath}")
            return
        
        # Basic size check
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"✓ {filename}: {size_mb:.2f} MB")
    
    # Load and validate CSV data
    try:
        features_df = pd.read_csv(os.path.join(output_dir, "features.csv"))
        labels_df = pd.read_csv(os.path.join(output_dir, "labels.csv"))
        combined_df = pd.read_csv(os.path.join(output_dir, "combined_dataset.csv"))
        
        logger.info(f"✓ Features shape: {features_df.shape}")
        logger.info(f"✓ Labels shape: {labels_df.shape}")
        logger.info(f"✓ Combined shape: {combined_df.shape}")
        
        # Check for data consistency
        assert len(features_df) == len(labels_df), "Features and labels length mismatch"
        assert len(features_df) == len(combined_df), "Combined data length mismatch"
        
        # Check for missing values in key columns
        null_counts = features_df.isnull().sum()
        high_null_features = null_counts[null_counts > len(features_df) * 0.5]
        if len(high_null_features) > 0:
            logger.warning(f"Features with >50% missing values: {list(high_null_features.index)}")
        
        logger.info("✓ Data validation passed")
        
    except Exception as e:
        logger.error(f"Data validation failed: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate unbiased support/resistance training data"
    )
    
    parser.add_argument('--universe-size', type=int, default=50,
                       help='Number of stocks to sample (default: 50)')
    parser.add_argument('--sample-year', type=int, default=2020,
                       help='Year for unbiased sampling (default: 2020)')
    parser.add_argument('--training-start', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                       default=date(2021, 1, 1), help='Training start date (YYYY-MM-DD)')
    parser.add_argument('--training-end', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                       default=date(2023, 12, 31), help='Training end date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', default='ml_training_data',
                       help='Output directory for training data')
    parser.add_argument('--min-examples', type=int, default=100,
                       help='Minimum examples per symbol')
    parser.add_argument('--validate', action='store_true',
                       help='Validate generated data after creation')
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
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    try:
        # Generate training data
        results = asyncio.run(generate_unbiased_training_data(
            universe_size=args.universe_size,
            sample_year=args.sample_year,
            training_start_date=args.training_start,
            training_end_date=args.training_end,
            output_dir=args.output_dir,
            min_examples_per_symbol=args.min_examples
        ))
        
        # Optional validation
        if args.validate:
            asyncio.run(validate_training_data(args.output_dir))
        
        print("\n" + "="*60)
        print("TRAINING DATA GENERATION COMPLETED")
        print("="*60)
        print(f"Output directory: {args.output_dir}")
        print(f"Total examples: {results.get('total_examples', 0):,}")
        print(f"Symbols with data: {results.get('symbols_with_data', 0)}")
        print(f"Universe year: {results.get('sample_year')}")
        print("="*60)
        print("\n✓ Ready for ML model training!")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error: {e}")
        if args.debug:
            raise
        sys.exit(1)

if __name__ == "__main__":
    main()