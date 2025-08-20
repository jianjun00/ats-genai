#!/usr/bin/env python3
"""
Generate Support/Resistance Training Data for AAPL and TSLA (2020-present)

This script generates training data using the SupportResistanceTrainingGenerator
specifically for AAPL and TSLA from 2020 to present day.
"""

import sys
import asyncio
import logging
import pickle
import json
from datetime import date, datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config.environment import Environment
from ml.training_data.support_resistance_generator import SupportResistanceTrainingGenerator

async def main():
    """Main function to generate S/R training data for AAPL and TSLA"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("SUPPORT/RESISTANCE TRAINING DATA GENERATION")
    logger.info("Symbols: AAPL, TSLA")
    logger.info("Period: 2020-01-01 to 2024-12-31")
    logger.info("="*60)
    
    # Initialize environment
    env = Environment()
    logger.info(f"Database URL: {env.get_database_url()}")
    
    # Create generator
    generator = SupportResistanceTrainingGenerator(env=env)
    
    # Generate training data
    symbols = ['AAPL', 'TSLA']
    start_date = date(2020, 1, 1)
    end_date = date(2024, 12, 31)
    min_examples_per_symbol = 50
    
    logger.info(f"Generating training data for {symbols}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Minimum examples per symbol: {min_examples_per_symbol}")
    
    try:
        training_examples = await generator.generate_training_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            min_examples_per_symbol=min_examples_per_symbol
        )
        
        logger.info(f"✅ Generated {len(training_examples)} training examples")
        
        if training_examples:
            # Save to pickle file
            output_file = f"sr_training_aapl_tsla_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
            with open(output_file, 'wb') as f:
                pickle.dump(training_examples, f)
            
            logger.info(f"💾 Saved training data to: {output_file}")
            
            # Generate summary statistics
            symbol_counts = {}
            feature_count = 0
            support_levels_total = 0
            resistance_levels_total = 0
            
            for example in training_examples:
                symbol_counts[example.symbol] = symbol_counts.get(example.symbol, 0) + 1
                if feature_count == 0:
                    feature_count = len(example.features)
                support_levels_total += len(example.next_day_support_levels)
                resistance_levels_total += len(example.next_day_resistance_levels)
            
            # Create summary report
            summary = {
                'generation_timestamp': datetime.now().isoformat(),
                'symbols': symbols,
                'date_range': {
                    'start': start_date.isoformat(),
                    'end': end_date.isoformat()
                },
                'total_examples': len(training_examples),
                'examples_per_symbol': symbol_counts,
                'feature_count': feature_count,
                'avg_support_levels_per_day': support_levels_total / len(training_examples) if training_examples else 0,
                'avg_resistance_levels_per_day': resistance_levels_total / len(training_examples) if training_examples else 0,
                'output_file': output_file
            }
            
            summary_file = output_file.replace('.pkl', '_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"📊 Summary report saved to: {summary_file}")
            
            # Print summary
            logger.info("\n" + "="*40)
            logger.info("TRAINING DATA SUMMARY")
            logger.info("="*40)
            logger.info(f"Total Examples: {len(training_examples):,}")
            logger.info(f"Feature Count: {feature_count}")
            logger.info(f"Average Support Levels per Day: {support_levels_total / len(training_examples):.2f}")
            logger.info(f"Average Resistance Levels per Day: {resistance_levels_total / len(training_examples):.2f}")
            logger.info("\nExamples per Symbol:")
            for symbol, count in symbol_counts.items():
                logger.info(f"  {symbol}: {count:,} examples")
            
            # Show sample features from first example
            if training_examples:
                first_example = training_examples[0]
                logger.info(f"\nSample Features (from {first_example.symbol} on {first_example.date}):")
                sample_features = list(first_example.features.items())[:10]
                for feature, value in sample_features:
                    logger.info(f"  {feature}: {value}")
                logger.info(f"  ... and {len(first_example.features) - 10} more features")
                
                logger.info(f"\nSample Labels:")
                logger.info(f"  Support Levels: {len(first_example.next_day_support_levels)}")
                logger.info(f"  Resistance Levels: {len(first_example.next_day_resistance_levels)}")
                logger.info(f"  Next Day High: {first_example.next_day_high}")
                logger.info(f"  Next Day Low: {first_example.next_day_low}")
                logger.info(f"  Next Day Close: {first_example.next_day_close}")
            
            logger.info("="*40)
            logger.info("✅ TRAINING DATA GENERATION COMPLETED!")
            logger.info("="*40)
            
        else:
            logger.warning("❌ No training examples generated")
            logger.warning("This could be due to:")
            logger.warning("- Insufficient data in database for the specified symbols/dates")
            logger.warning("- Database connection issues")
            logger.warning("- Missing minute-level data for support/resistance detection")
            
    except Exception as e:
        logger.error(f"❌ Error generating training data: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())