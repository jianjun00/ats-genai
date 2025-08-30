#!/usr/bin/env python3
"""
Generate Training Data for AAPL from 1995 to Present

This script generates comprehensive training data for AAPL using the existing
ATS platform infrastructure, covering the full period from listing to present.
"""

import sys
import asyncio
import logging
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from pathlib import Path
import json
import pickle

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from config.environment import Environment
    from ml.training_data.support_resistance_generator import SupportResistanceTrainingGenerator
except ImportError as e:
    print(f"Import error: {e}")
    print("This script should be run in the Docker environment with run_dev.py")
    sys.exit(1)

async def generate_aapl_comprehensive_training_data():
    """Generate comprehensive training data for AAPL from 1995 to present."""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("="*80)
    logger.info("AAPL COMPREHENSIVE TRAINING DATA GENERATION")
    logger.info("Period: 1995-09-05 (AAPL listing) to Present")
    logger.info("="*80)
    
    try:
        # Initialize environment
        env = Environment()
        logger.info(f"Database URL: {env.get_database_url()}")
        
        # Create generator
        generator = SupportResistanceTrainingGenerator(env=env)
        
        # Generate training data for AAPL full history
        symbols = ['AAPL']
        
        # AAPL was listed on 1995-09-05, but let's start from 1995-10-01 for clean data
        start_date = date(1995, 10, 1)
        end_date = date.today() - timedelta(days=1)  # Yesterday
        
        # Create multiple training datasets with different parameters
        training_configs = [
            {
                'name': 'aapl_30year_full',
                'start_date': start_date,
                'end_date': end_date,
                'min_examples': 100,
                'description': 'AAPL 30-year complete historical training data'
            },
            {
                'name': 'aapl_last_10_years', 
                'start_date': end_date - timedelta(days=365*10),
                'end_date': end_date,
                'min_examples': 50,
                'description': 'AAPL last 10 years training data'
            },
            {
                'name': 'aapl_last_5_years',
                'start_date': end_date - timedelta(days=365*5), 
                'end_date': end_date,
                'min_examples': 30,
                'description': 'AAPL last 5 years training data'
            }
        ]
        
        results = {}
        
        for config in training_configs:
            logger.info(f"\n🚀 Generating {config['name']}...")
            logger.info(f"   Period: {config['start_date']} to {config['end_date']}")
            logger.info(f"   Duration: {(config['end_date'] - config['start_date']).days} days")
            
            try:
                # Generate training examples
                training_examples = await generator.generate_training_data(
                    symbols=symbols,
                    start_date=config['start_date'],
                    end_date=config['end_date'],
                    min_examples_per_symbol=config['min_examples']
                )
                
                if training_examples:
                    logger.info(f"✅ Generated {len(training_examples)} training examples")
                    
                    # Save training data
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    output_dir = Path(f"/data/training/{config['name']}")
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Save pickle file
                    pickle_file = output_dir / f"{config['name']}_{timestamp}.pkl"
                    with open(pickle_file, 'wb') as f:
                        pickle.dump(training_examples, f)
                    
                    # Save CSV file for analysis
                    csv_file = output_dir / f"{config['name']}_{timestamp}.csv"
                    training_df = []
                    
                    for example in training_examples:
                        row = {
                            'symbol': example.symbol,
                            'date': example.date,
                            'next_day_high': example.next_day_high,
                            'next_day_low': example.next_day_low,
                            'next_day_close': example.next_day_close,
                            'support_levels_count': len(example.next_day_support_levels),
                            'resistance_levels_count': len(example.next_day_resistance_levels)
                        }
                        
                        # Add feature columns (first 20 features for CSV)
                        feature_items = list(example.features.items())[:20]
                        for feature_name, value in feature_items:
                            row[f'feature_{feature_name}'] = value
                            
                        training_df.append(row)
                    
                    df = pd.DataFrame(training_df)
                    df.to_csv(csv_file, index=False)
                    
                    # Generate summary statistics
                    summary = {
                        'config': config,
                        'generation_timestamp': datetime.now().isoformat(),
                        'total_examples': len(training_examples),
                        'feature_count': len(training_examples[0].features) if training_examples else 0,
                        'date_range': {
                            'start': config['start_date'].isoformat(),
                            'end': config['end_date'].isoformat(),
                            'days': (config['end_date'] - config['start_date']).days
                        },
                        'files': {
                            'pickle': str(pickle_file),
                            'csv': str(csv_file)
                        },
                        'sample_features': list(training_examples[0].features.keys())[:10] if training_examples else [],
                        'statistics': {
                            'avg_support_levels': np.mean([len(ex.next_day_support_levels) for ex in training_examples]),
                            'avg_resistance_levels': np.mean([len(ex.next_day_resistance_levels) for ex in training_examples]),
                            'date_range_actual': {
                                'start': str(min(ex.date for ex in training_examples)),
                                'end': str(max(ex.date for ex in training_examples))
                            }
                        }
                    }
                    
                    # Save summary
                    summary_file = output_dir / f"{config['name']}_summary_{timestamp}.json"
                    with open(summary_file, 'w') as f:
                        json.dump(summary, f, indent=2, default=str)
                    
                    results[config['name']] = summary
                    
                    logger.info(f"📊 {config['name']} Summary:")
                    logger.info(f"   Examples: {len(training_examples):,}")
                    logger.info(f"   Features: {len(training_examples[0].features) if training_examples else 0}")
                    logger.info(f"   Files saved to: {output_dir}")
                    
                else:
                    logger.warning(f"❌ No training examples generated for {config['name']}")
                    results[config['name']] = {
                        'error': 'No training examples generated',
                        'config': config
                    }
                    
            except Exception as e:
                logger.error(f"❌ Error generating {config['name']}: {e}")
                results[config['name']] = {
                    'error': str(e),
                    'config': config
                }
        
        # Save overall results summary
        overall_summary = {
            'generation_timestamp': datetime.now().isoformat(),
            'symbol': 'AAPL',
            'total_configurations': len(training_configs),
            'successful_generations': len([r for r in results.values() if 'error' not in r]),
            'results': results
        }
        
        summary_file = Path("/data/training/aapl_comprehensive_summary.json")
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(summary_file, 'w') as f:
            json.dump(overall_summary, f, indent=2, default=str)
        
        logger.info("\n" + "="*80)
        logger.info("AAPL COMPREHENSIVE TRAINING DATA GENERATION COMPLETED")
        logger.info("="*80)
        logger.info(f"Successful datasets: {overall_summary['successful_generations']} / {overall_summary['total_configurations']}")
        logger.info(f"Overall summary saved to: {summary_file}")
        
        for name, result in results.items():
            if 'error' not in result:
                logger.info(f"✅ {name}: {result['total_examples']:,} examples")
            else:
                logger.info(f"❌ {name}: {result['error']}")
        
        logger.info("\n🎯 NEXT STEPS:")
        logger.info("1. Review generated training data files")
        logger.info("2. Populate TSLA data and generate TSLA training data") 
        logger.info("3. Combine AAPL and TSLA datasets for multi-symbol training")
        logger.info("4. Use data for model training and backtesting")
        
        return overall_summary
        
    except Exception as e:
        logger.error(f"❌ Error in AAPL training data generation: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    asyncio.run(generate_aapl_comprehensive_training_data())