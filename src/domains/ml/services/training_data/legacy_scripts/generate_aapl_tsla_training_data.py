#!/usr/bin/env python3
"""
Generate training data for AAPL and TSLA from their listing dates until now
- AAPL: 1995-09-05 to present (29+ years of data)
- TSLA: 2010-06-29 to present (14+ years of data)
"""

import asyncio
import logging
from datetime import date
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import the training data infrastructure
from src.ml.training_data.runners.training_data_callback_runner import TrainingDataJobRunner, TrainingDataJobConfig
from shared.utils.environment import Environment

async def generate_aapl_tsla_training_data():
    """Generate comprehensive training data for AAPL and TSLA."""
    
    # Configure symbols with their earliest available dates
    symbols_config = {
        'AAPL': date(1995, 9, 5),   # AAPL listing date
        'TSLA': date(2010, 6, 29)   # TSLA IPO date
    }
    
    end_date = date.today()
    results = {}
    
    for symbol, start_date in symbols_config.items():
        logger.info(f"\n🚀 Starting training data generation for {symbol}")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"📊 Duration: {(end_date - start_date).days} days")
        
        # Create training data job configuration
        # Use enhanced features with actual technical indicators (not normalized)
        config = TrainingDataJobConfig(
            job_name=f"historical_training_data_{symbol}",
            symbols=[symbol],
            start_date=start_date,
            end_date=end_date,
            
            # Technical configuration for sequence-based training data
            output_structure="sequences", # Generate sequences, not hourly rows
            sequence_length=60,         # 60-day sequences
            prediction_horizon=5,       # 5-day prediction horizon
            use_enhanced_features=True, # Enable all 9 technical indicators
            use_universe_state_indicators=False, # Disable universe state (causes issues)
            normalize_features=False,   # Use actual values, not normalized
            normalize_labels=False,     # Use actual returns, not normalized
            
            # Feature configuration (will be automatically configured by enhanced features)
            feature_configs=[
                {"name": "ohlcv", "enabled": True},
                {"name": "technical_indicators", "enabled": True}
            ],
            
            # Label configuration for future returns
            label_configs=[
                {"name": "future_returns", "enabled": True, "horizon": 5}
            ],
            
            # Output configuration - auto-generate based on environment and run_id
            output_dir="auto",  # Will be auto-generated as /mnt/d/ats-data/training_data/ats-dev/{run_id}
            dataset_name_prefix=f"historical_{symbol.lower()}",
            
            # Quality requirements
            min_sequences_required=1000,  # Require at least 1000 sequences
            min_quality_score=0.8,       # 80% quality score minimum
            
            # Processing configuration
            batch_size=10000,
            max_memory_mb=8192
        )
        
        # Initialize training data runner
        env = Environment()
        runner = TrainingDataJobRunner(config=config, env=env)
        
        try:
            # Run training data generation
            logger.info(f"⚡ Executing training data generation for {symbol}...")
            result = await runner.run_training_data_generation()
            
            # Store results
            results[symbol] = result
            
            # Log results
            if result['status'] == 'success':
                logger.info(f"✅ {symbol} training data generation completed successfully!")
                logger.info(f"   Run ID: {result['run_id']}")
                logger.info(f"   Dataset IDs: {result['dataset_ids']}")
                
                training_results = result['results']['training_results']
                logger.info(f"   Features shape: {training_results['features_shape']}")
                logger.info(f"   Labels shape: {training_results['labels_shape']}")
                logger.info(f"   Feature count: {len(training_results['feature_names'])}")
                logger.info(f"   Features: {', '.join(training_results['feature_names'][:8])}...")
                
            else:
                logger.error(f"❌ {symbol} training data generation failed!")
                logger.error(f"   Error: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Exception during {symbol} training data generation: {e}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            results[symbol] = {
                'status': 'failed',
                'error': str(e)
            }
    
    # Summary report
    logger.info(f"\n📊 TRAINING DATA GENERATION SUMMARY")
    logger.info(f"=" * 50)
    
    successful = []
    failed = []
    
    for symbol, result in results.items():
        if result['status'] == 'success':
            successful.append(symbol)
            training_results = result['results']['training_results']
            logger.info(f"✅ {symbol}: {training_results['features_shape'][0]} sequences generated")
        else:
            failed.append(symbol)
            logger.info(f"❌ {symbol}: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\n🎯 FINAL STATUS:")
    logger.info(f"   ✅ Successful: {len(successful)} symbols ({', '.join(successful)})")
    logger.info(f"   ❌ Failed: {len(failed)} symbols ({', '.join(failed)})")
    
    return results

if __name__ == "__main__":
    asyncio.run(generate_aapl_tsla_training_data())