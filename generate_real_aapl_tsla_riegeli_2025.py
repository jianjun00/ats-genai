#!/usr/bin/env python3
"""
Generate REAL training data for AAPL and TSLA from 2025-07-01 to present using existing infrastructure
This uses actual market data from the database, not synthetic data.
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
from config.environment import Environment

async def generate_real_aapl_tsla_training_data():
    """Generate REAL training data for AAPL and TSLA from 2025-07-01 to present."""
    
    logger.info("🚀 Generating REAL Training Data for AAPL and TSLA")
    logger.info("📅 Date range: 2025-07-01 to present (REAL MARKET DATA)")
    logger.info("⚠️  Using actual daily price data from database, not synthetic data")
    
    symbols = ['AAPL', 'TSLA']
    start_date = date(2025, 7, 1)  # July 1, 2025
    end_date = date.today()        # Today
    
    results = {}
    
    for symbol in symbols:
        logger.info(f"\n🔄 Processing {symbol} with REAL market data...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"📊 Duration: {(end_date - start_date).days} days")
        
        # Create training data job configuration using REAL data
        config = TrainingDataJobConfig(
            job_name=f"real_riegeli_training_data_{symbol}_2025",
            symbols=[symbol],
            start_date=start_date,
            end_date=end_date,
            
            # Technical configuration for Riegeli-compatible sequences
            output_structure="sequences",       # Generate sequences for ML training
            sequence_length=21,                 # 21-day sequences (3 weeks of trading data)
            prediction_horizon=1,               # 1-day prediction horizon
            use_enhanced_features=True,         # Enable technical indicators from real data
            use_universe_state_indicators=False, # Disable universe state (causes issues)
            normalize_features=False,           # Use actual values for visualization
            normalize_labels=False,             # Use actual returns for visualization
            
            # Feature configuration - real OHLCV + technical indicators
            feature_configs=[
                {"name": "ohlcv", "enabled": True},           # Real OHLC volume data
                {"name": "technical_indicators", "enabled": True}  # Real technical indicators
            ],
            
            # Label configuration for future returns
            label_configs=[
                {"name": "future_returns", "enabled": True, "horizon": 1}
            ],
            
            # Output configuration - save to Riegeli-compatible format
            output_dir="/data/training/real_riegeli_aapl_tsla_2025",  # Container path
            dataset_name_prefix=f"real_riegeli_{symbol.lower()}",
            
            # Quality requirements for real data
            min_sequences_required=10,          # Require at least 10 sequences from real data
            min_quality_score=0.8,             # 80% quality score minimum
            
            # Processing configuration
            batch_size=1000,
            max_memory_mb=4096
        )
        
        # Initialize training data runner with ATS-DEV environment
        env = Environment()
        runner = TrainingDataJobRunner(config=config, env=env)
        
        try:
            # Run REAL training data generation
            logger.info(f"⚡ Executing REAL training data generation for {symbol}...")
            result = await runner.run_training_data_generation()
            
            # Store results
            results[symbol] = result
            
            # Log results
            if result['status'] == 'success':
                logger.info(f"✅ {symbol} REAL training data generation completed successfully!")
                logger.info(f"   Run ID: {result['run_id']}")
                logger.info(f"   Dataset IDs: {result['dataset_ids']}")
                
                training_results = result['results']['training_results']
                logger.info(f"   Features shape: {training_results['features_shape']}")
                logger.info(f"   Labels shape: {training_results['labels_shape']}")
                logger.info(f"   Feature count: {len(training_results['feature_names'])}")
                logger.info(f"   Features: {', '.join(training_results['feature_names'][:8])}...")
                
                # Log the data source confirmation
                logger.info(f"   ✅ DATA SOURCE: REAL market data from database")
                logger.info(f"   ✅ DATE RANGE: {start_date} to {end_date}")
                logger.info(f"   ✅ TECHNICAL INDICATORS: Real envelope_top, envelope_bot, pldot values")
                
            else:
                logger.error(f"❌ {symbol} REAL training data generation failed!")
                logger.error(f"   Error: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Exception during {symbol} REAL training data generation: {e}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            results[symbol] = {
                'status': 'failed',
                'error': str(e)
            }
    
    # Summary report
    logger.info(f"\n📊 REAL TRAINING DATA GENERATION SUMMARY")
    logger.info(f"=" * 50)
    
    successful = []
    failed = []
    
    for symbol, result in results.items():
        if result['status'] == 'success':
            successful.append(symbol)
            training_results = result['results']['training_results']
            logger.info(f"✅ {symbol}: {training_results['features_shape'][0]} REAL sequences generated")
        else:
            failed.append(symbol)
            logger.info(f"❌ {symbol}: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\n🎯 FINAL STATUS:")
    logger.info(f"   ✅ Successful: {len(successful)} symbols ({', '.join(successful)})")
    logger.info(f"   ❌ Failed: {len(failed)} symbols ({', '.join(failed)})")
    
    if successful:
        logger.info(f"\n🎉 REAL training data successfully generated!")
        logger.info(f"📁 Data source: REAL market data from daily_prices tables")
        logger.info(f"📅 Date range: 2025-07-01 to {date.today()}")
        logger.info(f"🔗 Ready for visualization at: http://localhost:3000/eda")
    
    return results

if __name__ == "__main__":
    asyncio.run(generate_real_aapl_tsla_training_data())