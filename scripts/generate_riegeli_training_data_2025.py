#!/usr/bin/env python3
"""
Generate Riegeli-only training data for AAPL and TSLA from 2025-01-01
This script creates training datasets using only Riegeli format, removing CSV and Parquet dependencies.
"""

import asyncio
import logging
from datetime import date
from pathlib import Path
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import the training data infrastructure
from src.ml.training_data.runners.training_data_callback_runner import TrainingDataJobRunner, TrainingDataJobConfig
from src.ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
from config.environment import Environment

async def generate_riegeli_training_data():
    """Generate Riegeli-only training data for AAPL and TSLA from 2025-01-01."""
    
    # Configure symbols for 2025 data
    symbols_config = {
        'AAPL': date(2025, 1, 1),
        'TSLA': date(2025, 1, 1)
    }
    
    end_date = date.today()  # Up to current date
    results = {}
    
    # Configure Riegeli-only storage
    storage_config = StorageConfig(
        primary_format="riegeli",
        enable_indexing=True,
        compression_level=6
    )
    
    for symbol, start_date in symbols_config.items():
        logger.info(f"\n🚀 Starting Riegeli training data generation for {symbol}")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"📊 Duration: {(end_date - start_date).days} days")
        
        # Output directory for Riegeli data
        output_dir = f"/data/training/riegeli/{symbol.lower()}_2025"
        
        # Create training data job configuration
        config = TrainingDataJobConfig(
            job_name=f"riegeli_training_data_{symbol}_2025",
            symbols=[symbol],
            start_date=start_date,
            end_date=end_date,
            
            # Technical configuration for sequence-based training data
            output_structure="sequences",   # Generate sequences for Riegeli storage
            sequence_length=60,            # 60-day sequences
            prediction_horizon=5,          # 5-day prediction horizon
            use_enhanced_features=True,    # Enable all technical indicators
            
            # Feature configuration
            include_ohlc=True,
            include_volume=True,
            include_technical_indicators=True,
            technical_indicators=["etop", "ebot", "pldot", "sma_20", "ema_12", "ema_26"],
            
            # Multi-timeframe configuration
            enable_multi_timeframe=True,
            timeframes=['5m', '15m', '1h', '1d'],
            
            # Output configuration - Riegeli only
            output_format="riegeli",
            compression_enabled=True,
            output_directory=output_dir
        )
        
        # Initialize storage manager with Riegeli-only configuration
        storage_manager = SequenceStorageManager(
            base_path=Path(output_dir),
            config=storage_config
        )
        
        try:
            # Create job runner
            job_runner = TrainingDataJobRunner(
                config=config,
                environment=Environment.DEV,
                storage_manager=storage_manager
            )
            
            # Execute training data generation
            logger.info(f"⚙️ Generating Riegeli training data for {symbol}...")
            
            result = await job_runner.run()
            
            results[symbol] = {
                'status': 'success',
                'config': config.__dict__,
                'result': result,
                'output_directory': output_dir,
                'storage_format': 'riegeli'
            }
            
            logger.info(f"✅ Training data generation completed for {symbol}")
            logger.info(f"📁 Output directory: {output_dir}")
            
        except Exception as e:
            logger.error(f"❌ Training data generation failed for {symbol}: {e}")
            results[symbol] = {
                'status': 'failed',
                'error': str(e),
                'output_directory': output_dir
            }
    
    # Save generation summary
    summary_file = "/data/training/riegeli_generation_summary_2025.json"
    with open(summary_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"\n📋 Generation Summary:")
    logger.info(f"📄 Summary saved to: {summary_file}")
    
    successful_runs = sum(1 for r in results.values() if r['status'] == 'success')
    total_runs = len(results)
    
    logger.info(f"✅ Successful: {successful_runs}/{total_runs}")
    
    if successful_runs == total_runs:
        logger.info("🎉 All training data generation completed successfully!")
        return True
    else:
        logger.warning("⚠️ Some training data generation failed. Check logs for details.")
        return False

if __name__ == "__main__":
    # Run the training data generation
    success = asyncio.run(generate_riegeli_training_data())
    exit(0 if success else 1)