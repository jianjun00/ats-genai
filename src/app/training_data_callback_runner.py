#!/usr/bin/env python3
"""
Pure Callback-Based Training Data Generation

This implements training data generation PURELY as a callback,
not as a separate runner class. Uses the existing Runner framework
with DateBasedTrainingDataCallback to handle all training data logic.
"""

import argparse
import asyncio
import gin
from datetime import datetime, date
from pathlib import Path

from app.runner import Runner
from config.environment import Environment, EnvironmentType
from state.training_data_callback import DateBasedTrainingDataCallback
from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig


def parse_args():
    """Parse command line arguments for pure callback-based training data generation."""
    parser = argparse.ArgumentParser(
        description="Generate training data using pure callback approach"
    )
    
    # Data selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--symbols', nargs='+', help='List of symbols (e.g. AAPL TSLA)')
    group.add_argument('--universe-id', type=int, help='Universe ID to fetch all instruments')
    
    # Date range
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    
    # Configuration
    parser.add_argument('--environment', default='dev', 
                       choices=['dev', 'test', 'intg', 'prod'], 
                       help='Environment type')
    parser.add_argument('--gin-config', default=None, 
                       help='Path to Gin config file (optional)')
    
    # Training data parameters
    parser.add_argument('--base-interval', type=int, default=1,
                       help='Base data interval in minutes (default: 1)')
    parser.add_argument('--training-interval', type=int, default=60,
                       help='Training data generation interval in minutes (default: 60)')
    
    # Sequence configuration
    parser.add_argument('--sequence-5m', type=int, default=52,
                       help='Number of 5-minute intervals in sequence (default: 52)')
    parser.add_argument('--sequence-15m', type=int, default=52,
                       help='Number of 15-minute intervals in sequence (default: 52)')
    parser.add_argument('--sequence-1h', type=int, default=24,
                       help='Number of 1-hour intervals in sequence (default: 24)')
    parser.add_argument('--sequence-1d', type=int, default=20,
                       help='Number of daily intervals in sequence (default: 20)')
    
    # Prediction horizons
    parser.add_argument('--predict-1h', type=int, default=6,
                       help='Number of 1-hour intervals to predict (default: 6)')
    parser.add_argument('--predict-1d', type=int, default=5,
                       help='Number of daily intervals to predict (default: 5)')
    
    # Output configuration
    parser.add_argument('--output-dir', default='/data/training/sequences',
                       help='Output directory for training data')
    parser.add_argument('--storage-format', default='pickle',
                       choices=['riegeli', 'tfrecord', 'pickle', 'parquet'],
                       help='Storage format for training data')
    parser.add_argument('--use-advanced-storage', action='store_true',
                       help='Use SequenceStorageManager for advanced storage')
    parser.add_argument('--compression-level', type=int, default=6,
                       help='Compression level for advanced storage')
    
    # Processing options
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--base-duration', default='1h',
                       help='Runner base duration (default: 1h)')
    
    return parser.parse_args()


async def main():
    """
    Main entry point - PURE CALLBACK APPROACH.
    
    Creates ONLY a callback, not a separate runner class.
    Uses the existing Runner framework with the callback.
    """
    args = parse_args()
    
    # Load Gin configuration if provided
    if args.gin_config and Path(args.gin_config).exists():
        gin.parse_config_file(args.gin_config)
    
    # Map environment string to EnvironmentType
    env_map = {
        'dev': EnvironmentType.DEV,
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTG,
        'prod': EnvironmentType.PROD
    }
    
    env_type = env_map.get(args.environment.lower())
    if not env_type:
        raise ValueError(f"Unknown environment: {args.environment}")
    
    # Create environment
    environment = Environment(args.gin_config, env_type)
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Create training data configuration
    config = TrainingDataConfig(
        base_interval_minutes=args.base_interval,
        training_interval_minutes=args.training_interval,
        sequence_lengths={
            '5m': args.sequence_5m,
            '15m': args.sequence_15m,
            '1h': args.sequence_1h,
            '1d': args.sequence_1d,
        },
        prediction_horizons={
            '1h': args.predict_1h,
            '1d': args.predict_1d,
        }
    )
    
    # Set up storage manager if using advanced storage
    storage_manager = None
    if args.use_advanced_storage:
        storage_config = StorageConfig(
            primary_format=args.storage_format,
            compression_level=args.compression_level,
            chunk_size=1000,
            enable_indexing=True,
            enable_checksums=True
        )
        storage_manager = SequenceStorageManager(
            base_path=args.output_dir,
            config=storage_config
        )
        print(f"📦 Advanced storage enabled: {args.storage_format} format")
    
    # ✅ PURE CALLBACK APPROACH: Create ONLY the callback
    training_callback = DateBasedTrainingDataCallback(
        symbols=args.symbols,
        config=config,
        output_dir=args.output_dir,
        save_format='advanced' if args.use_advanced_storage else 'pickle',
        storage_manager=storage_manager
    )
    
    print(f"🎯 Created PURE callback-based training data generation")
    print(f"   Callback: {type(training_callback).__name__}")
    print(f"   NOT creating any TrainingDataRunner class")
    print(f"   Using existing Runner framework with callback")
    
    # ✅ Use existing Runner framework with our callback
    runner = Runner(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        environment=environment,
        universe_id=args.universe_id or 1,
        callbacks=[training_callback],  # ONLY the callback
        base_duration=args.base_duration
    )
    
    print(f"\n🚀 Starting PURE callback-based training data generation")
    print(f"   Symbols: {args.symbols}")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Base duration: {args.base_duration}")
    print(f"   Output: {args.output_dir}")
    print(f"   Storage: {args.storage_format}")
    print(f"   Method: Pure callback (no separate runner class)")
    
    # ✅ Run using ONLY the existing framework + callback
    await runner.run()
    
    print(f"\n✅ Pure callback-based training data generation completed!")
    print(f"   All logic handled by callback methods:")
    print(f"   - handleStart: Initialize training generator")
    print(f"   - handleStartOfDay: Open daily data collection")
    print(f"   - handleInterval: Generate training examples")
    print(f"   - handleEndOfDay: Save daily data")
    print(f"   - handleEnd: Final summary")
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)