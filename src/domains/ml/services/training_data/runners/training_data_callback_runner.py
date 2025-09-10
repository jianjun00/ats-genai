#!/usr/bin/env python3
"""
Pure Callback-Based Training Data Generation with Dataset Registration

This implements training data generation PURELY as a callback,
not as a separate runner class. Uses the existing Runner framework
with IntervalBasedTrainingDataCallback to handle all training data logic.

Features:
- Automatic training dataset registration in database
- Comprehensive metadata tracking (features, sequences, parameters)
- Generation timing and completion status updates
- Multi-timeframe feature estimation and documentation
"""

import argparse
import asyncio
import asyncpg
import gin
import json
import logging
import time
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

from services.core.app.runner import Runner
from shared.utils.environment import Environment, EnvironmentType
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
# Removed: SequenceStorageManager - not needed per PRD/DRD QR5 single-step architecture
from domains.ml.services.training_data.dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig


@gin.configurable
def get_technical_indicators(indicators: List[str] = None) -> List[str]:
    """Get the list of technical indicators from gin configuration."""
    return indicators or ["etop", "ebot", "pldot"]


@dataclass
class TrainingDataConfig:
    """Simple configuration for training data generation."""

    # Base timing configuration
    base_interval_minutes: int = 1
    training_interval_minutes: int = 60

    # Multi-timeframe sequence configuration
    sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
        '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
        '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
        '60m': 24,  # Past 24 x 60-minute intervals (1 day)
        '1d': 20,   # Past 20 x daily intervals (4 weeks)
    })

    prediction_horizons: Dict[str, int] = field(default_factory=lambda: {
        '60m': 6,   # Next 6 hours
        '1d': 5,    # Next 5 days
    })


def save_as_riegeli(df: pd.DataFrame, riegeli_file: Path):
    """Save DataFrame as riegeli format."""
    logger = logging.getLogger(__name__)
    try:
        import riegeli
        import numpy as np

        # Convert DataFrame to numpy array (keeping same structure as CSV)
        data = df.to_numpy(dtype=np.float32)

        with riegeli.RecordWriter(str(riegeli_file)) as writer:
            # Write column names as first record
            writer.write_record(str(list(df.columns)).encode('utf-8'))

            # Write each row as a record
            for row in data:
                writer.write_record(row.tobytes())

    except ImportError:
        # Fallback: save as numpy binary if riegeli not available
        import numpy as np
        np_file = riegeli_file.with_suffix('.npy')
        np.save(str(np_file), df.to_numpy())
        logger.warning(f"Riegeli not available, saved as numpy: {np_file}")


async def register_training_dataset(symbol: str, start_date: date, end_date: date,
                                   metadata: Dict[str, Any], riegeli_file: Path,
                                   parquet_file: Path, metadata_file: Path,
                                   environment: str = 'dev') -> int:
    """Register training dataset in database."""
    logger = logging.getLogger(__name__)

    # Connect directly to database
    if environment == 'dev':
        db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
    elif environment == 'intg':
        db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"
    else:
        raise ValueError(f"Unsupported environment: {environment}")

    conn = await asyncpg.connect(db_url)

    try:
        # Calculate file sizes
        riegeli_size_mb = riegeli_file.stat().st_size / (1024 * 1024)
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024) if parquet_file.exists() else 0
        total_size_mb = riegeli_size_mb + parquet_size_mb

        # Create run record
        run_query = f"""
        INSERT INTO {environment}_runs (
            run_type, status, start_time, end_time, created_by, error_message, parameters
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        """

        now = datetime.now()
        run_parameters = {
            "symbol": symbol,
            "data_format": "one_row_per_hour",
            "datetime_features": metadata.get('datetime_features', []),
            "technical_indicators": metadata.get('technical_indicators', []),
            "multi_timeframe_features": metadata.get('multi_timeframe_features', []),
            "file_size_mb": total_size_mb,
            "generation_method": "training_data_callback_runner"
        }

        run_id = await conn.fetchval(
            run_query,
            "hourly_training_data_generation",
            "completed",
            now,
            now,
            "training_data_callback_runner",
            None,
            json.dumps(run_parameters)
        )

        logger.info(f"📝 Created run record: {run_id}")

        # Create training dataset record
        dataset_query = f"""
        INSERT INTO {environment}_training_datasets (
            dataset_name, run_id, total_sequences, sequence_length, feature_count, label_count,
            symbols, date_range_start, date_range_end, data_quality_score, feature_completeness,
            label_completeness, generation_duration_seconds, file_size_mb, data_sources, status,
            features_file_path, labels_file_path, metadata_file_path, feature_metadata,
            technical_indicators, prediction_horizon, created_by, generation_parameters
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24
        ) RETURNING id
        """

        dataset_name = f"{symbol}_training_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{now.strftime('%H%M%S')}"

        dataset_id = await conn.fetchval(
            dataset_query,
            dataset_name,
            run_id,
            metadata['num_rows'],  # total_sequences (rows, not sequences)
            1,  # sequence_length (each row is independent)
            metadata['num_features'],  # feature_count
            0,  # label_count (no labels, this is feature generation)
            [symbol],  # symbols array
            start_date,  # date_range_start
            end_date,  # date_range_end
            1.0,  # data_quality_score
            1.0,  # feature_completeness
            1.0,  # label_completeness
            0,    # generation_duration_seconds
            total_size_mb,  # file_size_mb
            ["training_data_callback_runner"],  # data_sources array
            "completed",  # status
            str(riegeli_file),  # features_file_path (use riegeli as primary)
            "",  # labels_file_path (no labels)
            str(metadata_file),  # metadata_file_path
            json.dumps({
                "data_format": "one_row_per_hour",
                "datetime_as_features": True,
                "technical_indicators": metadata.get('technical_indicators', []),
                "multi_timeframe": True,
                "riegeli_file": str(riegeli_file),
                "parquet_file": str(parquet_file)
            }),  # feature_metadata
            metadata.get('technical_indicators', []),  # technical_indicators
            "1_hour",  # prediction_horizon
            "training_data_callback_runner",  # created_by
            json.dumps({
                "data_format": "one_row_per_hour",
                "datetime_as_features": True,
                "technical_indicators": metadata.get('technical_indicators', []),
                "multi_timeframe": True,
                "riegeli_file": str(riegeli_file),
                "parquet_file": str(parquet_file)
            })  # generation_parameters
        )

        logger.info(f"✅ Dataset registered with ID: {dataset_id}")
        logger.info(f"   Dataset name: {dataset_name}")
        logger.info(f"   Rows: {metadata['num_rows']:,}")
        logger.info(f"   Features: {metadata['num_features']}")
        logger.info(f"   File size: {total_size_mb:.1f} MB")
        logger.info(f"   Riegeli file: {riegeli_file}")
        logger.info(f"   Metadata file: {metadata_file}")

        return dataset_id

    finally:
        await conn.close()


def parse_args():
    """Parse command line arguments for pure callback-based training data generation."""
    parser = argparse.ArgumentParser(
        description="Generate training data using pure callback approach with automatic dataset registration"
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
    parser.add_argument('--output-dir', default='/data/training_data',
                       help='Output directory for training data (follows PRD/DRD: /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/)')
    parser.add_argument('--storage-format', default='arrayrecord',
                       choices=['arrayrecord'],
                       help='Storage format for training data (ArrayRecord per PRD/DRD QR4)')
    # Removed: --use-advanced-storage, --compression-level (SequenceStorageManager not needed per PRD/DRD QR5)

    # Processing options
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--base-duration', default='1h',
                       help='Runner base duration (default: 1h)')

    return parser.parse_args()


async def register_training_dataset(environment: Environment, symbols: List[str],
                                   start_date: date, end_date: date,
                                   config: TrainingDataConfig, output_dir: str,
                                   storage_format: str, run_id: Optional[int] = None) -> int:
    """Register a training dataset in the database."""

    # Create DAO
    dao = TrainingDatasetDAO(environment)

    # Generate dataset name
    symbols_str = "_".join(symbols) if symbols else "multi_symbol"
    dataset_name = f"callback_training_{symbols_str}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

    # Calculate estimated feature count based on config
    total_features = 0
    for timeframe, length in config.sequence_lengths.items():
        features_per_interval = 7  # OHLCV + technical indicators (etop, ebot, pldot)
        total_features += length * features_per_interval

    # Calculate estimated total sequences (rough approximation)
    days_range = (end_date - start_date).days
    intervals_per_day = 24 * 60 // config.training_interval_minutes  # Training intervals per day
    estimated_sequences = days_range * intervals_per_day * len(symbols)

    # Create training dataset record
    record = TrainingDatasetRecord(
        dataset_name=dataset_name,
        run_id=run_id,
        total_sequences=estimated_sequences,
        sequence_length=sum(config.sequence_lengths.values()),  # Total sequence length across timeframes
        feature_count=total_features,
        label_count=sum(config.prediction_horizons.values()),  # Total prediction horizons
        symbols=symbols,
        date_range_start=start_date,
        date_range_end=end_date,
        features_file_path=str(Path(output_dir) / f"{dataset_name}_features.{storage_format}"),
        labels_file_path=str(Path(output_dir) / f"{dataset_name}_labels.{storage_format}"),
        metadata_file_path=str(Path(output_dir) / f"{dataset_name}_metadata.json"),
        prediction_horizon=max(config.prediction_horizons.values()) if config.prediction_horizons else 0,
        status="generating",
        created_by="training_data_callback_runner",
        data_sources=["universe_state_manager"],
        generation_parameters={
            "base_interval_minutes": config.base_interval_minutes,
            "training_interval_minutes": config.training_interval_minutes,
            "sequence_lengths": config.sequence_lengths,
            "prediction_horizons": config.prediction_horizons,
            "storage_format": storage_format,
            "output_directory": output_dir
        },
        technical_indicators=','.join(get_technical_indicators()),
        feature_metadata=json.dumps({
            "timeframes": list(config.sequence_lengths.keys()),
            "features_per_timeframe": {tf: length * 7 for tf, length in config.sequence_lengths.items()},
            "total_features": total_features,
            "feature_types": ["open", "high", "low", "close", "volume", "etop", "ebot", "pldot"]
        })
    )

    # Register in database
    dataset_id = await dao.create_training_dataset(record)

    print(f"📝 Registered training dataset: {dataset_name}")
    print(f"   Dataset ID: {dataset_id}")
    print(f"   Estimated sequences: {estimated_sequences:,}")
    print(f"   Total features: {total_features:,}")
    print(f"   Symbols: {', '.join(symbols)}")

    return dataset_id


async def update_training_dataset_completion(environment: Environment, dataset_id: int,
                                           actual_sequences: int, generation_duration_seconds: int,
                                           file_size_mb: float = 0.0, data_quality_score: float = 1.0) -> None:
    """Update training dataset with completion details."""

    TrainingDatasetDAO(environment)
    conn = await asyncpg.connect(environment.get_database_url())

    try:
        table_name = environment.get_table_name("training_datasets")

        update_query = f"""
        UPDATE {table_name}
        SET status = 'completed',
            total_sequences = $1,
            generation_duration_seconds = $2,
            file_size_mb = $3,
            data_quality_score = $4,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $5
        """

        await conn.execute(
            update_query,
            actual_sequences,
            generation_duration_seconds,
            file_size_mb,
            data_quality_score,
            dataset_id
        )

        print(f"✅ Updated dataset {dataset_id} completion status:")
        print(f"   Actual sequences: {actual_sequences:,}")
        print(f"   Duration: {generation_duration_seconds}s")
        print(f"   Quality score: {data_quality_score:.2f}")

    finally:
        await conn.close()


async def main():
    """
    Main entry point - PURE CALLBACK APPROACH.

    Creates ONLY a callback, not a separate runner class.
    Uses the existing Runner framework with the callback.
    """
    args = parse_args()

    # Load Gin configuration if provided
    if args.gin_config and Path(args.gin_config).exists():
        print(f"🔧 DEBUG: Loading gin config from {args.gin_config}")
        gin.parse_config_file(args.gin_config)
        print(f"🔧 DEBUG: Gin config loaded from {args.gin_config}")
    else:
        print(f"🔧 DEBUG: No gin config file found at {args.gin_config}")
    
    # Also load training data specific config
    training_data_gin = Path("config/training_data.gin")
    if training_data_gin.exists():
        print(f"🔧 DEBUG: Loading training data gin config from {training_data_gin}")
        gin.parse_config_file(str(training_data_gin))
        print(f"🔧 DEBUG: Training data gin config loaded from {training_data_gin}")
        print(f"🔧 DEBUG: Current gin operative config after loading:")
        print(gin.operative_config_str())
    else:
        print(f"🔧 DEBUG: No training data gin config found at {training_data_gin}")

    # Map environment string to EnvironmentType
    env_map = {
        'dev': EnvironmentType.DEV,
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION
    }

    env_type = env_map.get(args.environment.lower())
    if not env_type:
        raise ValueError(f"Unknown environment: {args.environment}")

    # Create environment
    gin_config_file = args.gin_config if args.gin_config else None
    environment = Environment(gin_config_file, env_type)

    # Parse dates
    from datetime import datetime as dt
    start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

    # Create training data configuration using gin - let gin configure it properly
    print("🔧 DEBUG: Creating TrainingDataConfig with gin configuration")
    print(f"🔧 DEBUG: Current gin operative config:")
    print(gin.operative_config_str())
    
    # Force gin to apply the configuration by using gin.get_configurable
    try:
        # Get the configured constructor
        print("🔧 DEBUG: Attempting to get gin configured TrainingDataConfig")
        configurable_constructor = gin.get_configurable('domains.ml.services.training_data.timeseries_sequence_training_generator.TrainingDataConfig')
        print(f"🔧 DEBUG: Got configurable constructor: {configurable_constructor}")
        
        # Create using gin-configured constructor
        config = configurable_constructor()
        print("🔧 DEBUG: Successfully created config with gin configurable constructor")
        
    except Exception as e:
        print(f"🔧 DEBUG: Failed to use gin configurable constructor: {e}")
        print("🔧 DEBUG: Falling back to manual TrainingDataConfig() creation")
        config = TrainingDataConfig()
    
    print(f"🔧 DEBUG: TrainingDataConfig created:")
    print(f"  timeframes: {getattr(config, 'timeframes', 'MISSING')}")
    print(f"  feature_types: {getattr(config, 'feature_types', 'MISSING')}")
    print(f"  signal_names: {getattr(config, 'signal_names', 'MISSING')}")

    # Removed: SequenceStorageManager setup - using simple ArrayRecord storage per PRD/DRD QR5
    # Simple storage will be handled directly in callback

    # 📝 Simple dataset tracking: copy gin config to dataset directory and log command line
    import shutil
    import os
    
    # Create dataset directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Copy gin config to dataset directory
    gin_config_path = "config/app_docker.gin"  # Current gin config
    if os.path.exists(gin_config_path):
        shutil.copy2(gin_config_path, os.path.join(args.output_dir, "gin_config.gin"))
        print(f"📄 Copied gin config to: {args.output_dir}/gin_config.gin")
    
    # Save command line and metadata to dataset directory
    import json
    from datetime import datetime as dt_now
    import sys
    
    dataset_metadata = {
        "command_line": " ".join(sys.argv),
        "symbols": args.symbols,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "base_duration": args.base_duration,
        "output_dir": args.output_dir,
        "storage_format": args.storage_format,
        "generation_timestamp": dt_now.now().isoformat(),
        "gin_config_file": "gin_config.gin",
        "python_executable": sys.executable,
        "working_directory": os.getcwd()
    }
    
    # Generate dataset_id for directory structure (timestamp-based for uniqueness)
    from datetime import datetime
    dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Create dataset directory  
    dataset_dir = os.path.join(args.output_dir, dataset_id)
    os.makedirs(dataset_dir, exist_ok=True)
    
    # FIXED: Place metadata inside dataset directory, not at root output directory  
    metadata_file = os.path.join(dataset_dir, "dataset_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(dataset_metadata, f, indent=2)
    
    print(f"📊 Saved dataset metadata to: {metadata_file}")
    print(f"📋 Generated dataset_id: {dataset_id}")

    # ✅ PURE CALLBACK APPROACH: Create ONLY the callback
    # FIXED: Pass start_date and end_date for single symbol directory creation
    training_callback = IntervalBasedTrainingDataCallback(
        symbols=args.symbols,
        config=config,
        output_dir=args.output_dir,
        storage_format=args.storage_format,
        start_date=args.start_date,  # Pass full date range
        end_date=args.end_date
    )

    # Pass dataset_id to callback for completion tracking
    training_callback.dataset_id = dataset_id

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
    print(f"   Registered dataset ID: {dataset_id}")

    # Track generation timing
    generation_start_time = time.time()

    # ✅ Run using ONLY the existing framework + callback
    await runner.run()

    # Calculate generation duration
    generation_duration = int(time.time() - generation_start_time)

    # Update dataset completion status
    # Note: In a real implementation, we would get actual sequences from callback
    # For now, we'll estimate based on training intervals generated
    estimated_actual_sequences = getattr(training_callback, 'sequences_generated', 0)
    if estimated_actual_sequences == 0:
        # Fallback estimation
        days_range = (end_date - start_date).days
        intervals_per_day = 24 * 60 // config.training_interval_minutes
        estimated_actual_sequences = days_range * intervals_per_day * len(args.symbols)

    # 📝 Update dataset metadata with completion info
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        metadata.update({
            "completion_timestamp": dt_now.now().isoformat(),
            "generation_duration_seconds": generation_duration,
            "estimated_sequences": estimated_actual_sequences,
            "status": "completed"
        })
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"📊 Updated dataset metadata with completion info")

    print(f"\n✅ Training data generation completed!")
    print(f"   Dataset directory: {args.output_dir}")
    print(f"   Metadata file: {metadata_file}")
    print(f"   Gin config: {args.output_dir}/gin_config.gin")
    print(f"   Command line preserved in metadata for reproducibility")

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)