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

from domains.trading.services.core.app.runner import Runner
from core.platform.config.environment import Environment, EnvironmentType
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
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

    # Day offsets for expanded data collection window
    parser.add_argument('--start-day-offset', type=int, default=0,
                       help='Days to extend backwards from start date for data collection (default: 0)')
    parser.add_argument('--end-day-offset', type=int, default=0,
                       help='Days to extend forwards from end date for data collection (default: 0)')

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
    parser.add_argument('--base-duration', default='5m',
                       help='Runner base duration (default: 5m)')

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
    # FIX: Use actual TrainingDataConfig attributes (timeframes, feature_types)
    total_features = 0
    timeframes = config.timeframes if hasattr(config, 'timeframes') else ['5m', '15m', '60m', '1d']
    feature_types = config.feature_types if hasattr(config, 'feature_types') else ['ohlcv', 'technical']

    # Rough estimation: features per timeframe * number of timeframes
    features_per_timeframe = 7  # OHLCV + volume + basic technical
    total_features = len(timeframes) * features_per_timeframe * len(feature_types)

    # Calculate estimated total sequences (rough approximation)
    days_range = (end_date - start_date).days
    intervals_per_day = 24 * 60 // config.training_interval_minutes  # Training intervals per day
    estimated_sequences = days_range * intervals_per_day * len(symbols)

    # Create training dataset record
    record = TrainingDatasetRecord(
        dataset_name=dataset_name,
        run_id=run_id,
        total_sequences=estimated_sequences,
        sequence_length=len(timeframes),  # Use number of timeframes as sequence length
        feature_count=total_features,
        label_count=1,  # FIX: Default to 1 label (price prediction)
        symbols=symbols,
        date_range_start=start_date,
        date_range_end=end_date,
        features_file_path=str(Path(output_dir) / f"{dataset_name}_features.{storage_format}"),
        labels_file_path=str(Path(output_dir) / f"{dataset_name}_labels.{storage_format}"),
        metadata_file_path=str(Path(output_dir) / f"{dataset_name}_metadata.json"),
        prediction_horizon=1,  # FIX: Default to 1-day prediction horizon
        status="generating",
        created_by="training_data_callback_runner",
        data_sources=["universe_state_manager"],
        generation_parameters={
            "base_interval_minutes": config.base_interval_minutes,
            "training_interval_minutes": config.training_interval_minutes,
            "timeframes": timeframes,  # FIX: Use actual timeframes list
            "storage_format": storage_format,
            "output_directory": output_dir
        },
        technical_indicators=','.join(get_technical_indicators()),
        feature_metadata=json.dumps({
            "timeframes": timeframes,  # FIX: Use actual timeframes list
            "features_per_timeframe": {tf: 7 for tf in timeframes},  # FIX: Simple mapping
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
    """Update training dataset with completion details (legacy function)."""
    await update_training_dataset_completion_with_status(
        environment, dataset_id, actual_sequences, generation_duration_seconds,
        file_size_mb, data_quality_score, "completed"
    )


async def update_training_dataset_completion_with_status(environment: Environment, dataset_id: int,
                                                       actual_sequences: int, generation_duration_seconds: int,
                                                       file_size_mb: float = 0.0, data_quality_score: float = 1.0,
                                                       status: str = "completed") -> None:
    """Update training dataset with completion details and specific status."""

    TrainingDatasetDAO(environment)
    conn = await asyncpg.connect(environment.get_database_url())

    try:
        table_name = environment.get_table_name("training_datasets")

        update_query = f"""
        UPDATE {table_name}
        SET status = $1,
            total_sequences = $2,
            generation_duration_seconds = $3,
            file_size_mb = $4,
            data_quality_score = $5,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $6
        """

        await conn.execute(
            update_query,
            status,
            actual_sequences,
            generation_duration_seconds,
            file_size_mb,
            data_quality_score,
            dataset_id
        )

        print(f"✅ Updated dataset {dataset_id} completion status:")
        print(f"   Status: {status}")
        print(f"   Actual sequences: {actual_sequences:,}")
        print(f"   Duration: {generation_duration_seconds}s")
        print(f"   File size: {file_size_mb:.2f} MB")
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

    # Set up comprehensive logging for debug mode
    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('training_data_generation_debug.log')
            ]
        )

    logger = logging.getLogger(__name__)

    # DEBUG STEP 1: Configuration Loading
    logger.info("🔧 STEP 1: Loading configuration files")

    # Load Gin configuration if provided
    if args.gin_config and Path(args.gin_config).exists():
        logger.debug(f"Loading gin config from {args.gin_config}")
        gin.parse_config_file(args.gin_config)
        logger.info(f"✅ Gin config loaded successfully from {args.gin_config}")
    else:
        logger.warning(f"❌ No gin config file found at {args.gin_config}")

    # Also load training data specific config
    training_data_gin = Path("config/training_data.gin")
    if training_data_gin.exists():
        logger.debug(f"Loading training data gin config from {training_data_gin}")
        gin.parse_config_file(str(training_data_gin))
        logger.info(f"✅ Training data gin config loaded from {training_data_gin}")
        operative_config = gin.operative_config_str()
        logger.debug(f"Current gin operative config after loading:\n{operative_config}")
    else:
        logger.warning(f"❌ No training data gin config found at {training_data_gin}")

    logger.info("✅ STEP 1 COMPLETE: Configuration loading finished")

    # DEBUG STEP 2: Environment and Data Validation
    logger.info("🌍 STEP 2: Environment setup and data validation")

    # Map environment string to EnvironmentType
    env_map = {
        'dev': EnvironmentType.DEV,
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION
    }

    env_type = env_map.get(args.environment.lower())
    if not env_type:
        logger.error(f"❌ Unknown environment: {args.environment}")
        raise ValueError(f"Unknown environment: {args.environment}")

    logger.info(f"✅ Environment type resolved: {env_type} ({args.environment})")

    # Create environment
    gin_config_file = args.gin_config if args.gin_config else None
    environment = Environment(gin_config_file, env_type)
    logger.info(f"✅ Environment object created successfully")

    # Parse and validate dates
    from datetime import datetime as dt, timedelta
    try:
        start_date = dt.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = dt.strptime(args.end_date, "%Y-%m-%d").date()

        # Calculate actual data collection window with offsets
        collection_start_date = start_date - timedelta(days=args.start_day_offset)
        collection_end_date = end_date + timedelta(days=args.end_day_offset)

        logger.info(f"📅 Date Range Configuration:")
        logger.info(f"   Target range: {start_date} to {end_date} ({(end_date - start_date).days + 1} days)")
        logger.info(f"   Collection window: {collection_start_date} to {collection_end_date} ({(collection_end_date - collection_start_date).days + 1} days)")
        logger.info(f"   Start offset: {args.start_day_offset} days backward")
        logger.info(f"   End offset: {args.end_day_offset} days forward")

        # Validate offsets
        if args.start_day_offset < 0 or args.end_day_offset < 0:
            logger.error(f"❌ Invalid offsets: start_day_offset and end_day_offset must be >= 0")
            raise ValueError(f"Offsets must be non-negative: start_day_offset={args.start_day_offset}, end_day_offset={args.end_day_offset}")

        # Validate date range
        if end_date < start_date:
            logger.error(f"❌ Invalid date range: end_date ({end_date}) < start_date ({start_date})")
            raise ValueError(f"End date {end_date} cannot be before start date {start_date}")

        date_range_days = (end_date - start_date).days + 1
        logger.info(f"✅ Date range validated: {start_date} to {end_date} ({date_range_days} days)")

        # Validate symbols or universe_id
        if args.universe_id:
            # Fetch symbols from universe membership
            logger.info(f"🌍 Fetching instruments from universe_id={args.universe_id}")
            try:
                from core.platform.database.connection_manager import get_raw_connection
                with get_raw_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            SELECT DISTINCT i.symbol
                            FROM intg_universe_membership um
                            JOIN intg_instrument_xrefs i ON um.instrument_id = i.instrument_id
                            WHERE um.universe_id = %s AND i.vendor_id = 1
                            ORDER BY i.symbol
                        """, (args.universe_id,))

                        symbols = [row['symbol'] for row in cursor.fetchall()]

                if not symbols:
                    logger.error(f"❌ No instruments found in universe_id={args.universe_id}")
                    raise ValueError(f"Universe {args.universe_id} contains no instruments")

                # Set symbols from universe
                args.symbols = symbols
                logger.info(f"✅ Symbols loaded from universe {args.universe_id}: {len(symbols)} symbols")
                logger.info(f"   First 10 symbols: {symbols[:10]}")

            except Exception as e:
                logger.error(f"❌ Failed to fetch instruments from universe {args.universe_id}: {e}")
                raise ValueError(f"Could not load instruments from universe {args.universe_id}: {e}")

        elif not args.symbols:
            logger.error("❌ No symbols or universe_id provided for training data generation")
            raise ValueError("At least one symbol or universe_id must be provided")
        else:
            logger.info(f"✅ Symbols validated: {args.symbols} ({len(args.symbols)} symbols)")
    except ValueError as e:
        logger.error(f"❌ Date parsing failed: {e}")
        raise

    logger.info("✅ STEP 2 COMPLETE: Environment and data validation finished")

    # DEBUG STEP 3: Training Configuration Creation
    logger.info("⚙️ STEP 3: Creating training data configuration")

    # Create training data configuration using gin - let gin configure it properly
    logger.debug("Creating TrainingDataConfig with gin configuration")
    operative_config = gin.operative_config_str()
    logger.debug(f"Current gin operative config:\n{operative_config}")

    # Force gin to apply the configuration by using gin.get_configurable
    try:
        # Get the configured constructor
        logger.debug("Attempting to get gin configured TrainingDataConfig")
        configurable_constructor = gin.get_configurable('domains.ml.services.training_data.timeseries_sequence_training_generator.TrainingDataConfig')
        logger.debug(f"Got configurable constructor: {configurable_constructor}")

        # Create using gin-configured constructor
        training_config = configurable_constructor()
        logger.info("✅ Successfully created config with gin configurable constructor")

    except Exception as e:
        logger.warning(f"⚠️ Failed to use gin configurable constructor: {e}")
        logger.info("Falling back to manual TrainingDataConfig() creation")
        training_config = TrainingDataConfig()

    # Log configuration details
    config_details = {
        'timeframes': getattr(training_config, 'timeframes', 'MISSING'),
        'feature_types': getattr(training_config, 'feature_types', 'MISSING'),
        'signal_names': getattr(training_config, 'signal_names', 'MISSING'),
        'base_interval_minutes': getattr(training_config, 'base_interval_minutes', 'MISSING'),
        'training_interval_minutes': getattr(training_config, 'training_interval_minutes', 'MISSING')
    }

    logger.info(f"✅ TrainingDataConfig created with settings:")
    for key, value in config_details.items():
        logger.info(f"  {key}: {value}")

    logger.info("✅ STEP 3 COMPLETE: Training configuration created successfully")

    # DEBUG STEP 4: Dataset Setup and Metadata Creation
    logger.info("📁 STEP 4: Setting up dataset directory and metadata")

    # Create dataset directory
    import shutil
    import os

    # Validate output directory path
    logger.debug(f"Validating output directory: {args.output_dir}")
    try:
        os.makedirs(args.output_dir, exist_ok=True)
        logger.info(f"✅ Output directory created/verified: {args.output_dir}")
    except Exception as e:
        logger.error(f"❌ Failed to create output directory {args.output_dir}: {e}")
        raise

    # Copy gin config to dataset directory for reproducibility
    gin_config_path = "config/app_docker.gin"  # Current gin config
    if os.path.exists(gin_config_path):
        try:
            shutil.copy2(gin_config_path, os.path.join(args.output_dir, "gin_config.gin"))
            logger.info(f"✅ Copied gin config to: {args.output_dir}/gin_config.gin")
        except Exception as e:
            logger.warning(f"⚠️ Failed to copy gin config: {e}")
    else:
        logger.warning(f"⚠️ Gin config file not found at: {gin_config_path}")

    # Generate unique dataset_id
    from datetime import datetime
    dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"✅ Generated dataset_id: {dataset_id}")

    # Create dataset-specific directory
    dataset_dir = os.path.join(args.output_dir, dataset_id)
    try:
        os.makedirs(dataset_dir, exist_ok=True)
        logger.info(f"✅ Dataset directory created: {dataset_dir}")
    except Exception as e:
        logger.error(f"❌ Failed to create dataset directory {dataset_dir}: {e}")
        raise

    # Create comprehensive dataset metadata
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
        "working_directory": os.getcwd(),
        "environment": args.environment,
        "dataset_id": dataset_id,
        "debug_mode": args.debug if hasattr(args, 'debug') else False
    }

    # Save metadata to dataset directory
    metadata_file = os.path.join(dataset_dir, "dataset_metadata.json")
    try:
        with open(metadata_file, 'w') as f:
            json.dump(dataset_metadata, f, indent=2)
        logger.info(f"✅ Dataset metadata saved to: {metadata_file}")
    except Exception as e:
        logger.error(f"❌ Failed to save metadata file {metadata_file}: {e}")
        raise

    logger.info("✅ STEP 4 COMPLETE: Dataset setup and metadata creation finished")

    # DEBUG STEP 5: Callback and Runner Creation
    logger.info("🔄 STEP 5: Creating training callback and runner")

    # Create training callback with comprehensive logging
    try:
        logger.debug("Creating IntervalBasedTrainingDataCallback")
        training_callback = IntervalBasedTrainingDataCallback(
            symbols=args.symbols,
            config=training_config,
            output_dir=args.output_dir,
            storage_format=args.storage_format,
            start_date=args.start_date,  # Pass target date range (not collection window)
            end_date=args.end_date,
            start_day_offset=args.start_day_offset,
            end_day_offset=args.end_day_offset,
            collection_start_date=collection_start_date,
            collection_end_date=collection_end_date
        )

        # Pass dataset_id to callback for completion tracking
        training_callback.dataset_id = dataset_id

        # Create a run record for tracking monthly training data
        try:
            from domains.trading.services.core.app.database_manager import DatabaseManager
            db_manager = DatabaseManager(environment)

            # Create run record for this training data generation
            run_parameters = {
                "symbols": args.symbols,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "start_day_offset": args.start_day_offset,
                "end_day_offset": args.end_day_offset,
                "storage_format": args.storage_format,
                "monthly_storage": True,
                "dataset_id": dataset_id
            }

            async with db_manager.get_connection() as conn:
                runs_table = environment.get_table_name("runs")
                run_query = f"""
                INSERT INTO {runs_table} (
                    run_type, status, start_time, created_by, parameters
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """

                run_id = await conn.fetchval(
                    run_query,
                    "monthly_training_data_generation",
                    "running",
                    datetime.now(),
                    "training_data_callback_runner",
                    json.dumps(run_parameters)
                )

            # 🚨 REMOVED PROBLEMATIC LINE: training_callback.run_id = run_id  
            # ISSUE: This was setting a database integer run_id, but the callback should use Runner's run_context.run_id
            # FIX: Callback now gets run_id from runner.run_context.run_id in handleInterval method
            logger.info(f"✅ Created run record for monthly training data tracking: {run_id}")
            logger.info("✅ Callback will use Runner's run_context.run_id for database insertions")

        except Exception as e:
            logger.warning(f"⚠️ Failed to create run record: {e}")
            # Continue without run_id - monthly records won't be saved but training data will still be generated
        logger.info(f"✅ Training callback created successfully")
        logger.info(f"   Callback type: {type(training_callback).__name__}")
        logger.info(f"   Dataset ID: {dataset_id}")
        logger.info(f"   Storage format: {args.storage_format}")

    except Exception as e:
        logger.error(f"❌ Failed to create training callback: {e}")
        raise

    # Create Runner with callback using expanded collection window
    try:
        logger.debug("Creating Runner with training callback using expanded collection window")

        # CRITICAL FIX: Use UnifiedMarketDataManager for training data generation  
        # Supports multiple data sources including FirstRate minute bar data
        from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType
        
        # Initialize unified market data manager with correct path for minute bar data
        from core.market_data.unified_manager import StorageBackend
        market_data_config = MarketDataConfig(
            vendors=[VendorType.FIRSTRATE],  # Use FirstRate for minute data
            storage_backend=StorageBackend.FILE, 
            file_storage_path="/data/minute-bars/firstrate"  # FIXED: Use container path not host path
        )
        minute_data_manager = UnifiedMarketDataManager(market_data_config)
        logger.info(f"✅ Created UnifiedMarketDataManager for training data generation")
        logger.info(f"   Storage backend: file")
        logger.info(f"   Base path: /data/minute-bars/firstrate")

        # ARCHITECTURE FIX: Add UniverseStateBuilder to populate universe state cache
        # UniverseStateBuilder calls get_minute_ohlc_batch to access cached data from FileBasedMinuteMarketDataManager
        # This populates the universe state cache that training data generator needs
        from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        
        # Create universe state manager for shared cache
        # 🚨 CRITICAL FIX: Do NOT create UniverseStateManager manually
        # Let Runner create its own with proper run_context and unique run_id
        # This prevents duplicate key violations from multiple runners using same run_id
        
        # DEBUG: Check what gin config values are being loaded
        try:
            gin_base_duration = gin.query_parameter('domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.base_duration')
            gin_target_durations = gin.query_parameter('domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.target_durations')
            logger.info(f"🔍 [DEBUG] Gin config values:")
            logger.info(f"   gin_base_duration: {gin_base_duration}")
            logger.info(f"   gin_target_durations: {gin_target_durations}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to query gin config: {e}")
        
        # 🚨 CRITICAL FIX: Create UniverseStateBuilder without universe_state_manager
        # It will get the proper manager from Runner after Runner is created
        universe_state_builder = UniverseStateIntervalBuilder(
            env=environment,
            base_duration=args.base_duration,  # Use same base_duration as the runner
            # target_durations will use gin config: '5m,15m,60m,1d' 
            # universe_state_manager will be set from Runner after it's created
        )
        
        # DEBUG: Check what values were actually set
        logger.info(f"🔍 [DEBUG] UniverseStateBuilder actual configuration:")
        logger.info(f"   base_duration: {universe_state_builder.base_duration}")
        logger.info(f"   target_durations: {universe_state_builder.target_durations}")
        logger.info(f"   target_durations count: {len(universe_state_builder.target_durations)}")
        
        logger.info(f"✅ Created UniverseStateBuilder to populate universe state cache")
        logger.info(f"   Base duration: {args.base_duration}")
        logger.info(f"   Target durations: {len(universe_state_builder.target_durations)} timeframes")
        
        # 🚨 CRITICAL FIX: Create UniverseManager with proper symbols and initialize it
        from domains.trading.services.universe.universe_manager import UniverseManager
        universe_manager = UniverseManager(
            env=environment,
            universe_id=args.universe_id or 1,
            symbols=args.symbols  # Pass the actual symbols instead of hardcoded ['TSLA']
        )
        
        # Initialize the universe manager to resolve symbols to instrument_ids
        logger.info(f"🔄 Initializing UniverseManager with symbols: {args.symbols}")
        await universe_manager.initialize()
        logger.info(f"✅ UniverseManager initialized with instrument_ids: {universe_manager.instrument_ids}")
        
        logger.info(f"✅ UniverseManager and UniverseStateBuilder ready for callback execution")
        
        runner = Runner(
            start_date=collection_start_date.strftime("%Y-%m-%d"),
            end_date=collection_end_date.strftime("%Y-%m-%d"),
            environment=environment,
            universe_id=args.universe_id or 1,
            callbacks=[universe_state_builder, training_callback],  # UniverseStateBuilder builds cache during offset period, training generates data from start_date
            market_data_manager=minute_data_manager,  # CRITICAL: Use minute data manager instead of daily price manager
            universe_manager=universe_manager,  # 🚨 CRITICAL FIX: Use custom universe manager with proper symbols
            # 🚨 CRITICAL FIX: Do NOT pass universe_state_manager - let Runner create its own with unique run_id
            base_duration=args.base_duration
        )
        
        # 🚨 CRITICAL FIX: Set the universe_state_manager on the builder AFTER Runner creates it
        # This ensures the builder uses the Runner's properly configured manager with unique run_id
        universe_state_builder.universe_state_manager = runner.universe_state_manager

        logger.info(f"✅ Runner created successfully")
        logger.info(f"   Target date range: {start_date} to {end_date}")
        logger.info(f"   Collection window: {collection_start_date} to {collection_end_date}")
        logger.info(f"   Base duration: {args.base_duration}")
        logger.info(f"   Universe ID: {args.universe_id or 1}")
        logger.info(f"   Environment: {args.environment}")

    except Exception as e:
        logger.error(f"❌ Failed to create runner: {e}")
        raise

    logger.info("✅ STEP 5 COMPLETE: Callback and runner created successfully")

    # DEBUG STEP 6: Training Data Generation Execution
    logger.info("🚀 STEP 6: Starting training data generation execution")

    # Log execution summary
    execution_summary = {
        'symbols': args.symbols,
        'symbol_count': len(args.symbols),
        'target_date_range_days': (end_date - start_date).days + 1,
        'collection_date_range_days': (collection_end_date - collection_start_date).days + 1,
        'start_day_offset': args.start_day_offset,
        'end_day_offset': args.end_day_offset,
        'base_duration': args.base_duration,
        'output_directory': args.output_dir,
        'dataset_id': dataset_id,
        'storage_format': args.storage_format,
        'environment': args.environment
    }

    logger.info("📊 Execution Summary:")
    for key, value in execution_summary.items():
        logger.info(f"   {key}: {value}")

    # Track generation timing with detailed logging
    generation_start_time = time.time()
    logger.info(f"⏱️ Generation started at: {datetime.now().isoformat()}")

    # Execute training data generation with proper ArrayRecord cleanup
    try:
        logger.info("🔄 Running training data generation...")
        # 🚨 CRITICAL FIX: Use context manager to ensure ArrayRecord writers are always closed
        with training_callback:
            await runner.run()
        logger.info("✅ Training data generation runner completed successfully")

    except Exception as e:
        logger.error(f"❌ Training data generation failed: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Note: Context manager already ensured cleanup in finally block
        raise

    # Calculate and log generation duration
    generation_duration = int(time.time() - generation_start_time)
    generation_end_time = datetime.now()
    logger.info(f"⏱️ Generation completed at: {generation_end_time.isoformat()}")
    logger.info(f"⏱️ Total generation duration: {generation_duration} seconds ({generation_duration/60:.1f} minutes)")

    logger.info("✅ STEP 6 COMPLETE: Training data generation execution finished")

    # DEBUG STEP 7: Post-Generation Analysis and Metadata Update
    logger.info("📈 STEP 7: Analyzing generation results and updating metadata")

    # Analyze generated sequences from callback
    try:
        estimated_actual_sequences = getattr(training_callback, 'sequences_generated', 0)
        interval_counter = getattr(training_callback, 'interval_counter', 0)

        logger.debug(f"Sequences generated by callback: {estimated_actual_sequences}")
        logger.debug(f"Intervals processed by callback: {interval_counter}")

        if estimated_actual_sequences == 0:
            # Fallback estimation based on date range
            days_range = (end_date - start_date).days + 1
            intervals_per_day = 24 * 60 // training_config.training_interval_minutes
            estimated_actual_sequences = days_range * intervals_per_day * len(args.symbols)
            logger.warning(f"⚠️ Using fallback sequence estimation: {estimated_actual_sequences}")
        else:
            logger.info(f"✅ Actual sequences generated: {estimated_actual_sequences}")

    except Exception as e:
        logger.error(f"❌ Error analyzing generation results: {e}")
        estimated_actual_sequences = 0

    # Update dataset metadata with completion info
    try:
        if os.path.exists(metadata_file):
            logger.debug(f"Updating metadata file: {metadata_file}")

            with open(metadata_file, 'r') as f:
                metadata = json.load(f)

            completion_info = {
                "completion_timestamp": dt_now.now().isoformat(),
                "generation_duration_seconds": generation_duration,
                "estimated_sequences": estimated_actual_sequences,
                "actual_intervals_processed": interval_counter,
                "status": "completed"
            }

            metadata.update(completion_info)

            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"✅ Dataset metadata updated successfully")
            logger.info(f"   Completion status: {completion_info['status']}")
            logger.info(f"   Duration: {generation_duration} seconds")
            logger.info(f"   Sequences: {estimated_actual_sequences}")

        else:
            logger.warning(f"⚠️ Metadata file not found for update: {metadata_file}")

    except Exception as e:
        logger.error(f"❌ Failed to update metadata file: {e}")

    # DEBUG STEP 8: Database Registration
    logger.info("🗄️ STEP 8: Registering training dataset in database")

    try:
        logger.debug("Registering dataset in database...")

        # Register the dataset in the database
        db_dataset_id = await register_training_dataset(
            environment=environment,
            symbols=args.symbols,
            start_date=start_date,
            end_date=end_date,
            config=training_config,
            output_dir=args.output_dir,
            storage_format=args.storage_format
        )

        logger.info(f"✅ Dataset registered in database with ID: {db_dataset_id}")

        # CRITICAL FIX: Verify actual files were created before marking as completed
        logger.debug(f"Verifying actual ArrayRecord files were created...")

        # Check if any ArrayRecord files actually exist
        dataset_dir = Path(args.output_dir) / dataset_id
        arrayrecord_files = list(dataset_dir.rglob("*.arrayrecord"))

        total_file_size_mb = 0.0
        actual_files_with_content = 0

        for file_path in arrayrecord_files:
            file_size_bytes = file_path.stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)
            total_file_size_mb += file_size_mb

            # ArrayRecord files have 128KB minimum size, check if they have actual data
            # Files with only the 128KB header should be considered empty
            if file_size_bytes > 131072:  # More than 128KB indicates actual data
                actual_files_with_content += 1

        logger.info(f"File verification results:")
        logger.info(f"   ArrayRecord files found: {len(arrayrecord_files)}")
        logger.info(f"   Files with actual content: {actual_files_with_content}")
        logger.info(f"   Total file size: {total_file_size_mb:.2f} MB")

        # Determine actual status and sequences based on file verification
        if len(arrayrecord_files) == 0:
            # No files created at all - complete failure
            actual_status = "failed"
            actual_sequences = 0
            actual_file_size_mb = 0.0
            logger.error(f"❌ CRITICAL: No ArrayRecord files were created - generation failed completely")

        elif actual_files_with_content == 0:
            # Files created but empty - partial failure
            actual_status = "partial"
            actual_sequences = 0
            actual_file_size_mb = total_file_size_mb
            logger.warning(f"⚠️ ArrayRecord files created but contain no actual data - empty generation")

        else:
            # Files with content - success
            actual_status = "completed"
            actual_sequences = estimated_actual_sequences
            actual_file_size_mb = total_file_size_mb
            logger.info(f"✅ ArrayRecord files successfully created with content")

        # Update database with actual results
        logger.debug(f"Updating database with actual status: {actual_status}")
        await update_training_dataset_completion_with_status(
            environment=environment,
            dataset_id=db_dataset_id,
            actual_sequences=actual_sequences,
            generation_duration_seconds=generation_duration,
            file_size_mb=actual_file_size_mb,
            status=actual_status
        )

        logger.info(f"✅ Dataset completion status updated in database")

        # Add database info to metadata file
        try:
            if os.path.exists(metadata_file):
                logger.debug("Adding database registration info to metadata file")

                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)

                database_info = {
                    "database_id": db_dataset_id,
                    "database_registered": True,
                    "database_table": environment.get_table_name("training_dataset")
                }

                metadata.update(database_info)

                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)

                logger.info(f"✅ Added database registration info to metadata file")

        except Exception as e:
            logger.warning(f"⚠️ Failed to update metadata with database info: {e}")

        logger.info("✅ STEP 8 COMPLETE: Database registration completed successfully")

    except Exception as e:
        logger.error(f"❌ Failed to register dataset in database: {e}")
        logger.warning(f"⚠️ Training data files created successfully, but database registration failed")
        logger.warning(f"   Dataset will not appear in UI until manually registered")
        # Don't fail the entire process - files are still created successfully

        logger.info("⚠️ STEP 8 PARTIAL: Database registration failed but files created")

    # DEBUG STEP 9: Final Summary and Completion
    logger.info("🎯 STEP 9: Final summary and completion")

    # Create completion summary
    completion_summary = {
        'status': 'completed',
        'dataset_directory': args.output_dir,
        'dataset_id': dataset_id,
        'metadata_file': metadata_file,
        'gin_config': f"{args.output_dir}/gin_config.gin",
        'database_id': locals().get('db_dataset_id', 'not_registered'),
        'generation_duration': f"{generation_duration} seconds ({generation_duration/60:.1f} minutes)",
        'estimated_sequences': estimated_actual_sequences,
        'symbols_processed': len(args.symbols),
        'date_range': f"{start_date} to {end_date}"
    }

    logger.info("🎉 TRAINING DATA GENERATION COMPLETED SUCCESSFULLY!")
    for key, value in completion_summary.items():
        logger.info(f"   {key}: {value}")

    if 'db_dataset_id' in locals():
        logger.info(f"   Database table: {environment.get_table_name('training_dataset')}")

    logger.info("✅ STEP 9 COMPLETE: All training data generation steps finished successfully")

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)