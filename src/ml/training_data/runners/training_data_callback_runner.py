#!/usr/bin/env python3
"""
Pure Callback-Based Training Data Generation with Dataset Registration

This implements training data generation PURELY as a callback,
not as a separate runner class. Uses the existing Runner framework
with DateBasedTrainingDataCallback to handle all training data logic.

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
import subprocess
import sys
import os
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

from app.runner import Runner
from core.config.environment import Environment, EnvironmentType
from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
from ml.training_data.dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord
from market_data.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager


def get_git_metadata() -> Dict[str, str]:
    """Get git metadata (commit hash, branch) for run tracking."""
    try:
        # Get commit hash
        commit_hash = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], 
            cwd=os.getcwd(),
            text=True
        ).strip()
        
        # Get branch name
        branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            cwd=os.getcwd(), 
            text=True
        ).strip()
        
        return {
            'git_commit_hash': commit_hash,
            'git_branch': branch,
            'working_directory': os.getcwd()
        }
    except Exception as e:
        logging.warning(f"Could not get git metadata: {e}")
        return {
            'git_commit_hash': '',
            'git_branch': '',
            'working_directory': os.getcwd()
        }

def get_command_line() -> str:
    """Get the command line used to invoke this script."""
    return ' '.join(sys.argv)

async def create_run_record(environment: Environment, symbols: List[str], 
                           start_date: date, end_date: date) -> int:
    """Create a run record in dev_runs table with metadata."""
    
    # Get metadata
    git_metadata = get_git_metadata()
    command_line = get_command_line()
    
    # Connect to database
    from core.database.connection_manager import get_raw_connection
    
    with get_raw_connection() as conn:
        with conn.cursor() as cursor:
            # Insert run record
            insert_query = """
                INSERT INTO dev_runs (
                    run_type, status, start_time, created_by, parameters,
                    command_line, git_commit_hash, git_branch, 
                    working_directory, python_version, environment
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
            """
            
            cursor.execute(insert_query, (
                'training_data_generation',  # run_type
                'running',                   # status
                datetime.now(),              # start_time
                'training_data_callback_runner',  # created_by
                json.dumps({                 # parameters
                    'symbols': symbols,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                }),
                command_line,                # command_line
                git_metadata['git_commit_hash'],  # git_commit_hash
                git_metadata['git_branch'],  # git_branch
                git_metadata['working_directory'],  # working_directory
                f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",  # python_version
                'dev'                        # environment
            ))
            
            result = cursor.fetchone()
            if result is None:
                raise ValueError("Failed to create run record - no result returned")
            
            # Handle different cursor result types
            try:
                run_id = result[0]  # Try index access first
            except (KeyError, TypeError):
                try:
                    run_id = result['id']  # Try dict access
                except (KeyError, TypeError):
                    # Last resort - get first value
                    run_id = next(iter(result)) if hasattr(result, '__iter__') else None
            
            if run_id is None:
                raise ValueError("Failed to extract run ID from result")
            conn.commit()
            
            logging.info(f"✅ Created run record ID: {run_id}")
            logging.info(f"   Command: {command_line}")
            logging.info(f"   Git commit: {git_metadata['git_commit_hash']}")
            logging.info(f"   Git branch: {git_metadata['git_branch']}")
            
            return run_id

async def update_run_status(run_id: int, status: str, error_message: str = None):
    """Update run status in dev_runs table."""
    from core.database.connection_manager import get_raw_connection
    
    with get_raw_connection() as conn:
        with conn.cursor() as cursor:
            if error_message:
                cursor.execute(
                    "UPDATE dev_runs SET status = %s, end_time = %s, error_message = %s WHERE id = %s",
                    (status, datetime.now(), error_message, run_id)
                )
            else:
                cursor.execute(
                    "UPDATE dev_runs SET status = %s, end_time = %s WHERE id = %s", 
                    (status, datetime.now(), run_id)
                )
            conn.commit()

@gin.configurable
def get_technical_indicators(indicators: List[str] = None) -> List[str]:
    """Get the list of technical indicators from gin configuration."""
    return indicators or ["etop", "ebot", "pldot"]


@gin.configurable
@dataclass 
class TrainingDataConfig:
    """Simple configuration for training data generation with multi-timeframe support."""
    
    # Base timing configuration
    base_interval_minutes: int = 1  # Always 1m base data
    training_interval_minutes: int = 60  # Generate training examples every hour
    
    # Multi-timeframe aggregation configuration
    timeframes: Dict[str, int] = field(default_factory=lambda: {
        '5m': 5,     # 5-minute bars
        '15m': 15,   # 15-minute bars  
        '1h': 60,    # 1-hour bars
        '1d': 1440,  # Daily bars (1440 minutes)
        '1w': 10080  # Weekly bars (10080 minutes)
    })
    
    # Multi-timeframe sequence configuration
    sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
        '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
        '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
        '1h': 24,   # Past 24 x 1-hour intervals (1 day)
        '1d': 20,   # Past 20 x daily intervals (4 weeks)
        '1w': 12    # Past 12 x weekly intervals (3 months)
    })
    
    prediction_horizons: Dict[str, int] = field(default_factory=lambda: {
        '1h': 6,    # Next 6 hours
        '1d': 5,    # Next 5 days
        '1w': 2     # Next 2 weeks
    })
    
    # File-based minute data configuration (Container-friendly default)
    minute_data_base_path: str = "/data/minute-bars"
    
    # Output directory structure configuration (Container-friendly default)
    output_base_path: str = "/data/training"


def save_as_arrayrecord(df: pd.DataFrame, arrayrecord_file: Path):
    """Save DataFrame as ArrayRecord format."""
    logger = logging.getLogger(__name__)
    import array_record
    import numpy as np
    
    # Convert DataFrame to numpy array 
    data = df.to_numpy(dtype=np.float32)
    
    with array_record.ArrayRecordWriter(str(arrayrecord_file), 'group_size:1') as writer:
        # Write column names as first record
        writer.write(str(list(df.columns)).encode('utf-8'))
        
        # Write each row as a record
        for row in data:
            writer.write(row.tobytes())


async def register_training_dataset(symbol: str, start_date: date, end_date: date,
                                   metadata: Dict[str, Any], arrayrecord_file: Path, 
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
        arrayrecord_size_mb = arrayrecord_file.stat().st_size / (1024 * 1024)
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024) if parquet_file.exists() else 0
        total_size_mb = arrayrecord_size_mb + parquet_size_mb
        
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
            technical_indicators, prediction_horizon, created_by, generation_parameters, file_metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24, $25
        ) RETURNING id
        """
        
        dataset_name = f"{symbol}_training_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{now.strftime('%H%M%S')}"
        
        # Create file_metadata structure
        file_metadata = {
            "files": [
                {
                    "symbol": symbol,
                    "timeframe": "1h",  # This callback processes hourly data
                    "file_path": arrayrecord_file.name,
                    "sequences": metadata['num_rows'],
                    "file_size_bytes": arrayrecord_file.stat().st_size,
                    "created_at": now.strftime("%Y-%m-%d %H:%M:%S")
                }
            ],
            "total_sequences": metadata['num_rows'],
            "total_files": 1,
            "timeframes": ["1h"],
            "symbols": [symbol],
            "generation_date": now.strftime("%Y-%m-%d %H:%M:%S")
        }
        
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
            str(arrayrecord_file),  # features_file_path (use arrayrecord as primary)
            "",  # labels_file_path (no labels)
            str(metadata_file),  # metadata_file_path
            json.dumps({
                "data_format": "one_row_per_hour",
                "datetime_as_features": True,
                "technical_indicators": metadata.get('technical_indicators', []),
                "multi_timeframe": True,
                "arrayrecord_file": str(arrayrecord_file),
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
                "arrayrecord_file": str(arrayrecord_file),
                "parquet_file": str(parquet_file)
            }),  # generation_parameters
            json.dumps(file_metadata)  # file_metadata
        )
        
        logger.info(f"✅ Dataset registered with ID: {dataset_id}")
        logger.info(f"   Dataset name: {dataset_name}")
        logger.info(f"   Rows: {metadata['num_rows']:,}")
        logger.info(f"   Features: {metadata['num_features']}")
        logger.info(f"   File size: {total_size_mb:.1f} MB")
        logger.info(f"   ArrayRecord file: {arrayrecord_file}")
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
    parser.add_argument('--sequence-1w', type=int, default=12,
                       help='Number of weekly intervals in sequence (default: 12)')
    
    # Prediction horizons
    parser.add_argument('--predict-1h', type=int, default=6,
                       help='Number of 1-hour intervals to predict (default: 6)')
    parser.add_argument('--predict-1d', type=int, default=5,
                       help='Number of daily intervals to predict (default: 5)')
    parser.add_argument('--predict-1w', type=int, default=2,
                       help='Number of weekly intervals to predict (default: 2)')
    
    # Output configuration
    parser.add_argument('--output-dir', default='/mnt/d/ats-data/training',
                       help='Base output directory for training data')
    parser.add_argument('--storage-format', default='arrayrecord',
                       choices=['arrayrecord'],
                       help='Storage format for training data (ArrayRecord only)')
    parser.add_argument('--use-advanced-storage', action='store_true',
                       help='Use SequenceStorageManager for advanced storage')
    parser.add_argument('--compression-level', type=int, default=6,
                       help='Compression level for advanced storage')
    
    # Minute data configuration (Container-friendly default)
    parser.add_argument('--minute-data-path', default='/data/minute-bars',
                       help='Base path to minute-level OHLC data files')
    
    # Processing options
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    parser.add_argument('--base-duration', default='60m',
                       help='Runner base duration (default: 60m)')
    
    return parser.parse_args()


async def register_training_dataset(environment: Environment, symbols: List[str], 
                                   start_date: date, end_date: date,
                                   config: TrainingDataConfig, output_dir: str,
                                   storage_format: str, run_id: Optional[int] = None) -> int:
    """Register a training dataset in the database."""
    
    # Create DAO
    dao = TrainingDatasetDAO(environment)
    
    # Generate dataset name with generation datetime for uniqueness
    symbols_str = "_".join(symbols) if symbols else "multi_symbol"
    generation_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    dataset_name = f"training_{symbols_str}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{generation_time}"
    
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
        technical_indicators=json.dumps(get_technical_indicators()),
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


async def validate_training_data_pipeline(run_id: int, dataset_id: int, output_dir: Path, 
                                         symbols: List[str], environment: str) -> bool:
    """
    CRITICAL: Validate complete training data pipeline end-to-end.
    
    This validation catches the gap where files are created but not usable.
    Tests:
    1. Files actually exist on disk
    2. ArrayRecord files can be read 
    3. Visualization API can load the data
    4. Data structure matches expected format
    
    Returns True only if complete pipeline works.
    """
    logger = logging.getLogger(__name__)
    validation_errors = []
    
    try:
        logger.info(f"🔍 Starting pipeline validation for dataset {dataset_id}, run {run_id}")
        
        # Step 1: Verify files exist AND have correct structure
        logger.info("Step 1: Checking file existence and structure...")
        arrayrecord_files = list(output_dir.rglob("*.arrayrecord"))
        
        if not arrayrecord_files:
            validation_errors.append("No ArrayRecord files found in output directory")
            logger.error(f"❌ No ArrayRecord files in {output_dir}")
        else:
            logger.info(f"✅ Found {len(arrayrecord_files)} ArrayRecord files")
            
            # CRITICAL: Validate file structure matches visualization API expectations
            expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
            structure_valid = True
            
            for timeframe in expected_timeframes:
                # Check if files exist in correct timeframe structure
                timeframe_pattern = f"*/{timeframe}/*_*.arrayrecord"
                timeframe_files = list(output_dir.glob(timeframe_pattern))
                
                if not timeframe_files:
                    validation_errors.append(f"No files found for timeframe {timeframe} in expected structure: {output_dir}/*/{timeframe}/")
                    structure_valid = False
                    logger.error(f"❌ Missing timeframe directory: {timeframe}")
                else:
                    logger.info(f"✅ Found {len(timeframe_files)} files for timeframe {timeframe}")
                    
                    # Validate filename format: {SYMBOL}_{start}_{end}.arrayrecord
                    for file_path in timeframe_files:
                        filename = file_path.name
                        if not ('_' in filename and filename.endswith('.arrayrecord')):
                            validation_errors.append(f"Invalid filename format: {filename} (expected: SYMBOL_START_END.arrayrecord)")
                            structure_valid = False
            
            if not structure_valid:
                validation_errors.append("File structure doesn't match visualization API expectations")
                logger.error("❌ CRITICAL: File structure validation failed")
                logger.error("   Expected: {output_dir}/{run_id}/{timeframe}/{SYMBOL}_{start}_{end}.arrayrecord")
                logger.error("   Example: output_dir/123/5m/AAPL_20250801_000000_20250802_000000.arrayrecord")
            
            # Check file sizes
            for file_path in arrayrecord_files:
                file_size = file_path.stat().st_size
                if file_size == 0:
                    validation_errors.append(f"Empty ArrayRecord file: {file_path}")
                    logger.error(f"❌ Empty file: {file_path}")
                elif file_size < 1024:  # Less than 1KB is suspicious
                    validation_errors.append(f"Suspiciously small file: {file_path} ({file_size} bytes)")
                    logger.warning(f"⚠️ Small file: {file_path} ({file_size} bytes)")
                else:
                    logger.info(f"✅ File size OK: {file_path.name} ({file_size:,} bytes)")
        
        # Step 2: Verify ArrayRecord files can be read
        logger.info("Step 2: Testing ArrayRecord readability...")
        try:
            import array_record
            from array_record.python.array_record_module import ArrayRecordReader
            
            readable_files = 0
            total_records = 0
            
            for file_path in arrayrecord_files[:3]:  # Test first 3 files
                try:
                    logger.info(f"Testing ArrayRecord: {file_path.name}")
                    
                    with ArrayRecordReader(str(file_path)) as reader:
                        records = list(reader)
                        
                        if not records:
                            validation_errors.append(f"ArrayRecord file has no records: {file_path}")
                            logger.error(f"❌ No records: {file_path}")
                            continue
                        
                        # Test first record structure
                        first_record = records[0]
                        if not isinstance(first_record, bytes):
                            validation_errors.append(f"ArrayRecord returns wrong type: {type(first_record)}")
                            continue
                        
                        # Try to parse as JSON
                        try:
                            record_data = json.loads(first_record.decode())
                            if not isinstance(record_data, dict):
                                validation_errors.append(f"ArrayRecord data not JSON dict: {type(record_data)}")
                                continue
                            
                            # Verify expected structure
                            if "features" not in record_data:
                                validation_errors.append(f"ArrayRecord missing 'features': {file_path}")
                                continue
                            
                            readable_files += 1
                            total_records += len(records)
                            logger.info(f"✅ ArrayRecord readable: {file_path.name} ({len(records)} records)")
                            
                        except json.JSONDecodeError as e:
                            validation_errors.append(f"ArrayRecord contains invalid JSON: {file_path} - {e}")
                            logger.error(f"❌ Invalid JSON in {file_path}: {e}")
                            
                except Exception as e:
                    validation_errors.append(f"Cannot read ArrayRecord file {file_path}: {e}")
                    logger.error(f"❌ ArrayRecord read failed {file_path}: {e}")
            
            if readable_files == 0:
                validation_errors.append("No ArrayRecord files are readable")
                logger.error("❌ No readable ArrayRecord files")
            else:
                logger.info(f"✅ ArrayRecord validation: {readable_files} files, {total_records} total records")
                
        except ImportError:
            validation_errors.append("ArrayRecord package not available - required for pipeline")
            logger.error("❌ ArrayRecord package missing")
        
        # Step 3: Test visualization API integration
        logger.info("Step 3: Testing visualization API integration...")
        try:
            import requests
            
            # Test if analytics service is running
            try:
                base_url = "http://localhost:3000"
                health_response = requests.get(f"{base_url}/health", timeout=5)
                if health_response.status_code != 200:
                    validation_errors.append("Analytics service not healthy")
                    logger.error("❌ Analytics service not healthy")
                    return False  # Cannot test API if service is down
                    
            except requests.ConnectionError:
                validation_errors.append("Analytics service not accessible")
                logger.error("❌ Analytics service not accessible")
                return False
            
            # Test training datasets API
            datasets_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
            if datasets_response.status_code != 200:
                validation_errors.append("Training datasets API not working")
                logger.error("❌ Training datasets API failed")
            else:
                datasets_data = datasets_response.json()
                datasets = datasets_data.get("datasets", [])
                
                # Find our dataset
                our_dataset = next((ds for ds in datasets if ds.get("id") == dataset_id), None)
                if not our_dataset:
                    validation_errors.append(f"Generated dataset {dataset_id} not found in API")
                    logger.error(f"❌ Dataset {dataset_id} not in API")
                else:
                    logger.info(f"✅ Dataset {dataset_id} found in API: {our_dataset.get('dataset_name')}")
                    
                    # Test sequences API
                    sequences_response = requests.get(f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences", timeout=10)
                    if sequences_response.status_code != 200:
                        validation_errors.append(f"Sequences API failed for dataset {dataset_id}")
                        logger.error(f"❌ Sequences API failed for dataset {dataset_id}")
                    else:
                        sequences_data = sequences_response.json()
                        sequences = sequences_data.get("sequences", [])
                        
                        if not sequences:
                            validation_errors.append("Sequences API returns empty array - files not accessible")
                            logger.error("❌ CRITICAL: Sequences API returns empty - files not accessible by API")
                        else:
                            logger.info(f"✅ Sequences API working: {len(sequences)} sequences")
                            
                            # CRITICAL TEST: Visualization data API
                            viz_response = requests.get(
                                f"{base_url}/api/v1/training-datasets/{dataset_id}/visualization-data?start_idx=0", 
                                timeout=15
                            )
                            
                            if viz_response.status_code != 200:
                                validation_errors.append("Visualization data API failed")
                                logger.error("❌ Visualization data API failed")
                            else:
                                viz_data = viz_response.json()
                                data_array = viz_data.get("data", [])
                                total_records = viz_data.get("total_records", 0)
                                
                                if not data_array or total_records == 0:
                                    validation_errors.append("Visualization API returns empty data - ArrayRecord files not readable by API")
                                    logger.error("❌ CRITICAL: Visualization API returns empty data")
                                else:
                                    logger.info(f"✅ CRITICAL SUCCESS: Visualization API returns data ({len(data_array)} records, {total_records} total)")
                                    
                                    # Verify data structure
                                    if data_array:
                                        first_record = data_array[0]
                                        required_fields = ["timestamp", "open", "high", "low", "close", "volume"]
                                        missing_fields = [f for f in required_fields if f not in first_record]
                                        
                                        if missing_fields:
                                            validation_errors.append(f"Visualization data missing fields: {missing_fields}")
                                            logger.error(f"❌ Missing fields: {missing_fields}")
                                        else:
                                            logger.info("✅ Visualization data structure correct")
        
        except Exception as e:
            validation_errors.append(f"API testing failed: {e}")
            logger.error(f"❌ API testing failed: {e}")
        
        # Summary
        if validation_errors:
            logger.error(f"\n❌ VALIDATION FAILED: {len(validation_errors)} errors:")
            for i, error in enumerate(validation_errors, 1):
                logger.error(f"   {i}. {error}")
            return False
        else:
            logger.info("\n✅ VALIDATION PASSED: Complete training data pipeline working")
            logger.info("   Files exist ✅")
            logger.info("   ArrayRecord readable ✅") 
            logger.info("   API integration working ✅")
            logger.info("   Visualization data accessible ✅")
            return True
            
    except Exception as e:
        logger.error(f"❌ Validation failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


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
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION
    }
    
    env_type = env_map.get(args.environment.lower())
    if not env_type:
        raise ValueError(f"Unknown environment: {args.environment}")
    
    # Create environment
    environment = Environment(args.gin_config, env_type)
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Create run record in dev_runs table with metadata tracking
    run_id = await create_run_record(environment, args.symbols, start_date, end_date)
    
    # Create training data configuration
    config = TrainingDataConfig(
        base_interval_minutes=args.base_interval,
        training_interval_minutes=args.training_interval,
        timeframes={
            '5m': 5,
            '15m': 15,
            '1h': 60,
            '1d': 1440,
            '1w': 10080
        },
        sequence_lengths={
            '5m': args.sequence_5m,
            '15m': args.sequence_15m,
            '1h': args.sequence_1h,
            '1d': args.sequence_1d,
            '1w': args.sequence_1w,
        },
        prediction_horizons={
            '1h': args.predict_1h,
            '1d': args.predict_1d,
            '1w': args.predict_1w,
        },
        minute_data_base_path=args.minute_data_path,
        output_base_path=args.output_dir
    )
    
    # Set up storage manager if using advanced storage
    storage_manager = None
    # Disable advanced storage to avoid Riegeli dependency issues
    print(f"📦 Using standard numpy format for training data storage")
    
    # 📝 Register training dataset in database
    dataset_id = await register_training_dataset(
        environment=environment,
        symbols=args.symbols,
        start_date=start_date,
        end_date=end_date,
        config=config,
        output_dir=args.output_dir,
        storage_format=args.storage_format,
        run_id=run_id
    )
    
    # Create structured output directory with requested format: <ATS_DATA_PATH>/training_data/<run_id>/<timeframe>/
    base_data_path = os.getenv('ATS_DATA_PATH', '/mnt/d/ats-data')
    structured_output_dir = Path(base_data_path) / "training_data" / str(run_id)
    structured_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories for each timeframe
    timeframe_dirs = {}
    for timeframe in config.timeframes.keys():
        timeframe_dir = structured_output_dir / timeframe
        timeframe_dir.mkdir(exist_ok=True)
        timeframe_dirs[timeframe] = timeframe_dir
    
    print(f"📁 Created structured output directory: {structured_output_dir}")
    print(f"   Run ID: {run_id}")
    print(f"   Expected output pattern:")
    for timeframe in config.timeframes.keys():
        for symbol in args.symbols:
            start_datetime = start_date.strftime('%Y%m%d_%H%M%S')
            end_datetime = end_date.strftime('%Y%m%d_%H%M%S')
            print(f"     {timeframe_dirs[timeframe]}/{symbol}_{start_datetime}_{end_datetime}.arrayrecord")
    
    # Initialize FileBasedMinuteMarketDataManager
    print(f"🔍 DEBUG: config.minute_data_base_path = {config.minute_data_base_path}")
    print(f"🔍 DEBUG: Using base_path: {config.minute_data_base_path}")
    minute_data_manager = FileBasedMinuteMarketDataManager(
        env=environment,
        base_path=config.minute_data_base_path
    )
    print(f"📊 Initialized minute data manager: {config.minute_data_base_path}")
    
    # ✅ INTERVAL-BASED CALLBACK APPROACH: Create IntervalBasedTrainingDataCallback
    training_callback = IntervalBasedTrainingDataCallback(
        symbols=args.symbols,
        config=config,
        storage_manager=storage_manager,
        output_dir=str(structured_output_dir)
    )
    
    # Inject minute data manager and configuration into callback for multi-timeframe processing
    run_timestamp = datetime.now()
    training_callback.minute_data_manager = minute_data_manager
    training_callback.start_date = start_date
    training_callback.end_date = end_date
    training_callback.run_timestamp = run_timestamp
    
    # Pass dataset_id to callback for completion tracking
    training_callback.dataset_id = dataset_id
    
    print(f"🎯 Created interval-based training data generation with multi-timeframe support")
    print(f"   Callback: {type(training_callback).__name__}")
    print(f"   Timeframes: {list(config.timeframes.keys())}")
    print(f"   Using FileBasedMinuteMarketDataManager for 1m base data")
    print(f"   Building aggregated timeframes: 5m, 15m, 1h, 1d, 1w from 1m data")
    
    # ✅ Use existing Runner framework with our callback
    runner = Runner(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        environment=environment,
        universe_id=args.universe_id or 1,
        callbacks=[training_callback],  # ONLY the callback
        base_duration=args.base_duration
    )
    
    print(f"\n🚀 Starting interval-based multi-timeframe training data generation")
    print(f"   Run ID: {run_id}")
    print(f"   Symbols: {args.symbols}")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Base duration: {args.base_duration}")
    
    try:
        # Execute the runner
        await runner.run()
        
        # CRITICAL: Validate generated files before marking as completed
        print(f"\n🔍 Validating generated training data...")
        validation_success = await validate_training_data_pipeline(
            run_id=run_id,
            dataset_id=dataset_id, 
            output_dir=structured_output_dir,
            symbols=args.symbols,
            environment=args.environment
        )
        
        if validation_success:
            # Update run status to completed only if validation passes
            await update_run_status(run_id, 'completed')
            
            print(f"\n✅ Training data generation and validation completed successfully!")
            print(f"   Run ID: {run_id}")
            print(f"   Dataset ID: {dataset_id}")
            print(f"   Output directory: {structured_output_dir}")
            print(f"   Files validated: ✅ Exist, ✅ Readable, ✅ API Compatible")
            
            return 0
        else:
            # Validation failed - mark as failed
            await update_run_status(run_id, 'failed', 'Training data validation failed - generated files not usable')
            
            print(f"\n❌ Training data generation failed validation!")
            print(f"   Run ID: {run_id}")
            print(f"   Files were generated but are not usable by the pipeline")
            
            return 1
        
    except Exception as e:
        # Update run status to failed with error message
        error_msg = str(e)
        await update_run_status(run_id, 'failed', error_msg)
        
        print(f"\n❌ Training data generation failed!")
        print(f"   Run ID: {run_id}")
        print(f"   Error: {error_msg}")
        
        import traceback
        traceback.print_exc()
        
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)