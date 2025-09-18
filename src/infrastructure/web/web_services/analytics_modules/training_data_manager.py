"""
Training dataset management and visualization
"""

#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from src.core.database.connection_manager import get_connection_manager
from src.core.config.settings import get_settings


    # ==============================================
    # TRAINING DATASET MANAGEMENT (from analytics_service.py)
    # ==============================================

    def get_training_datasets(self):
        """Get training datasets from database for dual-tab functionality."""
        try:
            from src.core.database.connection_manager import get_raw_connection

            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                from psycopg2.extras import RealDictCursor

                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = f"""
                    SELECT
                        id, dataset_name, total_sequences, sequence_length, feature_count,
                        label_count, data_quality_score, feature_completeness, label_completeness,
                        file_size_mb, technical_indicators, symbols, date_range_start,
                        date_range_end, created_at
                    FROM {table_name}
                    WHERE status IN ('completed', 'generating')
                    ORDER BY created_at DESC
                    LIMIT 50
                    """

                    cursor.execute(query)
                    datasets = cursor.fetchall()

                    # Convert to list of dictionaries for JSON serialization
                    datasets_list = []
                    for dataset in datasets:
                        dataset_dict = dict(dataset)
                        # Convert datetime objects to strings
                        if 'created_at' in dataset_dict and dataset_dict['created_at']:
                            dataset_dict['created_at'] = dataset_dict['created_at'].isoformat()
                        if 'date_range_start' in dataset_dict and dataset_dict['date_range_start']:
                            dataset_dict['date_range_start'] = dataset_dict['date_range_start'].isoformat()
                        if 'date_range_end' in dataset_dict and dataset_dict['date_range_end']:
                            dataset_dict['date_range_end'] = dataset_dict['date_range_end'].isoformat()

                        # Split symbols field into array if it's a string
                        if 'symbols' in dataset_dict and isinstance(dataset_dict['symbols'], str):
                            dataset_dict['symbols'] = [s.strip() for s in dataset_dict['symbols'].split(',') if s.strip()]

                        datasets_list.append(dataset_dict)

                    logger.info(f"Retrieved {len(datasets_list)} training datasets from {table_name}")
                    return {
                        'datasets': datasets_list,
                        'total_count': len(datasets_list)
                    }

        except Exception as e:
            logger.error(f"Error getting training datasets: {e}")
            return {
                'datasets': [],
                'total_count': 0,
                'error': str(e)
            }

    def get_training_dataset_sequence(self, dataset_id: int, row_index: int, timeframe: str = "5m") -> Dict[str, Any]:
        """Get training dataset sequence data for OHLC visualization."""
        try:
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            import numpy as np
            from pathlib import Path

            # Determine environment and table name - assume dev for now
            environment = "dev"  # Can be made configurable later
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, sequence_length, total_sequences,
                               data_format
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        raise ValueError(f"Dataset {dataset_id} not found")

                    # Try to load actual sequence data from files
                    try:
                        # Check for the generated Riegeli-compatible files
                        symbol = dataset_info['symbols'] or 'unknown'
                        symbol_lower = symbol.lower()

                        logger.info(f"Looking for training data files for symbol: {symbol} (dataset_id: {dataset_id})")

                        # Look for timeframe-specific training data files
                        # Priority order: Riegeli format > numpy files > fallback paths
                        possible_file_paths = [
                            # New structure: /data/training_data/<run_id>/<timeframe>/<symbol>_<startdatetime>_<enddatetime>.arrayrecord
                            f"/data/training_data/*/{timeframe}/{symbol_lower}_*.arrayrecord",
                            f"/data/training_data/*/*/{symbol_lower}_*.arrayrecord",
                            # Legacy numpy files (existing structure)
                            f"/data/training_data/arrayrecord_aapl_tsla_2025/{symbol_lower}_features.npy",
                            f"/data/training_data/{dataset_info['dataset_name'].lower()}/{symbol_lower}_features.npy",
                            f"/data/training_data/{dataset_id}/{symbol_lower}_features.npy",
                        ]

                        logger.info(f"Checking file paths for timeframe {timeframe}: {possible_file_paths}")

                        features_file = None
                        import glob

                        for path_pattern in possible_file_paths:
                            logger.info(f"Checking pattern: {path_pattern}")

                            # Handle glob patterns for new file structure
                            if '*' in path_pattern:
                                matching_files = glob.glob(path_pattern)
                                logger.info(f"Glob pattern {path_pattern} found {len(matching_files)} matches")
                                if matching_files:
                                    # Use the first match (could be enhanced to pick best match)
                                    features_file = matching_files[0]
                                    logger.info(f"Selected file from glob: {features_file}")
                                    break
                            else:
                                # Handle exact path matches (legacy numpy files)
                                if Path(path_pattern).exists():
                                    features_file = path_pattern
                                    logger.info(f"Found exact file: {path_pattern}")
                                    break
                                else:
                                    logger.info(f"File not found: {path_pattern}")

                        if features_file:
                            logger.info(f"Loading training data from: {features_file}")
                            # Load numpy features file
                            features = np.load(features_file)
                            logger.info(f"Loaded features shape: {features.shape}")

                            # Ensure row_index is within bounds
                            if row_index >= len(features):
                                row_index = min(len(features) - 1, 0)

                            # Get the sequence for the specified row
                            sequence = features[row_index]  # Shape should be (sequence_length, feature_count)

                            # Convert to OHLC format - features are ordered as:
                            # [open, high, low, close, volume, envelope_top, envelope_bot, pldot, datetime_features...]
                            # Enhanced to handle variable feature counts with new datetime features
                            ohlc_sequence = []
                            for i, time_step in enumerate(sequence):
                                bar = {
                                    "time_step": i,
                                    # Core OHLCV features (indices 0-4)
                                    "open": float(time_step[0]) if len(time_step) > 0 else 0.0,
                                    "high": float(time_step[1]) if len(time_step) > 1 else 0.0,
                                    "low": float(time_step[2]) if len(time_step) > 2 else 0.0,
                                    "close": float(time_step[3]) if len(time_step) > 3 else 0.0,
                                    "volume": int(time_step[4]) if len(time_step) > 4 else 0,
                                    # Technical indicators (indices 5-7)
                                    "envelope_top": float(time_step[5]) if len(time_step) > 5 else 0.0,
                                    "envelope_bot": float(time_step[6]) if len(time_step) > 6 else 0.0,
                                    "pldot": float(time_step[7]) if len(time_step) > 7 else 0.0,
                                    # Traditional technical indicators (indices 8-11, if present)
                                    "sma_20": float(time_step[8]) if len(time_step) > 8 else 0.0,
                                    "ema_12": float(time_step[9]) if len(time_step) > 9 else 0.0,
                                    "rsi_14": float(time_step[10]) if len(time_step) > 10 else 0.0,
                                    "macd": float(time_step[11]) if len(time_step) > 11 else 0.0,
                                    # Datetime features (indices 12+, if present)
                                    "datetime": str(time_step[12]) if len(time_step) > 12 else None,
                                    "hour_of_day_edt": int(time_step[13]) if len(time_step) > 13 else 0,
                                    "day_of_week": int(time_step[14]) if len(time_step) > 14 else 0,
                                    "week_of_month": int(time_step[15]) if len(time_step) > 15 else 0,
                                    "week_of_year": int(time_step[16]) if len(time_step) > 16 else 0,
                                    "year": int(time_step[17]) if len(time_step) > 17 else 2025,
                                }
                                ohlc_sequence.append(bar)

                            return {
                                "dataset_id": dataset_id,
                                "dataset_name": dataset_info['dataset_name'],
                                "row_index": row_index,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "selected_bar": min(10, len(ohlc_sequence) // 2),  # Middle bar or bar 10
                                "sequence_length": len(ohlc_sequence),
                                "total_sequences": dataset_info['total_sequences'],
                                "data": ohlc_sequence,
                                "source": "arrayrecord_compatible_numpy" if features_file.endswith('.npy') else "arrayrecord_format",
                                "file_path": features_file
                            }

                    except Exception as file_error:
                        logger.warning(f"Could not load actual sequence data: {file_error}")
                        # Fall back to sample data

                    # Generate sample data if actual data not available
                    sample_data = self._generate_sample_sequence_for_dataset(dataset_info)
                    sample_data.update({
                        "dataset_id": dataset_id,
                        "row_index": row_index,
                        "timeframe": timeframe,
                        "source": "sample_data"
                    })
                    return sample_data

        except Exception as e:
            logger.error(f"Error getting sequence data for dataset {dataset_id}: {e}")
            # Return sample data as fallback
            sample_data = self._generate_sample_sequence_for_dataset({
                'dataset_name': f'Dataset {dataset_id}',
                'symbols': 'DEMO',
                'sequence_length': 21
            }, dataset_id, row_index)
            sample_data['timeframe'] = timeframe
            return sample_data

    def _generate_sample_sequence_for_dataset(self, dataset_info: Dict, dataset_id: int = 0, row_index: int = 0) -> Dict[str, Any]:
        """Generate sample sequence data based on dataset info."""
        import random

        sequence_length = dataset_info.get('sequence_length', 21)
        symbol = dataset_info.get('symbols', 'DEMO').split(',')[0] if dataset_info.get('symbols') else 'DEMO'

        # Generate realistic OHLC sequence
        base_price = 150.0 + random.uniform(-50, 50)  # Start price
        sequence = []

        for i in range(sequence_length):
            # Simulate price movement
            change = random.uniform(-3.0, 3.0)
            base_price += change
            base_price = max(base_price, 10.0)  # Minimum price

            open_price = base_price + random.uniform(-1.0, 1.0)
            high_price = open_price + random.uniform(0, 4.0)
            low_price = open_price - random.uniform(0, 3.0)
            close_price = open_price + random.uniform(-2.0, 2.0)

            # Ensure OHLC consistency
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)

            # Technical indicators
            envelope_top = high_price * 1.025  # 2.5% above high
            envelope_bot = low_price * 0.975   # 2.5% below low
            pldot = low_price * 0.99 if i % 4 == 0 else 0  # Pivot low dots occasionally

            bar = {
                "time_step": i,
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": random.randint(50000, 2000000),
                "envelope_top": round(envelope_top, 2),
                "envelope_bot": round(envelope_bot, 2),
                "pldot": round(pldot, 2) if pldot > 0 else 0
            }
            sequence.append(bar)

        return {
            "dataset_id": dataset_id,
            "dataset_name": dataset_info.get('dataset_name', f'Dataset {dataset_id}'),
            "row_index": row_index,
            "symbol": symbol,
            "selected_bar": min(10, sequence_length // 2),  # Middle bar
            "sequence_length": sequence_length,
            "data": sequence
        }

