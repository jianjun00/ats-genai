"""
Data filtering, aggregation, and statistical analysis
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
    # RAY DISTRIBUTED COMPUTING INTEGRATION
    # ==============================================

    def get_training_dataset_sequences(self, dataset_id: int) -> Dict[str, Any]:
        """Get available sequences for a training dataset."""
        try:
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path

            # Determine environment and table name
            environment = "dev"  # Can be made configurable later
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, total_sequences, run_id,
                               dataset_path, symbol_files, file_metadata
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        return {
                            "datasets": [{"id": dataset_id, "error": "Dataset not found"}],
                            "sequences": [],
                            "total_count": 0
                        }

                    # Get sequences info from visualization data
                    sequences = []
                    total_count = 0

                    # Use file_metadata from database for precise sequence information
                    try:
                        file_metadata = dataset_info.get('file_metadata', {})

                        if file_metadata and file_metadata.get('files'):
                            logger.info(f"Using database file_metadata for dataset {dataset_id}")

                            sequence_id_counter = 0
                            files_info = file_metadata.get('files', [])

                            # Generate sequence-based menu items from file metadata
                            # Group files by sequence (symbol+daterange) instead of individual timeframes
                            sequences_by_id = {}

                            for file_info in files_info:
                                symbol = file_info.get('symbol')
                                timeframe = file_info.get('timeframe')
                                file_path = file_info.get('file_path', '')
                                file_size_bytes = file_info.get('file_size_bytes', 0)

                                # Extract sequence ID from file path (e.g., AAPL_20250701_000000_20250906_000000.arrayrecord)
                                if file_path.endswith('.arrayrecord'):
                                    sequence_id = file_path.replace('.arrayrecord', '')
                                else:
                                    sequence_id = f"{symbol}_sequence"

                                if sequence_id not in sequences_by_id:
                                    sequences_by_id[sequence_id] = {
                                        'symbol': symbol,
                                        'timeframes': {},
                                        'total_size': 0,
                                        'file_count': 0
                                    }

                                # Add this timeframe to the sequence
                                sequences_by_id[sequence_id]['timeframes'][timeframe] = {
                                    'file_path': file_path,
                                    'file_size_bytes': file_size_bytes
                                }
                                sequences_by_id[sequence_id]['total_size'] += file_size_bytes
                                sequences_by_id[sequence_id]['file_count'] += 1

                            # Create sequence menu items (one per symbol+daterange)
                            for sequence_id, sequence_info in sequences_by_id.items():
                                sequences.append({
                                    "id": sequence_id_counter,
                                    "sequence_id": sequence_id,
                                    "symbol": sequence_info['symbol'],
                                    "timeframes": list(sequence_info['timeframes'].keys()),
                                    "timeframe_count": len(sequence_info['timeframes']),
                                    "description": sequence_id,  # Show sequence ID as description
                                    "file_count": sequence_info['file_count'],
                                    "total_size_mb": round(sequence_info['total_size'] / (1024 * 1024), 2)
                                })
                                sequence_id_counter += 1

                            total_count = len(sequences)
                            logger.info(f"Generated {total_count} sequence-based menu items from file_metadata for dataset {dataset_id}")

                        else:
                            # Fallback: use filesystem scanning (legacy approach)
                            logger.warning(f"No file_metadata available for dataset {dataset_id}, using filesystem fallback")

                            symbols = dataset_info.get('symbols', [])
                            if isinstance(symbols, str):
                                if symbols.startswith('{') and symbols.endswith('}'):
                                    symbols = [s.strip() for s in symbols.strip('{}').split(',') if s.strip()]
                                else:
                                    symbols = [s.strip() for s in symbols.split(',') if s.strip()]

                            for symbol in symbols:
                                try:
                                    # Each symbol has exactly ONE sequence file
                                    # The sequence file contains multiple time steps/bars, not multiple sequences
                                    symbol_files = dataset_info.get('symbol_files', {})
                                    sequence_filename = symbol_files.get(symbol, f"{symbol}_20250701_000000_20250906_000000")

                                    sequences.append({
                                        "id": len(sequences),
                                        "sequence_id": sequence_filename,  # Use actual filename as sequence ID
                                        "symbol": symbol,
                                        "timeframe": "multi",
                                        "filename": f"{sequence_filename}.arrayrecord",
                                        "description": f"{symbol} Training Sequence ({sequence_filename})",
                                        "file_size_mb": 0.1
                                    })
                                except Exception as e:
                                    logger.error(f"Error processing symbol {symbol}: {e}")

                            total_count = len(sequences)

                    except Exception as e:
                        logger.error(f"Error getting visualization data for sequences: {e}")
                        total_count = 0

                    return {
                        "datasets": [dataset_info],
                        "sequences": sequences,
                        "total_count": total_count
                    }

        except Exception as e:
            logger.error(f"Error getting sequences for dataset {dataset_id}: {e}")
            return {
                "datasets": [{"id": dataset_id, "error": str(e)}],
                "sequences": [],
                "total_count": 0
            }


    def get_training_dataset_visualization_data(self, dataset_id: int, start_idx: int = 0, sequence_id: str = None, target_symbol: str = None) -> Dict[str, Any]:
        """Get visualization data for training dataset sequences (OHLC + indicators for Plotly charts)."""
        try:
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            import json

            # Determine environment and table name
            environment = "dev"  # Can be made configurable later
            table_name = f"{environment}_training_datasets"  # Fixed: plural form to match main API

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info using actual table columns from plural table
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, id, run_id
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        raise ValueError(f"Dataset {dataset_id} not found")

                    # Use actual run_id from database
                    run_id = dataset_info.get('run_id') or str(dataset_id)  # Use actual run_id or fallback to dataset_id
                    symbols_data = dataset_info.get('symbols', '')

                    # Parse symbols - could be string, array, or PostgreSQL array format
                    if isinstance(symbols_data, str):
                        if symbols_data.startswith('{') and symbols_data.endswith('}'):
                            # PostgreSQL array format: {TSLA} or {AAPL,TSLA}
                            symbols = [s.strip() for s in symbols_data.strip('{}').split(',') if s.strip()]
                        else:
                            # Comma-separated string
                            symbols = [s.strip() for s in symbols_data.split(',') if s.strip()]
                    else:
                        symbols = symbols_data if symbols_data else []

                    if not symbols:
                        raise ValueError(f"Dataset {dataset_id} missing symbols")

                    # Determine target symbol - use parameter if provided, otherwise logic
                    if target_symbol is None:
                        # If sequence_id provided, find specific sequence
                        if sequence_id is not None:
                            try:
                                seq_idx = int(sequence_id)
                                if seq_idx < len(symbols):
                                    target_symbol = symbols[seq_idx]
                                else:
                                    target_symbol = symbols[0]
                            except (ValueError, IndexError):
                                target_symbol = symbols[0]
                        else:
                            target_symbol = symbols[0]  # Default to first symbol

                    # Search for actual files in all potential locations (container-aware)
                    training_base_paths = [
                        Path("/data/training_data"),  # Container path
                        Path("/mnt/d/ats-data/training_data")  # Host path fallback
                    ]

                    arrayrecord_files = []
                    for base_path in training_base_paths:
                        if base_path.exists():
                            logger.info(f"Searching for {target_symbol} files in run {run_id} at: {base_path}")
                            # Search specifically in the run_id directory first
                            run_path = base_path / str(run_id)
                            if run_path.exists():
                                logger.info(f"Found run directory: {run_path}")
                                # Look for files in specific run directory
                                for arrayrecord_file in list(run_path.rglob("*.arrayrecord")):
                                    # Check if file contains our target symbol (case insensitive)
                                    file_name = arrayrecord_file.name.lower()
                                    symbol_lower = target_symbol.lower()

                                    logger.info(f"Checking file: {arrayrecord_file}, symbol_lower: {symbol_lower}")

                                    # Check filename for symbol match
                                    if symbol_lower in file_name:
                                        arrayrecord_files.append(arrayrecord_file)
                                        logger.info(f"Found matching file for run {run_id}: {arrayrecord_file}")
                                        break  # Use first match in correct run

                            # If not found in run directory, fallback to general search
                            if not arrayrecord_files:
                                logger.warning(f"No files found in run {run_id}, falling back to general search")
                                for arrayrecord_file in list(base_path.rglob("*.arrayrecord")):
                                    file_name = arrayrecord_file.name.lower()
                                    file_path_str = str(arrayrecord_file).lower()
                                    symbol_lower = target_symbol.lower()

                                    if symbol_lower in file_name or f"/{symbol_lower}/" in file_path_str:
                                        arrayrecord_files.append(arrayrecord_file)
                                        logger.info(f"Found fallback file: {arrayrecord_file}")
                                        break  # Use first match

                            # If files found in this base_path, stop searching other base_paths
                            if arrayrecord_files:
                                break

                    if arrayrecord_files:
                        # Read actual ArrayRecord data
                        arrayrecord_file = arrayrecord_files[0]
                        try:
                            from array_record.python.array_record_module import ArrayRecordReader

                            visualization_data = []
                            reader = ArrayRecordReader(str(arrayrecord_file))

                            # Read all records using proper ArrayRecord API
                            total_records = reader.num_records()
                            logger.info(f"ArrayRecord has {total_records} records")

                            columns = None
                            training_data_array = None

                            # Read records using seek + read
                            for i in range(total_records):
                                reader.seek(i)
                                record = reader.read()

                                if i == 0:
                                    # First record is column names
                                    try:
                                        columns_str = record.decode('utf-8')
                                        import ast
                                        columns = ast.literal_eval(columns_str)
                                        logger.info(f"ArrayRecord columns: {len(columns)} columns")
                                    except (UnicodeDecodeError, ValueError) as e:
                                        logger.warning(f"Could not parse columns from first record: {e}")

                                elif i == 1:
                                    # Second record is the training data array
                                    try:
                                        import numpy as np
                                        training_data_array = np.frombuffer(record, dtype=np.float32)
                                        logger.info(f"Training data array: {len(training_data_array)} elements")
                                        non_zero_count = np.count_nonzero(training_data_array)
                                        logger.info(f"Non-zero elements: {non_zero_count} / {len(training_data_array)} ({100*non_zero_count/len(training_data_array):.1f}%)")
                                    except Exception as e:
                                        logger.error(f"Error parsing training data: {e}")

                            reader.close()

                            # Process the training data array
                            if training_data_array is not None and columns is not None:
                                logger.info(f"Processing training data array with {len(columns)} columns")

                                # The training data is a flattened array - reshape it into records
                                num_features = len(columns) - 2  # Subtract timestamp and symbol columns
                                if len(training_data_array) >= num_features:
                                    # For visualization, we'll create a single record from the training data
                                    # Extract OHLCV data for different timeframes
                                    record_data = {}
                                    for j, col in enumerate(columns):
                                        if j < len(training_data_array):
                                            val = training_data_array[j]
                                            # Handle NaN values
                                            if np.isnan(val):
                                                val = 0.0
                                            record_data[col] = float(val)
                                        else:
                                            record_data[col] = 0.0

                                    # Create visualization data from the training record
                                    # Extract 5m timeframe data for visualization (first sequence)
                                    visualization_data = []

                                    # Look for 5m OHLCV columns
                                    for seq_idx in range(52):  # 5m has 52 time steps according to config
                                        # Calculate Unix timestamp for this bar in Eastern Time
                                        from datetime import datetime, timezone, timedelta
                                        from zoneinfo import ZoneInfo
                                        base_dt = datetime(2025, 7, 1, 2, 0, 0, tzinfo=ZoneInfo("America/New_York"))
                                        bar_dt = base_dt + timedelta(minutes=seq_idx * 5)

                                        bar_data = {
                                            "time_step": seq_idx,
                                            "timestamp": int(bar_dt.timestamp()),
                                            "symbol": record_data.get('symbol', target_symbol),
                                            "open": record_data.get(f'5m_open_{seq_idx:03d}', 0),
                                            "high": record_data.get(f'5m_high_{seq_idx:03d}', 0),
                                            "low": record_data.get(f'5m_low_{seq_idx:03d}', 0),
                                            "close": record_data.get(f'5m_close_{seq_idx:03d}', 0),
                                            "volume": record_data.get(f'5m_volume_{seq_idx:03d}', 0),
                                            "vwap": record_data.get(f'5m_vwap_{seq_idx:03d}', 0)
                                        }
                                        visualization_data.append(bar_data)

                                    logger.info(f"Created {len(visualization_data)} visualization bars from training data")
                                else:
                                    logger.error(f"Training data array too short: {len(training_data_array)} < {num_features}")
                                    raise ValueError("Insufficient training data")
                            else:
                                logger.error("No training data or columns found")
                                raise ValueError("No training data found")


                            # Calculate available sequences from visualization data
                            total_records = len(visualization_data)
                            window_size = 21  # Default window size for visualization
                            available_sequences = max(1, total_records - window_size + 1)

                            # ENFORCE: No fake data allowed - check response before returning
                            from services.fake_data_detector import fail_on_fake_data

                            response = {
                                "dataset_id": dataset_id,
                                "sequence_id": sequence_id,
                                "start_idx": start_idx,
                                "symbol": target_symbol,
                                "selected_bar": 10,  # Middle bar as selected
                                "sequence_length": len(visualization_data),
                                "total_sequences": available_sequences,
                                "total_records": total_records,
                                "data": visualization_data,
                                "source": "arrayrecord"
                            }

                            # Fail fast if fake data detected
                            fail_on_fake_data(response, f"visualization_data_response_dataset_{dataset_id}")

                            # Sanitize response to prevent NaN/Infinity JSON serialization errors
                            try:
                                from src.core.sanitizers.json_sanitizer import validate_api_response
                                response = validate_api_response(response)
                                logger.info("✅ Visualization API response sanitized for JSON safety")
                            except Exception as sanitizer_error:
                                logger.warning(f"Visualization response sanitization failed: {sanitizer_error}")
                                # Continue with original response if sanitization fails

                            return response

                        except ImportError as e:
                            logger.error(f"ArrayRecord import failed: {e}")
                            raise RuntimeError(f"ArrayRecord library not available: {e}. Cannot read training data without proper dependencies.")
                        except Exception as e:
                            logger.error(f"ArrayRecord reading failed: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                            raise RuntimeError(f"Failed to read training data file: {e}. No fallback data provided.")

                    # No files found
                    raise ValueError(f"No Riegeli/ArrayRecord files found for dataset {dataset_id}, symbol {target_symbol}")

        except Exception as e:
            logger.error(f"Error getting visualization data for dataset {dataset_id}: {e}")
            # No fake data - re-raise the error
            raise

    def get_training_dataset_sequence_multi_timeframe(self, dataset_id: int, sequence_id: str, row_index: int = 50) -> Dict[str, Any]:
        """Get multi-timeframe OHLC data for a specific sequence, showing 21 bars centered around row_index."""
        try:
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras
            from pathlib import Path
            import json

            logger.info(f"Getting multi-timeframe data for dataset {dataset_id}, sequence {sequence_id}")

            # Determine environment and table name
            environment = "dev"
            table_name = f"{environment}_training_datasets"

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    # Get dataset info including file_metadata
                    cursor.execute(f"""
                        SELECT dataset_name, symbols, run_id, file_metadata
                        FROM {table_name}
                        WHERE id = %s
                    """, (dataset_id,))

                    dataset_info = cursor.fetchone()
                    if not dataset_info:
                        return {"error": f"Dataset {dataset_id} not found"}

                    file_metadata = dataset_info.get('file_metadata', {})
                    run_id = dataset_info.get('run_id')

                    # Define training base paths (container-aware)
                    training_base_paths = [
                        Path("/data/training_data"),  # Container path
                        Path("/mnt/d/ats-data/training_data")  # Host path fallback
                    ]

                    # Find the sequence directory and read all timeframes
                    multi_timeframe_data = {}
                    timeframes = ['5m', '15m', '1h', '1d', '1w']

                    for base_path in training_base_paths:
                        sequence_dir = Path(base_path) / str(run_id) / sequence_id
                        logger.info(f"Checking sequence directory: {sequence_dir}")
                        if sequence_dir.exists():
                            logger.info(f"✅ Found sequence directory: {sequence_dir}")

                            for timeframe in timeframes:
                                timeframe_dir = sequence_dir / timeframe
                                arrayrecord_file = timeframe_dir / f"{sequence_id}.arrayrecord"
                                logger.info(f"Checking ArrayRecord file: {arrayrecord_file}")

                                if arrayrecord_file.exists():
                                    try:
                                        logger.info(f"✅ Found file: {arrayrecord_file}")
                                        # Read ArrayRecord data for this timeframe
                                        ohlc_data = self._read_arrayrecord_ohlc(arrayrecord_file)
                                        if ohlc_data:
                                            multi_timeframe_data[timeframe] = ohlc_data
                                            logger.info(f"✅ Loaded {len(ohlc_data)} OHLC bars for {timeframe}")
                                        else:
                                            logger.warning(f"⚠️  ArrayRecord file returned no data: {arrayrecord_file}")
                                    except Exception as e:
                                        logger.error(f"❌ Failed to read {timeframe} data from {arrayrecord_file}: {e}")
                                else:
                                    logger.warning(f"❌ ArrayRecord file not found: {arrayrecord_file}")

                            break  # Found sequence directory
                        else:
                            logger.warning(f"❌ Sequence directory not found: {sequence_dir}")

                    if not multi_timeframe_data:
                        return {"error": f"No data files found for sequence {sequence_id}"}

                    # Apply 21-bar selection logic (10 before + 1 current + 10 after) to all timeframes
                    logger.info(f"🎯 Applying 21-bar selection logic with row_index={row_index}")
                    for timeframe, data in multi_timeframe_data.items():
                        if data and len(data) > 0:
                            data_len = len(data)
                            logger.info(f"   {timeframe}: {data_len} bars available")

                            # If row_index is beyond data length, use all available data
                            if row_index >= data_len:
                                logger.warning(f"   {timeframe}: row_index {row_index} >= data_len {data_len}, using all available data")
                                start_idx = 0
                                end_idx = data_len
                            else:
                                # Calculate start and end indices for 21-bar window
                                start_idx = max(0, row_index - 10)
                                end_idx = min(data_len, row_index + 11)  # +11 to include row_index + 10 bars after

                                # Ensure we have 21 bars if possible
                                if end_idx - start_idx < 21 and data_len >= 21:
                                    if start_idx == 0:
                                        # If we're at the beginning, extend end
                                        end_idx = min(data_len, 21)
                                    elif end_idx == data_len:
                                        # If we're at the end, extend start backward
                                        start_idx = max(0, data_len - 21)

                                # Safety check: if we still have insufficient data, use all available
                                if data_len < 21:
                                    logger.info(f"   {timeframe}: Only {data_len} bars available, using all data instead of 21-bar window")
                                    start_idx = 0
                                    end_idx = data_len

                            # Apply the slice
                            selected_bars = data[start_idx:end_idx]
                            multi_timeframe_data[timeframe] = selected_bars

                            logger.info(f"   {timeframe}: Selected bars {start_idx}-{end_idx-1} ({len(selected_bars)} bars)")
                            if len(selected_bars) > 0:
                                actual_row_idx = row_index - start_idx
                                logger.info(f"   {timeframe}: Target row is now at index {actual_row_idx} in selected data")

                    # Prepare comprehensive table view data with all training features
                    table_data = []
                    feature_matrix = []

                    # Try to get comprehensive training data from ArrayRecord
                    for base_path in training_base_paths:
                        if not table_data:  # Only process first successful path
                            arrayrecord_path = Path(base_path) / str(run_id) / sequence_id / "1h" / f"{sequence_id}.arrayrecord"
                            if arrayrecord_path.exists():
                                try:
                                    from array_record.python.array_record_module import ArrayRecordReader
                                    import ast
                                    import numpy as np

                                    reader = ArrayRecordReader(str(arrayrecord_path))

                                    # Read column names
                                    reader.seek(0)
                                    columns_record = reader.read()
                                    columns = ast.literal_eval(columns_record.decode('utf-8'))

                                    # Read training data
                                    reader.seek(1)
                                    data_record = reader.read()
                                    training_array = np.frombuffer(data_record, dtype=np.float32)

                                    reader.close()

                                    # Create comprehensive feature matrix
                                    if len(training_array) == len(columns):
                                        # Create a single row with all features
                                        feature_row = {}
                                        for i, col_name in enumerate(columns):
                                            val = training_array[i]
                                            # Handle NaN values to prevent JSON serialization errors
                                            import math
                                            if math.isnan(val):
                                                # Special handling for specific fields
                                                if 'symbol' in col_name.lower():
                                                    # Symbol fields should be strings, not numeric
                                                    # Extract symbol from sequence_id (e.g., "AAPL_20250701_000000_20250906_000000" -> "AAPL")
                                                    symbol = sequence_id.split('_')[0] if sequence_id else 'UNKNOWN'
                                                    feature_row[col_name] = symbol
                                                else:
                                                    val = 0.0
                                                    feature_row[col_name] = float(val)
                                            else:
                                                feature_row[col_name] = float(val)

                                        feature_matrix.append(feature_row)
                                        logger.info(f"✅ Comprehensive feature data: {len(columns)} features extracted from ArrayRecord")

                                except Exception as e:
                                    logger.error(f"Error reading comprehensive training data: {e}")

                    # Prepare table data - use OHLC format for table display compatibility
                    # while still providing comprehensive features for detailed analysis
                    if '1h' in multi_timeframe_data and multi_timeframe_data['1h']:
                        # Use 1h OHLC data for table display (UI expects basic OHLC fields)
                        table_data = multi_timeframe_data['1h']
                        logger.info(f"✅ Table data prepared: {len(table_data)} rows from 1h OHLC data for table display")
                    else:
                        # Fallback to empty array if no OHLC data available
                        table_data = []
                        logger.warning("⚠️  No OHLC data available for table display")

                    # Store comprehensive features separately for advanced analysis
                    comprehensive_features = feature_matrix if feature_matrix else []

                    # Create response and sanitize for JSON safety
                    response = {
                        "sequence_id": sequence_id,
                        "dataset_name": dataset_info.get('dataset_name'),
                        "ohlc_data": multi_timeframe_data,
                        "table_data": table_data,  # OHLC data for table display
                        "comprehensive_features": comprehensive_features,  # All 962 training features
                        "available_timeframes": list(multi_timeframe_data.keys()),
                        "success": True
                    }

                    # Sanitize response to prevent NaN/Infinity JSON serialization errors
                    try:
                        from src.core.sanitizers.json_sanitizer import validate_api_response
                        response = validate_api_response(response)
                        logger.info("✅ API response sanitized for JSON safety")
                    except Exception as sanitizer_error:
                        logger.warning(f"Response sanitization failed: {sanitizer_error}")
                        # Continue with original response if sanitization fails

                    logger.info(f"🎯 MULTI-TIMEFRAME API RESPONSE DEBUG:")
                    logger.info(f"   Sequence ID: {response['sequence_id']}")
                    logger.info(f"   Dataset: {response['dataset_name']}")
                    logger.info(f"   OHLC timeframes: {list(response['ohlc_data'].keys())}")
                    logger.info(f"   OHLC data counts: {[(tf, len(data)) for tf, data in response['ohlc_data'].items()]}")
                    logger.info(f"   Table rows: {len(response['table_data'])}")
                    logger.info(f"   Success: {response['success']}")

                    return response

        except Exception as e:
            logger.error(f"Error getting multi-timeframe sequence data: {e}")
            return {"error": str(e)}

    def _read_arrayrecord_ohlc(self, file_path: Path) -> List[Dict]:
        """Read OHLC data from ArrayRecord file."""
        try:
            # Try to read ArrayRecord file with proper error handling
            from array_record.python.array_record_module import ArrayRecordReader

            ohlc_data = []
            reader = ArrayRecordReader(str(file_path))

            total_records = reader.num_records()
            logger.debug(f"ArrayRecord has {total_records} records")

            if total_records < 2:
                logger.warning(f"Insufficient records in ArrayRecord: {total_records}")
                return []

            columns = None
            training_data_array = None

            # Read column names (record 0)
            reader.seek(0)
            columns_record = reader.read()
            try:
                columns_str = columns_record.decode('utf-8')
                import ast
                columns = ast.literal_eval(columns_str)
                logger.debug(f"ArrayRecord columns: {len(columns)} columns")
            except Exception as e:
                logger.error(f"Failed to parse columns: {e}")
                reader.close()
                return []

            # Read training data (record 1)
            reader.seek(1)
            data_record = reader.read()
            try:
                import numpy as np
                training_data_array = np.frombuffer(data_record, dtype=np.float32)
                logger.debug(f"Training data array: {len(training_data_array)} elements")
            except Exception as e:
                logger.error(f"Failed to parse training data: {e}")
                reader.close()
                return []

            reader.close()

            # Convert training data array to OHLC records
            if training_data_array is not None and columns is not None:
                # Map training data array to column names
                record_data = {}
                for j, col in enumerate(columns):
                    if j < len(training_data_array):
                        val = training_data_array[j]
                        # Handle NaN values
                        import math
                        if math.isnan(val):
                            val = 0.0
                        record_data[col] = float(val)
                    else:
                        record_data[col] = 0.0

                # Extract OHLC data for the specific timeframe from column names
                # Determine which timeframe this file represents based on file path
                file_name = file_path.name
                if '/5m/' in str(file_path):
                    timeframe_prefix = '5m'
                    sequence_length = 52
                elif '/15m/' in str(file_path):
                    timeframe_prefix = '15m'
                    sequence_length = 52
                elif '/1h/' in str(file_path):
                    timeframe_prefix = '1h'
                    sequence_length = 24
                elif '/1d/' in str(file_path):
                    timeframe_prefix = '1d'
                    sequence_length = 20
                elif '/1w/' in str(file_path):
                    timeframe_prefix = '1w'
                    sequence_length = 12
                else:
                    timeframe_prefix = '5m'  # Default
                    sequence_length = 52

                # Create OHLC records for this timeframe
                for i in range(sequence_length):
                    # Calculate timestamp as Unix epoch seconds in Eastern Time
                    from datetime import datetime, timezone, timedelta
                    from zoneinfo import ZoneInfo
                    base_date = datetime(2025, 7, 1, 2, 0, 0, tzinfo=ZoneInfo("America/New_York"))
                    # Add time based on timeframe and index
                    if timeframe_prefix == '5m':
                        timestamp_dt = base_date + timedelta(minutes=i * 5)
                    elif timeframe_prefix == '15m':
                        timestamp_dt = base_date + timedelta(minutes=i * 15)
                    elif timeframe_prefix == '1h':
                        timestamp_dt = base_date + timedelta(hours=i)
                    elif timeframe_prefix == '1d':
                        timestamp_dt = base_date + timedelta(days=i)
                    elif timeframe_prefix == '1w':
                        timestamp_dt = base_date + timedelta(days=i * 7)
                    else:
                        timestamp_dt = base_date

                    ohlc_record = {
                        "timestamp": int(timestamp_dt.timestamp()),
                        "open": record_data.get(f'{timeframe_prefix}_open_{i:03d}', 0),
                        "high": record_data.get(f'{timeframe_prefix}_high_{i:03d}', 0),
                        "low": record_data.get(f'{timeframe_prefix}_low_{i:03d}', 0),
                        "close": record_data.get(f'{timeframe_prefix}_close_{i:03d}', 0),
                        "volume": record_data.get(f'{timeframe_prefix}_volume_{i:03d}', 0),
                        "vwap": record_data.get(f'{timeframe_prefix}_vwap_{i:03d}', 0)
                    }

                    # Only add records with non-zero data
                    if (ohlc_record["open"] != 0 or ohlc_record["high"] != 0 or
                        ohlc_record["low"] != 0 or ohlc_record["close"] != 0):
                        ohlc_data.append(ohlc_record)

                logger.debug(f"Extracted {len(ohlc_data)} OHLC records for {timeframe_prefix}")
            return ohlc_data

        except Exception as e:
            logger.error(f"Failed to read ArrayRecord file {file_path}: {e}")
            return []

    def get_bar_collection_metrics(self) -> Dict[str, Any]:
        """Get metrics about bars collected organized by collection time and bar time."""
        try:
            from src.core.database.connection_manager import get_raw_connection
            import psycopg2.extras

            with get_raw_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                    # Get metrics from all minute bar tables
                    minute_tables = ['dev_one_minute_live_polygon', 'dev_one_minute_live_tiingo', 'dev_one_minute_live_fmp']

                    collection_metrics = {}

                    for table_name in minute_tables:
                        try:
                            # Check if table exists
                            cursor.execute("""
                                SELECT COUNT(*) as count FROM information_schema.tables
                                WHERE table_name = %s AND table_schema = 'public'
                            """, (table_name,))

                            if cursor.fetchone()['count'] == 0:
                                continue  # Skip non-existent tables

                            # Bars collected per collection time (received_at hour)
                            cursor.execute(f"""
                                SELECT
                                    DATE_TRUNC('hour', received_at) as collection_hour,
                                    COUNT(*) as bars_collected,
                                    COUNT(DISTINCT symbol) as unique_symbols,
                                    AVG(data_latency_ms) as avg_latency_ms,
                                    AVG(quality_score) as avg_quality_score
                                FROM {table_name}
                                WHERE received_at >= NOW() - INTERVAL '24 hours'
                                GROUP BY DATE_TRUNC('hour', received_at)
                                ORDER BY collection_hour DESC
                                LIMIT 24
                            """)

                            collection_time_data = cursor.fetchall()

                            # Bars per bar time (timestamp hour)
                            cursor.execute(f"""
                                SELECT
                                    DATE_TRUNC('hour', timestamp) as bar_hour,
                                    COUNT(*) as bars_count,
                                    COUNT(DISTINCT symbol) as unique_symbols,
                                    AVG(volume) as avg_volume,
                                    AVG(CASE WHEN high_price > 0 THEN ((high_price - low_price) / high_price) * 100 ELSE 0 END) as avg_volatility_pct
                                FROM {table_name}
                                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                                GROUP BY DATE_TRUNC('hour', timestamp)
                                ORDER BY bar_hour DESC
                                LIMIT 24
                            """)

                            bar_time_data = cursor.fetchall()

                            # Overall table stats
                            cursor.execute(f"""
                                SELECT
                                    COUNT(*) as total_bars,
                                    COUNT(DISTINCT symbol) as total_symbols,
                                    MIN(timestamp) as earliest_bar,
                                    MAX(timestamp) as latest_bar,
                                    MIN(received_at) as first_collected,
                                    MAX(received_at) as last_collected,
                                    AVG(data_latency_ms) as avg_latency_ms,
                                    AVG(quality_score) as avg_quality_score
                                FROM {table_name}
                                WHERE timestamp >= NOW() - INTERVAL '7 days'
                            """)

                            overall_stats = cursor.fetchone()

                            collection_metrics[table_name] = {
                                'collection_time_metrics': [dict(row) for row in collection_time_data],
                                'bar_time_metrics': [dict(row) for row in bar_time_data],
                                'overall_stats': dict(overall_stats) if overall_stats else {},
                                'vendor': table_name.split('_')[-1].upper()  # Extract vendor name
                            }

                        except Exception as e:
                            logger.error(f"Error getting metrics for {table_name}: {e}")
                            collection_metrics[table_name] = {
                                'error': str(e),
                                'vendor': table_name.split('_')[-1].upper()
                            }

                    return {
                        'metrics': collection_metrics,
                        'timestamp': datetime.now().isoformat(),
                        'summary': self._generate_collection_summary(collection_metrics)
                    }

        except Exception as e:
            logger.error(f"Error getting bar collection metrics: {e}")
            return {
                'error': str(e),
                'metrics': {},
                'timestamp': datetime.now().isoformat()
            }

    def _generate_collection_summary(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics across all vendors."""
        total_bars = 0
        total_symbols = 0
        vendors_active = 0
        avg_latencies = []
        avg_quality_scores = []

        for table_name, data in metrics.items():
            if 'error' in data:
                continue

            overall_stats = data.get('overall_stats', {})
            if overall_stats.get('total_bars', 0) > 0:
                vendors_active += 1
                total_bars += overall_stats.get('total_bars', 0)
                total_symbols = max(total_symbols, overall_stats.get('total_symbols', 0))

                if overall_stats.get('avg_latency_ms'):
                    avg_latencies.append(float(overall_stats['avg_latency_ms']))
                if overall_stats.get('avg_quality_score'):
                    avg_quality_scores.append(float(overall_stats['avg_quality_score']))

        return {
            'total_bars_collected': total_bars,
            'total_unique_symbols': total_symbols,
            'active_vendors': vendors_active,
            'avg_latency_ms': sum(avg_latencies) / len(avg_latencies) if avg_latencies else 0,
            'avg_quality_score': sum(avg_quality_scores) / len(avg_quality_scores) if avg_quality_scores else 0
        }

    async def get_ray_analytics(self, dataset_id: str, analysis_type: str = "comprehensive") -> Dict[str, Any]:
        """Get distributed analytics using Ray if available."""
        if not self.ray_enabled:
            raise RuntimeError("Ray analytics service is not enabled - cannot perform distributed analytics")

        ray_service = get_ray_eda_service()
        return await ray_service.analyze_dataset(dataset_id, analysis_type)

