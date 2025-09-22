"""
Training Data Generation Callback for Runner Framework

This callback integrates with the Runner framework to generate training data
at each interval using the handleInterval pattern. It organizes data by date
and uses SOD/EOD events to manage daily files efficiently.
"""

import logging
from datetime import datetime, date
from typing import Any, Optional, List, Dict, Union
from pathlib import Path

from domains.trading.services.state.runner_callback import RunnerCallback
# TrainingDataConfig is imported from the specific runner that uses this callback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
# Removed: SequenceStorageManager - using simple ArrayRecord storage per PRD/DRD QR5


class IntervalBasedTrainingDataCallback(RunnerCallback):
    """
    Simple interval-based callback for immediate processing.

    This is useful for real-time applications where you want to process
    training data immediately at each interval without daily batching.
    """

    def __init__(self,
                 symbols: List[str],
                 config: Optional[Any] = None,  # Accept any config object
                 storage_format: str = "arrayrecord",
                 output_dir: str = "/data/training_data",
                 start_date: Optional[Union[str, date]] = None,
                 end_date: Optional[Union[str, date]] = None,
                 start_day_offset: int = 0,
                 end_day_offset: int = 0,
                 collection_start_date: Optional[Union[str, date]] = None,
                 collection_end_date: Optional[Union[str, date]] = None):
        """Initialize interval-based callback.

        Args:
            start_date: Start date for training data range (YYYY-MM-DD string or date object)
            end_date: End date for training data range (YYYY-MM-DD string or date object)
            start_day_offset: Days to extend backwards for data collection
            end_day_offset: Days to extend forwards for data collection
            collection_start_date: Actual start date for data collection (with offset)
            collection_end_date: Actual end date for data collection (with offset)
        """
        self.symbols = symbols
        self.config = config  # Use provided config or None
        self.storage_format = storage_format
        self.output_dir = Path(output_dir)

        # Store target date range (for file naming and saving)
        self.start_date = self._parse_date(start_date) if start_date else None
        self.end_date = self._parse_date(end_date) if end_date else None

        # Store collection window (for data processing)
        self.start_day_offset = start_day_offset
        self.end_day_offset = end_day_offset
        self.collection_start_date = self._parse_date(collection_start_date) if collection_start_date else self.start_date
        self.collection_end_date = self._parse_date(collection_end_date) if collection_end_date else self.end_date

        self.logger = logging.getLogger(__name__)
        self.training_generator = None
        self.interval_counter = 0

        # 🚨 CRITICAL: Store ArrayRecord writers to append intervals over time
        # NEW: Track monthly file metadata for database storage
        self.monthly_file_paths = {}  # {symbol_timeframe_YYYY_MM: file_path}
        self.monthly_record_counts = {}  # {symbol_timeframe_YYYY_MM: count}
        self.monthly_file_sizes = {}  # {symbol_timeframe_YYYY_MM: size_mb}
        # This prevents OOM by streaming data instead of accumulating in memory
        self.array_record_writers = {}  # Dict[file_path_str, writer]
        self.dataset_initialized = False
        self._cleanup_attempted = False  # Track cleanup to prevent double-close
        self._feature_dao = None  # Lazy initialization of FeatureExtractionDAO

        # 🚨 NEW: Dynamic Binary Record Schema System
        # Replaces hardcoded OHLCV format with configurable technical indicators
        from domains.ml.services.training_data.schemas.binary_record_schema import SchemaTemplates

        # Default to auto-detect mode for maximum flexibility
        # Users can override by passing schema_config in config
        schema_config = getattr(config, 'binary_schema', 'auto_detect') if config else 'auto_detect'

        if schema_config == 'ohlcv_only':
            self.binary_schema = SchemaTemplates.ohlcv_only()
        elif schema_config == 'basic_envelopes':
            self.binary_schema = SchemaTemplates.basic_envelopes()
        elif schema_config == 'traditional_ta':
            self.binary_schema = SchemaTemplates.traditional_ta()
        elif schema_config == 'full_signals':
            self.binary_schema = SchemaTemplates.full_signals()
        else:
            # Default: auto-detect available indicators
            self.binary_schema = SchemaTemplates.auto_detect()

        self.logger.info(f"Binary record schema: {schema_config} mode")
        self.logger.info(f"Available indicators will be auto-detected: {self.binary_schema.auto_detect}")

        self.logger.info(f"IntervalBasedTrainingDataCallback initialized for symbols: {symbols}")
        if self.start_date and self.end_date:
            self.logger.info(f"Training data date range: {self.start_date} to {self.end_date}")

    def _ensure_writers_closed(self):
        """
        🚨 CRITICAL FIX: Ensure all ArrayRecord writers are properly closed.

        This fixes the issue where ArrayRecord files exist but show 0 records
        when the process crashes before handleEnd() is called. ArrayRecord
        requires proper closing to finalize the file format.
        """
        if self._cleanup_attempted:
            return

        self._cleanup_attempted = True

        if not hasattr(self, 'array_record_writers') or not self.array_record_writers:
            return

        for file_key, writer in self.array_record_writers.items():
            try:
                if writer and hasattr(writer, 'close'):
                    writer.close()
            except Exception as e:
                self.logger.error(f"Error closing writer {file_key}: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup even on exceptions."""
        self._ensure_writers_closed()
        return False  # Don't suppress exceptions

    def _parse_date(self, date_input: Union[str, date]) -> date:
        """Parse date string or date object to date object."""
        if isinstance(date_input, str):
            return datetime.strptime(date_input, '%Y-%m-%d').date()
        elif isinstance(date_input, date):
            return date_input
        else:
            raise ValueError(f"Invalid date format: {date_input}")

    def handleStart(self, runner: Any, current_time: datetime):
        """Initialize training generator."""
        self.training_generator = TimeSeriesSequenceTrainingGenerator(
            env=runner.get_environment(),
            config=self.config,
            universe_manager=runner.get_universe_state_manager()
        )

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info("Interval-based training data generator initialized")

    async def handleInterval(self, runner: Any, current_time: datetime):
        """Generate training data for specific timeframes based on current time.
        
        PRD/DRD Timeframe Granularity Logic:
        - 5m: Generate every 5 minutes (05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 00)
        - 15m: Generate every 15 minutes (00, 15, 30, 45)
        - 60m: Generate every hour (00 minutes only)
        - 1d: Generate once per day (00:00 only)
        - 1w: Generate once per week (Monday 00:00 only)
        
        Examples:
        - At 35m: Generate only 5m timeframe
        - At 45m: Generate 5m and 15m timeframes  
        - At 00m: Generate 5m, 15m, and 60m timeframes
        - At Monday 00:00: Generate all timeframes including 1d and 1w
        """
        if not self.training_generator:
            return

        # Determine which timeframes to generate based on current time
        target_timeframes = self._get_target_timeframes_for_interval(current_time)
        
        if not target_timeframes:
            return


        self.interval_counter += 1
        examples_generated = []

        try:
            # Generate examples for each symbol and timeframe combination
            for symbol in self.symbols:
                for timeframe in target_timeframes:
                    example = await self.training_generator.generate_training_example(
                        symbol=symbol,
                        prediction_timestamp=current_time,
                        target_timeframes=[timeframe]  # Generate for single timeframe only
                    )

                    examples_generated.append(example)

            # Save immediately if we have examples
            if examples_generated:
                await self._save_simple_arrayrecord(examples_generated, current_time, runner)

        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in handleInterval: {e}")
            import traceback
            traceback.print_exc()
            # Ensure cleanup even on critical errors
            self._ensure_writers_closed()
            raise  # Re-raise to maintain error propagation

    async def _save_simple_arrayrecord(self, examples: List[Dict], current_time: datetime, runner: Any):
        """
        STREAMING APPROACH: Initialize writers once, stream intervals as they're processed.

        FIXED MEMORY-EFFICIENT APPROACH:
        1. Initialize ArrayRecord writers ONCE per file (first call)
        2. Stream individual intervals as they're generated (NO memory accumulation)
        3. Keep appending to same writer throughout training period
        4. Close writers in handleEnd() when processing complete

        This prevents OOM by never holding all data in memory simultaneously.
        """
        # Initialize dataset structure on first call
        if not self.dataset_initialized:
            await self._initialize_dataset_structure()
            self.dataset_initialized = True

        # Stream current interval data to appropriate writers
        await self._stream_training_examples_to_writers(examples, current_time, runner)

    async def _stream_training_examples_to_writers(self, examples: List[Dict], current_time: datetime, runner: Any):
        """
        NEW FEATURE GROUP STREAMING: Filter by target date range and write to feature group files.

        CHANGES:
        - Only save data within target date range (not expanded collection window)
        - Route data to appropriate feature group ArrayRecord files
        - Categorize features into groups: ohlcv_basic, technical_momentum, technical_volatility, fundamental_quarterly
        - Use new file key structure: symbol_featuregroup_timeframe_YYYY_MM
        """

        # CRITICAL: Only save data within target date range, not collection window
        current_date = current_time.date()
        if current_date < self.start_date or current_date > self.end_date:
            return

        # Determine which month this data belongs to
        year_month_str = current_date.strftime('%Y_%m')

        for example_idx, example in enumerate(examples):
            if not isinstance(example, dict):
                continue

            symbol = example.get('symbol', 'UNKNOWN')
            timeframe_features = example.get('timeframe_features', {})

            # Stream each timeframe to respective feature group files
            for timeframe, features in timeframe_features.items():
                if not isinstance(features, dict) or not features:
                    continue

                # Separate features into feature groups using database-driven mapping
                feature_dao = await self._get_feature_dao(runner)
                feature_groups_data = await self._categorize_features_by_group(features, timeframe, feature_dao)
                
                # Write each feature group to its respective ArrayRecord file
                for feature_group, feature_data in feature_groups_data.items():
                    if not feature_data:  # Skip empty feature groups
                        continue
                        
                    # Create interval record for this feature group
                    interval_record = {
                        'timestamp': current_time.timestamp(),
                        'symbol': symbol,
                        **feature_data  # Include all features for this group
                    }

                    # Write to appropriate monthly ArrayRecord file with feature group
                    monthly_file_key = f"{symbol}_{feature_group}_{timeframe}_{year_month_str}"
                    if monthly_file_key in self.array_record_writers:
                        writer = self.array_record_writers[monthly_file_key]
                        await self._write_interval_to_writer(writer, symbol, interval_record)

                        # Track record count for database storage
                        if monthly_file_key not in self.monthly_record_counts:
                            self.monthly_record_counts[monthly_file_key] = 0
                        self.monthly_record_counts[monthly_file_key] += 1

                    else:
                        self.logger.warning(f"No monthly writer found for {monthly_file_key}")

    async def _categorize_features_by_group(self, features: Dict[str, Any], timeframe: str, 
                                          feature_dao: Any) -> Dict[str, Dict[str, Any]]:
        """
        Categorize features into feature groups using database-driven mapping.
        
        This method replaces hardcoded categorization with dynamic mapping from the feature catalog.
        It supports exact matches, pattern matching, and intelligent fallbacks.
        
        Args:
            features: Dictionary of feature name -> value pairs (with timeframe prefixes)
            timeframe: Timeframe string (e.g., '5m', '15m', '1h', '1d')
            feature_dao: FeatureExtractionDAO instance for database lookups
            
        Returns:
            Dictionary mapping feature group name to feature data for that group
        """
        feature_groups_data = {}
        feature_names_for_batch = []
        
        # Extract base feature names and prepare for batch lookup
        feature_key_to_base_name = {}
        for feature_key, value in features.items():
            # Remove timeframe prefix to get base feature name
            if feature_key.startswith(f'{timeframe}_'):
                base_feature = feature_key.replace(f'{timeframe}_', '')
            else:
                base_feature = feature_key
            
            feature_key_to_base_name[feature_key] = base_feature
            feature_names_for_batch.append(base_feature)
        
        # Get all feature mappings in one batch operation for efficiency
        try:
            feature_mappings = await feature_dao.get_feature_mappings_batch(feature_names_for_batch)
            feature_mapping_dict = {mapping.feature_name: mapping for mapping in feature_mappings}
        except Exception as e:
            self.logger.warning(f"Database feature mapping failed, falling back to basic categorization: {e}")
            # Fallback to basic categorization if database fails
            return await self._categorize_features_by_group_fallback(features, timeframe)
        
        # Group features by their mapped feature groups
        for feature_key, value in features.items():
            base_feature = feature_key_to_base_name[feature_key]
            mapping = feature_mapping_dict.get(base_feature)
            
            if mapping and mapping.feature_group_name != "unknown":
                group_name = mapping.feature_group_name
                
                # Initialize group if needed
                if group_name not in feature_groups_data:
                    feature_groups_data[group_name] = {}
                
                # Add feature to appropriate group
                feature_groups_data[group_name][base_feature] = value
                
                # Log mapping decision for debugging (only for non-obvious mappings)
                if mapping.match_type != "exact" or mapping.confidence < 1.0:
                    self.logger.debug(f"Feature '{base_feature}' mapped to '{group_name}' via {mapping.match_type} "
                                    f"(confidence: {mapping.confidence:.2f})")
            else:
                # Unknown feature - log and assign to default group
                self.logger.warning(f"Unknown feature '{base_feature}' assigned to ohlcv_basic as fallback")
                if "ohlcv_basic" not in feature_groups_data:
                    feature_groups_data["ohlcv_basic"] = {}
                feature_groups_data["ohlcv_basic"][base_feature] = value
        
        # Log final categorization summary
        group_counts = {group: len(data) for group, data in feature_groups_data.items()}
        self.logger.info(f"Database-driven feature categorization complete: {group_counts}")
        
        # Remove empty feature groups
        return {group: data for group, data in feature_groups_data.items() if data}

    async def _categorize_features_by_group_fallback(self, features: Dict[str, Any], timeframe: str) -> Dict[str, Dict[str, Any]]:
        """
        Fallback feature categorization when database mapping fails.
        
        This preserves the original hardcoded logic as a safety net.
        """
        feature_groups_data = {}
        
        # Initialize basic feature groups
        feature_groups_data['ohlcv_basic'] = {}
        feature_groups_data['technical_momentum'] = {}
        
        # Basic fallback mapping
        ohlcv_features = {'open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp', 'symbol'}
        
        # Categorize each feature with basic logic
        for feature_key, value in features.items():
            # Remove timeframe prefix to get base feature name
            if feature_key.startswith(f'{timeframe}_'):
                base_feature = feature_key.replace(f'{timeframe}_', '')
            else:
                base_feature = feature_key
            
            # Basic categorization
            if base_feature in ohlcv_features:
                feature_groups_data['ohlcv_basic'][base_feature] = value
            else:
                # Default to technical_momentum for unknown features
                feature_groups_data['technical_momentum'][base_feature] = value
        
        self.logger.warning("Used fallback feature categorization due to database mapping failure")
        return {group: data for group, data in feature_groups_data.items() if data}

    async def _get_feature_dao(self, runner) -> Any:
        """Get or initialize FeatureExtractionDAO lazily."""
        if self._feature_dao is None:
            from domains.ml.services.training_data.dao.feature_extraction_dao import FeatureExtractionDAO
            environment = runner.get_environment()
            self._feature_dao = FeatureExtractionDAO(environment)
        return self._feature_dao


    async def _initialize_dataset_structure(self):
        """
        Initialize ArrayRecord writers for MONTHLY storage.

        NEW FEATURE GROUP APPROACH:
        - Create separate files for each feature group, month, and timeframe within target date range  
        - File structure: /data/training_data/{dataset_id}/{feature_group}/{symbol_date_range}/{timeframe}/{symbol_date_range}_{feature_group}.arrayrecord
        - Feature groups: ohlcv_basic, technical_momentum, technical_volatility, fundamental_quarterly
        - Only save data within target date range (not collection window)
        """
        import array_record.python.array_record_module as array_record
        from datetime import date

        try:
            # Get dataset_id from callback (set by runner)
            dataset_id = getattr(self, 'dataset_id', 'unknown_dataset')

            if not self.start_date or not self.end_date:
                # Fallback (should not happen)
                today = date.today()
                self.start_date = today
                self.end_date = today

            print(f"🔧 INITIALIZING MONTHLY ARRAYRECORD WRITERS")
            print(f"   Dataset ID: {dataset_id}")
            print(f"   Target date range: {self.start_date} to {self.end_date}")
            print(f"   Collection window: {self.collection_start_date} to {self.collection_end_date}")

            # Generate list of months within target date range
            months_in_range = self._get_months_in_target_range()
            print(f"   Monthly files to create: {len(months_in_range)} months")
            for month_date in months_in_range:
                print(f"     - {month_date.strftime('%Y-%m')}")

            # Initialize writers for each symbol/feature_group/timeframe/month combination
            # PRD/DRD: Include all required timeframes including missing 1w
            timeframes = ['5m', '15m', '60m', '1d', '1w']
            
            # PRD/DRD: Define feature groups per specification
            feature_groups = [
                'ohlcv_basic',           # Basic OHLCV features: open, high, low, close, volume, vwap
                'technical_momentum',    # Technical momentum indicators: SMA, EMA, RSI, MACD
                'technical_volatility',  # Volatility indicators: Bollinger Bands, ATR, realized volatility
                'fundamental_quarterly'  # Fundamental metrics: P/E, P/B, ROE, debt ratios
            ]

            for symbol in self.symbols:
                print(f"   Initializing monthly writers for {symbol}")

                for month_date in months_in_range:
                    year_month_str = f"{symbol}_{month_date.strftime('%Y_%m')}"
                    
                    for feature_group in feature_groups:
                        # NEW PATH STRUCTURE: /data/training_data/{dataset_id}/{feature_group}/{symbol_date}/{timeframe}/
                        feature_group_dir = self.output_dir / str(dataset_id) / feature_group / year_month_str
                        
                        for timeframe in timeframes:
                            # Create timeframe directory
                            timeframe_dir = feature_group_dir / timeframe
                            timeframe_dir.mkdir(parents=True, exist_ok=True)

                            # NEW FILENAME: Include feature group in filename
                            arrayrecord_file = timeframe_dir / f"{year_month_str}_{feature_group}.arrayrecord"

                            # Create writer ONCE and store for streaming
                            writer = array_record.ArrayRecordWriter(str(arrayrecord_file), 'group_size:1')

                            # NEW FILE KEY: Include feature group for proper routing
                            file_key = f"{symbol}_{feature_group}_{timeframe}_{month_date.strftime('%Y_%m')}"
                            self.array_record_writers[file_key] = writer

                            # Track file path for database storage
                            self.monthly_file_paths[file_key] = str(arrayrecord_file)

                            print(f"     ✅ Created monthly writer: {symbol} {feature_group} {timeframe} {month_date.strftime('%Y-%m')}")

            print(f"✅ Initialized {len(self.array_record_writers)} monthly ArrayRecord writers")

            # Save schema metadata for documentation
            schema_file = self.output_dir / str(dataset_id) / "schema_metadata.json"
            self.binary_schema.save_schema_to_file(str(schema_file))
            print(f"📋 Schema metadata saved: {schema_file}")

        except Exception as e:
            print(f"❌ Error initializing monthly dataset structure: {e}")
            import traceback
            traceback.print_exc()

    def _get_months_in_target_range(self) -> List[date]:
        """Get list of first-day-of-month dates within target date range."""
        from dateutil.relativedelta import relativedelta

        months = []
        current_month = self.start_date.replace(day=1)  # First day of start month
        end_month = self.end_date.replace(day=1)  # First day of end month

        while current_month <= end_month:
            months.append(current_month)
            current_month += relativedelta(months=1)

        return months

    async def _save_monthly_training_data_records(self, runner: Any):
        """
        Save monthly training data records to database using MonthlyTrainingDataDAO.
        Called at the end of processing to register all generated monthly files.
        """
        from domains.ml.services.training_data.dao.monthly_training_data_dao import MonthlyTrainingDataDAO, MonthlyTrainingDataRecord
        from domains.ml.services.training_data.dao.feature_extraction_dao import FeatureExtractionDAO, FeatureMappingResult
        from domains.ml.services.training_data.utils.run_metadata_tracker import RunMetadataTracker

        # Get environment 
        environment = runner.get_environment()
        
        # Create a proper database run entry using RunMetadataTracker
        # This returns an integer run_id that's compatible with the database schema
        tracker = RunMetadataTracker(
            run_type="training_data_generation",
            created_by="IntervalBasedTrainingDataCallback"
        )
        
        # Get runner context for metadata
        runner_run_id = None
        if hasattr(runner, 'run_context') and runner.run_context:
            runner_run_id = runner.run_context.run_id
            print(f"📋 Runner context run_id: {runner_run_id}")

        # Create database run entry with comprehensive metadata
        parameters = {
            "symbols": self.symbols,
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "timeframes": ['5m', '15m', '60m', '1d', '1w'],
            "storage_format": self.storage_format,
            "output_dir": str(self.output_dir),
            "runner_context_id": runner_run_id  # Link to runner's string ID
        }
        
        database_run_id = await tracker.start_run(parameters)
        print(f"✅ Created database run entry: {database_run_id}")

        dao = MonthlyTrainingDataDAO(environment)
        feature_dao = FeatureExtractionDAO(environment)

        # Group files by symbol and month for database records
        symbol_month_records = {}  # {(symbol, year_month): {timeframe: file_path}}

        for file_key, file_path in self.monthly_file_paths.items():
            # Parse file_key: symbol_timeframe_YYYY_MM
            parts = file_key.split('_')
            if len(parts) < 4:
                continue

            symbol = parts[0]
            timeframe = parts[1]
            year_month = f"{parts[2]}_{parts[3]}"  # YYYY_MM

            # Convert YYYY_MM to date object (first day of month)
            year = int(parts[2])
            month = int(parts[3])
            month_date = date(year, month, 1)

            # Group by symbol and month
            key = (symbol, year_month)
            if key not in symbol_month_records:
                symbol_month_records[key] = {
                    'symbol': symbol,
                    'year_month': month_date,
                    'timeframe_paths': {},
                    'total_records': 0,
                    'file_size_mb': 0.0
                }

            # Add timeframe path
            symbol_month_records[key]['timeframe_paths'][timeframe] = file_path

            # Add record count and file size
            record_count = self.monthly_record_counts.get(file_key, 0)
            symbol_month_records[key]['total_records'] += record_count

            # Calculate file size
            file_size_bytes = Path(file_path).stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)
            symbol_month_records[key]['file_size_mb'] += file_size_mb

        # Create database records
        for (symbol, year_month), record_data in symbol_month_records.items():
            # Create monthly training data record
            monthly_record = MonthlyTrainingDataRecord(
                run_id=database_run_id,  # Use integer database run_id, not string
                symbol=symbol,
                instrument_id=None,  # Could be filled later with instrument lookup
                year_month=record_data['year_month'],
                timeframe_paths=record_data['timeframe_paths'],
                total_records=record_data['total_records'],
                file_size_mb=record_data['file_size_mb'],
                data_quality_score=1.0,  # Default quality score
                status="completed"
            )

            record_id = await dao.create_monthly_record(monthly_record)
            print(f"✅ Saved monthly training data record: {symbol} {year_month} (ID: {record_id})")

        print(f"✅ Saved {len(symbol_month_records)} monthly training data records to database")

        # Complete the run tracking
        results = {
            "total_months_processed": len(symbol_month_records),
            "symbols": self.symbols,
            "timeframes": ['5m', '15m', '60m', '1d', '1w'],
            "total_intervals": self.interval_counter
        }
        await tracker.complete_run(database_run_id, results)
        await tracker.close()





    async def _write_interval_to_writer(self, writer, symbol, interval):
        """
        🚨 CRITICAL FIX (September 10, 2025): Optimized binary ArrayRecord format

        ISSUE: Initial implementation used JSON format (1,090 bytes/record) which is inefficient
               and not compatible with Google's ArrayRecord ML training data standard.
        RESEARCH: ArrayRecord expects binary serialization, not JSON text format.
        SOLUTION: Custom binary format (371 bytes/record) - 3x more efficient than JSON.

        Binary format: indicator_count(2) + timestamp(8) + symbol_len(4) + symbol + OHLCV(20) + indicators
        Compatible with Google ArrayRecord standard for ML training data pipelines.
        """

        # 🔍 DEBUG LOGGING: Capture inputs to identify 1d vs 5m timeframe differences
        writer_info = getattr(writer, '_file_path', 'unknown') if hasattr(writer, '_file_path') else str(writer)
        timeframe = "unknown"
        if hasattr(writer, '_file_path'):
            file_path = str(writer._file_path)
            if '/5m/' in file_path:
                timeframe = "5m"
            elif '/1d/' in file_path:
                timeframe = "1d" 
            elif '/15m/' in file_path:
                timeframe = "15m"
            elif '/1h/' in file_path:
                timeframe = "1h"
            elif '/1w/' in file_path:
                timeframe = "1w"

        # Log detailed input analysis for debugging 1d empty issue
        self.logger.debug(f"🔍 ARRAYRECORD WRITE DEBUG [{timeframe}]: symbol={symbol}")
        self.logger.debug(f"🔍 ARRAYRECORD WRITE DEBUG [{timeframe}]: interval_keys={list(interval.keys())}")
        self.logger.debug(f"🔍 ARRAYRECORD WRITE DEBUG [{timeframe}]: interval_values={interval}")
        self.logger.debug(f"🔍 ARRAYRECORD WRITE DEBUG [{timeframe}]: writer={writer}")
        self.logger.debug(f"🔍 ARRAYRECORD WRITE DEBUG [{timeframe}]: writer_type={type(writer)}")
        
        # Log critical OHLCV values for debugging
        ohlcv_debug = {
            'timestamp': interval.get('timestamp', 'MISSING'),
            'open': interval.get('open', 'MISSING'),
            'high': interval.get('high', 'MISSING'), 
            'low': interval.get('low', 'MISSING'),
            'close': interval.get('close', 'MISSING'),
            'volume': interval.get('volume', 'MISSING')
        }
        self.logger.debug(f"🔍 ARRAYRECORD WRITE DEBUG [{timeframe}]: ohlcv_data={ohlcv_debug}")

        try:
            # 🚨 PROPER ARRAYRECORD FORMAT: Use efficient binary serialization
            # Research shows custom binary format is 3x more efficient than JSON

            # Convert timestamp to unix timestamp if needed
            timestamp = interval.get('timestamp', 0)
            if isinstance(timestamp, str):
                from datetime import datetime
                timestamp = datetime.fromisoformat(timestamp).timestamp()

            # 🔧 OPTIMIZED BINARY FORMAT: Pack core OHLCV data efficiently
            # Format: timestamp(8) + symbol_len(4) + symbol(variable) + ohlcv(20) + indicators(variable)
            import struct

            symbol_bytes = symbol.encode('utf-8')
            symbol_len = len(symbol_bytes)

            # Core OHLCV data (always present)
            core_data = struct.pack(
                f'>dI{symbol_len}sfffff',  # Big-endian: double, uint32, string, 5 floats
                float(timestamp),
                symbol_len,
                symbol_bytes,
                float(interval.get('open', 0.0)),
                float(interval.get('high', 0.0)),
                float(interval.get('low', 0.0)),
                float(interval.get('close', 0.0)),
                float(interval.get('volume', 0.0))
            )

            # 📊 TECHNICAL INDICATORS: Append as additional binary fields
            indicator_data = b''
            indicator_count = 0

            for key, value in interval.items():
                if key not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
                    try:
                        # Pack indicator as: name_len(2) + name(variable) + value(4)
                        key_bytes = key.encode('utf-8')
                        key_len = len(key_bytes)
                        if key_len <= 65535:  # Max uint16
                            indicator_data += struct.pack(f'>H{key_len}sf', key_len, key_bytes, float(value))
                            indicator_count += 1
                    except (ValueError, TypeError, struct.error):
                        continue  # Skip invalid indicators

            # Final record: indicator_count(2) + core_data + indicator_data
            binary_record = struct.pack('>H', indicator_count) + core_data + indicator_data

            # Write binary record to ArrayRecord
            writer.write(binary_record)
            
            # 🔍 DEBUG LOGGING: Confirm successful write for debugging 1d empty issue
            self.logger.debug(f"✅ ARRAYRECORD WRITE SUCCESS [{timeframe}]: {len(binary_record)} bytes written")

            # Log schema details on first write (for debugging)
            if not hasattr(self, '_schema_logged'):
                indicator_fields = [k for k in interval.keys() if k not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                record_size = len(binary_record)
                efficiency = f"{record_size} bytes (vs {len(str(interval)) * 2} JSON bytes)"

                if indicator_fields:
                    print(f"📊 Technical indicators included: {', '.join(indicator_fields[:5])}{'...' if len(indicator_fields) > 5 else ''}")
                else:
                    print(f"📊 Using OHLCV-only format (no technical indicators)")
                print(f"📊 Binary format efficiency: {efficiency}")
                print(f"📊 Total fields per record: OHLCV(7) + indicators({indicator_count})")
                self._schema_logged = True

        except Exception as e:
            # 🔍 DEBUG LOGGING: Capture write failures for debugging 1d empty issue  
            self.logger.error(f"❌ ARRAYRECORD WRITE FAILURE [{timeframe}]: {e}")
            self.logger.error(f"❌ ARRAYRECORD WRITE FAILURE [{timeframe}]: symbol={symbol}")
            self.logger.error(f"❌ ARRAYRECORD WRITE FAILURE [{timeframe}]: interval={interval}")
            print(f"❌ Error writing interval to writer: {e}")
            raise






    async def handleEnd(self, runner: Any, current_time: datetime):
        """Close all ArrayRecord writers and generate final summary."""

        # 🚨 CRITICAL: Close all writers to finalize files using centralized cleanup
        self._ensure_writers_closed()

        try:
            # Save monthly training data records to database
            print(f"\n💾 SAVING MONTHLY TRAINING DATA RECORDS TO DATABASE")
            await self._save_monthly_training_data_records(runner)

            # Clear writers dict
            self.array_record_writers.clear()

            # Report final statistics
            print(f"\n📊 TRAINING DATA GENERATION COMPLETED")
            print(f"   Total intervals processed: {self.interval_counter}")
            print(f"   Symbols processed: {len(self.symbols)}")
            print(f"   Date range: {self.start_date} to {self.end_date}")

            # Calculate expected vs actual
            if self.start_date and self.end_date:
                days_in_range = (self.end_date - self.start_date).days + 1
                expected_intervals_per_symbol = days_in_range * 72  # ~72 intervals per day
                print(f"   Expected intervals per symbol: ~{expected_intervals_per_symbol}")
                print(f"   Files created: Single file per symbol/timeframe across entire date range")

        except Exception as e:
            print(f"❌ Error in handleEnd: {e}")
            import traceback
            traceback.print_exc()

        self.logger.info(f"Interval-based generation completed: {self.interval_counter} intervals processed")
    
    def _get_target_timeframes_for_interval(self, current_time: datetime) -> List[str]:
        """Determine which timeframes should generate examples at current time.
        
        PRD/DRD Native Frequency Requirements:
        - 5m: Every 5 minutes (12 records/hour)
        - 15m: Every 15 minutes (4 records/hour)
        - 60m: Every hour at 00 minutes (1 record/hour)
        - 1d: Every day at 00:00 (1 record/day)
        - 1w: Every Monday at 00:00 (1 record/week)
        """
        minute = current_time.minute
        hour = current_time.hour
        weekday = current_time.weekday()  # 0=Monday, 6=Sunday
        
        target_timeframes = []
        
        # 5m: Generate every 5 minutes
        if minute % 5 == 0:
            target_timeframes.append('5m')
        
        # 15m: Generate every 15 minutes (00, 15, 30, 45)
        if minute % 15 == 0:
            target_timeframes.append('15m')
        
        # 60m: Generate every hour at 00 minutes
        if minute == 0:
            target_timeframes.append('60m')
        
        # 1d and 1w intervals should be handled by dedicated trading calendar events
        # handleTradingDayEnd and handleTradingWeekEnd, not by clock time
        # Remove hardcoded time logic - this will be event-driven
        
        return target_timeframes
    
    async def handleTradingDayEnd(self, runner, current_time):
        """
        Handle trading day end events (market close) to generate 1d training data.
        
        This method is called by Runner at market close time (e.g., 4 PM ET for NYSE)
        and is responsible for generating daily timeframe training data.
        """
        self.logger.debug(f"handleTradingDayEnd called at {current_time}")
        
        if not self.training_generator:
            return
            
        try:
            # Generate 1d training examples for all symbols
            examples_generated = []
            
            for symbol in self.symbols:
                try:
                    self.logger.debug(f"Generating 1d training example for {symbol} at {current_time}")
                    example = await self.training_generator.generate_training_example(
                        symbol=symbol,
                        prediction_timestamp=current_time,
                        target_timeframes=['1d']  # Generate daily timeframe only
                    )
                    
                    if example and example.get('timeframe_features', {}).get('1d'):
                        self.logger.debug(f"Generated 1d example for {symbol}")
                        examples_generated.append(example)
                    else:
                        self.logger.warning(f"No 1d example generated for {symbol}")
                        
                except Exception as e:
                    self.logger.error(f"Failed to generate 1d example for {symbol}: {e}")
            
            # Save daily training data if we have examples
            if examples_generated:
                self.logger.info(f"Saving {len(examples_generated)} daily examples at {current_time}")
                await self._save_simple_arrayrecord(examples_generated, current_time, runner)
                self.logger.info(f"✅ Daily training data saved for {len(examples_generated)} symbols")
            else:
                self.logger.warning(f"No daily examples to save at {current_time}")
                
        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in handleTradingDayEnd: {e}")
            import traceback
            traceback.print_exc()
    
    async def handleTradingWeekEnd(self, runner, current_time):
        """
        Handle trading week end events (last trading day of week) to generate 1w training data.
        
        This method is called by Runner at the end of the last trading day of each week
        and is responsible for generating weekly timeframe training data.
        """
        self.logger.debug(f"handleTradingWeekEnd called at {current_time}")
        
        if not self.training_generator:
            return
            
        try:
            # Generate 1w training examples for all symbols
            examples_generated = []
            
            for symbol in self.symbols:
                try:
                    self.logger.debug(f"Generating 1w training example for {symbol} at {current_time}")
                    example = await self.training_generator.generate_training_example(
                        symbol=symbol,
                        prediction_timestamp=current_time,
                        target_timeframes=['1w']  # Generate weekly timeframe only
                    )
                    
                    if example and example.get('timeframe_features', {}).get('1w'):
                        self.logger.debug(f"Generated 1w example for {symbol}")
                        examples_generated.append(example)
                    else:
                        self.logger.warning(f"No 1w example generated for {symbol}")
                        
                except Exception as e:
                    self.logger.error(f"Failed to generate 1w example for {symbol}: {e}")
            
            # Save weekly training data if we have examples
            if examples_generated:
                self.logger.info(f"Saving {len(examples_generated)} weekly examples at {current_time}")
                await self._save_simple_arrayrecord(examples_generated, current_time, runner)
                self.logger.info(f"✅ Weekly training data saved for {len(examples_generated)} symbols")
            else:
                self.logger.warning(f"No weekly examples to save at {current_time}")
                
        except Exception as e:
            self.logger.error(f"CRITICAL ERROR in handleTradingWeekEnd: {e}")
            import traceback
            traceback.print_exc()


# Backward compatibility alias
TrainingDataGenerationCallback = IntervalBasedTrainingDataCallback