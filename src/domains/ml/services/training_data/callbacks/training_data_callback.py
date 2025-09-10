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
import json

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
                 end_date: Optional[Union[str, date]] = None):
        """Initialize interval-based callback.
        
        Args:
            start_date: Start date for training data range (YYYY-MM-DD string or date object)
            end_date: End date for training data range (YYYY-MM-DD string or date object)
        """
        self.symbols = symbols
        self.config = config  # Use provided config or None
        self.storage_format = storage_format
        self.output_dir = Path(output_dir)
        
        # FIXED: Store full date range for single symbol directory creation
        self.start_date = self._parse_date(start_date) if start_date else None
        self.end_date = self._parse_date(end_date) if end_date else None

        self.logger = logging.getLogger(__name__)
        self.training_generator = None
        self.interval_counter = 0

        # 🚨 CRITICAL: Store ArrayRecord writers to append intervals over time
        # This prevents OOM by streaming data instead of accumulating in memory
        self.array_record_writers = {}  # Dict[file_path_str, writer]
        self.dataset_initialized = False
        
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
        """Generate and immediately save training data for current interval."""
        if not self.training_generator:
            return

        # Store runner context for data access in helper methods
        self._current_runner = runner

        self.interval_counter += 1
        examples_generated = []

        # Generate examples for all symbols
        for symbol in self.symbols:
            try:
                print(f"🔄 DEBUG: Attempting to generate training example for {symbol} at {current_time}")
                example = await self.training_generator.generate_training_example(
                    symbol=symbol,
                    prediction_timestamp=current_time
                )

                print(f"📊 DEBUG: Generated example for {symbol}: {example is not None}")
                if example:
                    print(f"🔑 DEBUG: Example keys: {list(example.keys()) if isinstance(example, dict) else 'Not a dict'}")
                    if isinstance(example, dict) and 'timeframe_features' in example:
                        print(f"⏰ DEBUG: Timeframe features: {list(example['timeframe_features'].keys()) if isinstance(example['timeframe_features'], dict) else 'Not dict'}")
                    examples_generated.append(example)
                else:
                    print(f"❌ DEBUG: No example generated for {symbol} - example is None/empty")

            except Exception as e:
                print(f"💥 DEBUG: Exception generating example for {symbol}: {e}")
                import traceback
                print(f"📋 DEBUG: Full traceback: {traceback.format_exc()}")
                self.logger.error(f"Failed to generate example for {symbol}: {e}")

        # Save immediately if we have examples
        print(f"DEBUG: Generated {len(examples_generated)} examples for interval at {current_time}")
        if examples_generated:
            print(f"DEBUG: Saving {len(examples_generated)} examples...")
            await self._save_simple_arrayrecord(examples_generated, current_time)
            print(f"DEBUG: Save completed for {len(examples_generated)} examples")
        else:
            print(f"DEBUG: No examples to save at {current_time}")

    async def _save_simple_arrayrecord(self, examples: List[Dict], current_time: datetime):
        """
        STREAMING APPROACH: Initialize writers once, stream intervals as they're processed.
        
        FIXED MEMORY-EFFICIENT APPROACH:
        1. Initialize ArrayRecord writers ONCE per file (first call)
        2. Stream individual intervals as they're generated (NO memory accumulation)
        3. Keep appending to same writer throughout training period
        4. Close writers in handleEnd() when processing complete
        
        This prevents OOM by never holding all data in memory simultaneously.
        """
        try:
            # Initialize dataset structure on first call
            if not self.dataset_initialized:
                await self._initialize_dataset_structure()
                self.dataset_initialized = True
            
            # Stream current interval data to appropriate writers
            await self._stream_intervals_to_writers(examples, current_time)
            
        except Exception as e:
            print(f"❌ Error in streaming ArrayRecord save: {e}")
            import traceback
            traceback.print_exc()

    async def _initialize_dataset_structure(self):
        """
        Initialize ArrayRecord writers ONCE for the entire training period.
        
        CRITICAL: Create writers at the start, keep them open for streaming.
        This prevents OOM by never accumulating data in memory.
        """
        import array_record.python.array_record_module as array_record
        
        try:
            # Get dataset_id from callback (set by runner)
            dataset_id = getattr(self, 'dataset_id', 'unknown_dataset')
            
            # Calculate full date range naming
            if self.start_date and self.end_date:
                start_datetime = f"{self.start_date.strftime('%Y%m%d')}_000000"
                end_datetime = f"{self.end_date.strftime('%Y%m%d')}_235959"
            else:
                # Fallback (should not happen)
                from datetime import date
                today = date.today()
                start_datetime = f"{today.strftime('%Y%m%d')}_000000"
                end_datetime = f"{today.strftime('%Y%m%d')}_235959"
            
            print(f"🔧 INITIALIZING STREAMING ARRAYRECORD WRITERS")
            print(f"   Dataset ID: {dataset_id}")
            print(f"   Date range: {start_datetime} to {end_datetime}")
            
            # Initialize writers for each symbol/timeframe combination
            timeframes = ['5m', '15m', '1h', '1d']
            
            for symbol in self.symbols:
                symbol_datetime_str = f"{symbol}_{start_datetime}_{end_datetime}"
                dataset_dir = self.output_dir / str(dataset_id) / symbol_datetime_str
                
                print(f"   Initializing writers for {symbol}")
                
                for timeframe in timeframes:
                    # Create timeframe directory
                    timeframe_dir = dataset_dir / timeframe
                    timeframe_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Create ArrayRecord file path
                    arrayrecord_file = timeframe_dir / f"{symbol_datetime_str}.arrayrecord"
                    
                    # 🚨 CRITICAL: Create writer ONCE and store for streaming
                    writer = array_record.ArrayRecordWriter(str(arrayrecord_file), 'group_size:1')
                    
                    # Store writer by file path for later access
                    file_key = f"{symbol}_{timeframe}"
                    self.array_record_writers[file_key] = writer
                    
                    print(f"     ✅ Created writer for {symbol} {timeframe}: {arrayrecord_file.name}")
            
            print(f"✅ Initialized {len(self.array_record_writers)} ArrayRecord writers for streaming")
            
            # 🚨 NEW: Save schema metadata for documentation
            schema_file = self.output_dir / str(dataset_id) / "schema_metadata.json"
            self.binary_schema.save_schema_to_file(str(schema_file))
            print(f"📋 Schema metadata saved: {schema_file}")
            
        except Exception as e:
            print(f"❌ Error initializing dataset structure: {e}")
            import traceback
            traceback.print_exc()

    async def _stream_intervals_to_writers(self, examples: List[Dict], current_time: datetime):
        """
        Stream current interval data to appropriate writers (NO accumulation).
        
        CRITICAL: Process current interval immediately and append to existing writers.
        This maintains chronological order and prevents memory issues.
        """
        import struct
        
        try:
            for example in examples:
                symbol = example['symbol']
                
                # Get minute data for CURRENT interval only (no accumulation)
                current_interval_data = await self._get_current_interval_minute_data(symbol, current_time)
                
                if not current_interval_data:
                    continue
                
                # Create 5-minute intervals from current data
                five_min_intervals = self._create_5min_intervals_from_current(current_interval_data, current_time)
                
                if not five_min_intervals:
                    continue
                
                # Stream to each timeframe writer
                timeframes = ['5m', '15m', '1h', '1d']
                for timeframe in timeframes:
                    # Get appropriate intervals for this timeframe
                    intervals = self._get_intervals_for_timeframe(five_min_intervals, timeframe, current_time)
                    
                    if intervals:
                        # Stream to writer (append)
                        file_key = f"{symbol}_{timeframe}"
                        if file_key in self.array_record_writers:
                            writer = self.array_record_writers[file_key]
                            
                            for interval in intervals:
                                await self._write_interval_to_writer(writer, symbol, interval)
                                
            print(f"✅ Streamed intervals for {len(examples)} symbols at {current_time}")
                            
        except Exception as e:
            print(f"❌ Error streaming intervals: {e}")
            import traceback
            traceback.print_exc()

    async def _get_current_interval_minute_data(self, symbol: str, current_time: datetime):
        """Get minute data for CURRENT interval only (no accumulation)."""
        try:
            if hasattr(self, '_current_runner') and self._current_runner:
                runner = self._current_runner
                if hasattr(runner, 'minute_market_data_manager'):
                    data_manager = runner.minute_market_data_manager
                    
                    # Get data for current time window only
                    current_date = current_time.date()
                    
                    # Get minute data for just this date
                    daily_data = await data_manager.get_minute_data(
                        symbol=symbol,
                        start_date=current_date,
                        end_date=current_date
                    )
                    
                    return daily_data if daily_data else []
                else:
                    return []
            else:
                return []
                
        except Exception as e:
            print(f"❌ Error getting current interval data: {e}")
            return []

    def _create_5min_intervals_from_current(self, minute_data, current_time):
        """Create 5-minute intervals from current minute data."""
        # Simplified version - for current interval processing
        return self._create_5min_intervals(minute_data)

    def _get_intervals_for_timeframe(self, five_min_intervals, timeframe, current_time):
        """Get appropriate intervals for timeframe from current data."""
        # Use existing aggregation logic
        return self._aggregate_to_timeframe(five_min_intervals, timeframe)

    async def _write_interval_to_writer(self, writer, symbol, interval):
        """Write single interval to existing writer using dynamic schema (streaming)."""
        
        try:
            # 🚨 DYNAMIC SCHEMA: Use configurable binary record format instead of hardcoded OHLCV
            # This automatically includes available technical indicators
            binary_record = self.binary_schema.pack_interval(symbol, interval)
            
            # Stream to writer (append to same file)
            writer.write(binary_record)
            
            # Log schema details on first write (for debugging)
            if not hasattr(self, '_schema_logged'):
                schema_metadata = self.binary_schema.get_schema_metadata()
                indicators = [ind['name'] for ind in schema_metadata['technical_indicators']]
                if indicators:
                    print(f"📊 Technical indicators included: {', '.join(indicators)}")
                else:
                    print(f"📊 Using OHLCV-only format (no technical indicators)")
                print(f"📊 Total fields per record: {schema_metadata['total_fields']}")
                self._schema_logged = True
            
        except Exception as e:
            print(f"❌ Error writing interval to writer: {e}")
            raise


    def _create_5min_intervals(self, minute_data):
        """Aggregate minute data into 5-minute intervals."""
        if not minute_data:
            return []
            
        intervals = []
        current_interval = []
        
        # Sort by timestamp
        sorted_data = sorted(minute_data, key=lambda x: x.get('timestamp', ''))
        
        for i, minute in enumerate(sorted_data):
            current_interval.append(minute)
            
            # Every 5 minutes or at the end, create an interval
            if len(current_interval) == 5 or i == len(sorted_data) - 1:
                if current_interval:
                    # Create OHLCV for this 5-minute interval
                    interval_ohlcv = {
                        'timestamp': current_interval[0].get('timestamp'),
                        'open': current_interval[0].get('open'),
                        'high': max(m.get('high', 0) for m in current_interval),
                        'low': min(m.get('low', 999999) for m in current_interval),
                        'close': current_interval[-1].get('close'),
                        'volume': sum(m.get('volume', 0) for m in current_interval)
                    }
                    intervals.append(interval_ohlcv)
                    current_interval = []
        
        return intervals

    def _aggregate_to_timeframe(self, five_min_data, timeframe):
        """Aggregate 5-minute intervals to other timeframes."""
        if timeframe == '5m':
            return five_min_data
        
        # For other timeframes, aggregate accordingly
        if timeframe == '15m':
            # Every 3 intervals (15 min / 5 min = 3)
            return self._aggregate_intervals(five_min_data, 3)
        elif timeframe == '1h':
            # Every 12 intervals (60 min / 5 min = 12)
            return self._aggregate_intervals(five_min_data, 12) 
        elif timeframe == '1d':
            # All intervals in the day
            return self._aggregate_intervals(five_min_data, len(five_min_data))
        
        return five_min_data

    def _aggregate_intervals(self, intervals, group_size):
        """Aggregate intervals into larger timeframes."""
        aggregated = []
        
        for i in range(0, len(intervals), group_size):
            group = intervals[i:i + group_size]
            if group:
                agg_interval = {
                    'timestamp': group[0]['timestamp'],
                    'open': group[0]['open'],
                    'high': max(g['high'] for g in group),
                    'low': min(g['low'] for g in group), 
                    'close': group[-1]['close'],
                    'volume': sum(g['volume'] for g in group)
                }
                aggregated.append(agg_interval)
        
        return aggregated

    async def _write_binary_arrayrecord(self, file_path, symbol, intervals):
        """Write intervals as binary protobuf records (NOT JSON!)."""
        import array_record.python.array_record_module as array_record
        import struct
        
        try:
            writer = array_record.ArrayRecordWriter(str(file_path), 'group_size:1')
            
            for interval in intervals:
                # Create binary protobuf-like record
                # Format: timestamp(double) + symbol(string) + open(float) + high(float) + low(float) + close(float) + volume(float)
                
                # Convert timestamp to unix timestamp
                from datetime import datetime
                if isinstance(interval['timestamp'], str):
                    ts = datetime.fromisoformat(interval['timestamp']).timestamp()
                else:
                    ts = float(interval['timestamp'])
                
                # Pack as binary data (NOT JSON!)
                symbol_bytes = symbol.encode('utf-8')
                symbol_len = len(symbol_bytes)
                
                # Binary format: timestamp(8) + symbol_len(4) + symbol(variable) + ohlcv(5*4)
                binary_record = struct.pack(
                    f'>dI{symbol_len}sfffff',  # Big-endian: double, uint32, string, 5 floats
                    ts,                       # timestamp 
                    symbol_len,               # symbol length
                    symbol_bytes,             # symbol
                    float(interval.get('open', 0.0)),
                    float(interval.get('high', 0.0)), 
                    float(interval.get('low', 0.0)),
                    float(interval.get('close', 0.0)),
                    float(interval.get('volume', 0.0))
                )
                
                writer.write(binary_record)
            
            writer.close()
            print(f"✅ Successfully wrote {len(intervals)} binary records to {file_path.name}")
            
        except Exception as e:
            print(f"❌ Error writing binary ArrayRecord: {e}")
            import traceback
            traceback.print_exc()

    async def handleEnd(self, runner: Any, current_time: datetime):
        """Close all ArrayRecord writers and generate final summary."""
        
        # 🚨 CRITICAL: Close all writers to finalize files
        print(f"\n🔒 CLOSING ARRAYRECORD WRITERS")
        
        try:
            for file_key, writer in self.array_record_writers.items():
                try:
                    writer.close()
                    print(f"   ✅ Closed writer for {file_key}")
                except Exception as e:
                    print(f"   ❌ Error closing writer for {file_key}: {e}")
            
            print(f"✅ Closed {len(self.array_record_writers)} ArrayRecord writers")
            
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


# Backward compatibility alias
TrainingDataGenerationCallback = IntervalBasedTrainingDataCallback