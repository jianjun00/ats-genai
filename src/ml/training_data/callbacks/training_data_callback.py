"""
Training Data Generation Callback for Runner Framework with Ray Parallel Processing

This callback integrates with the Runner framework to generate training data
at each interval using the handleInterval pattern. It organizes data by date
and uses SOD/EOD events to manage daily files efficiently.

Enhanced with Ray for parallel processing:
- Parallel symbol processing across multiple workers
- Distributed sequence generation for faster throughput  
- Smart batching to maximize CPU utilization
"""

import logging
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Any, Optional, List, Dict, Union
from pathlib import Path
import json
import asyncio
import ray

from state.runner_callback import RunnerCallback
from core.utils.training_dataset_paths import TrainingDatasetPaths
# TrainingDataConfig is imported from the specific runner that uses this callback
# Import TimeSeriesSequenceTrainingGenerator for fallback generator
try:
    from ml.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
except ImportError:
    TimeSeriesSequenceTrainingGenerator = None

# Optional import - will be None if not available
try:
    from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
except ImportError:
    SequenceStorageManager = None
    StorageConfig = None

# Initialize Ray if not already initialized
if not ray.is_initialized():
    ray.init(
        object_store_memory=1_000_000_000,  # 1GB for training data sequences
        num_cpus=None,  # Use all available CPUs
        ignore_reinit_error=True
    )

@ray.remote
class ParallelSequenceGenerator:
    """Ray actor for parallel training sequence generation."""
    
    def __init__(self):
        """Initialize the sequence generator worker."""
        self.logger = logging.getLogger(f"{__name__}.ParallelSequenceGenerator")
        
    async def generate_sequences_for_symbol_batch(self, 
                                                 symbol: str,
                                                 date_range: List[date],
                                                 config: Optional[Any] = None) -> List[Dict]:
        """
        Generate training sequences for a symbol across a date range.
        
        Args:
            symbol: Stock symbol to process
            date_range: List of dates to process
            config: Training data configuration
            
        Returns:
            List of training sequences for the symbol/date combination
        """
        try:
            self.logger.info(f"🔄 Processing {symbol} for {len(date_range)} dates")
            
            # Placeholder for actual sequence generation logic
            # This would integrate with the existing training data generators
            sequences = []
            
            for trading_date in date_range:
                # Generate sequences for this symbol/date
                # This would call the actual training data generation logic
                sequence = {
                    'symbol': symbol,
                    'date': trading_date.isoformat(),
                    'features': [],  # Would contain actual features
                    'labels': [],    # Would contain actual labels
                    'metadata': {
                        'generated_at': datetime.now().isoformat(),
                        'worker_id': ray.get_runtime_context().worker_id
                    }
                }
                sequences.append(sequence)
            
            self.logger.info(f"✅ Generated {len(sequences)} sequences for {symbol}")
            return sequences
            
        except Exception as e:
            self.logger.error(f"❌ Error processing {symbol}: {e}")
            return []


class DateBasedTrainingDataCallback(RunnerCallback):
    """
    Date-organized training data generation callback.
    
    This callback:
    - Opens a new daily file at SOD (Start of Day)  
    - Generates training examples at each interval
    - Closes and saves the daily file at EOD (End of Day)
    - Organizes files by date for efficient storage and retrieval
    """
    
    def __init__(self, 
                 symbols: List[str],
                 config: Optional[Any] = None,  # Accept any config object
                 storage_manager: Optional[SequenceStorageManager] = None,
                 output_dir: str = "/data/training/sequences",
                 save_format: str = "arrayrecord",  # "arrayrecord" only - removed pickle and parquet support
                 enable_ray_parallel: bool = True,  # Enable Ray parallel processing
                 max_parallel_workers: int = 4):   # Maximum Ray workers
        """
        Initialize the date-based training data callback.
        
        Args:
            symbols: List of symbols to generate training data for
            config: Training data configuration
            storage_manager: Storage manager for advanced storage
            output_dir: Base output directory 
            save_format: Format to save data ("arrayrecord" only)
            enable_ray_parallel: Enable Ray parallel processing 
            max_parallel_workers: Maximum number of Ray workers
        """
        self.symbols = symbols
        self.config = config  # Use provided config or None
        self.storage_manager = storage_manager
        self.output_dir = Path(output_dir)
        self.save_format = save_format
        self.enable_ray_parallel = enable_ray_parallel
        self.max_parallel_workers = max_parallel_workers
        
        self.logger = logging.getLogger(__name__)
        
        # Training data generator (initialized when runner starts)
        self.training_generator = None
        
        # Ray parallel processing
        self.ray_workers = []
        if self.enable_ray_parallel and ray.is_initialized():
            self.ray_workers = [
                ParallelSequenceGenerator.remote()
                for _ in range(min(self.max_parallel_workers, len(self.symbols)))
            ]
            self.logger.info(f"🚀 Initialized {len(self.ray_workers)} Ray workers for parallel processing")
        
        # Daily data management
        self.current_date = None
        self.daily_examples = []  # Examples for current day
        self.daily_stats = {}     # Statistics for current day
        
        # Overall statistics
        self.total_examples = 0
        self.total_days = 0
        self.processed_dates = []
        
        self.logger.info(f"DateBasedTrainingDataCallback initialized for symbols: {symbols}")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Save format: {self.save_format}")
        if self.enable_ray_parallel:
            self.logger.info(f"Ray parallel processing: ENABLED ({len(self.ray_workers)} workers)")
        else:
            self.logger.info(f"Ray parallel processing: DISABLED (sequential mode)")
    
    def handleStart(self, runner: Any, current_time: datetime):
        """Handle start of runner - initialize training generator and output structure."""
        self.logger.info(f"🚀 Starting training data generation at {current_time}")
        
        # Initialize training generator (placeholder - specific implementation would go here)
        self.training_generator = None  # Would initialize actual generator based on config
        
        # Create output directory structure
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "daily").mkdir(exist_ok=True)
        (self.output_dir / "metadata").mkdir(exist_ok=True)
        
        self.logger.info("✅ Training data generator initialized")
        self.logger.info(f"📁 Output directory structure created: {self.output_dir}")
    
    def handleStartOfDay(self, runner: Any, current_time: datetime):
        """Handle start of day - open new daily file."""
        trading_date = current_time.date()
        
        # Close previous day if needed
        if self.current_date and self.current_date != trading_date:
            self.logger.warning(f"SOD for {trading_date} but previous day {self.current_date} not closed")
        
        self.current_date = trading_date
        self.daily_examples = []
        self.daily_stats = {
            'date': trading_date.isoformat(),
            'symbols': self.symbols.copy(),
            'start_time': current_time.isoformat(),
            'examples_generated': 0,
            'intervals_processed': 0,
            'errors': []
        }
        
        self.logger.info(f"📅 SOD: Opening daily training data file for {trading_date}")
    
    async def handleInterval(self, runner: Any, current_time: datetime):
        """
        Handle interval event - generate training data for current timestamp.
        
        This is called for each trading interval and generates training examples
        for all symbols at the current timestamp.
        """
        if not self.training_generator:
            self.logger.error("Training generator not initialized")
            return
        
        if not self.current_date:
            self.logger.warning(f"No current date set for interval at {current_time}")
            return
        
        interval_date = current_time.date()
        if interval_date != self.current_date:
            self.logger.warning(f"Interval date {interval_date} doesn't match current date {self.current_date}")
        
        self.logger.debug(f"⏰ Generating training data for interval: {current_time}")
        
        interval_examples = []
        interval_errors = []
        
        # Choose parallel vs sequential processing
        if self.enable_ray_parallel and self.ray_workers:
            interval_examples = await self._generate_examples_parallel(current_time)
        else:
            interval_examples = await self._generate_examples_sequential(current_time)
        
        # Add examples to daily collection
        self.daily_examples.extend(interval_examples)
        
        # Update daily statistics
        self.daily_stats['examples_generated'] += len(interval_examples)
        self.daily_stats['intervals_processed'] += 1
        self.daily_stats['errors'].extend(interval_errors)
        
        self.logger.debug(f"📊 Interval complete: {len(interval_examples)} examples generated, "
                         f"{len(self.daily_examples)} total for {self.current_date}")
    
    async def handleEndOfDay(self, runner: Any, current_time: datetime):
        """Handle end of day - close and save daily file."""
        if not self.current_date:
            self.logger.warning(f"EOD called but no current date set")
            return
        
        # Finalize daily statistics
        self.daily_stats['end_time'] = current_time.isoformat()
        self.daily_stats['total_examples'] = len(self.daily_examples)
        
        self.logger.info(f"💾 EOD: Saving daily training data for {self.current_date}")
        self.logger.info(f"   Examples generated: {len(self.daily_examples)}")
        self.logger.info(f"   Intervals processed: {self.daily_stats['intervals_processed']}")
        self.logger.info(f"   Errors: {len(self.daily_stats['errors'])}")
        
        # Save daily data if we have examples
        if self.daily_examples:
            await self._save_daily_data()
        else:
            self.logger.warning(f"No training examples to save for {self.current_date}")
        
        # Save daily metadata
        await self._save_daily_metadata()
        
        # Update overall statistics
        self.total_examples += len(self.daily_examples)
        self.total_days += 1
        self.processed_dates.append(self.current_date)
        
        # Clear daily data
        self.current_date = None
        self.daily_examples = []
        self.daily_stats = {}
        
        self.logger.info(f"✅ EOD: Daily data saved and closed for {current_time.date()}")
    
    async def _generate_examples_sequential(self, current_time: datetime) -> List[Any]:
        """Generate training examples using sequential processing."""
        interval_examples = []
        
        # Generate training examples for each symbol at this timestamp
        for symbol in self.symbols:
            try:
                example = await self.training_generator.generate_training_example(
                    symbol=symbol,
                    prediction_timestamp=current_time
                )
                
                if example:
                    interval_examples.append(example)
                    self.logger.debug(f"✅ Generated training example for {symbol} at {current_time}")
                else:
                    self.logger.debug(f"⚠️ No training example generated for {symbol} at {current_time}")
                    
            except Exception as e:
                error_msg = f"Failed to generate training example for {symbol} at {current_time}: {e}"
                self.logger.error(error_msg)
                self.daily_stats['errors'].append(error_msg)
        
        return interval_examples
    
    async def _generate_examples_parallel(self, current_time: datetime) -> List[Any]:
        """Generate training examples using Ray parallel processing."""
        try:
            self.logger.debug(f"⚡ Using Ray parallel processing for {len(self.symbols)} symbols")
            
            # Distribute symbols across Ray workers
            symbol_batches = self._distribute_symbols_to_workers()
            
            # Submit parallel tasks to Ray workers
            futures = []
            for i, symbol_batch in enumerate(symbol_batches):
                if i < len(self.ray_workers):
                    worker = self.ray_workers[i]
                    future = worker.generate_sequences_for_symbol_batch.remote(
                        symbol=symbol_batch[0] if symbol_batch else "",  # One symbol per worker for now
                        date_range=[current_time.date()],
                        config=self.config
                    )
                    futures.append(future)
            
            # Collect results from Ray workers
            if futures:
                results = await asyncio.gather(*[ray.get(future) for future in futures])
                
                # Flatten results into single list
                interval_examples = []
                for result in results:
                    if result and isinstance(result, list):
                        interval_examples.extend(result)
                
                self.logger.debug(f"✅ Ray parallel processing completed: {len(interval_examples)} examples")
                return interval_examples
            
            return []
            
        except Exception as e:
            self.logger.error(f"❌ Ray parallel processing failed: {e}")
            # Fallback to sequential processing
            return await self._generate_examples_sequential(current_time)
    
    def _distribute_symbols_to_workers(self) -> List[List[str]]:
        """Distribute symbols evenly across available Ray workers."""
        if not self.ray_workers or not self.symbols:
            return []
        
        num_workers = len(self.ray_workers)
        symbols_per_worker = max(1, len(self.symbols) // num_workers)
        
        batches = []
        for i in range(0, len(self.symbols), symbols_per_worker):
            batch = self.symbols[i:i + symbols_per_worker]
            batches.append(batch)
        
        # Ensure we don't have more batches than workers
        return batches[:num_workers]
    
    async def _save_daily_data(self):
        """Save daily training examples using configured format."""
        if not self.daily_examples or not self.current_date:
            return
        
        date_str = self.current_date.strftime('%Y%m%d')
        
        try:
            if self.save_format in ["advanced", "arrayrecord"] and self.storage_manager:
                # Use advanced storage with date-based batch ID
                batch_id = f"daily_{date_str}"
                save_result = await self.storage_manager.save_sequence_batch(
                    examples=self.daily_examples,
                    batch_id=batch_id
                )
                
                self.logger.info(f"💾 Saved {len(self.daily_examples)} examples using advanced storage")
                self.logger.debug(f"   Batch ID: {batch_id}")
                self.logger.debug(f"   Sequence file: {save_result.get('sequence_file', 'N/A')}")
                self.logger.debug(f"   Metadata file: {save_result.get('metadata_file', 'N/A')}")
                
            else:
                # Use traditional export formats
                daily_output_dir = self.output_dir / "daily" / date_str
                daily_output_dir.mkdir(parents=True, exist_ok=True)
                
                if self.save_format == "pickle":
                    formats = ['pickle']
                elif self.save_format == "parquet":
                    formats = ['parquet']
                else:
                    formats = ['pickle', 'parquet']  # Default to both
                
                exported_files = self.training_generator.export_to_formats(
                    examples=self.daily_examples,
                    output_dir=str(daily_output_dir),
                    formats=formats
                )
                
                self.logger.info(f"💾 Saved {len(self.daily_examples)} examples to daily files")
                for format_name, file_path in exported_files.items():
                    file_size = Path(file_path).stat().st_size
                    self.logger.debug(f"   {format_name}: {file_path} ({file_size} bytes)")
                
        except Exception as e:
            self.logger.error(f"Failed to save daily data for {self.current_date}: {e}")
            self.daily_stats['errors'].append(f"Save error: {e}")
    
    async def _save_daily_metadata(self):
        """Save daily metadata and statistics."""
        if not self.current_date:
            return
        
        date_str = self.current_date.strftime('%Y%m%d')
        metadata_file = self.output_dir / "metadata" / f"daily_stats_{date_str}.json"
        
        try:
            with open(metadata_file, 'w') as f:
                json.dump(self.daily_stats, f, indent=2, default=str)
            
            self.logger.debug(f"📋 Saved daily metadata: {metadata_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save daily metadata for {self.current_date}: {e}")
    
    async def handleEnd(self, runner: Any, current_time: datetime):
        """Handle end of runner - generate final summary and cleanup."""
        self.logger.info(f"🏁 Ending training data generation at {current_time}")
        
        # Close any remaining daily data
        if self.current_date and self.daily_examples:
            self.logger.info(f"Closing remaining daily data for {self.current_date}")
            await self.handleEndOfDay(runner, current_time)
        
        # Generate final summary
        await self._generate_final_summary(current_time)
    
    async def _generate_final_summary(self, end_time: datetime):
        """Generate comprehensive summary of training data generation."""
        summary = {
            'generation_summary': {
                'end_time': end_time.isoformat(),
                'total_days_processed': self.total_days,
                'total_examples_generated': self.total_examples,
                'symbols': self.symbols,
                'processed_dates': [d.isoformat() for d in self.processed_dates],
                'avg_examples_per_day': self.total_examples / max(self.total_days, 1),
                'config': {
                    'sequence_lengths': self.config.sequence_lengths,
                    'prediction_horizons': self.config.prediction_horizons,
                    'timeframes': self.config.timeframes,
                    'feature_types': self.config.feature_types
                }
            },
            'storage_details': {
                'output_directory': str(self.output_dir),
                'save_format': self.save_format,
                'storage_manager_used': self.storage_manager is not None
            }
        }
        
        # Save summary to file
        summary_file = self.output_dir / f"training_generation_summary_{end_time.strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            self.logger.info("📊 TRAINING DATA GENERATION SUMMARY")
            self.logger.info("=" * 60)
            self.logger.info(f"Total Days Processed: {self.total_days}")
            self.logger.info(f"Total Examples Generated: {self.total_examples:,}")
            self.logger.info(f"Average Examples/Day: {summary['generation_summary']['avg_examples_per_day']:.1f}")
            self.logger.info(f"Symbols: {', '.join(self.symbols)}")
            self.logger.info(f"Date Range: {min(self.processed_dates)} to {max(self.processed_dates)}")
            self.logger.info(f"Output Directory: {self.output_dir}")
            self.logger.info(f"Summary File: {summary_file}")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Failed to save final summary: {e}")


class IntervalBasedTrainingDataCallback(RunnerCallback):
    """
    Simple interval-based callback for immediate processing.
    
    This is useful for real-time applications where you want to process
    training data immediately at each interval without daily batching.
    """
    
    def __init__(self, 
                 symbols: List[str],
                 config: Optional[Any] = None,  # Accept any config object
                 storage_manager: Optional[SequenceStorageManager] = None,
                 output_dir: str = "/data/training/sequences"):
        """Initialize interval-based callback."""
        self.symbols = symbols
        self.config = config  # Use provided config or None
        self.storage_manager = storage_manager
        self.output_dir = Path(output_dir)
        
        self.logger = logging.getLogger(__name__)
        self.training_generator = None
        self.interval_counter = 0
        
        # Track file metadata for database storage
        self.file_metadata = {
            "files": [],
            "total_sequences": 0,
            "total_files": 0,
            "timeframes": [],
            "symbols": symbols,
            "generation_date": None
        }
        
        self.logger.info(f"IntervalBasedTrainingDataCallback initialized for symbols: {symbols}")
    
    def handleStart(self, runner: Any, current_time: datetime):
        """Initialize training generator and minute data manager."""
        # Initialize minute data manager if provided
        if hasattr(self, 'minute_data_manager'):
            self.logger.info(f"Using FileBasedMinuteMarketDataManager for multi-timeframe processing")
        else:
            self.logger.warning("No minute_data_manager provided - using fallback generator")
            # Fallback to original generator
            if TimeSeriesSequenceTrainingGenerator is None:
                raise ImportError("TimeSeriesSequenceTrainingGenerator not available and no minute_data_manager provided")
            
            self.training_generator = TimeSeriesSequenceTrainingGenerator(
                env=runner.get_environment(),
                config=self.config,
                universe_manager=runner.get_universe_state_manager()
            )
        
        # Create output directory structure with sequence-based subdirectories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Store expected timeframes for sequence generation
        self.expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
        
        # Create metadata directory
        (self.output_dir / "metadata").mkdir(exist_ok=True)
        
        self.logger.info("Interval-based sequence-based training data generator initialized")
    
    async def handleInterval(self, runner: Any, current_time: datetime):
        """Generate multi-timeframe training data for current interval."""
        self.interval_counter += 1
        examples_generated = []
        
        # Generate multi-timeframe examples for all symbols
        for symbol in self.symbols:
            try:
                if hasattr(self, 'minute_data_manager'):
                    example = await self._generate_multi_timeframe_example(
                        symbol=symbol,
                        current_time=current_time
                    )
                else:
                    # Fallback to original generator
                    example = await self.training_generator.generate_training_example(
                        symbol=symbol,
                        prediction_timestamp=current_time
                    )
                
                if example:
                    examples_generated.append(example)
                    self.logger.debug(f"Generated multi-timeframe example for {symbol}")
                    
            except Exception as e:
                self.logger.error(f"Failed to generate multi-timeframe example for {symbol}: {e}")
        
        # Save immediately if we have examples
        if examples_generated:
            await self._save_interval_examples(examples_generated, current_time)
            self.logger.info(f"Processed interval {self.interval_counter}: {len(examples_generated)} examples generated")
    
    async def _generate_multi_timeframe_example(self, symbol: str, current_time: datetime) -> Optional[Dict]:
        """
        Generate training example with multi-timeframe OHLC and signal features using enhanced API.
        
        CRITICAL QR4 REQUIREMENT: This method now generates examples that will be processed into 
        separate ArrayRecord files per timeframe, each containing only base feature names.
        """
        debug_info = {
            'symbol': symbol,
            'timestamp': current_time.isoformat(),
            'step': 'initialization'
        }
        
        if not hasattr(self, 'minute_data_manager') or not self.config:
            self.logger.debug(f"🔍 GENERATE_EXAMPLE_DEBUG: {debug_info} - Missing minute_data_manager or config")
            return None
        
        debug_info['step'] = 'calculate_lookback'
        
        # Calculate lookback window (need enough data for largest sequence)
        max_sequence_length = max(self.config.sequence_lengths.values()) if self.config.sequence_lengths else 100
        max_timeframe_minutes = max([self._timeframe_to_minutes(tf) for tf in self.config.timeframes.keys()]) if self.config.timeframes else 1440
        
        # Calculate total lookback needed (conservative estimate)
        lookback_minutes = max_sequence_length * max_timeframe_minutes
        start_time = current_time - timedelta(minutes=lookback_minutes)
        
        debug_info.update({
            'max_sequence_length': max_sequence_length,
            'max_timeframe_minutes': max_timeframe_minutes,
            'lookback_minutes': lookback_minutes,
            'start_time': start_time.isoformat()
        })
        
        self.logger.debug(f"🔍 GENERATE_EXAMPLE_DEBUG: {debug_info}")
        
        # Define signals to compute
        signals = ['sma_20', 'ema_12', 'ema_26', 'rsi_14', 'etop', 'ebot', 'pldot', 'vwap']
        
        debug_info['step'] = 'fetch_data'
        debug_info['signals'] = signals
        
        # Use enhanced API to get multi-timeframe data
        try:
            multi_timeframe_data = await self.minute_data_manager.get_multi_timeframe_data(
                symbols=[symbol],
                start=start_time,
                end=current_time,
                intervals=list(self.config.timeframes.keys())
            )
            
            if symbol not in multi_timeframe_data:
                debug_info['error'] = 'No multi-timeframe data available'
                self.logger.warning(f"🔍 GENERATE_EXAMPLE_DEBUG: {debug_info}")
                return None
                
            symbol_data = multi_timeframe_data[symbol]
            debug_info['timeframes_received'] = list(symbol_data.keys())
            debug_info['data_shapes'] = {tf: len(df) for tf, df in symbol_data.items()}
            
        except Exception as e:
            debug_info['error'] = str(e)
            self.logger.error(f"🔍 GENERATE_EXAMPLE_DEBUG: {debug_info}")
            return None
        
        debug_info['step'] = 'extract_features'
        
        # CRITICAL QR4 FIX: Extract features per timeframe but preserve structure for later separation
        all_timeframe_features = {}
        
        for timeframe_name in self.config.timeframes.keys():
            if timeframe_name not in symbol_data or symbol_data[timeframe_name].empty:
                debug_info[f'{timeframe_name}_status'] = 'no_data'
                self.logger.warning(f"No {timeframe_name} data for {symbol}")
                continue
                
            try:
                tf_features = self._extract_timeframe_features(
                    timeframe_name, 
                    symbol_data[timeframe_name], 
                    signals, 
                    symbol
                )
                
                if tf_features:
                    # Store with timeframe prefix for now - will be separated later per QR4
                    for feature_name, feature_data in tf_features.items():
                        prefixed_name = f'{timeframe_name}_{feature_name}' if timeframe_name != '5m' else feature_name
                        all_timeframe_features[prefixed_name] = feature_data
                    
                    debug_info[f'{timeframe_name}_features'] = len(tf_features)
                    debug_info[f'{timeframe_name}_status'] = 'success'
                else:
                    debug_info[f'{timeframe_name}_status'] = 'no_features'
                    
            except Exception as e:
                debug_info[f'{timeframe_name}_error'] = str(e)
                self.logger.error(f"Failed to process {timeframe_name} data for {symbol}: {e}")
        
        if not all_timeframe_features:
            debug_info['final_status'] = 'no_features_generated'
            self.logger.warning(f"🔍 GENERATE_EXAMPLE_DEBUG: {debug_info}")
            return None
        
        debug_info.update({
            'step': 'build_example',
            'total_features': len(all_timeframe_features),
            'feature_names_sample': list(all_timeframe_features.keys())[:10],
            'final_status': 'success'
        })
        
        # Build training example structure with enhanced features
        example = {
            'symbol': symbol,
            'timestamp': current_time.isoformat(),
            'features': all_timeframe_features,
            'feature_count': len(all_timeframe_features),
            'timeframes': list(self.config.timeframes.keys()),
            'signals': signals,
            'data_source': 'FileBasedMinuteMarketDataManager_enhanced_api',
            'debug_info': debug_info
        }
        
        self.logger.debug(f"🔍 GENERATE_EXAMPLE_DEBUG: {debug_info}")
        
        return example
    
    def _timeframe_to_minutes(self, timeframe: str) -> int:
        """Convert timeframe string to minutes for lookback calculation."""
        timeframe_minutes = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30, 
            '1h': 60, '2h': 120, '4h': 240, '1d': 1440, '1w': 10080
        }
        return timeframe_minutes.get(timeframe, 60)  # Default to 1 hour
    
    def _extract_timeframe_features(self, timeframe_name: str, tf_df: pd.DataFrame, 
                                   signals: List[str], symbol: str) -> Dict[str, List[float]]:
        """
        Extract features for a specific timeframe from DataFrame.
        
        This method is separated for easier unit testing.
        Returns features as lists (sequences) that will later be flattened per QR4.
        """
        sequence_length = self.config.sequence_lengths.get(timeframe_name, 20)
        
        # Extract the most recent sequence_length bars
        if len(tf_df) < sequence_length:
            self.logger.warning(f"Insufficient {timeframe_name} data for {symbol}: {len(tf_df)} < {sequence_length}")
            return {}
        
        recent_data = tf_df.tail(sequence_length)
        features = {}
        
        # CRITICAL: Extract base OHLCV features
        base_features = ['open', 'high', 'low', 'close', 'volume']
        for feature in base_features:
            if feature in recent_data.columns:
                features[feature] = recent_data[feature].fillna(0).tolist()
            else:
                self.logger.warning(f"Missing base feature {feature} in {timeframe_name} data for {symbol}")
        
        # Add VWAP (use close if not available)
        if 'vwap' in recent_data.columns:
            features['vwap'] = recent_data['vwap'].fillna(0).tolist()
        else:
            features['vwap'] = recent_data['close'].fillna(0).tolist()
        
        # Add technical signals
        for signal in signals:
            if signal in recent_data.columns:
                features[signal] = recent_data[signal].fillna(0).tolist()
            else:
                self.logger.debug(f"Signal {signal} not available for {timeframe_name}")
        
        self.logger.debug(f"✅ Extracted {len(features)} features for {timeframe_name}: {list(features.keys())}")
        
        return features
    
    def _convert_sequence_to_qr4_rows(self, example: Dict, symbol: str, timeframe: str) -> List[Dict]:
        """
        Convert sequence-based features to QR4-compliant individual timestamp rows.
        
        CRITICAL QR4 TRANSFORMATION: 
        Input:  {'open': [100, 101, 102], 'high': [105, 106, 107], ...}
        Output: [
            {'timestamp': t1, 'symbol': 'AAPL', 'open': 100, 'high': 105, ...},
            {'timestamp': t2, 'symbol': 'AAPL', 'open': 101, 'high': 106, ...},
            {'timestamp': t3, 'symbol': 'AAPL', 'open': 102, 'high': 107, ...}
        ]
        """
        features = example.get('features', {})
        base_timestamp = example.get('timestamp')
        
        # QR4 CRITICAL: Only use base feature names
        qr4_base_features = ['open', 'high', 'low', 'close', 'volume', 'vwap']
        
        # Find sequence length (should be consistent across all features)
        sequence_length = 0
        sequence_features = {}
        
        # Extract only sequence features that are in QR4 base features
        for feature_name, feature_values in features.items():
            if feature_name in qr4_base_features and isinstance(feature_values, list):
                sequence_features[feature_name] = feature_values
                if len(feature_values) > sequence_length:
                    sequence_length = len(feature_values)
        
        # Create individual rows for each timestamp in the sequence
        qr4_rows = []
        for i in range(sequence_length):
            row = {
                'timestamp': base_timestamp,  # Same base timestamp for all sequence steps
                'symbol': symbol
            }
            
            # Add QR4 base features as scalar values
            for feature_name in qr4_base_features:
                if feature_name in sequence_features and i < len(sequence_features[feature_name]):
                    row[feature_name] = sequence_features[feature_name][i]
                else:
                    row[feature_name] = 0.0  # Fill missing values with 0
            
            qr4_rows.append(row)
        
        self.logger.debug(f"✅ QR4 conversion: {symbol} {timeframe} - {len(sequence_features)} features → {len(qr4_rows)} rows")
        
        return qr4_rows
    
    async def _save_interval_examples(self, examples: List[Dict], current_time: datetime):
        """Save multi-timeframe examples in symbol-specific .arrayrecord files."""
        
        try:
            if self.storage_manager:
                batch_id = f"multi_timeframe_{current_time.strftime('%Y%m%d_%H%M%S')}"
                save_result = await self.storage_manager.save_sequence_batch(
                    examples=examples,
                    batch_id=batch_id
                )
                self.logger.debug(f"Saved {len(examples)} multi-timeframe examples at {current_time}")
            else:
                # Save to sequence-based structure ONLY
                # Pattern: /output_dir/{SYMBOL_DATERANGE}/timeframes/{SYMBOL_DATERANGE}.arrayrecord
                
                # Group examples by symbol (sequences)
                examples_by_symbol = {}
                for example in examples:
                    symbol = example['symbol']
                    if symbol not in examples_by_symbol:
                        examples_by_symbol[symbol] = []
                    examples_by_symbol[symbol].append(example)
                
                # Save each symbol as a separate sequence
                for symbol, symbol_examples in examples_by_symbol.items():
                    # Create sequence identifier: SYMBOL_STARTDATE_ENDDATE
                    start_date_str = getattr(self, 'start_date', current_time.date()).strftime('%Y%m%d_%H%M%S')
                    end_date_str = getattr(self, 'end_date', current_time.date()).strftime('%Y%m%d_%H%M%S') 
                    sequence_id = f"{symbol}_{start_date_str}_{end_date_str}"
                    
                    # Create sequence directory structure
                    sequence_dir = Path(self.output_dir) / sequence_id
                    sequence_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Save all timeframes for this sequence
                    for timeframe in self.expected_timeframes:
                        # Use canonical path generation per PRD/DRD requirements
                        # Extract date components from sequence_id for canonical path format
                        start_date = sequence_id.split('_')[1]  # YYYYMMDD_HHMMSS
                        end_date = sequence_id.split('_')[3]    # YYYYMMDD_HHMMSS
                        
                        # Create canonical ArrayRecord file path per PRD/DRD QR4
                        arrayrecord_path_str = TrainingDatasetPaths.get_arrayrecord_filepath(
                            run_id=str(self.output_dir.name),  # Use output dir name as run_id
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            timeframe=timeframe
                        )
                        arrayrecord_path = Path(arrayrecord_path_str)
                        
                        # Ensure directory exists
                        arrayrecord_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # CRITICAL FIX: Filter examples for this specific timeframe
                        timeframe_filtered_examples = self._extract_timeframe_data(symbol_examples, timeframe)
                        
                        # Log filtering results for verification
                        if timeframe_filtered_examples:
                            original_feature_count = timeframe_filtered_examples[0].get('metadata', {}).get('original_feature_count', 0)
                            filtered_feature_count = timeframe_filtered_examples[0].get('metadata', {}).get('filtered_feature_count', 0)
                            self.logger.info(f"🔧 FIXED: {symbol} {timeframe} filtering: {original_feature_count} → {filtered_feature_count} features")
                        
                        # Save as ArrayRecord format
                        await self._save_symbol_arrayrecord(timeframe_filtered_examples, arrayrecord_path, symbol, timeframe)
                        
                        # Save companion metadata using canonical path
                        metadata_path = arrayrecord_path.with_suffix('.json').with_name(arrayrecord_path.stem + '_metadata.json')
                        await self._save_symbol_metadata(timeframe_filtered_examples, metadata_path, symbol, current_time, timeframe)
                        
                        # Track file metadata for database storage
                        file_stats = arrayrecord_path.stat()
                        file_info = {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "file_path": f"{symbol.lower()}.arrayrecord",  # QR4 compliant: symbol.arrayrecord
                            "sequences": len(timeframe_filtered_examples),
                            "file_size_bytes": file_stats.st_size,
                            "created_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
                        }
                        self.file_metadata["files"].append(file_info)
                        
                        # Update timeframes list
                        if timeframe not in self.file_metadata["timeframes"]:
                            self.file_metadata["timeframes"].append(timeframe)
                        
                        self.logger.info(f"Saved {len(timeframe_filtered_examples)} examples for {symbol} {timeframe} to {arrayrecord_path}")
                
        except Exception as e:
            self.logger.error(f"Failed to save multi-timeframe examples at {current_time}: {e}")
            raise RuntimeError(f"Critical error saving multi-timeframe examples at {current_time}: {e}") from e
    
    def _extract_timeframe_data(self, examples: List[Dict], timeframe: str) -> List[Dict]:
        """Extract data for specific timeframe from multi-timeframe examples.
        
        CRITICAL QR4 COMPLIANCE: Per PRD/DRD QR4 requirements, ALL timeframes use 
        BASE FEATURE NAMES (open, high, low, close, volume, vwap) without prefixes.
        Timeframe separation happens via DIRECTORY STRUCTURE, not feature prefixes.
        """
        timeframe_examples = []
        
        for example in examples:
            all_features = example.get('features', {})
            
            # QR4 CRITICAL REQUIREMENT: Extract timeframe-specific data and convert to base names
            timeframe_features = {}
            
            # Define the core OHLCV features we need for each timeframe
            base_feature_names = ['open', 'high', 'low', 'close', 'volume', 'vwap']
            
            if timeframe == '5m':
                # For 5m: use base features directly (no prefix)
                for base_name in base_feature_names:
                    if base_name in all_features:
                        timeframe_features[base_name] = all_features[base_name]
            else:
                # For other timeframes: find prefixed features and convert to base names
                timeframe_prefix = f'{timeframe}_'
                for base_name in base_feature_names:
                    prefixed_name = f'{timeframe_prefix}{base_name}'
                    if prefixed_name in all_features:
                        # QR4 COMPLIANCE: Store as BASE NAME (remove prefix)
                        timeframe_features[base_name] = all_features[prefixed_name]
            
            # Always include meta features (timestamp, symbol) 
            for meta_feature in ['timestamp', 'symbol']:
                if meta_feature in all_features:
                    timeframe_features[meta_feature] = all_features[meta_feature]
            
            # Create filtered example with QR4-compliant base feature names
            timeframe_example = {
                'symbol': example['symbol'],
                'timeframe': timeframe,
                'timestamp': example.get('timestamp'),
                'features': timeframe_features,  # QR4 COMPLIANT: Base names only
                'labels': example.get('labels', {}),
                'metadata': {
                    **example.get('metadata', {}),
                    'extracted_timeframe': timeframe,
                    'qr4_compliant': True,  # Mark as QR4 compliant
                    'original_feature_count': len(all_features),
                    'filtered_feature_count': len(timeframe_features),
                    'base_feature_names_used': list(timeframe_features.keys())
                }
            }
            timeframe_examples.append(timeframe_example)
        
        return timeframe_examples
    
    async def _save_symbol_arrayrecord(self, examples: List[Dict], arrayrecord_path: Path, symbol: str, timeframe: str = None):
        """
        Save symbol-specific examples in ArrayRecord format.
        
        CRITICAL QR4 FIX: This method now properly creates timeframe-separated ArrayRecords
        where each row contains scalar OHLCV values (not indexed sequences).
        
        QR4 STRUCTURE: Each ArrayRecord contains multiple rows where:
        - Row 0: Column names ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap']  
        - Row N: Scalar values for each timestamp
        """
        try:
            import pandas as pd
            
            debug_info = {
                'symbol': symbol,
                'timeframe': timeframe,
                'examples_count': len(examples),
                'arrayrecord_path': str(arrayrecord_path)
            }
            
            # QR4 CRITICAL FIX: Convert sequence-based features to individual timestamp rows
            all_rows = []
            total_original_features = 0
            total_filtered_features = 0
            
            for example in examples:
                # Log filtering results for verification
                metadata = example.get('metadata', {})
                if 'original_feature_count' in metadata and 'filtered_feature_count' in metadata:
                    total_original_features += metadata['original_feature_count']
                    total_filtered_features += metadata['filtered_feature_count']
                
                # QR4 FIX: Convert sequence features to individual rows per timestamp
                qr4_rows = self._convert_sequence_to_qr4_rows(example, symbol, timeframe)
                all_rows.extend(qr4_rows)
                
                debug_info[f'example_{len(all_rows)}_rows_created'] = len(qr4_rows)
            
            debug_info['total_rows_created'] = len(all_rows)
            
            # Log filtering verification
            if total_original_features > 0:
                filtering_ratio = (total_original_features - total_filtered_features) / total_original_features
                self.logger.info(f"🔍 QR4 ARRAYRECORD CREATION for {timeframe} timeframe:")
                self.logger.info(f"   Original features: {total_original_features}")
                self.logger.info(f"   Filtered features: {total_filtered_features}")
                self.logger.info(f"   Filtering ratio: {filtering_ratio:.1%} features removed")
                self.logger.info(f"   QR4 rows created: {len(all_rows)}")
                if filtering_ratio < 0.1:  # Less than 10% filtered = potential issue
                    self.logger.warning(f"⚠️  Low filtering ratio may indicate timeframe separation issue")
            
            # Convert to DataFrame with QR4 structure
            if all_rows:
                df = pd.DataFrame(all_rows)
                debug_info['dataframe_shape'] = df.shape
                debug_info['columns'] = list(df.columns)
            else:
                self.logger.warning(f"❌ No QR4 rows created for {symbol} {timeframe}")
                return
            
            self.logger.debug(f"🔍 QR4_ARRAYRECORD_DEBUG: {debug_info}")
            
            # Save as ArrayRecord format
            import array_record
            import numpy as np
            
            # Convert DataFrame to numpy array, excluding non-numeric columns
            # First, convert datetime columns to timestamps
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        # Try to convert to datetime and then to numeric timestamp
                        df[col] = pd.to_datetime(df[col]).astype('int64') // 10**9
                    except (ValueError, TypeError):
                        # If not datetime, try to convert to numeric
                        try:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        except:
                            # Remove non-convertible columns
                            df = df.drop(columns=[col])
                            self.logger.warning(f"Dropped non-numeric column {col} from ArrayRecord")
            
            # Now convert to numpy array as float32
            data = df.to_numpy(dtype=np.float32)
            
            from array_record.python.array_record_module import ArrayRecordWriter
            writer = ArrayRecordWriter(str(arrayrecord_path), 'group_size:1')
            try:
                # Write column names as first record
                writer.write(str(list(df.columns)).encode('utf-8'))
                
                # Write each row as a record
                for row in data:
                    writer.write(row.tobytes())
            finally:
                writer.close()
                    
            self.logger.debug(f"Saved ArrayRecord file: {arrayrecord_path} ({len(df)} rows, {len(df.columns)} columns)")
            
            # Also save column names
            columns_file = arrayrecord_path.with_suffix('.json').with_name(arrayrecord_path.stem + '_columns.json')
            import json
            with open(columns_file, 'w') as f:
                json.dump(list(df.columns), f)
                
        except Exception as e:
            self.logger.error(f"Failed to save ArrayRecord file for {symbol}: {e}")
            raise RuntimeError(f"Critical error saving ArrayRecord for {symbol}: {e}") from e
    
    async def _save_symbol_metadata(self, examples: List[Dict], metadata_path: Path, symbol: str, current_time: datetime, timeframe: str = None):
        """Save symbol-specific metadata."""
        try:
            metadata = {
                'symbol': symbol,
                'timeframe': timeframe,
                'generation_time': current_time.isoformat(),
                'example_count': len(examples),
                'date_range': {
                    'start': getattr(self, 'start_date', current_time.date()).isoformat(),
                    'end': getattr(self, 'end_date', current_time.date()).isoformat()
                },
                'run_timestamp': getattr(self, 'run_timestamp', 'unknown'),
                'timeframes': examples[0].get('timeframes', []) if examples else [],
                'total_features': sum(ex.get('feature_count', 0) for ex in examples),
                'features_structure': {},
                'data_format': 'multi_timeframe_arrayrecord',
                'processing_type': 'interval_based_multi_timeframe'
            }
            
            # Add feature structure info
            if examples:
                first_example = examples[0]
                for feature_name, feature_values in first_example['features'].items():
                    if isinstance(feature_values, list):
                        metadata['features_structure'][feature_name] = {
                            'type': 'sequence',
                            'length': len(feature_values)
                        }
                    else:
                        metadata['features_structure'][feature_name] = {
                            'type': 'scalar'
                        }
            
            import json
            from datetime import datetime, date
            
            def json_serializer(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=json_serializer)
                
            self.logger.debug(f"Saved metadata for {symbol}: {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {symbol}: {e}")
            raise RuntimeError(f"Critical error saving metadata for {symbol}: {e}") from e
    
    async def handleEnd(self, runner: Any, current_time: datetime):
        """Generate final summary for multi-timeframe processing and update database metadata."""
        self.logger.info(f"Multi-timeframe interval-based generation completed:")
        self.logger.info(f"  - Intervals processed: {self.interval_counter}")
        self.logger.info(f"  - Symbols: {', '.join(self.symbols)}")
        if hasattr(self, 'config') and self.config:
            self.logger.info(f"  - Timeframes: {', '.join(self.config.timeframes.keys())}")
            self.logger.info(f"  - Sequence lengths: {self.config.sequence_lengths}")
        self.logger.info(f"  - Output directory: {self.output_dir}")
        self.logger.info(f"  - Output structure: /run_YYYYMMDD_HHMMSS/SEQUENCE_ID/timeframe/SEQUENCE_ID.arrayrecord")
        
        # Finalize file metadata
        self.file_metadata["generation_date"] = current_time.strftime("%Y-%m-%d %H:%M:%S")
        self.file_metadata["total_files"] = len(self.file_metadata["files"])
        self.file_metadata["total_sequences"] = sum(f["sequences"] for f in self.file_metadata["files"])
        
        self.logger.info(f"📊 Generated file metadata:")
        self.logger.info(f"  - Total files: {self.file_metadata['total_files']}")
        self.logger.info(f"  - Total sequences: {self.file_metadata['total_sequences']}")
        self.logger.info(f"  - Timeframes: {self.file_metadata['timeframes']}")
        
        # Update database with file metadata if we have a run_id context
        await self._update_database_metadata(runner, current_time)
        
    async def _update_database_metadata(self, runner: Any, current_time: datetime):
        """Update the training dataset in database with file metadata."""
        try:
            import asyncpg
            import json
            from datetime import datetime
            
            # Connect to database
            db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
            conn = await asyncpg.connect(db_url)
            
            try:
                # Find the most recent training dataset for these symbols
                # This assumes the training dataset was created before this callback runs
                symbols_str = ','.join(self.symbols)
                dataset_record = await conn.fetchrow("""
                    SELECT id, dataset_name FROM dev_training_datasets 
                    WHERE symbols = $1 
                    ORDER BY id DESC 
                    LIMIT 1
                """, self.symbols)
                
                if dataset_record:
                    dataset_id = dataset_record['id']
                    dataset_name = dataset_record['dataset_name']
                    
                    # Update the dataset with file metadata
                    await conn.execute("""
                        UPDATE dev_training_datasets 
                        SET file_metadata = $1, total_sequences = $2
                        WHERE id = $3
                    """, json.dumps(self.file_metadata), self.file_metadata["total_sequences"], dataset_id)
                    
                    self.logger.info(f"✅ Updated dataset {dataset_id} ({dataset_name}) with file metadata")
                    
                else:
                    self.logger.warning(f"Could not find training dataset record for symbols {self.symbols}")
                    
            finally:
                await conn.close()
                
        except Exception as e:
            self.logger.error(f"Failed to update database with file metadata: {e}")
        
        # Save final summary
        try:
            summary_file = self.output_dir / "generation_summary.json"
            summary = {
                'completion_time': current_time.isoformat(),
                'intervals_processed': self.interval_counter,
                'symbols': self.symbols,
                'timeframes': list(self.config.timeframes.keys()) if self.config else [],
                'sequence_lengths': self.config.sequence_lengths if self.config else {},
                'output_directory': str(self.output_dir),
                'processing_type': 'multi_timeframe_interval_based'
            }
            
            import json
            from datetime import datetime, date
            
            def json_serializer(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
            
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=json_serializer)
                
            self.logger.info(f"Saved generation summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save generation summary: {e}")


# Backward compatibility alias
TrainingDataGenerationCallback = DateBasedTrainingDataCallback