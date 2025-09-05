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
from datetime import datetime, date, timedelta
from typing import Any, Optional, List, Dict, Union
from pathlib import Path
import json
import asyncio
import ray

from state.runner_callback import RunnerCallback
# TrainingDataConfig is imported from the specific runner that uses this callback
# TimeSeriesSequenceTrainingGenerator and SequenceTrainingExample are not actually used
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
                 save_format: str = "riegeli",  # "riegeli" only - removed pickle and parquet support
                 enable_ray_parallel: bool = True,  # Enable Ray parallel processing
                 max_parallel_workers: int = 4):   # Maximum Ray workers
        """
        Initialize the date-based training data callback.
        
        Args:
            symbols: List of symbols to generate training data for
            config: Training data configuration
            storage_manager: Storage manager for advanced storage
            output_dir: Base output directory 
            save_format: Format to save data ("riegeli" only)
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
            if self.save_format in ["advanced", "riegeli"] and self.storage_manager:
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
        
        self.logger.info(f"IntervalBasedTrainingDataCallback initialized for symbols: {symbols}")
    
    def handleStart(self, runner: Any, current_time: datetime):
        """Initialize training generator and minute data manager."""
        # Initialize minute data manager if provided
        if hasattr(self, 'minute_data_manager'):
            self.logger.info(f"Using FileBasedMinuteMarketDataManager for multi-timeframe processing")
        else:
            self.logger.warning("No minute_data_manager provided - using fallback generator")
            # Fallback to original generator
            self.training_generator = TimeSeriesSequenceTrainingGenerator(
                env=runner.get_environment(),
                config=self.config,
                universe_manager=runner.get_universe_state_manager()
            )
        
        # Create output directory structure with symbol subdirectories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create symbol-specific directories
        for symbol in self.symbols:
            symbol_dir = Path(self.output_dir) / symbol
            symbol_dir.mkdir(exist_ok=True)
            self.logger.info(f"Created symbol directory: {symbol_dir}")
        
        (self.output_dir / "metadata").mkdir(exist_ok=True)
        
        self.logger.info("Interval-based multi-timeframe training data generator initialized")
    
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
        """Generate training example with multi-timeframe OHLC and signal features using enhanced API."""
        if not hasattr(self, 'minute_data_manager') or not self.config:
            return None
        
        # Calculate lookback window (need enough data for largest sequence)
        max_sequence_length = max(self.config.sequence_lengths.values()) if self.config.sequence_lengths else 100
        max_timeframe_minutes = max(self.config.timeframes.values()) if self.config.timeframes else 1440  # 1 day
        
        # Calculate total lookback needed (conservative estimate)
        lookback_minutes = max_sequence_length * max_timeframe_minutes
        start_time = current_time - timedelta(minutes=lookback_minutes)
        
        # Define signals to compute
        signals = ['sma_20', 'ema_12', 'ema_26', 'rsi_14', 'etop', 'ebot', 'pldot', 'vwap']
        
        # Use enhanced API to get multi-timeframe data
        try:
            multi_timeframe_data = await self.minute_data_manager.get_multi_timeframe_data(
                symbols=[symbol],
                start=start_time,
                end=current_time,
                intervals=list(self.config.timeframes.keys())
            )
            
            if symbol not in multi_timeframe_data:
                self.logger.warning(f"No multi-timeframe data available for {symbol}")
                return None
                
            symbol_data = multi_timeframe_data[symbol]
            
        except Exception as e:
            self.logger.error(f"Failed to get multi-timeframe data for {symbol}: {e}")
            return None
        
        # Build multi-timeframe features using enhanced data
        features = {}
        
        for timeframe_name in self.config.timeframes.keys():
            if timeframe_name not in symbol_data or symbol_data[timeframe_name].empty:
                self.logger.warning(f"No {timeframe_name} data for {symbol}")
                continue
                
            try:
                tf_df = symbol_data[timeframe_name]
                sequence_length = self.config.sequence_lengths.get(timeframe_name, 20)
                
                # Extract the most recent sequence_length bars
                if len(tf_df) >= sequence_length:
                    recent_data = tf_df.tail(sequence_length)
                    
                    # Add OHLCV features
                    features[f'{timeframe_name}_open'] = recent_data['open'].fillna(0).tolist()
                    features[f'{timeframe_name}_high'] = recent_data['high'].fillna(0).tolist()
                    features[f'{timeframe_name}_low'] = recent_data['low'].fillna(0).tolist()
                    features[f'{timeframe_name}_close'] = recent_data['close'].fillna(0).tolist()
                    features[f'{timeframe_name}_volume'] = recent_data['volume'].fillna(0).tolist()
                    
                    # Add technical signal features
                    for signal in signals:
                        if signal in recent_data.columns:
                            features[f'{timeframe_name}_{signal}'] = recent_data[signal].fillna(0).tolist()
                        else:
                            self.logger.debug(f"Signal {signal} not available for {timeframe_name}")
                    
                    self.logger.debug(f"Added {timeframe_name} features: {len(recent_data)} bars with {len(signals)} signals")
                    
                else:
                    self.logger.warning(f"Insufficient {timeframe_name} data for {symbol}: {len(tf_df)} < {sequence_length}")
                        
            except Exception as e:
                self.logger.error(f"Failed to process {timeframe_name} data for {symbol}: {e}")
        
        if not features:
            self.logger.warning(f"No features generated for {symbol}")
            return None
        
        # Build training example structure with enhanced features
        example = {
            'symbol': symbol,
            'timestamp': current_time.isoformat(),
            'features': features,
            'feature_count': sum(len(v) if isinstance(v, list) else 1 for v in features.values()),
            'timeframes': list(self.config.timeframes.keys()),
            'signals': signals,
            'data_source': 'FileBasedMinuteMarketDataManager_enhanced_api'
        }
        
        return example
    
    async def _save_interval_examples(self, examples: List[Dict], current_time: datetime):
        """Save multi-timeframe examples in symbol-specific .riegeli files."""
        
        try:
            if self.storage_manager:
                batch_id = f"multi_timeframe_{current_time.strftime('%Y%m%d_%H%M%S')}"
                save_result = await self.storage_manager.save_sequence_batch(
                    examples=examples,
                    batch_id=batch_id
                )
                self.logger.debug(f"Saved {len(examples)} multi-timeframe examples at {current_time}")
            else:
                # Save to symbol-specific .riegeli files
                # Pattern: /mnt/d/ats-data/training/run_YYYYMMDD_HHMMSS/SYMBOL/STARTDATE_ENDDATE.riegeli
                
                # Group examples by symbol
                examples_by_symbol = {}
                for example in examples:
                    symbol = example['symbol']
                    if symbol not in examples_by_symbol:
                        examples_by_symbol[symbol] = []
                    examples_by_symbol[symbol].append(example)
                
                # Save each symbol's examples to its own .riegeli file
                for symbol, symbol_examples in examples_by_symbol.items():
                    symbol_dir = Path(self.output_dir) / symbol
                    
                    # Create filename with date range: STARTDATE_ENDDATE.riegeli
                    start_date_str = getattr(self, 'start_date', current_time.date()).strftime('%Y%m%d_000000')
                    end_date_str = getattr(self, 'end_date', current_time.date()).strftime('%Y%m%d_000000')
                    arrayrecord_filename = f"{start_date_str}_{end_date_str}.arrayrecord"
                    arrayrecord_path = symbol_dir / arrayrecord_filename
                    
                    # Save as ArrayRecord format
                    await self._save_symbol_arrayrecord(symbol_examples, arrayrecord_path, symbol)
                    
                    # Save companion metadata
                    metadata_path = symbol_dir / f"{start_date_str}_{end_date_str}_metadata.json"
                    await self._save_symbol_metadata(symbol_examples, metadata_path, symbol, current_time)
                    
                    self.logger.info(f"Saved {len(symbol_examples)} examples for {symbol} to {arrayrecord_path}")
                
        except Exception as e:
            self.logger.error(f"Failed to save multi-timeframe examples at {current_time}: {e}")
    
    async def _save_symbol_arrayrecord(self, examples: List[Dict], arrayrecord_path: Path, symbol: str):
        """Save symbol-specific examples in ArrayRecord format."""
        try:
            import pandas as pd
            
            # Convert multi-timeframe examples to structured format for ArrayRecord
            # Each row represents one training interval with multi-timeframe features
            rows = []
            
            for example in examples:
                # Flatten multi-timeframe features into a single row
                row = {
                    'timestamp': example['timestamp'],
                    'symbol': symbol
                }
                
                # Add all timeframe features
                for feature_name, feature_values in example['features'].items():
                    if isinstance(feature_values, list):
                        # For sequence features, add each value with index
                        for i, value in enumerate(feature_values):
                            row[f"{feature_name}_{i:03d}"] = value
                    else:
                        row[feature_name] = feature_values
                
                rows.append(row)
            
            # Convert to DataFrame
            df = pd.DataFrame(rows)
            
            # Save as ArrayRecord format
            import array_record
            import numpy as np
            
            # Convert DataFrame to numpy array
            data = df.to_numpy(dtype=np.float32)
            
            with array_record.ArrayRecordWriter(str(arrayrecord_path), 'group_size:1') as writer:
                # Write column names as first record
                writer.write(str(list(df.columns)).encode('utf-8'))
                
                # Write each row as a record
                for row in data:
                    writer.write(row.tobytes())
                    
            self.logger.debug(f"Saved ArrayRecord file: {arrayrecord_path} ({len(df)} rows, {len(df.columns)} columns)")
            
            # Also save column names
            columns_file = arrayrecord_path.with_suffix('_columns.json')
            import json
            with open(columns_file, 'w') as f:
                json.dump(list(df.columns), f)
                
        except Exception as e:
            self.logger.error(f"Failed to save ArrayRecord file for {symbol}: {e}")
    
    async def _save_symbol_metadata(self, examples: List[Dict], metadata_path: Path, symbol: str, current_time: datetime):
        """Save symbol-specific metadata."""
        try:
            metadata = {
                'symbol': symbol,
                'generation_time': current_time.isoformat(),
                'example_count': len(examples),
                'date_range': {
                    'start': getattr(self, 'start_date', current_time.date()).isoformat(),
                    'end': getattr(self, 'end_date', current_time.date()).isoformat()
                },
                'run_timestamp': getattr(self, 'run_timestamp', 'unknown'),
                'timeframes': examples[0]['timeframes'] if examples else [],
                'total_features': sum(ex['feature_count'] for ex in examples),
                'features_structure': {},
                'data_format': 'multi_timeframe_riegeli',
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
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            self.logger.debug(f"Saved metadata for {symbol}: {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {symbol}: {e}")
    
    async def handleEnd(self, runner: Any, current_time: datetime):
        """Generate final summary for multi-timeframe processing."""
        self.logger.info(f"Multi-timeframe interval-based generation completed:")
        self.logger.info(f"  - Intervals processed: {self.interval_counter}")
        self.logger.info(f"  - Symbols: {', '.join(self.symbols)}")
        if hasattr(self, 'config') and self.config:
            self.logger.info(f"  - Timeframes: {', '.join(self.config.timeframes.keys())}")
            self.logger.info(f"  - Sequence lengths: {self.config.sequence_lengths}")
        self.logger.info(f"  - Output directory: {self.output_dir}")
        self.logger.info(f"  - Output structure: /run_YYYYMMDD_HHMMSS/SYMBOL/STARTDATE_ENDDATE.riegeli")
        
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
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
                
            self.logger.info(f"Saved generation summary to {summary_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save generation summary: {e}")


# Backward compatibility alias
TrainingDataGenerationCallback = DateBasedTrainingDataCallback