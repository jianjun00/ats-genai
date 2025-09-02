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

from state.runner_callback import RunnerCallback
# TrainingDataConfig is imported from the specific runner that uses this callback
# TimeSeriesSequenceTrainingGenerator and SequenceTrainingExample are not actually used
from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig


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
                 save_format: str = "riegeli"):  # "riegeli" only - removed pickle and parquet support
        """
        Initialize the date-based training data callback.
        
        Args:
            symbols: List of symbols to generate training data for
            config: Training data configuration
            storage_manager: Storage manager for advanced storage
            output_dir: Base output directory 
            save_format: Format to save data ("riegeli" only)
        """
        self.symbols = symbols
        self.config = config  # Use provided config or None
        self.storage_manager = storage_manager
        self.output_dir = Path(output_dir)
        self.save_format = save_format
        
        self.logger = logging.getLogger(__name__)
        
        # Training data generator (initialized when runner starts)
        self.training_generator = None
        
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
                interval_errors.append(error_msg)
        
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
    
    async def _save_daily_data(self):
        """Save daily training examples using configured format."""
        if not self.daily_examples or not self.current_date:
            return
        
        date_str = self.current_date.strftime('%Y%m%d')
        
        try:
            if self.save_format == "advanced" and self.storage_manager:
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
        
        self.interval_counter += 1
        examples_generated = []
        
        # Generate examples for all symbols
        for symbol in self.symbols:
            try:
                example = await self.training_generator.generate_training_example(
                    symbol=symbol,
                    prediction_timestamp=current_time
                )
                
                if example:
                    examples_generated.append(example)
                    
            except Exception as e:
                self.logger.error(f"Failed to generate example for {symbol}: {e}")
        
        # Save immediately if we have examples
        if examples_generated:
            await self._save_interval_examples(examples_generated, current_time)
    
    async def _save_interval_examples(self, examples: List[Dict], current_time: datetime):
        """Save examples immediately with timestamp-based naming."""
        timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')
        
        try:
            if self.storage_manager:
                batch_id = f"interval_{timestamp_str}"
                save_result = await self.storage_manager.save_sequence_batch(
                    examples=examples,
                    batch_id=batch_id
                )
                self.logger.debug(f"Saved {len(examples)} examples at {current_time}")
            else:
                # Export to interval directory
                interval_dir = self.output_dir / "intervals" / timestamp_str
                interval_dir.mkdir(parents=True, exist_ok=True)
                
                exported_files = self.training_generator.export_to_formats(
                    examples=examples,
                    output_dir=str(interval_dir),
                    formats=['pickle']
                )
                self.logger.debug(f"Exported {len(examples)} examples to {interval_dir}")
                
        except Exception as e:
            self.logger.error(f"Failed to save interval examples at {current_time}: {e}")
    
    async def handleEnd(self, runner: Any, current_time: datetime):
        """Generate final summary."""
        self.logger.info(f"Interval-based generation completed: {self.interval_counter} intervals processed")


# Backward compatibility alias
TrainingDataGenerationCallback = DateBasedTrainingDataCallback