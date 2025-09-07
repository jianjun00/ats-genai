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
# TimeSeriesSequenceTrainingGenerator and SequenceTrainingExample are not actually used
from domains.ml.services.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig


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
                # No storage manager - examples processed but not saved
                self.logger.debug(f"Processed {len(examples)} examples at {current_time} (no storage configured)")
                
        except Exception as e:
            self.logger.error(f"Failed to save interval examples at {current_time}: {e}")
    
    async def handleEnd(self, runner: Any, current_time: datetime):
        """Generate final summary."""
        self.logger.info(f"Interval-based generation completed: {self.interval_counter} intervals processed")


# Backward compatibility alias
TrainingDataGenerationCallback = IntervalBasedTrainingDataCallback