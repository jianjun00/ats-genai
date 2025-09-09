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
                 output_dir: str = "/data/training_data"):
        """Initialize interval-based callback."""
        self.symbols = symbols
        self.config = config  # Use provided config or None
        self.storage_format = storage_format
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
        Save examples using simple ArrayRecord format per PRD/DRD QR4/QR5 requirements.
        
        Directory structure: /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/
        File naming: SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord
        Each timeframe gets separate ArrayRecord file with scalar values only.
        """
        import array_record.python.array_record_module as array_record
        import numpy as np
        from datetime import datetime, date
        
        try:
            print(f"DEBUG SIMPLE SAVE: Saving {len(examples)} examples using simple ArrayRecord format")
            
            # Get dataset_id from callback (set by runner)
            dataset_id = getattr(self, 'dataset_id', 'unknown_dataset')
            print(f"DEBUG SIMPLE SAVE: Retrieved dataset_id: '{dataset_id}'")
            print(f"DEBUG SIMPLE SAVE: self.output_dir: '{self.output_dir}'")
            
            # Determine start/end datetime from the training run
            # For now, use current date for both start and end (can be enhanced later)
            current_date = current_time.date() if isinstance(current_time, datetime) else date.today()
            start_datetime = f"{current_date.strftime('%Y%m%d')}_000000"
            end_datetime = f"{current_date.strftime('%Y%m%d')}_235959"
            print(f"DEBUG SIMPLE SAVE: start_datetime: '{start_datetime}', end_datetime: '{end_datetime}'")
            
            for example in examples:
                symbol = example['symbol']
                prediction_timestamp = example['prediction_timestamp']
                
                # Create PRD/DRD compliant directory structure
                # /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/
                symbol_datetime_str = f"{symbol}_{start_datetime}_{end_datetime}"
                dataset_dir = self.output_dir / str(dataset_id) / symbol_datetime_str
                
                print(f"DEBUG SIMPLE SAVE: symbol_datetime_str: '{symbol_datetime_str}'")
                print(f"DEBUG SIMPLE SAVE: Full dataset directory path: '{dataset_dir}'")
                print(f"DEBUG SIMPLE SAVE: Expected structure: /data/training_data/{dataset_id}/{symbol_datetime_str}/{{timeframe}}/")
                print(f"DEBUG SIMPLE SAVE: Creating dataset directory: {dataset_dir}")
                
                # Extract timeframe features per QR4 requirements
                if 'timeframe_features' in example and isinstance(example['timeframe_features'], dict):
                    for timeframe, features in example['timeframe_features'].items():
                        if not features:
                            continue
                            
                        # Create timeframe directory
                        timeframe_dir = dataset_dir / timeframe
                        timeframe_dir.mkdir(parents=True, exist_ok=True)
                        
                        # Create QR4-compliant row with scalar values only
                        qr4_row = {
                            'timestamp': prediction_timestamp,
                            'symbol': symbol,
                            'open': float(features.get('open', 0.0)),
                            'high': float(features.get('high', 0.0)), 
                            'low': float(features.get('low', 0.0)),
                            'close': float(features.get('close', 0.0)),
                            'volume': float(features.get('volume', 0.0)),
                            'vwap': float(features.get('vwap', 0.0))
                        }
                        
                        # Save as ArrayRecord file per PRD/DRD
                        # File naming: SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord
                        arrayrecord_file = timeframe_dir / f"{symbol_datetime_str}.arrayrecord"
                        
                        print(f"DEBUG SIMPLE SAVE: timeframe_dir: '{timeframe_dir}'")
                        print(f"DEBUG SIMPLE SAVE: ArrayRecord filename: '{symbol_datetime_str}.arrayrecord'")
                        print(f"DEBUG SIMPLE SAVE: Full ArrayRecord path: '{arrayrecord_file}'")
                        print(f"DEBUG SIMPLE SAVE: QR4 row data: {qr4_row}")
                        
                        # Ensure the timeframe directory exists
                        print(f"DEBUG SIMPLE SAVE: timeframe_dir.exists(): {timeframe_dir.exists()}")
                        
                        # Convert QR4 row to JSON bytes for ArrayRecord
                        import json
                        # Convert datetime to ISO string for JSON serialization
                        qr4_serializable = qr4_row.copy()
                        if isinstance(qr4_serializable['timestamp'], datetime):
                            qr4_serializable['timestamp'] = qr4_serializable['timestamp'].isoformat()
                        
                        print(f"DEBUG SIMPLE SAVE: About to create ArrayRecordWriter for {arrayrecord_file}")
                        writer = array_record.ArrayRecordWriter(str(arrayrecord_file), 'group_size:1')
                        print(f"DEBUG SIMPLE SAVE: ArrayRecordWriter created successfully")
                        
                        json_bytes = json.dumps(qr4_serializable).encode('utf-8')
                        print(f"DEBUG SIMPLE SAVE: JSON bytes length: {len(json_bytes)}")
                        
                        writer.write(json_bytes)
                        print(f"DEBUG SIMPLE SAVE: Data written to ArrayRecord")
                        
                        writer.close()
                        print(f"DEBUG SIMPLE SAVE: ArrayRecordWriter closed")
                        
                        # Verify file was created
                        if arrayrecord_file.exists():
                            file_size = arrayrecord_file.stat().st_size
                            print(f"✅ Saved {timeframe} data to {arrayrecord_file} (size: {file_size} bytes)")
                        else:
                            print(f"❌ ArrayRecord file was NOT created: {arrayrecord_file}")
                
                print(f"✅ Completed saving example for {symbol} at {prediction_timestamp}")
            
            self.logger.info(f"Successfully saved {len(examples)} examples using simple ArrayRecord format")
            
        except Exception as e:
            print(f"❌ DEBUG SIMPLE SAVE: Exception during save: {e}")
            import traceback
            print(f"❌ DEBUG SIMPLE SAVE: Full traceback: {traceback.format_exc()}")
            self.logger.error(f"Failed to save examples at {current_time}: {e}")

    async def _save_interval_examples(self, examples: List[Dict], current_time: datetime):
        """Legacy method - redirects to simple ArrayRecord save."""
        await self._save_simple_arrayrecord(examples, current_time)

    async def handleEnd(self, runner: Any, current_time: datetime):
        """Generate final summary."""
        self.logger.info(f"Interval-based generation completed: {self.interval_counter} intervals processed")


# Backward compatibility alias
TrainingDataGenerationCallback = IntervalBasedTrainingDataCallback