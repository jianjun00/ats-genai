#!/usr/bin/env python3
"""
TRAINING PIPELINE STEP-BY-STEP DEBUG

We know:
✅ Data exists (874 AAPL records for July 1st)
✅ FileBasedMinuteMarketDataManager works
❌ Training data generation fails with "No monthly file paths tracked"

This tests each step of the training pipeline to find the exact failure point.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add src and set environment
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

from core.platform.config.environment import EnvironmentType
from domains.market_data.services.core.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager


class MinimalEnvironment:
    def __init__(self):
        self.environment_type = EnvironmentType.INTEGRATION
        self.db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"

    def get_database_url(self):
        return self.db_url

    def get_table_name(self, base_name):
        return f"intg_{base_name}"


async def debug_training_pipeline_steps():
    """Debug each step of the training data pipeline."""

    print("🔍 TRAINING PIPELINE STEP-BY-STEP DEBUG")
    print("=" * 50)

    env = MinimalEnvironment()

    # Step 1: Confirm data manager works
    print(f"\n📊 STEP 1: Market Data Manager Test")
    market_data_manager = FileBasedMinuteMarketDataManager(env, "/data/minute-bars")

    start_time = datetime(2025, 7, 1, 0, 0, 0)
    end_time = datetime(2025, 7, 1, 23, 59, 59)

    batch_data = await market_data_manager.get_minute_ohlc_batch(
        symbols=["AAPL"],
        start=start_time,
        end=end_time,
        timeframe_minutes=1
    )

    if "AAPL" in batch_data:
        print(f"✅ Market data manager: {len(batch_data['AAPL'])} records")
    else:
        print(f"❌ Market data manager failed")
        return

    # Step 2: Test UniverseStateIntervalBuilder (fixed constructor)
    print(f"\n🌌 STEP 2: UniverseStateIntervalBuilder Test")

    try:
        from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder

        universe_builder = UniverseStateIntervalBuilder(
            env=env,
            market_data_manager=market_data_manager,
            base_duration="60m"
        )
        print(f"✅ UniverseStateIntervalBuilder created")

    except Exception as e:
        print(f"❌ UniverseStateIntervalBuilder failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # Step 3: Test UniverseStateManager
    print(f"\n🏗️ STEP 3: UniverseStateManager Test")

    try:
        from domains.trading.services.state.universe_state_manager import UniverseStateManager

        universe_manager = UniverseStateManager(
            env=env,
            symbols=["AAPL"],
            universe_state_interval_builder=universe_builder
        )
        print(f"✅ UniverseStateManager created")
        print(f"   Instrument IDs: {universe_manager.instrument_ids}")

    except Exception as e:
        print(f"❌ UniverseStateManager failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # Step 4: Test TimeSeriesSequenceTrainingGenerator
    print(f"\n🤖 STEP 4: TimeSeriesSequenceTrainingGenerator Test")

    try:
        from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
        from domains.ml.services.training_data.training_data_config import TrainingDataConfig

        # Create minimal training config
        config = TrainingDataConfig()
        print(f"✅ TrainingDataConfig created")

        training_generator = TimeSeriesSequenceTrainingGenerator(
            env=env,
            config=config,
            universe_manager=universe_manager
        )
        print(f"✅ TimeSeriesSequenceTrainingGenerator created")

        # Test generating training example for specific time
        test_datetime = datetime(2025, 7, 1, 10, 0)  # 10 AM July 1st

        print(f"🎯 Testing training example generation at {test_datetime}")

        training_examples = training_generator.generate_training_examples(
            symbols=["AAPL"],
            current_time=test_datetime
        )

        if training_examples and len(training_examples) > 0:
            print(f"✅ Training examples generated: {len(training_examples)}")

            for i, example in enumerate(training_examples):
                print(f"   Example {i}: {type(example)}")
                if isinstance(example, dict):
                    print(f"     Keys: {list(example.keys())}")
                    if 'symbol' in example:
                        print(f"     Symbol: {example['symbol']}")
                    if 'timeframe_features' in example:
                        tf_features = example['timeframe_features']
                        if isinstance(tf_features, dict):
                            print(f"     Timeframes: {list(tf_features.keys())}")

        else:
            print(f"❌ No training examples generated")
            print(f"   This is likely the root cause!")

    except Exception as e:
        print(f"❌ TimeSeriesSequenceTrainingGenerator failed: {e}")
        import traceback
        traceback.print_exc()

    # Step 5: Test the actual training data callback initialization
    print(f"\n📝 STEP 5: Training Data Callback Test")

    try:
        from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
        from pathlib import Path

        callback = IntervalBasedTrainingDataCallback(
            symbols=["AAPL"],
            start_date="2025-07-01",
            end_date="2025-07-01",
            output_dir=Path("/data/training_data"),
            config=config
        )
        print(f"✅ IntervalBasedTrainingDataCallback created")

        # Test the callback's training generator
        if hasattr(callback, 'training_generator') and callback.training_generator:
            print(f"✅ Callback has training generator")
        else:
            print(f"❌ Callback missing training generator")

    except Exception as e:
        print(f"❌ IntervalBasedTrainingDataCallback failed: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n" + "=" * 50)
    print(f"🎯 PIPELINE DEBUG COMPLETE")
    print(f"The failure point should now be clear!")


if __name__ == "__main__":
    asyncio.run(debug_training_pipeline_steps())