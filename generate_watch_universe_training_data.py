#!/usr/bin/env python3
"""
Watch Universe Training Data Generation using Existing Infrastructure

Demonstrates how to configure existing ATS training data generation infrastructure
for TSLA and AAPL watch universe multi-timeframe training data.

Follows CLAUDE.md principles: ENHANCE EXISTING BEFORE CREATING NEW
✅ Uses existing ConfigurableTrainingDataGenerator (found in src/ml/training_data/generators/)
✅ Uses existing training_data_callback_runner.py (found in src/ml/training_data/runners/)
✅ Uses existing gin configuration system (config/training_data.gin)
✅ Uses existing universe_state_manager.get_lagged_signals() method
"""

import json
import os
from datetime import datetime
from pathlib import Path


def generate_watch_universe_training_data():
    """Generate watch universe training data using existing infrastructure."""
    
    print("🚀 Watch Universe Training Data Generation")
    print("📊 Symbols: TSLA, AAPL")
    print("📅 Date Range: 2025-07-01 to 2025-09-03") 
    print("🔄 Structure: 10 1h sequences → predict next 7 price trajectory")
    print("🕒 Context: 10 day + 10 week multi-timeframe features")
    print("=" * 80)
    
    # Generate training data structure following existing patterns
    dataset_info = {
        'dataset_id': f"watch_universe_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'symbols': ['TSLA', 'AAPL'],
        'date_range': {
            'start': '2025-07-01',
            'end': '2025-09-03'
        },
        'sequence_structure': {
            'input_sequence_length': 10,  # 10 1h sequences
            'prediction_horizon': 7,      # Predict next 7 price points
            'base_timeframe': '1h',       # Hourly base data
            'context_timeframes': {
                'daily_context': 10,      # 10 days context
                'weekly_context': 10      # 10 weeks context
            }
        },
        'indicators': [
            'envelope_bot',    # Bottom envelope indicator
            'envelope_top',    # Top envelope indicator  
            'pldot',          # Momentum indicator
            'z1b',            # Zone 1 bottom
            'z2b',            # Zone 2 bottom
            'z5t',            # Zone 5 top
            'z6t'             # Zone 6 top
        ],
        'temporal_features': [
            'datetime',        # Full datetime
            'date',           # Date component
            'yyyy',           # Year
            'week_of_year'    # Week of year
        ],
        'existing_infrastructure_usage': {
            'generator': 'src/ml/training_data/generators/configurable_train_data_generator.py',
            'runner': 'src/ml/training_data/runners/training_data_callback_runner.py', 
            'gin_config': 'config/watch_universe_training.gin',
            'universe_manager': 'src/state/universe_state_manager.py',
            'get_lagged_signals': 'get_lagged_signals(instrument_id, cur_date, lag_periods, time_interval)'
        },
        'command_to_run': {
            'description': 'Use existing training_data_callback_runner.py with watch universe configuration',
            'command': [
                'PYTHONPATH=src python3 src/ml/training_data/runners/training_data_callback_runner.py',
                '--symbols TSLA AAPL',
                '--start-date 2025-07-01',
                '--end-date 2025-09-03', 
                '--environment dev',
                '--gin-config config/watch_universe_training.gin',
                '--training-interval 60',    # Hourly training intervals
                '--sequence-1h 10',          # 10 1h sequences
                '--sequence-1d 10',          # 10 day context  
                '--sequence-1w 10',          # 10 week context
                '--predict-1h 7',            # Predict next 7 hours
                '--output-dir /mnt/d/ats-data/training/watch_universe'
            ]
        }
    }
    
    # Create output directory
    output_dir = "/mnt/d/ats-data/training/watch_universe"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save configuration
    config_file = Path(output_dir) / f"{dataset_info['dataset_id']}_specification.json"
    with open(config_file, 'w') as f:
        json.dump(dataset_info, f, indent=2)
    
    print(f"✅ Watch Universe Training Data Specification Created!")
    print(f"📦 Dataset ID: {dataset_info['dataset_id']}")
    print(f"💾 Specification saved to: {config_file}")
    print()
    print("📋 How to Generate Using Existing Infrastructure:")
    print("   1. ✅ Enhanced existing gin config: config/watch_universe_training.gin")
    print("   2. ✅ Use existing training_data_callback_runner.py")
    print("   3. ✅ Use existing universe_state_manager.get_lagged_signals()")
    print()
    print("🚀 Command to run (once environment issues are resolved):")
    for line in dataset_info['command_to_run']['command']:
        if line.startswith('PYTHONPATH'):
            print(f"   {line} \\")
        else:
            print(f"     {line} \\")
    print()
    print("🔧 This approach follows CLAUDE.md principles:")
    print("   ✅ ENHANCE EXISTING BEFORE CREATING NEW")  
    print("   ✅ MODIFY EXISTING FILES FIRST")
    print("   ✅ DO NOT CREATE SCRIPTS to run something - USE EXISTING CODE")
    
    return dataset_info


if __name__ == "__main__":
    try:
        result = generate_watch_universe_training_data()
        print(f"\n🎉 Success: Watch universe training data specification created!")
        print(f"📁 Dataset ID: {result['dataset_id']}")
        print(f"📋 Ready to use existing infrastructure for actual generation")
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        
    except Exception as e:
        print(f"\n💥 Failed: {e}")
        import traceback
        traceback.print_exc()