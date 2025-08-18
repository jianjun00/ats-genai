#!/usr/bin/env python3
"""
Run ID System Demonstration

This script demonstrates the new run_id system for organizing artifacts
and ensuring universe_state isolation between runs.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path

from core.run_context import RunContextManager, create_run_context
from state.run_aware_universe_state_manager import RunAwareUniverseStateManager
from config.environment import Environment, EnvironmentType

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sample_universe_state_data(run_id: str, timestamp: str) -> pd.DataFrame:
    """Create sample universe state data for testing."""
    import numpy as np
    
    # Create sample data with run_id in the data for demonstration
    n_instruments = 100
    n_intervals = 50
    
    data = []
    for i in range(n_instruments):
        for j in range(n_intervals):
            data.append({
                'instrument_id': i + 1,
                'interval_id': j + 1,
                'timestamp': timestamp,
                'run_id': run_id,
                'price': 100 + np.random.normal(0, 5),
                'volume': np.random.randint(1000, 10000),
                'open': 100 + np.random.normal(0, 5),
                'high': 105 + np.random.normal(0, 3),
                'low': 95 + np.random.normal(0, 3),
                'close': 100 + np.random.normal(0, 5),
                'source': 'polygon' if i % 2 == 0 else 'tiingo'
            })
    
    return pd.DataFrame(data)


def demo_run_id_system():
    """Demonstrate the run_id system."""
    
    logger.info("🚀 Starting Run ID System Demonstration")
    logger.info("=" * 60)
    
    # Initialize run context manager
    run_manager = RunContextManager(base_artifacts_dir="demo_runs")
    
    # Demo 1: Create multiple runs to show isolation
    logger.info("📋 Demo 1: Creating multiple isolated runs")
    
    runs = []
    for i in range(3):
        # Create run context with custom metadata
        metadata = {
            'demo_number': i + 1,
            'demo_type': 'isolation_test',
            'universe_size': 100,
            'intervals': 50
        }
        
        run_context = run_manager.create_run_context(metadata=metadata)
        runs.append(run_context)
        
        logger.info(f"  ✅ Created run {i+1}: {run_context.run_id}")
        logger.info(f"     Directory: {run_context.base_dir}")
        
        # Create run-aware universe state manager
        env = Environment(EnvironmentType.TEST)
        universe_manager = RunAwareUniverseStateManager(env=env, run_context=run_context)
        
        # Create and save sample data for this run
        for j in range(3):  # Multiple timestamps per run
            timestamp = f"2024-12-18T{10+j:02d}:00:00"
            df = create_sample_universe_state_data(run_context.run_id, timestamp)
            
            saved_path = universe_manager.save_universe_state(df, timestamp)
            if saved_path:
                logger.info(f"     📁 Saved universe state: {Path(saved_path).name}")
            else:
                logger.warning(f"     ❌ Failed to save universe state for {timestamp}")
        
        # Show run summary
        summary = universe_manager.get_run_summary()
        logger.info(f"     📊 Run summary: {summary['file_count']} files, {summary['total_size_mb']} MB")
    
    logger.info("")
    
    # Demo 2: Show cross-run data access
    logger.info("🔄 Demo 2: Cross-run data access")
    
    # Use the first run's manager to access data from other runs
    main_manager = RunAwareUniverseStateManager(env=env, run_context=runs[0])
    
    for run_context in runs:
        logger.info(f"  📖 Accessing data from run: {run_context.run_id}")
        
        # List available timestamps for this run
        timestamps = main_manager.list_available_timestamps(run_context.run_id)
        logger.info(f"     Available timestamps: {timestamps}")
        
        # Load data from this run
        if timestamps:
            df = main_manager.load_universe_state(timestamps[0], from_run_id=run_context.run_id)
            if df is not None:
                logger.info(f"     ✅ Loaded {len(df)} records from {timestamps[0]}")
                
                # Show unique run_ids in the data
                unique_run_ids = df['run_id'].unique()
                logger.info(f"     🔍 Data contains run_ids: {list(unique_run_ids)}")
    
    logger.info("")
    
    # Demo 3: Show run management features
    logger.info("🛠️  Demo 3: Run management features")
    
    # List all runs
    all_runs = run_manager.list_runs()
    logger.info(f"  📋 Total runs in system: {len(all_runs)}")
    
    for i, run_data in enumerate(all_runs[:3]):  # Show first 3
        logger.info(f"     {i+1}. Run ID: {run_data['run_id']}")
        logger.info(f"        Created: {run_data['metadata']['created_at']}")
        logger.info(f"        Demo type: {run_data['metadata'].get('demo_type', 'unknown')}")
    
    logger.info("")
    
    # Demo 4: Directory structure
    logger.info("📁 Demo 4: Generated directory structure")
    
    def show_directory_tree(path: Path, prefix: str = "", max_depth: int = 3, current_depth: int = 0):
        """Show directory tree structure."""
        if current_depth >= max_depth:
            return
            
        if path.is_dir():
            items = sorted(path.iterdir())
            for i, item in enumerate(items):
                is_last = i == len(items) - 1
                current_prefix = "└── " if is_last else "├── "
                logger.info(f"{prefix}{current_prefix}{item.name}")
                
                if item.is_dir() and current_depth < max_depth - 1:
                    next_prefix = prefix + ("    " if is_last else "│   ")
                    show_directory_tree(item, next_prefix, max_depth, current_depth + 1)
    
    logger.info(f"  Directory structure in: {run_manager.base_artifacts_dir}")
    show_directory_tree(run_manager.base_artifacts_dir)
    
    logger.info("")
    
    # Demo 5: Run-specific artifact organization
    logger.info("🎯 Demo 5: Run-specific artifacts")
    
    for i, run_context in enumerate(runs[:2]):  # Show first 2 runs
        logger.info(f"  Run {i+1}: {run_context.run_id}")
        logger.info(f"    Base directory: {run_context.base_dir}")
        logger.info(f"    Universe state: {run_context.universe_state_dir}")
        logger.info(f"    Artifacts: {run_context.artifacts_dir}")
        logger.info(f"    Logs: {run_context.logs_dir}")
        
        # Show files in universe_state directory
        universe_files = list(run_context.universe_state_dir.glob("**/*"))
        logger.info(f"    Universe state files ({len(universe_files)}):")
        for file_path in universe_files[:5]:  # Show first 5
            if file_path.is_file():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"      - {file_path.name} ({size_mb:.2f} MB)")
    
    logger.info("")
    logger.info("✅ Run ID System Demonstration Complete!")
    logger.info("=" * 60)
    
    # Summary of benefits
    logger.info("🎉 Key Benefits Demonstrated:")
    logger.info("  ✅ Unique run_id generation with timestamps")
    logger.info("  ✅ Isolated directory structure per run")
    logger.info("  ✅ Universe state isolation between runs")
    logger.info("  ✅ Cross-run data access capabilities")
    logger.info("  ✅ Enhanced metadata and traceability")
    logger.info("  ✅ Organized artifact management")
    
    return runs


def demo_runner_integration():
    """Demonstrate integration with the Runner class."""
    
    logger.info("")
    logger.info("🏃 Bonus Demo: Runner Integration")
    logger.info("-" * 40)
    
    from app.runner import Runner
    
    # Create a run context
    run_context = create_run_context(metadata={
        'demo_type': 'runner_integration',
        'component': 'runner'
    })
    
    logger.info(f"  📋 Created run context: {run_context.run_id}")
    
    # Create Runner with run context (would normally be configured via gin)
    try:
        env = Environment(EnvironmentType.TEST)
        
        # Note: This creates a minimal runner for demo purposes
        # In real usage, you'd configure this properly with gin
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10", 
            environment=env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            run_context=run_context,
            enable_run_isolation=True
        )
        
        logger.info(f"  ✅ Created Runner with run_id: {runner.run_context.run_id}")
        logger.info(f"  🗂️  Universe state directory: {runner.universe_state_manager.base_path}")
        logger.info(f"  📊 Using run-aware manager: {isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)}")
        
    except Exception as e:
        logger.warning(f"  ⚠️  Runner demo failed (missing dependencies): {e}")
        logger.info("  💡 This is normal in isolated demo environment")


if __name__ == "__main__":
    # Run the demonstration
    try:
        runs = demo_run_id_system()
        demo_runner_integration()
        
        # Optional: Clean up demo runs
        cleanup = input("\n🗑️  Clean up demo runs? (y/N): ").lower().strip()
        if cleanup == 'y':
            for run_context in runs:
                try:
                    import shutil
                    shutil.rmtree(run_context.base_dir)
                    logger.info(f"  ✅ Cleaned up run: {run_context.run_id}")
                except Exception as e:
                    logger.warning(f"  ⚠️  Failed to clean up {run_context.run_id}: {e}")
    
    except KeyboardInterrupt:
        logger.info("\n👋 Demo interrupted by user")
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise