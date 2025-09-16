#!/usr/bin/env python3
"""
Tests for Run-Aware Universe State Manager
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.run_context import RunContext
from state.run_aware_universe_state_manager import RunAwareUniverseStateManager, create_run_aware_universe_state_manager
from shared.utils.environment import Environment, EnvironmentType

class TestRunAwareUniverseStateManager:
    """Test run-aware universe state manager functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.env = Environment(EnvironmentType.TEST)

        # Create test run context
        self.run_context = RunContext(
            run_id="test_run_20241218_123456_abcd1234",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run",
            artifacts_dir=Path(self.temp_dir) / "test_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run" / "logs",
            metadata={"test": "universe_state_manager"}
        )

        # Create directories
        self.run_context.universe_state_dir.mkdir(parents=True)
        self.run_context.artifacts_dir.mkdir(parents=True)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def create_test_dataframe(self, run_id: str = None, size: int = 100) -> pd.DataFrame:
        """Create test universe state dataframe."""
        import numpy as np

        if run_id is None:
            run_id = self.run_context.run_id

        return pd.DataFrame({
            'instrument_id': range(1, size + 1),
            'timestamp': '2024-12-18T10:00:00',
            'run_id': run_id,
            'price': np.random.uniform(50, 150, size),
            'volume': np.random.randint(1000, 10000, size),
            'source': ['polygon' if i % 2 == 0 else 'tiingo' for i in range(size)]
        })

    def test_initialization_with_run_context(self):
        """Test manager initialization with run context."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        assert manager.run_context == self.run_context
        assert manager.run_id == self.run_context.run_id
        assert manager.base_path == self.run_context.universe_state_dir

        # Check directories were created
        assert manager.states_dir.exists()
        assert manager.metadata_dir.exists()
        assert manager.cache_dir.exists()
        assert manager.run_artifacts_dir.exists()

    def test_initialization_without_run_context(self):
        """Test manager initialization without run context."""
        manager = RunAwareUniverseStateManager(env=self.env)

        assert manager.run_context is None
        assert manager.run_id is None
        assert "data/universe_state" in str(manager.base_path)

    def test_save_universe_state_with_run_id(self):
        """Test saving universe state with run ID organization."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context,
            write_metadata=True
        )

        df = self.create_test_dataframe()
        timestamp = "2024-12-18T10:00:00"

        # Save universe state
        saved_path = manager.save_universe_state(df, timestamp, persist_to_db=False)

        assert saved_path is not None
        saved_file = Path(saved_path)
        assert saved_file.exists()

        # Check filename format includes run_id
        expected_filename = f"{self.run_context.run_id}_universe_state_{timestamp}.parquet"
        assert saved_file.name == expected_filename

        # Verify data can be loaded
        loaded_df = pd.read_parquet(saved_path)
        assert len(loaded_df) == len(df)
        assert 'run_id' in loaded_df.columns
        assert loaded_df['run_id'].iloc[0] == self.run_context.run_id

    def test_save_and_load_universe_state(self):
        """Test saving and loading universe state."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        df = self.create_test_dataframe()
        timestamp = "2024-12-18T10:00:00"

        # Save
        saved_path = manager.save_universe_state(df, timestamp, persist_to_db=False)
        assert saved_path is not None

        # Load
        loaded_df = manager.load_universe_state(timestamp)
        assert loaded_df is not None
        assert len(loaded_df) == len(df)

        # Compare data
        pd.testing.assert_frame_equal(
            df.sort_values('instrument_id').reset_index(drop=True),
            loaded_df.sort_values('instrument_id').reset_index(drop=True)
        )

    def test_load_universe_state_from_different_run(self):
        """Test loading universe state from a different run."""
        # Create managers for two different runs
        run1_context = self.run_context
        run2_context = RunContext(
            run_id="test_run_20241218_234567_efgh5678",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "test_run2",
            artifacts_dir=Path(self.temp_dir) / "test_run2" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "test_run2" / "universe_state",
            logs_dir=Path(self.temp_dir) / "test_run2" / "logs",
            metadata={"test": "second_run"}
        )
        run2_context.universe_state_dir.mkdir(parents=True)

        manager1 = RunAwareUniverseStateManager(env=self.env, run_context=run1_context)
        manager2 = RunAwareUniverseStateManager(env=self.env, run_context=run2_context)

        # Save data in run1
        df1 = self.create_test_dataframe(run1_context.run_id)
        timestamp = "2024-12-18T10:00:00"
        manager1.save_universe_state(df1, timestamp, persist_to_db=False)

        # Save data in run2
        df2 = self.create_test_dataframe(run2_context.run_id)
        manager2.save_universe_state(df2, timestamp, persist_to_db=False)

        # Load run1 data from run2 manager
        loaded_df = manager2.load_universe_state(timestamp, from_run_id=run1_context.run_id)
        assert loaded_df is not None
        assert loaded_df['run_id'].iloc[0] == run1_context.run_id

    def test_list_available_timestamps(self):
        """Test listing available timestamps."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        # Save multiple timestamps
        timestamps = [
            "2024-12-18T10:00:00",
            "2024-12-18T11:00:00",
            "2024-12-18T12:00:00"
        ]

        for timestamp in timestamps:
            df = self.create_test_dataframe()
            manager.save_universe_state(df, timestamp, persist_to_db=False)

        # List timestamps for this run
        available = manager.list_available_timestamps(self.run_context.run_id)
        assert len(available) == 3
        for timestamp in timestamps:
            assert timestamp in available

    def test_get_run_summary(self):
        """Test getting run summary."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        # Save some data
        df = self.create_test_dataframe(size=500)  # Larger dataset
        timestamp = "2024-12-18T10:00:00"
        manager.save_universe_state(df, timestamp, persist_to_db=False)

        # Get summary
        summary = manager.get_run_summary()

        assert summary['run_id'] == self.run_context.run_id
        assert summary['file_count'] == 1
        assert summary['total_size_bytes'] > 0
        assert summary['total_size_mb'] > 0
        assert len(summary['available_timestamps']) == 1
        assert summary['available_timestamps'][0] == timestamp

    def test_enhanced_metadata_creation(self):
        """Test enhanced metadata creation."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context,
            write_metadata=True
        )

        df = self.create_test_dataframe()
        timestamp = "2024-12-18T10:00:00"

        # Save with metadata
        saved_path = manager.save_universe_state(df, timestamp, persist_to_db=False)

        # Check metadata file exists
        metadata_file = manager.metadata_dir / f"{Path(saved_path).name}.metadata.json"
        assert metadata_file.exists()

        # Load and verify metadata
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)

        assert metadata['timestamp'] == timestamp
        assert metadata['record_count'] == len(df)
        assert metadata['universe_type'].startswith('run_aware_')
        assert 'polygon' in metadata['data_sources'] or 'tiingo' in metadata['data_sources']
        assert metadata['version'] == "1.1"

    def test_data_source_extraction(self):
        """Test data source extraction from DataFrame."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        # Test DataFrame with polygon columns
        df_polygon = pd.DataFrame({
            'polygon_price': [100, 101],
            'polygon_volume': [1000, 1001],
            'other_col': ['a', 'b']
        })

        sources = manager._extract_data_sources(df_polygon)
        assert 'polygon' in sources

        # Test DataFrame with multiple sources
        df_multi = pd.DataFrame({
            'polygon_price': [100],
            'tiingo_volume': [1000],
            'finnhub_sentiment': [0.5]
        })

        sources = manager._extract_data_sources(df_multi)
        assert 'polygon' in sources
        assert 'tiingo' in sources
        assert 'finnhub' in sources

    def test_cleanup_current_run(self):
        """Test cleaning up current run files."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context,
            write_metadata=True
        )

        # Save some data
        df = self.create_test_dataframe()
        timestamp = "2024-12-18T10:00:00"
        manager.save_universe_state(df, timestamp, persist_to_db=False)

        # Verify files exist
        run_files = list(manager.states_dir.glob(f"{self.run_context.run_id}_*"))
        assert len(run_files) > 0

        # Cleanup
        manager.cleanup_current_run()

        # Verify files are removed
        run_files_after = list(manager.states_dir.glob(f"{self.run_context.run_id}_*"))
        assert len(run_files_after) == 0

    def test_error_handling_empty_dataframe(self):
        """Test error handling with empty DataFrame."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        empty_df = pd.DataFrame()
        timestamp = "2024-12-18T10:00:00"

        # Should return None for empty DataFrame
        saved_path = manager.save_universe_state(empty_df, timestamp, persist_to_db=False)
        assert saved_path is None

    def test_artifact_copying(self):
        """Test copying files to run artifacts directory."""
        manager = RunAwareUniverseStateManager(
            env=self.env,
            run_context=self.run_context
        )

        df = self.create_test_dataframe()
        timestamp = "2024-12-18T10:00:00"

        # Save universe state
        saved_path = manager.save_universe_state(df, timestamp, persist_to_db=False)

        # Check file was copied to artifacts directory
        expected_filename = f"{self.run_context.run_id}_universe_state_{timestamp}.parquet"
        artifact_file = manager.run_artifacts_dir / expected_filename
        assert artifact_file.exists()

        # Verify content is the same
        original_df = pd.read_parquet(saved_path)
        artifact_df = pd.read_parquet(artifact_file)
        pd.testing.assert_frame_equal(original_df, artifact_df)

class TestRunAwareUniverseStateManagerCreation:
    """Test convenience functions for creating run-aware managers."""

    def test_create_run_aware_universe_state_manager(self):
        """Test convenience function for creating manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            run_context = RunContext(
                run_id="test_convenience",
                start_time=datetime.now(timezone.utc),
                base_dir=Path(temp_dir) / "test_run",
                artifacts_dir=Path(temp_dir) / "test_run" / "artifacts",
                universe_state_dir=Path(temp_dir) / "test_run" / "universe_state",
                logs_dir=Path(temp_dir) / "test_run" / "logs",
                metadata={}
            )
            run_context.universe_state_dir.mkdir(parents=True)

            manager = create_run_aware_universe_state_manager(
                env=Environment(EnvironmentType.TEST),
                run_context=run_context
            )

            assert isinstance(manager, RunAwareUniverseStateManager)
            assert manager.run_context == run_context

class TestRunAwareUniverseStateManagerIntegration:
    """Integration tests for run-aware universe state manager."""

    def test_multiple_runs_isolation(self):
        """Test that multiple runs are properly isolated."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create two different run contexts
            contexts = []
            managers = []

            for i in range(2):
                context = RunContext(
                    run_id=f"test_run_{i}_20241218_123456_abc{i}",
                    start_time=datetime.now(timezone.utc),
                    base_dir=Path(temp_dir) / f"test_run_{i}",
                    artifacts_dir=Path(temp_dir) / f"test_run_{i}" / "artifacts",
                    universe_state_dir=Path(temp_dir) / f"test_run_{i}" / "universe_state",
                    logs_dir=Path(temp_dir) / f"test_run_{i}" / "logs",
                    metadata={"run_number": i}
                )
                context.universe_state_dir.mkdir(parents=True)
                contexts.append(context)

                manager = RunAwareUniverseStateManager(
                    env=Environment(EnvironmentType.TEST),
                    run_context=context
                )
                managers.append(manager)

            # Save data in each run
            timestamp = "2024-12-18T10:00:00"
            dfs = []

            for i, manager in enumerate(managers):
                df = pd.DataFrame({
                    'instrument_id': [1, 2, 3],
                    'run_id': contexts[i].run_id,
                    'run_number': i,
                    'price': [100 + i, 101 + i, 102 + i]
                })
                dfs.append(df)

                saved_path = manager.save_universe_state(df, timestamp, persist_to_db=False)
                assert saved_path is not None

            # Verify isolation - each manager can only access its own data by default
            for i, manager in enumerate(managers):
                loaded_df = manager.load_universe_state(timestamp)
                assert loaded_df is not None
                assert loaded_df['run_number'].iloc[0] == i

                # But can access other run's data when specified
                other_run_id = contexts[1 - i].run_id
                other_df = manager.load_universe_state(timestamp, from_run_id=other_run_id)
                assert other_df is not None
                assert other_df['run_number'].iloc[0] == (1 - i)

    def test_run_summary_accuracy(self):
        """Test that run summary provides accurate information."""
        with tempfile.TemporaryDirectory() as temp_dir:
            run_context = RunContext(
                run_id="test_summary_run",
                start_time=datetime.now(timezone.utc),
                base_dir=Path(temp_dir) / "test_run",
                artifacts_dir=Path(temp_dir) / "test_run" / "artifacts",
                universe_state_dir=Path(temp_dir) / "test_run" / "universe_state",
                logs_dir=Path(temp_dir) / "test_run" / "logs",
                metadata={"test": "summary"}
            )
            run_context.universe_state_dir.mkdir(parents=True)

            manager = RunAwareUniverseStateManager(
                env=Environment(EnvironmentType.TEST),
                run_context=run_context
            )

            # Save multiple files
            timestamps = ["2024-12-18T10:00:00", "2024-12-18T11:00:00", "2024-12-18T12:00:00"]
            total_records = 0

            for timestamp in timestamps:
                df = pd.DataFrame({
                    'instrument_id': range(1, 101),  # 100 records each
                    'timestamp': timestamp,
                    'price': range(100, 200)
                })
                total_records += len(df)
                manager.save_universe_state(df, timestamp, persist_to_db=False)

            # Get summary
            summary = manager.get_run_summary()

            assert summary['run_id'] == "test_summary_run"
            assert summary['file_count'] == 3
            assert len(summary['available_timestamps']) == 3
            assert all(ts in summary['available_timestamps'] for ts in timestamps)
            assert summary['total_size_bytes'] > 0
            assert summary['total_size_mb'] > 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])