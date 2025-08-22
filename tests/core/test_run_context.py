#!/usr/bin/env python3
"""
Tests for Run Context Management System
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.run_context import (
    RunContext, RunIdGenerator, RunContextManager, 
    create_run_context, get_run_manager
)


@pytest.mark.unit
class TestRunIdGenerator:
    """Test run ID generation functionality."""
    
    def test_generate_run_id_format(self):
        """Test run ID format matches expected pattern."""
        run_id = RunIdGenerator.generate()
        
        # Should match: run_YYYYMMDD_HHMMSS_<8char_uuid>
        parts = run_id.split('_')
        assert len(parts) == 4
        assert parts[0] == 'run'
        assert len(parts[1]) == 8  # YYYYMMDD
        assert len(parts[2]) == 6  # HHMMSS
        assert len(parts[3]) == 8  # short UUID
        
        # Check date format
        assert parts[1].isdigit()
        assert parts[2].isdigit()
    
    def test_generate_unique_run_ids(self):
        """Test that generated run IDs are unique."""
        run_ids = [RunIdGenerator.generate() for _ in range(10)]
        assert len(set(run_ids)) == 10  # All unique
    
    def test_extract_timestamp_valid_run_id(self):
        """Test extracting timestamp from valid run ID."""
        # Create a known run ID
        test_run_id = "run_20241218_143025_abcd1234"
        timestamp = RunIdGenerator.extract_timestamp(test_run_id)
        
        assert timestamp is not None
        assert timestamp.year == 2024
        assert timestamp.month == 12
        assert timestamp.day == 18
        assert timestamp.hour == 14
        assert timestamp.minute == 30
        assert timestamp.second == 25
    
    def test_extract_timestamp_invalid_run_id(self):
        """Test extracting timestamp from invalid run ID."""
        invalid_ids = [
            "invalid_format",
            "run_invalid_date_abc123",
            "not_a_run_id",
            ""
        ]
        
        for invalid_id in invalid_ids:
            timestamp = RunIdGenerator.extract_timestamp(invalid_id)
            assert timestamp is None


@pytest.mark.unit  
class TestRunContext:
    """Test RunContext dataclass functionality."""
    
    def test_run_context_creation(self):
        """Test creating a run context."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir) / "test_run"
            run_context = RunContext(
                run_id="test_run_123",
                start_time=datetime.now(timezone.utc),
                base_dir=base_dir,
                artifacts_dir=base_dir / "artifacts",
                universe_state_dir=base_dir / "universe_state",
                logs_dir=base_dir / "logs",
                metadata={"test": "data"}
            )
            
            assert run_context.run_id == "test_run_123"
            assert isinstance(run_context.start_time, datetime)
            assert run_context.metadata["test"] == "data"
    
    def test_run_context_to_dict(self):
        """Test converting run context to dictionary."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir) / "test_run"
            start_time = datetime.now(timezone.utc)
            
            run_context = RunContext(
                run_id="test_run_123",
                start_time=start_time,
                base_dir=base_dir,
                artifacts_dir=base_dir / "artifacts",
                universe_state_dir=base_dir / "universe_state",
                logs_dir=base_dir / "logs",
                metadata={"test": "data"}
            )
            
            result_dict = run_context.to_dict()
            
            assert result_dict["run_id"] == "test_run_123"
            assert result_dict["start_time"] == start_time.isoformat()
            assert result_dict["metadata"]["test"] == "data"
    
    def test_save_metadata(self):
        """Test saving run metadata to file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir) / "test_run"
            artifacts_dir = base_dir / "artifacts"
            artifacts_dir.mkdir(parents=True)
            
            run_context = RunContext(
                run_id="test_run_123",
                start_time=datetime.now(timezone.utc),
                base_dir=base_dir,
                artifacts_dir=artifacts_dir,
                universe_state_dir=base_dir / "universe_state",
                logs_dir=base_dir / "logs",
                metadata={"test": "data"}
            )
            
            run_context.save_metadata()
            
            metadata_file = artifacts_dir / "run_metadata.json"
            assert metadata_file.exists()
            
            with open(metadata_file, 'r') as f:
                saved_data = json.load(f)
            
            assert saved_data["run_id"] == "test_run_123"
            assert saved_data["metadata"]["test"] == "data"


@pytest.mark.integration
class TestRunContextManager:
    """Test RunContextManager functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = RunContextManager(base_artifacts_dir=self.temp_dir)
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    def test_create_run_context(self):
        """Test creating a run context."""
        metadata = {"test_type": "unit", "version": "1.0"}
        run_context = self.manager.create_run_context(metadata=metadata)
        
        assert run_context.run_id.startswith("run_")
        assert run_context.base_dir.exists()
        assert run_context.artifacts_dir.exists()
        assert run_context.universe_state_dir.exists()
        assert run_context.logs_dir.exists()
        assert run_context.metadata["test_type"] == "unit"
        
        # Check metadata file was created
        metadata_file = run_context.artifacts_dir / "run_metadata.json"
        assert metadata_file.exists()
    
    def test_create_run_context_custom_id(self):
        """Test creating run context with custom ID."""
        custom_id = "custom_test_run_123"
        run_context = self.manager.create_run_context(run_id=custom_id)
        
        assert run_context.run_id == custom_id
        assert custom_id in str(run_context.base_dir)
    
    def test_set_and_get_current_context(self):
        """Test setting and getting current context."""
        run_context = self.manager.create_run_context()
        
        # Initially no current context
        assert self.manager.get_current_context() is None
        
        # Set current context
        self.manager.set_current_context(run_context)
        current = self.manager.get_current_context()
        
        assert current is not None
        assert current.run_id == run_context.run_id
    
    def test_list_runs(self):
        """Test listing runs."""
        # Create multiple runs
        run1 = self.manager.create_run_context(metadata={"test": "run1"})
        run2 = self.manager.create_run_context(metadata={"test": "run2"})
        
        runs = self.manager.list_runs()
        
        assert len(runs) >= 2
        run_ids = [run["run_id"] for run in runs]
        assert run1.run_id in run_ids
        assert run2.run_id in run_ids
        
        # Check metadata is included
        for run in runs:
            if run["run_id"] == run1.run_id:
                assert run["metadata"]["test"] == "run1"
    
    def test_list_runs_with_limit(self):
        """Test listing runs with limit."""
        # Create more runs than limit
        for i in range(5):
            self.manager.create_run_context(metadata={"test": f"run{i}"})
        
        runs = self.manager.list_runs(limit=3)
        assert len(runs) == 3
    
    def test_cleanup_old_runs(self):
        """Test cleaning up old runs."""
        # Create a run
        run_context = self.manager.create_run_context()
        assert run_context.base_dir.exists()
        
        # Clean up runs older than 0 days (should remove all)
        self.manager.cleanup_old_runs(keep_days=0)
        
        # Directory should be removed
        assert not run_context.base_dir.exists()


@pytest.mark.unit
class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_create_run_context_function(self):
        """Test global create_run_context function."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Temporarily replace global manager
            original_base = get_run_manager().base_artifacts_dir
            get_run_manager().base_artifacts_dir = Path(temp_dir)
            
            try:
                run_context = create_run_context(metadata={"test": "global"})
                assert run_context.run_id.startswith("run_")
                assert run_context.metadata["test"] == "global"
            finally:
                get_run_manager().base_artifacts_dir = original_base
    
    def test_get_run_manager(self):
        """Test getting global run manager."""
        manager1 = get_run_manager()
        manager2 = get_run_manager()
        
        # Should return same instance
        assert manager1 is manager2


@pytest.mark.integration
class TestRunContextIntegration:
    """Integration tests for run context system."""
    
    def test_directory_structure_creation(self):
        """Test that correct directory structure is created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = RunContextManager(base_artifacts_dir=temp_dir)
            run_context = manager.create_run_context()
            
            # Check directory structure
            assert run_context.base_dir.is_dir()
            assert run_context.artifacts_dir.is_dir()
            assert run_context.universe_state_dir.is_dir()
            assert run_context.logs_dir.is_dir()
            
            # Check subdirectories exist
            assert (run_context.universe_state_dir / "states").is_dir()
            assert (run_context.universe_state_dir / "metadata").is_dir()
            assert (run_context.universe_state_dir / "cache").is_dir()
    
    def test_metadata_persistence(self):
        """Test that metadata is properly persisted and can be loaded."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = RunContextManager(base_artifacts_dir=temp_dir)
            
            original_metadata = {
                "experiment": "test_experiment",
                "version": "1.2.3",
                "parameters": {"learning_rate": 0.01, "epochs": 100}
            }
            
            run_context = manager.create_run_context(metadata=original_metadata)
            
            # Load runs and verify metadata
            runs = manager.list_runs()
            found_run = None
            for run in runs:
                if run["run_id"] == run_context.run_id:
                    found_run = run
                    break
            
            assert found_run is not None
            assert found_run["metadata"]["experiment"] == "test_experiment"
            assert found_run["metadata"]["parameters"]["learning_rate"] == 0.01
    
    def test_multiple_managers_isolation(self):
        """Test that multiple managers with different base dirs are isolated."""
        with tempfile.TemporaryDirectory() as temp_dir1, tempfile.TemporaryDirectory() as temp_dir2:
            manager1 = RunContextManager(base_artifacts_dir=temp_dir1)
            manager2 = RunContextManager(base_artifacts_dir=temp_dir2)
            
            # Create runs in each manager
            run1 = manager1.create_run_context(metadata={"manager": "1"})
            run2 = manager2.create_run_context(metadata={"manager": "2"})
            
            # Each manager should only see its own runs
            runs1 = manager1.list_runs()
            runs2 = manager2.list_runs()
            
            run_ids1 = [run["run_id"] for run in runs1]
            run_ids2 = [run["run_id"] for run in runs2]
            
            assert run1.run_id in run_ids1
            assert run1.run_id not in run_ids2
            assert run2.run_id in run_ids2
            assert run2.run_id not in run_ids1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])