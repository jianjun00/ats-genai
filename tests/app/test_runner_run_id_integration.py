#!/usr/bin/env python3
"""
Tests for Runner Integration with Run ID System
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.run_context import RunContext, create_run_context
from core.run_aware_logging import get_current_run_id
from app.runner import Runner
from config.environment import Environment, EnvironmentType
from state.run_aware_universe_state_manager import RunAwareUniverseStateManager


class TestRunnerRunIdIntegration:
    """Test Runner integration with run_id system."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.env = Environment(EnvironmentType.TEST)
        
        # Mock dependencies to avoid database requirements
        self.mock_security_master = None
        self.mock_universe_manager = None
        self.mock_market_data_manager = None
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
        # Reset logging
        logging.getLogger().handlers.clear()
    
    def test_runner_automatic_run_context_creation(self):
        """Test that Runner automatically creates run context when enabled."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Should have created run context
        assert runner.run_context is not None
        assert runner.run_context.run_id.startswith("run_")
        assert runner.enable_run_isolation is True
        
        # Should use run-aware universe state manager
        assert isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)
        assert runner.universe_state_manager.run_context == runner.run_context
    
    def test_runner_with_provided_run_context(self):
        """Test Runner with pre-provided run context."""
        # Create custom run context
        run_context = RunContext(
            run_id="custom_test_run_123",
            start_time=datetime.now(timezone.utc),
            base_dir=Path(self.temp_dir) / "custom_run",
            artifacts_dir=Path(self.temp_dir) / "custom_run" / "artifacts",
            universe_state_dir=Path(self.temp_dir) / "custom_run" / "universe_state",
            logs_dir=Path(self.temp_dir) / "custom_run" / "logs",
            metadata={"custom": "test"}
        )
        run_context.universe_state_dir.mkdir(parents=True)
        run_context.logs_dir.mkdir(parents=True)
        
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            run_context=run_context,
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Should use provided run context
        assert runner.run_context == run_context
        assert runner.run_context.run_id == "custom_test_run_123"
        
        # Should use run-aware universe state manager
        assert isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)
        assert runner.universe_state_manager.run_context == run_context
    
    def test_runner_run_isolation_disabled(self):
        """Test Runner with run isolation disabled."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            enable_run_isolation=False,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Should not have run context
        assert runner.run_context is None
        assert runner.enable_run_isolation is False
        
        # Should use legacy universe state manager
        assert not isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)
    
    def test_runner_metadata_creation(self):
        """Test that Runner creates proper metadata for run context."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=42,
            callbacks=["test.callback"],
            base_duration="1h",
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Check metadata
        metadata = runner.run_context.metadata
        assert metadata['start_date'] == "2024-01-01T00:00:00"
        assert metadata['end_date'] == "2024-01-10T00:00:00"
        assert metadata['universe_id'] == 42
        assert metadata['base_duration'] == "1h"
        assert metadata['environment'] == "test"
        assert 'test.callback' in metadata['callbacks']
    
    def test_runner_logging_integration(self):
        """Test that Runner integrates with run-aware logging."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Should have set up run-aware logging
        assert hasattr(runner, 'logger')
        
        # Current run ID should be set
        current_run_id = get_current_run_id()
        assert current_run_id == runner.run_context.run_id
        
        # Test logging works
        runner.logger.info("Test runner logging")
        
        # Check log file exists
        log_file = runner.run_context.logs_dir / "ats_genai.log"
        assert log_file.exists()
        
        # Check log content
        content = log_file.read_text()
        assert "Test runner logging" in content
        assert runner.run_context.run_id in content
    
    def test_runner_directory_structure_creation(self):
        """Test that Runner creates proper directory structure."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Check directory structure exists
        assert runner.run_context.base_dir.exists()
        assert runner.run_context.artifacts_dir.exists()
        assert runner.run_context.universe_state_dir.exists()
        assert runner.run_context.logs_dir.exists()
        
        # Check universe state subdirectories
        assert (runner.run_context.universe_state_dir / "states").exists()
        assert (runner.run_context.universe_state_dir / "metadata").exists()
        assert (runner.run_context.universe_state_dir / "cache").exists()
        
        # Check metadata file
        metadata_file = runner.run_context.artifacts_dir / "run_metadata.json"
        assert metadata_file.exists()
    
    def test_multiple_runners_isolation(self):
        """Test that multiple runners are properly isolated."""
        runners = []
        
        for i in range(2):
            runner = Runner(
                start_date="2024-01-01",
                end_date="2024-01-10",
                environment=self.env,
                universe_id=i + 1,
                callbacks=[],
                base_duration="1d",
                enable_run_isolation=True,
                security_master=self.mock_security_master,
                universe_manager=self.mock_universe_manager,
                market_data_manager=self.mock_market_data_manager
            )
            runners.append(runner)
        
        # Should have different run contexts
        assert runners[0].run_context.run_id != runners[1].run_context.run_id
        
        # Should have different universe state managers
        assert runners[0].universe_state_manager != runners[1].universe_state_manager
        
        # Should have different base directories
        assert runners[0].run_context.base_dir != runners[1].run_context.base_dir
        
        # Each should have its own universe state directory
        assert runners[0].universe_state_manager.base_path != runners[1].universe_state_manager.base_path
    
    def test_runner_universe_state_manager_integration(self):
        """Test integration between Runner and run-aware universe state manager."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Universe state manager should be run-aware
        assert isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)
        
        # Should be configured with the same run context
        assert runner.universe_state_manager.run_context == runner.run_context
        assert runner.universe_state_manager.run_id == runner.run_context.run_id
        
        # Should use run-specific directory
        assert str(runner.run_context.run_id) in str(runner.universe_state_manager.base_path)


class TestRunnerBackwardCompatibility:
    """Test that Runner maintains backward compatibility."""
    
    def setup_method(self):
        """Set up test environment."""
        self.env = Environment(EnvironmentType.TEST)
        
        # Mock dependencies
        self.mock_security_master = None
        self.mock_universe_manager = None
        self.mock_market_data_manager = None
    
    def test_runner_legacy_behavior_default(self):
        """Test that Runner defaults to run isolation enabled."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
            # enable_run_isolation defaults to True
        )
        
        # Should enable run isolation by default
        assert runner.enable_run_isolation is True
        assert runner.run_context is not None
        assert isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)
    
    def test_runner_with_existing_universe_state_manager(self):
        """Test Runner with pre-existing universe state manager."""
        from state.universe_state_manager import UniverseStateManager
        
        # Create existing manager
        existing_manager = UniverseStateManager(self.env, write_metadata=False)
        
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            universe_state_manager=existing_manager,
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # Should use provided manager even with run isolation enabled
        assert runner.universe_state_manager == existing_manager
        assert not isinstance(runner.universe_state_manager, RunAwareUniverseStateManager)
        
        # But should still have run context
        assert runner.run_context is not None
    
    def test_runner_test_environment_behavior(self):
        """Test Runner behavior in test environment."""
        test_env = Environment(EnvironmentType.TEST)
        
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=test_env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            enable_run_isolation=True,
            security_master=self.mock_security_master,
            universe_manager=self.mock_universe_manager,
            market_data_manager=self.mock_market_data_manager
        )
        
        # In test environment, metadata writing should be disabled
        assert runner.universe_state_manager.write_metadata is False


class TestRunnerErrorHandling:
    """Test error handling in Runner with run_id system."""
    
    def setup_method(self):
        """Set up test environment."""
        self.env = Environment(EnvironmentType.TEST)
    
    def test_runner_invalid_dates(self):
        """Test Runner with invalid date formats."""
        with pytest.raises(TypeError):
            Runner(
                start_date=123,  # Invalid type
                end_date="2024-01-10",
                environment=self.env,
                universe_id=1,
                callbacks=[],
                base_duration="1d"
            )
    
    def test_runner_with_none_run_context_and_disabled_isolation(self):
        """Test Runner with None run context and disabled isolation."""
        runner = Runner(
            start_date="2024-01-01",
            end_date="2024-01-10",
            environment=self.env,
            universe_id=1,
            callbacks=[],
            base_duration="1d",
            run_context=None,
            enable_run_isolation=False
        )
        
        # Should work fine without run context
        assert runner.run_context is None
        assert runner.enable_run_isolation is False


class TestRunnerPerformance:
    """Performance tests for Runner with run_id system."""
    
    def setup_method(self):
        """Set up test environment."""
        self.env = Environment(EnvironmentType.TEST)
    
    def test_runner_creation_performance(self):
        """Test that Runner creation with run_id system is reasonably fast."""
        import time
        
        start_time = time.time()
        
        # Create multiple runners
        runners = []
        for i in range(5):
            runner = Runner(
                start_date="2024-01-01",
                end_date="2024-01-10",
                environment=self.env,
                universe_id=i + 1,
                callbacks=[],
                base_duration="1d",
                enable_run_isolation=True
            )
            runners.append(runner)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should create 5 runners in reasonable time (less than 5 seconds)
        assert duration < 5.0, f"Runner creation took too long: {duration} seconds"
        
        # All runners should have unique run IDs
        run_ids = [runner.run_context.run_id for runner in runners]
        assert len(set(run_ids)) == 5  # All unique


if __name__ == "__main__":
    pytest.main([__file__, "-v"])