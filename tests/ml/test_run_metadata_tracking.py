#!/usr/bin/env python3
"""
Comprehensive test suite for ML run metadata tracking system.

This test suite validates the complete metadata tracking functionality including:
- Git information capture
- Command line argument tracking
- Environment and host information
- Database schema compatibility
- CLI tool functionality
- Reproducibility validation

These tests ensure the metadata tracking system meets enterprise compliance
requirements for financial ML workflows.
"""

import asyncio
import pytest
import json
import sys
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / 'src'))

from ml.training_data.utils.run_metadata_tracker import (
    RunMetadataTracker, RunTracker, track_training_run, complete_training_run
)

class TestRunMetadataTracker:
    """Test suite for RunMetadataTracker class."""

    @pytest.mark.asyncio
    async def test_basic_run_lifecycle(self):
        """Test basic run start/update/complete lifecycle."""
        tracker = RunMetadataTracker(
            run_type="test_basic_lifecycle",
            created_by="test_run_metadata_tracking.py"
        )

        parameters = {
            "test_param": "test_value",
            "batch_size": 32,
            "learning_rate": 0.001
        }

        # Start run
        run_id = await tracker.start_run(parameters)
        assert isinstance(run_id, int)
        assert run_id > 0

        # Update progress
        progress_data = {
            "epoch": 5,
            "loss": 0.25,
            "accuracy": 0.85
        }
        await tracker.update_progress(run_id, progress_data)

        # Complete run
        results = {
            "final_accuracy": 0.92,
            "total_epochs": 10,
            "best_loss": 0.15,
            "model_size_mb": 15.7
        }
        await tracker.complete_run(run_id, results, "completed")

        # Verify metadata
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata is not None
        assert metadata['id'] == run_id
        assert metadata['run_type'] == "test_basic_lifecycle"
        assert metadata['status'] == "completed"

        # Verify parameters were merged with progress
        stored_params = json.loads(metadata['parameters'])
        assert stored_params['test_param'] == "test_value"
        assert stored_params['epoch'] == 5
        assert stored_params['loss'] == 0.25

        # Verify results
        stored_results = json.loads(metadata['results'])
        assert stored_results['final_accuracy'] == 0.92
        assert stored_results['model_size_mb'] == 15.7

        await tracker.close()

    @pytest.mark.asyncio
    async def test_git_information_capture(self):
        """Test that git information is properly captured."""
        tracker = RunMetadataTracker(
            run_type="test_git_capture",
            created_by="test_git_capture.py"
        )

        run_id = await tracker.start_run({"test": True})
        metadata = await tracker.get_run_metadata(run_id)

        # Verify git information is captured
        assert metadata['git_commit_hash'] is not None
        assert metadata['git_branch'] is not None
        assert len(metadata['git_commit_hash']) >= 8  # At least short hash

        # Check if running in actual git repo
        try:
            result = subprocess.run(['git', 'rev-parse', 'HEAD'],
                                  capture_output=True, text=True, check=True)
            expected_commit = result.stdout.strip()
            assert metadata['git_commit_hash'] == expected_commit
        except subprocess.CalledProcessError:
            # Not in git repo or git not available - that's ok
            pass

        await tracker.complete_run(run_id, {}, "completed")
        await tracker.close()

    @pytest.mark.asyncio
    async def test_command_line_capture(self):
        """Test that command line arguments are properly captured."""
        # Save original argv
        original_argv = sys.argv.copy()

        try:
            # Mock command line arguments
            sys.argv = [
                'test_script.py',
                '--batch-size', '64',
                '--learning-rate', '0.01',
                '--epochs', '100',
                '--output-dir', '/tmp/test'
            ]

            tracker = RunMetadataTracker(
                run_type="test_command_line",
                created_by="test_command_line.py"
            )

            run_id = await tracker.start_run({"test": True})
            metadata = await tracker.get_run_metadata(run_id)

            # Verify command line was captured
            assert metadata['command_line'] is not None
            assert '--batch-size 64' in metadata['command_line']
            assert '--learning-rate 0.01' in metadata['command_line']
            assert 'test_script.py' in metadata['command_line']

            await tracker.complete_run(run_id, {}, "completed")
            await tracker.close()

        finally:
            # Restore original argv
            sys.argv = original_argv

    @pytest.mark.asyncio
    async def test_host_information_capture(self):
        """Test that host and environment information is captured."""
        tracker = RunMetadataTracker(
            run_type="test_host_info",
            created_by="test_host_info.py"
        )

        run_id = await tracker.start_run({"test": True})
        metadata = await tracker.get_run_metadata(run_id)

        # Verify host info was captured
        assert metadata['host_info'] is not None
        host_info = json.loads(metadata['host_info'])

        assert 'hostname' in host_info
        assert 'platform' in host_info
        assert 'python_version' in host_info
        assert 'working_directory' in host_info
        assert host_info['hostname'] != 'unknown'
        assert host_info['platform'] != 'unknown'

        # Verify python version matches current
        assert host_info['python_version'] == sys.version.split()[0]

        await tracker.complete_run(run_id, {}, "completed")
        await tracker.close()

    @pytest.mark.asyncio
    async def test_environment_detection(self):
        """Test automatic environment detection."""
        # Test dev environment detection
        with patch.dict('os.environ', {'DB_PORT': '3432'}):
            tracker = RunMetadataTracker("test_env", "test.py")
            assert tracker.environment == 'dev'

        # Test intg environment detection
        with patch.dict('os.environ', {'DB_PORT': '4432'}):
            tracker = RunMetadataTracker("test_env", "test.py")
            assert tracker.environment == 'intg'

        # Test explicit environment override
        tracker = RunMetadataTracker("test_env", "test.py", environment="prod")
        assert tracker.environment == 'prod'

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling and failed run tracking."""
        tracker = RunMetadataTracker(
            run_type="test_error_handling",
            created_by="test_error.py"
        )

        run_id = await tracker.start_run({"test": True})

        # Simulate error scenario
        error_message = "Test error: Connection timeout"
        await tracker.complete_run(run_id, {}, "failed", error_message)

        # Verify error was recorded
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata['status'] == 'failed'
        assert metadata['error_message'] == error_message

        await tracker.close()

class TestRunTrackerContextManager:
    """Test suite for RunTracker context manager."""

    @pytest.mark.asyncio
    async def test_successful_context_manager(self):
        """Test context manager with successful completion."""
        parameters = {"test": "context_manager", "value": 42}

        async with RunTracker(
            run_type="test_context_success",
            created_by="test_context_manager.py",
            parameters=parameters
        ) as (tracker, run_id):
            assert isinstance(tracker, RunMetadataTracker)
            assert isinstance(run_id, int)
            assert run_id > 0

            # Update progress during execution
            await tracker.update_progress(run_id, {"step": "processing"})

        # Verify run was completed successfully
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata['status'] == 'completed'
        assert metadata['error_message'] is None

    @pytest.mark.asyncio
    async def test_context_manager_with_exception(self):
        """Test context manager with exception handling."""
        parameters = {"test": "context_exception"}

        with pytest.raises(ValueError, match="Test exception"):
            async with RunTracker(
                run_type="test_context_exception",
                created_by="test_exception.py",
                parameters=parameters
            ) as (tracker, run_id):
                # Simulate exception during execution
                await tracker.update_progress(run_id, {"step": "failing"})
                raise ValueError("Test exception")

        # Verify run was marked as failed
        tracker = RunMetadataTracker("", "", "dev")  # For cleanup
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata['status'] == 'failed'
        assert 'ValueError: Test exception' in metadata['error_message']
        await tracker.close()

class TestConvenienceFunctions:
    """Test convenience functions for common use cases."""

    @pytest.mark.asyncio
    async def test_track_training_run(self):
        """Test track_training_run convenience function."""
        parameters = {"model": "lstm", "layers": 3}

        run_id = await track_training_run(
            run_type="convenience_test",
            created_by="test_convenience.py",
            parameters=parameters
        )

        assert isinstance(run_id, int)
        assert run_id > 0

        # Complete the run
        results = {"accuracy": 0.88}
        await complete_training_run(run_id, results)

        # Verify run exists and is completed
        tracker = RunMetadataTracker("", "", "dev")
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata['status'] == 'completed'

        stored_results = json.loads(metadata['results'])
        assert stored_results['accuracy'] == 0.88

        await tracker.close()

class TestMetadataCompliance:
    """Test metadata completeness and compliance features."""

    @pytest.mark.asyncio
    async def test_metadata_completeness_calculation(self):
        """Test that all required metadata fields are captured."""
        tracker = RunMetadataTracker(
            run_type="test_completeness",
            created_by="test_compliance.py"
        )

        run_id = await tracker.start_run({"compliance_test": True})
        await tracker.complete_run(run_id, {"test_complete": True})

        metadata = await tracker.get_run_metadata(run_id)

        # Check all critical fields are present and non-empty
        critical_fields = [
            'git_commit_hash', 'git_branch', 'command_line',
            'environment', 'host_info', 'python_version'
        ]

        for field in critical_fields:
            assert metadata[field] is not None, f"Field {field} is None"
            if isinstance(metadata[field], str):
                assert metadata[field] != '', f"Field {field} is empty"
                assert metadata[field] != 'unknown', f"Field {field} is unknown"

        # Verify JSONB fields are valid JSON
        host_info = json.loads(metadata['host_info'])
        assert isinstance(host_info, dict)
        assert len(host_info) > 0

        # Verify timestamps
        assert metadata['start_time'] is not None
        assert metadata['end_time'] is not None
        assert metadata['created_at'] is not None

        await tracker.close()

    @pytest.mark.asyncio
    async def test_reproducibility_requirements(self):
        """Test that runs contain sufficient information for reproduction."""
        tracker = RunMetadataTracker(
            run_type="test_reproducibility",
            created_by="test_reproduction.py"
        )

        parameters = {
            "seed": 42,
            "model_architecture": "transformer",
            "training_data": "dataset_v1.0",
            "hyperparameters": {
                "lr": 0.001,
                "batch_size": 32,
                "dropout": 0.1
            }
        }

        run_id = await tracker.start_run(parameters)

        results = {
            "model_file": "model_v1.0.h5",
            "metrics": {"accuracy": 0.95, "f1": 0.93},
            "training_time": 3600,
            "dataset_fingerprint": "sha256:abc123..."
        }

        await tracker.complete_run(run_id, results)

        metadata = await tracker.get_run_metadata(run_id)

        # Verify all reproduction requirements
        assert metadata['git_commit_hash'] is not None
        assert metadata['command_line'] is not None
        assert metadata['environment'] is not None

        stored_params = json.loads(metadata['parameters'])
        assert 'seed' in stored_params
        assert 'model_architecture' in stored_params
        assert 'hyperparameters' in stored_params

        stored_results = json.loads(metadata['results'])
        assert 'model_file' in stored_results
        assert 'dataset_fingerprint' in stored_results

        await tracker.close()

class TestDatabaseIntegration:
    """Test database schema and integration."""

    @pytest.mark.asyncio
    async def test_database_schema_compatibility(self):
        """Test that metadata tracking works with actual database schema."""
        tracker = RunMetadataTracker(
            run_type="test_schema_compat",
            created_by="test_database.py"
        )

        # Test with large parameter and result objects
        large_parameters = {
            "large_config": {f"param_{i}": f"value_{i}" for i in range(100)},
            "model_config": {
                "layers": [
                    {"type": "dense", "units": 128, "activation": "relu"},
                    {"type": "dropout", "rate": 0.2},
                    {"type": "dense", "units": 64, "activation": "relu"},
                    {"type": "dense", "units": 1, "activation": "sigmoid"}
                ]
            }
        }

        run_id = await tracker.start_run(large_parameters)

        large_results = {
            "training_history": {
                "loss": [0.8, 0.6, 0.4, 0.3, 0.25],
                "accuracy": [0.6, 0.7, 0.8, 0.85, 0.88],
                "val_loss": [0.9, 0.65, 0.45, 0.35, 0.3],
                "val_accuracy": [0.55, 0.68, 0.78, 0.82, 0.85]
            },
            "model_metrics": {
                "precision": 0.87,
                "recall": 0.82,
                "f1_score": 0.84,
                "auc_roc": 0.91
            }
        }

        await tracker.complete_run(run_id, large_results)

        # Verify large objects were stored correctly
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata is not None

        stored_params = json.loads(metadata['parameters'])
        assert len(stored_params['large_config']) == 100
        assert len(stored_params['model_config']['layers']) == 4

        stored_results = json.loads(metadata['results'])
        assert len(stored_results['training_history']['loss']) == 5
        assert stored_results['model_metrics']['f1_score'] == 0.84

        await tracker.close()

class TestCLIIntegration:
    """Test CLI tool integration with metadata tracking."""

    @pytest.mark.asyncio
    async def test_cli_query_functionality(self):
        """Test that CLI can query metadata created by tracker."""
        # Import CLI class
        sys.path.append(str(Path(__file__).parent.parent.parent / 'scripts'))
        from run_metadata_cli import RunMetadataCLI

        # Create a run with tracker
        tracker = RunMetadataTracker(
            run_type="test_cli_integration",
            created_by="test_cli_query.py"
        )

        parameters = {"cli_test": True, "version": "1.0"}
        run_id = await tracker.start_run(parameters)
        results = {"cli_result": "success"}
        await tracker.complete_run(run_id, results)
        await tracker.close()

        # Query with CLI
        cli = RunMetadataCLI(environment='dev')

        # Test list functionality
        runs = await cli.list_runs(limit=5)
        assert isinstance(runs, list)
        assert len(runs) > 0

        # Find our test run
        test_run = None
        for run in runs:
            if run['id'] == run_id:
                test_run = run
                break

        assert test_run is not None
        assert test_run['run_type'] == 'test_cli_integration'

        # Test detailed metadata retrieval
        details = await cli.get_run_details(run_id)
        assert details is not None
        assert details['id'] == run_id

        # Test reproducibility validation
        validation = await cli.validate_reproducibility(run_id)
        assert 'reproducible' in validation
        assert 'metadata_completeness' in validation
        assert validation['metadata_completeness'] > 80.0  # Should be high

        await cli.close()

@pytest.mark.asyncio
async def test_end_to_end_workflow():
    """Test complete end-to-end workflow with all components."""
    # Simulate a complete ML training workflow

    # Stage 1: Data preparation run
    async with RunTracker(
        run_type="data_preparation",
        created_by="prepare_data.py",
        parameters={
            "source_data": "market_data_2025",
            "preprocessing": ["normalization", "feature_engineering"],
            "output_format": "parquet"
        }
    ) as (prep_tracker, prep_run_id):
        await prep_tracker.update_progress(prep_run_id, {
            "stage": "loading_data",
            "records_loaded": 1000000
        })
        await prep_tracker.update_progress(prep_run_id, {
            "stage": "preprocessing",
            "features_created": 50
        })

    # Stage 2: Model training run
    async with RunTracker(
        run_type="model_training",
        created_by="train_model.py",
        parameters={
            "input_data_run_id": prep_run_id,  # Reference to prep run
            "model_type": "lstm",
            "architecture": {
                "lstm_units": 128,
                "dense_layers": [64, 32],
                "dropout": 0.2
            },
            "training": {
                "epochs": 100,
                "batch_size": 32,
                "learning_rate": 0.001,
                "optimizer": "adam"
            }
        }
    ) as (train_tracker, train_run_id):
        # Simulate training progress
        for epoch in [10, 25, 50, 75, 100]:
            await train_tracker.update_progress(train_run_id, {
                "epoch": epoch,
                "loss": 1.0 - (epoch / 120),  # Decreasing loss
                "accuracy": 0.5 + (epoch / 200),  # Increasing accuracy
                "val_loss": 1.1 - (epoch / 130),
                "val_accuracy": 0.45 + (epoch / 220)
            })

    # Stage 3: Evaluation run
    async with RunTracker(
        run_type="model_evaluation",
        created_by="evaluate_model.py",
        parameters={
            "model_run_id": train_run_id,  # Reference to training run
            "test_data": "holdout_set_2025",
            "metrics": ["accuracy", "precision", "recall", "f1", "auc"]
        }
    ) as (eval_tracker, eval_run_id):
        eval_results = {
            "test_accuracy": 0.89,
            "precision": 0.87,
            "recall": 0.85,
            "f1_score": 0.86,
            "auc_roc": 0.92,
            "confusion_matrix": [[150, 20], [25, 180]],
            "model_file": f"model_{train_run_id}.h5",
            "evaluation_dataset": "test_2025_q3.parquet"
        }

    # Verify all runs were tracked successfully
    tracker = RunMetadataTracker("", "", "dev")

    prep_metadata = await tracker.get_run_metadata(prep_run_id)
    train_metadata = await tracker.get_run_metadata(train_run_id)
    eval_metadata = await tracker.get_run_metadata(eval_run_id)

    assert prep_metadata['status'] == 'completed'
    assert train_metadata['status'] == 'completed'
    assert eval_metadata['status'] == 'completed'

    # Verify run relationships
    train_params = json.loads(train_metadata['parameters'])
    assert train_params['input_data_run_id'] == prep_run_id

    eval_params = json.loads(eval_metadata['parameters'])
    assert eval_params['model_run_id'] == train_run_id

    # Verify metadata completeness for audit
    for run_id in [prep_run_id, train_run_id, eval_run_id]:
        metadata = await tracker.get_run_metadata(run_id)
        assert metadata['git_commit_hash'] is not None
        assert metadata['command_line'] is not None
        assert metadata['environment'] is not None
        host_info = json.loads(metadata['host_info'])
        assert len(host_info) > 5  # Should have comprehensive host info

    await tracker.close()

    print(f"✅ End-to-end workflow completed successfully!")
    print(f"   Data Preparation Run: {prep_run_id}")
    print(f"   Model Training Run: {train_run_id}")
    print(f"   Model Evaluation Run: {eval_run_id}")

if __name__ == "__main__":
    # Run tests directly if executed as script
    import pytest
    pytest.main([__file__, "-v"])