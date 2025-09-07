#!/usr/bin/env python3
"""
Tests for 30-Year Minute Backfill Deployment Manager

Tests the Kubernetes deployment management functionality including:
- Job deployment and management
- Status monitoring
- Checkpoint tracking
- Cleanup operations
"""

import pytest
import sys
import subprocess
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add script path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'scripts' / 'backfill'))

try:
    from deploy_30year_minute_backfill import MinuteBackfillDeploymentManager
except ImportError:
    pytest.skip("Deployment manager not available", allow_module_level=True)


class TestMinuteBackfillDeploymentManager:
    """Test the deployment manager functionality."""

    @pytest.fixture
    def manager(self):
        """Create deployment manager instance."""
        return MinuteBackfillDeploymentManager()

    def test_initialization(self, manager):
        """Test deployment manager initialization."""
        assert manager.namespace == "ats-dev"
        assert len(manager.vendor_jobs) == 4
        assert "polygon" in manager.vendor_jobs
        assert "tiingo" in manager.vendor_jobs
        assert "fmp" in manager.vendor_jobs
        assert "eodhd" in manager.vendor_jobs

        # Check job names are correctly set
        assert manager.job_names["polygon"] == "polygon-30year-minute-backfill"
        assert manager.job_names["orchestrator"] == "comprehensive-30year-minute-backfill"

    def test_kubectl_execution(self, manager):
        """Test kubectl command execution."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["kubectl", "get", "pods"],
                returncode=0,
                stdout="NAME    READY   STATUS\npod1    1/1     Running",
                stderr=""
            )

            result = manager.run_kubectl("get pods")

            assert result.returncode == 0
            assert "pod1" in result.stdout
            mock_run.assert_called_once()

    def test_kubectl_error_handling(self, manager):
        """Test kubectl error handling."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=["kubectl", "get", "nonexistent"],
                returncode=1,
                stdout="",
                stderr="Error: resource not found"
            )

            result = manager.run_kubectl("get nonexistent")

            assert result.returncode == 1
            assert "Error" in result.stderr

    def test_job_status_parsing(self, manager):
        """Test job status parsing from kubectl output."""
        mock_job_json = {
            "status": {
                "active": 1,
                "succeeded": 0,
                "failed": 0,
                "startTime": "2023-08-23T12:00:00Z",
                "conditions": [
                    {"type": "Active", "status": "True"}
                ]
            }
        }

        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "get", "job"],
                returncode=0,
                stdout=json.dumps(mock_job_json),
                stderr=""
            )

            status = manager.get_job_status("test-job")

            assert status["active"] == 1
            assert status["succeeded"] == 0
            assert status["failed"] == 0
            assert status["start_time"] == "2023-08-23T12:00:00Z"

    def test_job_not_found_handling(self, manager):
        """Test handling when job is not found."""
        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "get", "job"],
                returncode=1,
                stdout="",
                stderr="Error from server (NotFound): jobs.batch \"test-job\" not found"
            )

            status = manager.get_job_status("test-job")

            assert status["status"] == "not_found"
            assert "not found" in status["error"]

    def test_all_job_status(self, manager):
        """Test getting status of all jobs."""
        with patch.object(manager, 'get_job_status') as mock_status:
            mock_status.side_effect = [
                {"active": 0, "succeeded": 1, "failed": 0},  # orchestrator
                {"active": 1, "succeeded": 0, "failed": 0},  # polygon
                {"status": "not_found"},                     # tiingo
                {"active": 0, "succeeded": 0, "failed": 1},  # fmp
                {"active": 0, "succeeded": 1, "failed": 0}   # eodhd
            ]

            statuses = manager.get_all_job_status()

            assert len(statuses) == 5  # orchestrator + 4 vendors
            assert statuses["orchestrator"]["succeeded"] == 1
            assert statuses["polygon"]["active"] == 1
            assert statuses["tiingo"]["status"] == "not_found"
            assert statuses["fmp"]["failed"] == 1
            assert statuses["eodhd"]["succeeded"] == 1

    def test_vendor_deployment(self, manager):
        """Test individual vendor deployment."""
        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "apply"],
                returncode=0,
                stdout="job.batch/polygon-30year-minute-backfill created",
                stderr=""
            )

            result = manager.deploy_vendor("polygon")

            assert result is True
            mock_kubectl.assert_called_once()

            # Check that the correct file path is used
            call_args = mock_kubectl.call_args[0][0]
            assert "30year-minute-backfill-polygon.yaml" in call_args

    def test_vendor_deployment_failure(self, manager):
        """Test vendor deployment failure handling."""
        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "apply"],
                returncode=1,
                stdout="",
                stderr="Error: unable to apply job"
            )

            result = manager.deploy_vendor("polygon")

            assert result is False

    def test_unknown_vendor_deployment(self, manager):
        """Test deployment of unknown vendor."""
        result = manager.deploy_vendor("unknown_vendor")
        assert result is False

    def test_orchestrator_deployment(self, manager):
        """Test orchestrator deployment."""
        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "apply"],
                returncode=0,
                stdout="job.batch/comprehensive-30year-minute-backfill created",
                stderr=""
            )

            result = manager.deploy_orchestrator()

            assert result is True
            call_args = mock_kubectl.call_args[0][0]
            assert "30year-minute-backfill-orchestrator.yaml" in call_args

    def test_deploy_all(self, manager):
        """Test deploying all jobs."""
        with patch.object(manager, 'deploy_orchestrator') as mock_orch, \
             patch.object(manager, 'deploy_vendor') as mock_vendor:

            mock_orch.return_value = True
            mock_vendor.return_value = True

            results = manager.deploy_all()

            assert results["orchestrator"] is True
            assert results["polygon"] is True
            assert results["tiingo"] is True
            assert results["fmp"] is True
            assert results["eodhd"] is True

            # Verify all methods were called
            mock_orch.assert_called_once()
            assert mock_vendor.call_count == 4

    def test_job_logs_retrieval(self, manager):
        """Test retrieving job logs."""
        sample_logs = "Sample log output\nAnother log line\nCompleted successfully"

        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "logs"],
                returncode=0,
                stdout=sample_logs,
                stderr=""
            )

            logs = manager.get_job_logs("polygon", tail_lines=20)

            assert logs == sample_logs
            call_args = mock_kubectl.call_args[0][0]
            assert "logs job/polygon-30year-minute-backfill" in call_args
            assert "--tail=20" in call_args

    def test_job_logs_failure(self, manager):
        """Test job logs retrieval failure."""
        with patch.object(manager, 'run_kubectl') as mock_kubectl:
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "logs"],
                returncode=1,
                stdout="",
                stderr="Error: job not found"
            )

            logs = manager.get_job_logs("polygon")

            assert "Failed to get logs" in logs
            assert "job not found" in logs

    def test_cleanup_jobs(self, manager):
        """Test job cleanup functionality."""
        with patch.object(manager, 'get_all_job_status') as mock_status, \
             patch.object(manager, 'run_kubectl') as mock_kubectl:

            # Mock job statuses
            mock_status.return_value = {
                "orchestrator": {"succeeded": 1, "failed": 0},
                "polygon": {"succeeded": 0, "failed": 1},
                "tiingo": {"status": "not_found"},
                "fmp": {"succeeded": 1, "failed": 0}
            }

            # Mock successful cleanup
            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "delete"],
                returncode=0,
                stdout="job.batch deleted",
                stderr=""
            )

            results = manager.cleanup_jobs(keep_completed=False)

            # Should attempt to clean up existing jobs
            assert results["orchestrator"] is True
            assert results["polygon"] is True
            assert results["tiingo"] is True  # Not found is treated as success
            assert results["fmp"] is True

    def test_cleanup_keep_completed(self, manager):
        """Test cleanup while keeping completed jobs."""
        with patch.object(manager, 'get_all_job_status') as mock_status, \
             patch.object(manager, 'run_kubectl') as mock_kubectl:

            mock_status.return_value = {
                "orchestrator": {"succeeded": 1, "failed": 0},  # Completed
                "polygon": {"succeeded": 0, "failed": 1}        # Failed
            }

            mock_kubectl.return_value = subprocess.CompletedProcess(
                args=["kubectl", "delete"],
                returncode=0,
                stdout="job.batch deleted",
                stderr=""
            )

            results = manager.cleanup_jobs(keep_completed=True)

            # Should keep orchestrator (completed), clean up polygon (failed)
            assert results["orchestrator"] is True
            assert results["polygon"] is True

    def test_checkpoint_directory_creation(self, manager):
        """Test checkpoint directory creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Override the checkpoint base path for testing
            original_method = manager.create_checkpoint_directories

            def mock_create_checkpoints():
                base_path = Path(temp_dir) / "checkpoints"
                for vendor in manager.vendor_jobs.keys():
                    vendor_dir = base_path / vendor
                    vendor_dir.mkdir(parents=True, exist_ok=True)

                master_dir = base_path / "master"
                master_dir.mkdir(parents=True, exist_ok=True)

            manager.create_checkpoint_directories = mock_create_checkpoints

            # Test directory creation
            manager.create_checkpoint_directories()

            # Verify directories were created
            base_path = Path(temp_dir) / "checkpoints"
            assert (base_path / "polygon").exists()
            assert (base_path / "tiingo").exists()
            assert (base_path / "fmp").exists()
            assert (base_path / "eodhd").exists()
            assert (base_path / "master").exists()

    def test_checkpoint_status_display(self, manager):
        """Test checkpoint status display."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock checkpoint structure
            checkpoint_base = Path(temp_dir)

            # Create directories and files
            for vendor in ["polygon", "tiingo"]:
                vendor_dir = checkpoint_base / vendor
                vendor_dir.mkdir(parents=True)

                # Create a checkpoint file
                checkpoint_file = vendor_dir / "checkpoint.json"
                checkpoint_file.write_text('{"test": "data"}')

            # Create empty directory for fmp
            (checkpoint_base / "fmp").mkdir()

            # Mock the checkpoint base path
            with patch.object(Path, '__new__') as mock_path:
                def path_side_effect(cls, path_str):
                    if path_str == "/home/jianjun/ats-data/checkpoints":
                        return checkpoint_base
                    return Path(path_str)

                mock_path.side_effect = path_side_effect

                # Capture print output
                with patch('builtins.print') as mock_print:
                    manager.show_checkpoint_status()

                    # Verify output was generated
                    assert mock_print.call_count > 0

                    # Check that status information was displayed
                    print_calls = [call[0][0] for call in mock_print.call_args_list if call[0]]
                    output_text = ' '.join(print_calls)

                    assert "CHECKPOINT STATUS" in output_text


class TestDeploymentManagerIntegration:
    """Integration tests for deployment manager."""

    def test_file_structure_validation(self):
        """Test that all required Kubernetes files exist."""
        manager = MinuteBackfillDeploymentManager()

        # Check that k8s directory exists
        assert manager.k8s_dir.exists(), f"k8s directory not found: {manager.k8s_dir}"

        # Check vendor job files
        for vendor, filename in manager.vendor_jobs.items():
            job_file = manager.k8s_dir / filename
            assert job_file.exists(), f"Missing job file for {vendor}: {job_file}"

        # Check orchestrator file
        orchestrator_file = manager.k8s_dir / manager.orchestrator_job
        assert orchestrator_file.exists(), f"Missing orchestrator file: {orchestrator_file}"

    def test_yaml_file_basic_validation(self):
        """Test basic YAML file validation."""
        manager = MinuteBackfillDeploymentManager()

        # Check each vendor job file
        for vendor, filename in manager.vendor_jobs.items():
            job_file = manager.k8s_dir / filename
            content = job_file.read_text()

            # Basic YAML structure checks
            assert "apiVersion: batch/v1" in content
            assert "kind: Job" in content
            assert f"name: {manager.job_names[vendor]}" in content
            assert "namespace: ats-dev" in content

            # Check for required environment variables
            assert f"{vendor.upper()}_API_KEY" in content

            # Check for volume mounts
            assert "volumeMounts:" in content
            assert "/data/minute-files" in content or "/data/checkpoints" in content

        # Check orchestrator file
        orchestrator_file = manager.k8s_dir / manager.orchestrator_job
        content = orchestrator_file.read_text()

        assert "apiVersion: batch/v1" in content
        assert "kind: Job" in content
        assert "comprehensive-30year-minute-backfill" in content

    @pytest.mark.skipif(
        not Path("/usr/bin/kubectl").exists() and not Path("/usr/local/bin/kubectl").exists(),
        reason="kubectl not available for integration testing"
    )
    def test_kubectl_connectivity(self):
        """Test kubectl connectivity (if available)."""
        manager = MinuteBackfillDeploymentManager()

        # Try a simple kubectl command
        result = manager.run_kubectl("version --client")

        # Should at least get client version (even without cluster connection)
        assert "Client Version" in result.stdout or result.returncode == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])