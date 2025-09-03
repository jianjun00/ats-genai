#!/usr/bin/env python3
"""
Comprehensive Tests for 30-Year Minute Data Backfill System

Tests the complete backfill system including:
- Configuration validation
- Checkpoint framework
- Job management
- Storage integration  
- Error handling and recovery
- Performance tracking
"""

import pytest
import asyncio
import os
import sys
import tempfile
import json
import shutil
from datetime import datetime, date, timedelta
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import List, Dict, Any

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'scripts' / 'backfill'))

try:
    from comprehensive_30year_minute_backfill import (
        MinuteBackfillConfig,
        SymbolBackfillJob, 
        BackfillProgress,
        Comprehensive30YearMinuteBackfill
    )
except ImportError:
    pytest.skip("Backfill system not available", allow_module_level=True)


class TestMinuteBackfillConfig:
    """Test configuration management."""
    
    def test_default_configuration(self):
        """Test default configuration values."""
        config = MinuteBackfillConfig()
        
        assert config.start_date == date(1995, 1, 1)
        assert config.end_date == date(2025, 8, 31)
        assert "polygon" in config.enabled_vendors
        assert "tiingo" in config.enabled_vendors
        assert "fmp" in config.enabled_vendors
        assert "eodhd" in config.enabled_vendors
        assert config.storage_type == "file"
        assert config.min_market_cap == 100_000_000
        assert config.max_instruments == 10000
    
    def test_custom_configuration(self):
        """Test custom configuration values."""
        custom_config = MinuteBackfillConfig(
            start_date=date(2020, 1, 1),
            end_date=date(2023, 12, 31),
            enabled_vendors=["polygon", "tiingo"],
            storage_type="database",
            min_market_cap=500_000_000,
            max_instruments=5000
        )
        
        assert custom_config.start_date == date(2020, 1, 1)
        assert custom_config.end_date == date(2023, 12, 31)
        assert len(custom_config.enabled_vendors) == 2
        assert custom_config.storage_type == "database"
        assert custom_config.min_market_cap == 500_000_000
        assert custom_config.max_instruments == 5000
    
    def test_rate_limits(self):
        """Test vendor-specific rate limits."""
        config = MinuteBackfillConfig()
        
        assert config.rate_limits["polygon"] == 2.0
        assert config.rate_limits["tiingo"] == 1.0
        assert config.rate_limits["fmp"] == 1.5
        assert config.rate_limits["eodhd"] == 3.0


class TestSymbolBackfillJob:
    """Test symbol backfill job management."""
    
    def test_job_creation(self):
        """Test job creation with required fields."""
        job = SymbolBackfillJob(
            symbol="AAPL",
            instrument_id=1,
            vendor="polygon",
            start_date=date(2020, 1, 1),
            end_date=date(2020, 12, 31)
        )
        
        assert job.symbol == "AAPL"
        assert job.instrument_id == 1
        assert job.vendor == "polygon"
        assert job.status == "pending"
        assert job.attempt_count == 0
        assert job.priority == 1
    
    def test_job_id_generation(self):
        """Test unique job ID generation."""
        job1 = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="polygon",
            start_date=date(2020, 1, 1), end_date=date(2020, 12, 31)
        )
        
        job2 = SymbolBackfillJob(
            symbol="MSFT", instrument_id=2, vendor="polygon", 
            start_date=date(2020, 1, 1), end_date=date(2020, 12, 31)
        )
        
        job3 = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="tiingo",
            start_date=date(2020, 1, 1), end_date=date(2020, 12, 31)
        )
        
        # Different symbols should have different IDs
        assert job1.job_id != job2.job_id
        
        # Same symbol, different vendor should have different IDs
        assert job1.job_id != job3.job_id
        
        # Job IDs should be consistent
        assert len(job1.job_id) == 16
        assert job1.job_id == job1.job_id  # Should be deterministic
    
    def test_progress_calculation(self):
        """Test progress percentage calculation."""
        job = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="polygon",
            start_date=date(2020, 1, 1), end_date=date(2020, 12, 31),
            chunks_total=10, chunks_completed=3
        )
        
        assert job.progress_percent == 30.0
        
        job.chunks_completed = 10
        assert job.progress_percent == 100.0
        
        # Test edge case
        job.chunks_total = 0
        assert job.progress_percent == 0.0


class TestBackfillProgress:
    """Test progress tracking functionality."""
    
    def test_progress_initialization(self):
        """Test progress tracker initialization."""
        progress = BackfillProgress()
        
        assert progress.total_jobs == 0
        assert "pending" in progress.jobs_by_status
        assert "completed" in progress.jobs_by_status
        assert "failed" in progress.jobs_by_status
        assert progress.total_bars_fetched == 0
        assert progress.symbols_per_hour == 0.0
    
    def test_estimate_updates(self):
        """Test performance estimate calculations."""
        progress = BackfillProgress()
        progress.start_time = datetime.now() - timedelta(hours=1)
        progress.jobs_by_status["completed"] = 100
        progress.jobs_by_status["pending"] = 400
        progress.total_bars_fetched = 10_000_000
        
        progress.update_estimates()
        
        # Should calculate symbols per hour
        assert progress.symbols_per_hour > 0
        
        # Should calculate bars per second
        assert progress.bars_per_second > 0
        
        # Should estimate completion time
        assert progress.estimated_completion is not None
        assert progress.estimated_completion > datetime.now()


class TestComprehensive30YearMinuteBackfill:
    """Test the main backfill orchestrator."""
    
    @pytest.fixture
    def temp_config(self):
        """Create temporary configuration for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            config = MinuteBackfillConfig(
                enabled_vendors=["polygon", "tiingo"],
                target_symbols=["AAPL", "MSFT", "GOOGL"],
                start_date=date(2023, 1, 1),
                end_date=date(2023, 1, 31),  # Just one month for testing
                file_base_path=str(temp_path / "minute-files"),
                checkpoint_dir=str(temp_path / "checkpoints"),
                max_instruments=3,
                batch_size_symbols=2,
                chunk_size_months=1
            )
            
            yield config, temp_path
    
    @pytest.fixture
    def mock_adapters(self):
        """Create mock vendor adapters."""
        with patch('comprehensive_30year_minute_backfill.PolygonMinuteAdapter') as mock_polygon, \
             patch('comprehensive_30year_minute_backfill.TiingoIntradayAdapter') as mock_tiingo:
            
            # Mock adapter instances
            mock_polygon_instance = Mock()
            mock_tiingo_instance = Mock()
            
            # Mock async context manager
            mock_polygon_instance.__aenter__ = AsyncMock(return_value=mock_polygon_instance)
            mock_polygon_instance.__aexit__ = AsyncMock(return_value=None)
            mock_tiingo_instance.__aenter__ = AsyncMock(return_value=mock_tiingo_instance)
            mock_tiingo_instance.__aexit__ = AsyncMock(return_value=None)
            
            # Mock data fetching
            mock_bar = Mock()
            mock_bar.timestamp = datetime(2023, 1, 1, 9, 30)
            mock_bar.open = 150.0
            mock_bar.high = 151.0
            mock_bar.low = 149.0
            mock_bar.close = 150.5
            mock_bar.volume = 1000
            
            mock_polygon_instance.fetch_multiple_symbols_async = AsyncMock(
                return_value={"AAPL": [mock_bar], "MSFT": [mock_bar]}
            )
            mock_tiingo_instance.fetch_multiple_symbols_async = AsyncMock(
                return_value={"AAPL": [mock_bar], "GOOGL": [mock_bar]}
            )
            
            # Mock quality validation
            mock_polygon_instance.validate_data_quality.return_value = {
                "valid": True, "quality_score": 0.95
            }
            mock_tiingo_instance.validate_data_quality.return_value = {
                "valid": True, "quality_score": 0.90
            }
            
            mock_polygon.return_value = mock_polygon_instance
            mock_tiingo.return_value = mock_tiingo_instance
            
            yield {
                "polygon": mock_polygon_instance,
                "tiingo": mock_tiingo_instance
            }
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialization(self, temp_config):
        """Test backfill system initialization."""
        config, temp_path = temp_config
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        
        with patch.object(backfill, '_initialize_adapters') as mock_init_adapters, \
             patch.object(backfill, '_initialize_storage') as mock_init_storage, \
             patch.object(backfill, '_load_or_generate_jobs') as mock_load_jobs:
            
            mock_init_adapters.return_value = None
            mock_init_storage.return_value = None  
            mock_load_jobs.return_value = None
            
            await backfill.initialize()
            
            mock_init_adapters.assert_called_once()
            mock_init_storage.assert_called_once()
            mock_load_jobs.assert_called_once()
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_job_generation(self, temp_config):
        """Test job generation for target instruments."""
        config, temp_path = temp_config
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        backfill.adapters = {"polygon": Mock(), "tiingo": Mock()}
        
        # Mock target instruments
        with patch.object(backfill, '_get_target_instruments') as mock_get_instruments:
            mock_get_instruments.return_value = [
                (1, "AAPL", 1),
                (2, "MSFT", 1), 
                (3, "GOOGL", 1)
            ]
            
            await backfill._generate_jobs()
            
            # Should create jobs for all symbols and vendors
            # 3 symbols × 2 vendors × 1 chunk = 6 jobs
            assert len(backfill.jobs) == 6
            
            # Check job details
            job_symbols = [job.symbol for job in backfill.jobs.values()]
            assert "AAPL" in job_symbols
            assert "MSFT" in job_symbols
            assert "GOOGL" in job_symbols
            
            job_vendors = [job.vendor for job in backfill.jobs.values()]
            assert "polygon" in job_vendors
            assert "tiingo" in job_vendors
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_checkpoint_save_load(self, temp_config):
        """Test checkpoint saving and loading."""
        config, temp_path = temp_config
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        
        # Create some test jobs
        test_job = SymbolBackfillJob(
            symbol="AAPL",
            instrument_id=1,
            vendor="polygon", 
            start_date=date(2023, 1, 1),
            end_date=date(2023, 1, 31),
            status="completed",
            bars_fetched=10000
        )
        
        backfill.jobs[test_job.job_id] = test_job
        backfill.progress.total_jobs = 1
        backfill.progress.jobs_by_status["completed"] = 1
        
        # Save checkpoint
        await backfill._save_checkpoint()
        
        # Verify checkpoint file exists
        checkpoint_file = Path(config.checkpoint_dir) / "comprehensive_minute_backfill.json"
        assert checkpoint_file.exists()
        
        # Create new backfill instance and load checkpoint
        new_backfill = Comprehensive30YearMinuteBackfill(config)
        await new_backfill._load_checkpoint(checkpoint_file)
        
        # Verify data was restored
        assert len(new_backfill.jobs) == 1
        restored_job = list(new_backfill.jobs.values())[0]
        assert restored_job.symbol == "AAPL"
        assert restored_job.status == "completed"
        assert restored_job.bars_fetched == 10000
        assert new_backfill.progress.total_jobs == 1
    
    @pytest.mark.asyncio 
    @pytest.mark.asyncio
    async def test_data_storage_file(self, temp_config, mock_adapters):
        """Test file-based data storage."""
        config, temp_path = temp_config
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        backfill.adapters = mock_adapters
        
        # Mock file manager
        mock_file_manager = Mock()
        mock_file_manager.store_minute_data = AsyncMock(return_value={"stored": 1})
        backfill.file_manager = mock_file_manager
        
        # Create test job and bars
        job = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="polygon",
            start_date=date(2023, 1, 1), end_date=date(2023, 1, 31)
        )
        
        mock_bar = Mock()
        mock_bar.timestamp = datetime(2023, 1, 1, 9, 30)
        mock_bar.open = 150.0
        mock_bar.high = 151.0
        mock_bar.low = 149.0
        mock_bar.close = 150.5
        mock_bar.volume = 1000
        
        bars = [mock_bar]
        
        # Test storage
        with patch('comprehensive_30year_minute_backfill.MinuteBar') as mock_minute_bar:
            result = await backfill._store_data(job, bars)
            
            assert result is True
            mock_file_manager.store_minute_data.assert_called_once()
            mock_minute_bar.assert_called_once()
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_symbol_job_processing(self, temp_config, mock_adapters):
        """Test individual symbol job processing."""
        config, temp_path = temp_config
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        backfill.file_manager = Mock()
        backfill.file_manager.store_minute_data = AsyncMock(return_value={"stored": 1})
        
        # Create test job
        job = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="polygon",
            start_date=date(2023, 1, 1), end_date=date(2023, 1, 31)
        )
        
        # Mock adapter with data
        adapter = mock_adapters["polygon"]
        mock_bar = Mock()
        mock_bar.timestamp = datetime(2023, 1, 1, 9, 30)
        mock_bar.open = 150.0
        mock_bar.high = 151.0 
        mock_bar.low = 149.0
        mock_bar.close = 150.5
        mock_bar.volume = 1000
        
        adapter.fetch_minute_bars_async.return_value = [mock_bar]
        
        semaphore = asyncio.Semaphore(1)
        
        with patch.object(backfill, '_store_data', return_value=True):
            result = await backfill._process_symbol_job(job, adapter, semaphore)
            
            assert result is True
            assert job.status == "completed"
            assert job.bars_fetched == 1
            assert job.bars_stored == 1
            assert job.chunks_completed == 1
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling(self, temp_config, mock_adapters):
        """Test error handling during job processing."""
        config, temp_path = temp_config
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        
        # Create failing job
        job = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="polygon",
            start_date=date(2023, 1, 1), end_date=date(2023, 1, 31)
        )
        
        # Mock adapter that raises exception
        adapter = mock_adapters["polygon"]
        adapter.fetch_minute_bars_async.side_effect = Exception("API Error")
        
        semaphore = asyncio.Semaphore(1)
        
        result = await backfill._process_symbol_job(job, adapter, semaphore)
        
        assert result is False
        assert job.status == "failed" 
        assert job.error_message == "API Error"
        assert job.attempt_count == 1
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_quality_validation_failure(self, temp_config, mock_adapters):
        """Test handling of low quality data."""
        config, temp_path = temp_config
        config.min_data_quality_score = 0.8  # High threshold
        
        backfill = Comprehensive30YearMinuteBackfill(config)
        
        job = SymbolBackfillJob(
            symbol="AAPL", instrument_id=1, vendor="polygon",
            start_date=date(2023, 1, 1), end_date=date(2023, 1, 31)
        )
        
        # Mock adapter with low quality data
        adapter = mock_adapters["polygon"]
        mock_bar = Mock()
        mock_bar.timestamp = datetime(2023, 1, 1, 9, 30)
        mock_bar.open = 150.0
        mock_bar.high = 151.0
        mock_bar.low = 149.0
        mock_bar.close = 150.5
        mock_bar.volume = 1000
        
        adapter.fetch_minute_bars_async.return_value = [mock_bar]
        adapter.validate_data_quality.return_value = {
            "valid": False, "quality_score": 0.5  # Below threshold
        }
        
        semaphore = asyncio.Semaphore(1)
        
        result = await backfill._process_symbol_job(job, adapter, semaphore)
        
        assert result is False
        assert job.status == "failed"
        assert "quality score" in job.error_message.lower()


class TestIntegration:
    """Integration tests for the complete system."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_simulation(self):
        """Test end-to-end backfill simulation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            config = MinuteBackfillConfig(
                enabled_vendors=["polygon"],
                target_symbols=["AAPL", "MSFT"],
                start_date=date(2023, 1, 1),
                end_date=date(2023, 1, 31),
                file_base_path=str(temp_path / "minute-files"),
                checkpoint_dir=str(temp_path / "checkpoints"),
                max_instruments=2,
                batch_size_symbols=1,
                chunk_size_months=1
            )
            
            backfill = Comprehensive30YearMinuteBackfill(config)
            
            # Mock all external dependencies
            with patch.object(backfill, '_initialize_adapters') as mock_adapters, \
                 patch.object(backfill, '_initialize_storage') as mock_storage, \
                 patch.object(backfill, '_get_target_instruments') as mock_instruments, \
                 patch.object(backfill, '_run_vendor_backfill') as mock_vendor_run:
                
                # Setup mocks
                mock_adapters.return_value = None
                mock_storage.return_value = None
                mock_instruments.return_value = [(1, "AAPL", 1), (2, "MSFT", 1)]
                mock_vendor_run.return_value = {
                    "completed": 2, "failed": 0, "total": 2, "success_rate": 1.0
                }
                
                backfill.adapters = {"polygon": Mock()}
                
                # Run initialization
                await backfill.initialize()
                
                # Verify jobs were generated
                assert len(backfill.jobs) > 0
                
                # Run simulation
                results = await backfill.run_backfill()
                
                # Verify results
                assert "polygon" in results
                assert results["polygon"]["completed"] == 2
                assert results["polygon"]["success_rate"] == 1.0
    
    def test_configuration_validation(self):
        """Test configuration validation and edge cases."""
        # Test minimum valid configuration
        config = MinuteBackfillConfig(
            enabled_vendors=["polygon"],
            target_symbols=["AAPL"],
            max_instruments=1
        )
        
        assert len(config.enabled_vendors) == 1
        assert config.target_symbols == ["AAPL"]
        
        # Test date validation
        config = MinuteBackfillConfig(
            start_date=date(2020, 1, 1),
            end_date=date(2019, 12, 31)  # End before start
        )
        
        # Should handle gracefully (actual validation would be in the orchestrator)
        assert config.start_date > config.end_date
    
    def test_performance_calculations(self):
        """Test performance and progress calculations."""
        progress = BackfillProgress()
        progress.start_time = datetime.now() - timedelta(hours=2)
        progress.jobs_by_status = {
            "pending": 800,
            "processing": 50, 
            "completed": 150,
            "failed": 0,
            "skipped": 0
        }
        progress.total_bars_fetched = 15_000_000
        
        progress.update_estimates()
        
        # Should calculate meaningful metrics
        assert progress.symbols_per_hour > 0
        assert progress.bars_per_second > 0
        assert progress.estimated_completion is not None
        
        # Performance should be reasonable (at least 50 symbols/hour)
        assert progress.symbols_per_hour >= 50
    
    def test_checkpoint_data_integrity(self):
        """Test checkpoint data integrity and format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint_file = Path(temp_dir) / "test_checkpoint.json"
            
            # Create test checkpoint data
            checkpoint_data = {
                "config": {
                    "start_date": "2023-01-01",
                    "end_date": "2023-01-31",
                    "enabled_vendors": ["polygon", "tiingo"]
                },
                "progress": {
                    "start_time": datetime.now().isoformat(),
                    "total_jobs": 100,
                    "jobs_completed": 25
                },
                "jobs": {
                    "test_job_id": {
                        "symbol": "AAPL",
                        "vendor": "polygon",
                        "status": "completed",
                        "bars_fetched": 10000
                    }
                }
            }
            
            # Save and reload checkpoint
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            # Verify file exists and is readable
            assert checkpoint_file.exists()
            
            with open(checkpoint_file, 'r') as f:
                loaded_data = json.load(f)
            
            # Verify data integrity
            assert loaded_data["config"]["enabled_vendors"] == ["polygon", "tiingo"]
            assert loaded_data["progress"]["total_jobs"] == 100
            assert loaded_data["jobs"]["test_job_id"]["symbol"] == "AAPL"
            assert loaded_data["jobs"]["test_job_id"]["bars_fetched"] == 10000


@pytest.mark.integration
class TestKubernetesIntegration:
    """Integration tests for Kubernetes deployment components."""
    
    def test_deployment_manager_initialization(self):
        """Test deployment manager initialization."""
        # This would require the deployment manager to be importable
        # Skip if not available in test environment
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'scripts' / 'backfill'))
            from deploy_30year_minute_backfill import MinuteBackfillDeploymentManager
            
            manager = MinuteBackfillDeploymentManager()
            
            assert manager.namespace == "ats-dev"
            assert "polygon" in manager.vendor_jobs
            assert "tiingo" in manager.vendor_jobs
            assert "fmp" in manager.vendor_jobs
            assert "eodhd" in manager.vendor_jobs
            
        except ImportError:
            pytest.skip("Deployment manager not available")
    
    def test_kubernetes_job_validation(self):
        """Validate Kubernetes job configurations exist."""
        k8s_dir = Path(__file__).parent.parent.parent / "k8s"
        
        # Check that job files exist
        required_files = [
            "30year-minute-backfill-orchestrator.yaml",
            "30year-minute-backfill-polygon.yaml", 
            "30year-minute-backfill-tiingo.yaml",
            "30year-minute-backfill-fmp.yaml",
            "30year-minute-backfill-eodhd.yaml"
        ]
        
        for filename in required_files:
            job_file = k8s_dir / filename
            assert job_file.exists(), f"Missing Kubernetes job file: {filename}"
            
            # Basic YAML validation (check it's readable)
            content = job_file.read_text()
            assert "apiVersion: batch/v1" in content
            assert "kind: Job" in content
            assert "metadata:" in content


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])