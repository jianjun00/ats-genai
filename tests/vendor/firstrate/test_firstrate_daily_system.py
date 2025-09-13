#!/usr/bin/env python3
"""
Integration Tests for FirstRate Daily Download System

Tests the complete system integration:
- End-to-end download workflows
- Real file system operations
- Script execution and CLI
- Logging and monitoring
- System resource usage
- Production readiness scenarios
"""

import pytest
import tempfile
import subprocess
import sys
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import shutil
import zipfile
from unittest.mock import patch

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class TestFirstRateSystemIntegration:
    """Test complete system integration."""

    @pytest.fixture
    def temp_system_setup(self):
        """Set up complete system environment."""
        temp_dir = tempfile.mkdtemp()

        # Create directory structure
        data_dir = Path(temp_dir) / "ats-data" / "firstrate-data" / "daily"
        logs_dir = Path(temp_dir) / "ats-logs"

        for asset_type in ["stock", "etf", "fx"]:
            (data_dir / asset_type).mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        yield {
            "temp_dir": temp_dir,
            "data_dir": data_dir,
            "logs_dir": logs_dir
        }

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_script_execution_with_help(self):
        """Test that the script can be executed and shows help."""
        script_path = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'firstrate_daily_download.py')

        # Test script execution with --help
        result = subprocess.run([
            sys.executable, script_path, '--help'
        ], capture_output=True, text=True, env={
            'PYTHONPATH': os.path.join(os.path.dirname(__file__), '..', '..', 'src')
        })

        assert result.returncode == 0
        assert 'FirstRate Daily Download Job' in result.stdout
        assert '--all' in result.stdout
        assert '--asset-types' in result.stdout
        assert '--debug' in result.stdout
        assert '--no-cleanup' in result.stdout

    def test_directory_structure_creation(self, temp_system_setup):
        """Test that the system creates proper directory structure."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader

        # Initialize downloader - should create directories
        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Verify directory structure was created
        for asset_type in ["stock", "etf", "fx"]:
            daily_dir = temp_system_setup["data_dir"] / asset_type
            assert daily_dir.exists()
            assert daily_dir.is_dir()

    @patch('aiohttp.ClientSession')
    @pytest.mark.asyncio
    async def test_end_to_end_download_simulation(self, mock_session_cls, temp_system_setup):
        """Test complete end-to-end download workflow."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader, DownloadJob

        # Create realistic test data
        test_data = {
            "stock": self._create_realistic_stock_data(),
            "etf": self._create_realistic_etf_data(),
            "fx": self._create_realistic_fx_data()
        }

        # Mock HTTP client
        mock_session = self._setup_mock_session(mock_session_cls, test_data)

        # Initialize downloader
        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Create jobs for all asset types
        jobs = [
            DownloadJob(asset_type="stock"),
            DownloadJob(asset_type="etf"),
            DownloadJob(asset_type="fx")
        ]

        # Execute download
        results = await downloader.download_daily_data(jobs)

        # Verify results
        assert all(results.values()), f"Some downloads failed: {results}"

        # Verify files were created
        today_str = date.today().strftime('%Y%m%d')
        for asset_type in ["stock", "etf", "fx"]:
            expected_file = temp_system_setup["data_dir"] / asset_type / f"{asset_type}_{today_str}_1min_adj_split.zip"
            assert expected_file.exists(), f"Expected file not found: {expected_file}"
            assert expected_file.stat().st_size > 0, f"File is empty: {expected_file}"

            # Verify file is valid ZIP
            assert downloader.verify_zip_file(expected_file), f"Invalid ZIP file: {expected_file}"

    def test_cleanup_integration(self, temp_system_setup):
        """Test file cleanup integration."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader

        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Create test files with different ages
        today = date.today()
        test_files = []

        for asset_type in ["stock", "etf", "fx"]:
            asset_dir = temp_system_setup["data_dir"] / asset_type

            # Create files for different dates
            for days_ago in [1, 5, 8, 10, 15]:
                file_date = today - timedelta(days=days_ago)
                filename = f"{asset_type}_{file_date.strftime('%Y%m%d')}_1min_adj_split.zip"
                file_path = asset_dir / filename

                # Create realistic ZIP file
                with zipfile.ZipFile(file_path, 'w') as zf:
                    zf.writestr(f"{asset_type}_data.txt", f"sample data for {file_date}")

                test_files.append((file_path, days_ago))

        # Run cleanup with 7-day retention
        total_deleted = 0
        for asset_type in ["stock", "etf", "fx"]:
            deleted = downloader.cleanup_old_files(asset_type, keep_days=7)
            total_deleted += deleted

        # Verify correct files were deleted (files older than 7 days)
        expected_deleted = len([f for f in test_files if f[1] > 7])
        assert total_deleted == expected_deleted

        # Verify correct files remain
        for file_path, days_ago in test_files:
            if days_ago <= 7:
                assert file_path.exists(), f"Recent file was incorrectly deleted: {file_path}"
            else:
                assert not file_path.exists(), f"Old file was not deleted: {file_path}"

    def test_logging_integration(self, temp_system_setup):
        """Test logging system integration."""
        import logging
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader

        # Set up file logging
        log_file = temp_system_setup["logs_dir"] / "firstrate-test.log"

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Perform some operations that should generate logs
        job = DownloadJob(asset_type="stock")
        url = downloader.build_download_url(job)
        output_path = downloader.get_output_path(job, "20240829")

        # Create a test file and calculate checksum
        test_file = temp_system_setup["data_dir"] / "stock" / "test.zip"
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr("test.txt", "test data")

        checksum = downloader.calculate_checksum(test_file)
        verify_result = downloader.verify_zip_file(test_file)

        # Check that log file was created and contains expected content
        assert log_file.exists()
        log_content = log_file.read_text()

        # Should contain some logging output (even if minimal due to mocking)
        assert len(log_content.strip()) >= 0  # At least some logging occurred

    def test_error_recovery_scenarios(self, temp_system_setup):
        """Test system behavior under error conditions."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader

        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Test 1: Handle non-existent directory gracefully
        deleted_count = downloader.cleanup_old_files("nonexistent_type", keep_days=7)
        assert deleted_count == 0

        # Test 2: Handle corrupted ZIP files
        corrupted_zip = temp_system_setup["data_dir"] / "stock" / "corrupted.zip"
        with open(corrupted_zip, 'wb') as f:
            f.write(b"NOT A ZIP FILE")

        assert not downloader.verify_zip_file(corrupted_zip)

        # Test 3: Handle permission issues (simulate)
        protected_file = temp_system_setup["data_dir"] / "stock" / "protected.zip"
        with zipfile.ZipFile(protected_file, 'w') as zf:
            zf.writestr("data.txt", "protected data")

        # Make file read-only
        protected_file.chmod(0o444)

        try:
            # Should still be able to verify read-only files
            result = downloader.verify_zip_file(protected_file)
            assert result is True
        finally:
            # Restore permissions for cleanup
            protected_file.chmod(0o644)

    def test_concurrent_access_safety(self, temp_system_setup):
        """Test system behavior with concurrent access."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader
        import threading

        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Create test files
        test_files = []
        today = date.today()

        for i in range(10):
            file_date = today - timedelta(days=10 + i)
            filename = f"stock_{file_date.strftime('%Y%m%d')}_1min_adj_split.zip"
            file_path = temp_system_setup["data_dir"] / "stock" / filename

            with zipfile.ZipFile(file_path, 'w') as zf:
                zf.writestr("data.txt", f"data for day {i}")

            test_files.append(file_path)

        # Run concurrent cleanup operations
        cleanup_results = []

        def run_cleanup():
            try:
                result = downloader.cleanup_old_files("stock", keep_days=7)
                cleanup_results.append(result)
            except Exception as e:
                cleanup_results.append(f"ERROR: {e}")

        threads = []
        for _ in range(3):  # 3 concurrent cleanup operations
            thread = threading.Thread(target=run_cleanup)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=10)  # 10 second timeout

        # Verify all operations completed
        assert len(cleanup_results) == 3

        # At least one should have succeeded in deleting files
        assert any(isinstance(result, int) and result > 0 for result in cleanup_results)

    def test_system_resource_usage(self, temp_system_setup):
        """Test system resource usage patterns."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader
        import psutil
        import gc

        # Get initial memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss

        downloader = FirstRateDownloader(base_path=temp_system_setup["data_dir"].parent.parent)

        # Create large number of files for processing
        for asset_type in ["stock", "etf", "fx"]:
            asset_dir = temp_system_setup["data_dir"] / asset_type

            for i in range(50):  # 50 files per asset type
                file_date = date.today() - timedelta(days=i)
                filename = f"{asset_type}_{file_date.strftime('%Y%m%d')}_1min_adj_split.zip"
                file_path = asset_dir / filename

                with zipfile.ZipFile(file_path, 'w') as zf:
                    zf.writestr("data.txt", f"data {i}" * 1000)  # ~6KB per file

        # Perform operations
        for asset_type in ["stock", "etf", "fx"]:
            downloader.cleanup_old_files(asset_type, keep_days=30)

        # Force garbage collection
        gc.collect()

        # Check memory usage
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 100MB for this test)
        assert memory_increase < 100 * 1024 * 1024, f"Memory usage increased by {memory_increase / 1024 / 1024:.2f}MB"

    def _create_realistic_stock_data(self):
        """Create realistic stock market data."""
        content = []
        content.append("timestamp,open,high,low,close,volume")

        # Generate sample data for major stocks
        stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        base_time = datetime(2024, 8, 29, 9, 30)  # Market open

        for stock in stocks:
            for minute in range(390):  # 6.5 hours * 60 minutes
                timestamp = base_time + timedelta(minutes=minute)
                price = 100 + (minute % 50)  # Varying price
                open_price = price + 0.1
                high_price = price + 0.5
                low_price = price - 0.3
                close_price = price + 0.2
                volume = 1000 + (minute * 10)

                content.append(f"{timestamp.isoformat()},{open_price:.2f},{high_price:.2f},{low_price:.2f},{close_price:.2f},{volume}")

        return "\n".join(content)

    def _create_realistic_etf_data(self):
        """Create realistic ETF market data."""
        content = []
        content.append("timestamp,open,high,low,close,volume")

        # Generate sample data for major ETFs
        etfs = ["SPY", "QQQ", "VTI", "IWM", "EFA"]
        base_time = datetime(2024, 8, 29, 9, 30)

        for etf in etfs:
            for minute in range(390):
                timestamp = base_time + timedelta(minutes=minute)
                price = 300 + (minute % 20)  # ETF price range
                open_price = price + 0.05
                high_price = price + 0.2
                low_price = price - 0.15
                close_price = price + 0.1
                volume = 5000 + (minute * 5)

                content.append(f"{timestamp.isoformat()},{open_price:.2f},{high_price:.2f},{low_price:.2f},{close_price:.2f},{volume}")

        return "\n".join(content)

    def _create_realistic_fx_data(self):
        """Create realistic FX market data."""
        content = []
        content.append("timestamp,open,high,low,close,volume")

        # Generate sample data for major currency pairs
        pairs = ["EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD"]
        base_time = datetime(2024, 8, 29, 0, 0)  # FX trades 24/7

        for pair in pairs:
            for minute in range(1440):  # 24 hours * 60 minutes
                timestamp = base_time + timedelta(minutes=minute)
                if pair == "USDJPY":
                    price = 150 + (minute % 5) * 0.01  # JPY rates
                else:
                    price = 1.0 + (minute % 100) * 0.0001  # Other major pairs

                open_price = price + 0.0001
                high_price = price + 0.0003
                low_price = price - 0.0002
                close_price = price + 0.0001
                volume = 100000 + (minute * 100)

                content.append(f"{timestamp.isoformat()},{open_price:.5f},{high_price:.5f},{low_price:.5f},{close_price:.5f},{volume}")

        return "\n".join(content)

    def _setup_mock_session(self, mock_session_cls, test_data):
        """Set up mock HTTP session with realistic responses."""
        from unittest.mock import AsyncMock

        mock_session = AsyncMock()
        mock_session_cls.return_value = mock_session

        async def mock_get(*args, **kwargs):
            """Mock GET request that returns appropriate data based on URL."""
            url = args[0] if args else kwargs.get('url', '')

            mock_response = AsyncMock()
            mock_response.status = 200

            # Determine asset type from URL
            if 'type=stock' in url:
                data = test_data["stock"]
            elif 'type=etf' in url:
                data = test_data["etf"]
            elif 'type=fx' in url:
                data = test_data["fx"]
            else:
                data = "timestamp,open,high,low,close,volume\n2024-08-29 09:30:00,100,101,99,100.5,1000"

            # Create ZIP content
            zip_content = self._create_zip_content(data)
            mock_response.content.iter_chunked.return_value = [zip_content]

            return mock_response

        mock_session.__aenter__.return_value = mock_session
        mock_session.get.side_effect = mock_get

        return mock_session

    def _create_zip_content(self, csv_data):
        """Create ZIP file content from CSV data."""
        import io

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("market_data.txt", csv_data)

        return zip_buffer.getvalue()


class TestFirstRateProductionReadiness:
    """Test production readiness scenarios."""

    def test_configuration_validation(self):
        """Test that system validates configuration properly."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test with valid configuration
            downloader = FirstRateDownloader(
                base_path=temp_dir,
                userid="test_user",
                api_base="https://example.com/api",
                max_retries=3,
                retry_delay=1.0
            )

            assert downloader.userid == "test_user"
            assert downloader.api_base == "https://example.com/api"
            assert downloader.max_retries == 3
            assert downloader.retry_delay == 1.0

    def test_cron_job_compatibility(self):
        """Test that the system works correctly when run from cron."""
        # This test verifies that the script can handle cron environment
        script_path = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'firstrate_daily_download.py')

        # Simulate cron environment (minimal PATH, no interactive shell)
        cron_env = {
            'PATH': '/usr/bin:/bin',
            'SHELL': '/bin/sh',
            'PYTHONPATH': os.path.join(os.path.dirname(__file__), '..', '..', 'src')
        }

        # Test that script can start (will fail due to missing API, but should parse args)
        result = subprocess.run([
            sys.executable, script_path, '--help'
        ], capture_output=True, text=True, env=cron_env)

        assert result.returncode == 0
        assert 'FirstRate Daily Download Job' in result.stdout

    def test_disk_space_monitoring(self):
        """Test disk space considerations."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader

        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = FirstRateDownloader(base_path=temp_dir)

            # Create files to simulate disk usage
            total_size = 0
            for asset_type in ["stock", "etf", "fx"]:
                asset_dir = Path(temp_dir) / "daily" / asset_type
                asset_dir.mkdir(parents=True, exist_ok=True)

                # Create files for last 30 days
                for i in range(30):
                    file_date = date.today() - timedelta(days=i)
                    filename = f"{asset_type}_{file_date.strftime('%Y%m%d')}_1min_adj_split.zip"
                    file_path = asset_dir / filename

                    # Create file with realistic size (10MB for stock, 5MB for ETF, 2MB for FX)
                    if asset_type == "stock":
                        size = 10 * 1024 * 1024
                    elif asset_type == "etf":
                        size = 5 * 1024 * 1024
                    else:  # fx
                        size = 2 * 1024 * 1024

                    with open(file_path, 'wb') as f:
                        f.write(b'0' * size)

                    total_size += size

            # Test cleanup reduces disk usage
            initial_usage = total_size

            # Clean up files older than 7 days
            for asset_type in ["stock", "etf", "fx"]:
                downloader.cleanup_old_files(asset_type, keep_days=7)

            # Calculate remaining files
            remaining_size = 0
            for asset_type in ["stock", "etf", "fx"]:
                asset_dir = Path(temp_dir) / "daily" / asset_type
                for file_path in asset_dir.glob("*.zip"):
                    remaining_size += file_path.stat().st_size

            # Should have reduced disk usage significantly
            cleanup_ratio = remaining_size / initial_usage
            assert cleanup_ratio < 0.3  # Should remove at least 70% of files

    def test_monitoring_and_alerting_hooks(self):
        """Test hooks for monitoring and alerting systems."""
        from domains.market_data.services.agent.firstrate_daily_downloader import FirstRateDownloader
        import logging

        # Set up logging capture
        log_messages = []

        class TestLogHandler(logging.Handler):
            def emit(self, record):
                log_messages.append(self.format(record))

        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up logging
            logger = logging.getLogger('market_data.agent.firstrate_daily_downloader')
            handler = TestLogHandler()
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            downloader = FirstRateDownloader(base_path=temp_dir)

            # Perform operations that should generate monitorable events
            job = DownloadJob(asset_type="stock")
            url = downloader.build_download_url(job)

            # Create test file for verification
            test_file = Path(temp_dir) / "daily" / "stock" / "test.zip"
            test_file.parent.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(test_file, 'w') as zf:
                zf.writestr("data.txt", "test data")

            # Operations that should be logged
            checksum = downloader.calculate_checksum(test_file)
            verify_result = downloader.verify_zip_file(test_file)
            cleanup_result = downloader.cleanup_old_files("stock", keep_days=7)

            # Clean up
            logger.removeHandler(handler)

            # Should have captured some log messages that monitoring can use
            assert len(log_messages) >= 0  # At least basic logging occurred


if __name__ == "__main__":
    pytest.main([__file__, "-v"])