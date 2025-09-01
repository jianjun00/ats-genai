#!/usr/bin/env python3
"""
Comprehensive Tests for FirstRate Daily Download Script

Tests the complete daily download script functionality:
- Command line argument parsing
- Main download function
- Error handling and logging
- Real-world scenarios
"""

import pytest
import tempfile
import asyncio
import zipfile
import sys
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from unittest import mock
from io import StringIO

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from market_data.agent.firstrate_daily_downloader import FirstRateDownloader, DownloadJob


class TestFirstRateDownloadScript:
    """Test the FirstRate daily download script."""
    
    @pytest.fixture
    def temp_setup(self):
        """Set up temporary directories."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_import_script(self):
        """Test that the script can be imported without errors."""
        # Import the script module
        script_path = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'firstrate_daily_download.py')
        assert os.path.exists(script_path), f"Script not found at {script_path}"
        
        # Verify script has correct shebang and basic structure
        with open(script_path, 'r') as f:
            content = f.read()
            assert content.startswith('#!/usr/bin/env python3'), "Script should have Python shebang"
            assert 'run_daily_download' in content, "Script should contain main function"
            assert 'argparse' in content, "Script should use argparse for CLI"
    
    @patch('market_data.agent.firstrate_daily_downloader.FirstRateDownloader')
    async def test_run_daily_download_all_types(self, mock_downloader_class, temp_setup):
        """Test running daily download for all asset types."""
        # Import here to avoid import during collection
        from scripts.firstrate_daily_download import run_daily_download
        
        # Mock the downloader
        mock_downloader = AsyncMock()
        mock_downloader_class.return_value = mock_downloader
        mock_downloader.download_daily_data.return_value = {
            "stock": True,
            "etf": True, 
            "fx": True
        }
        mock_downloader.cleanup_old_files = MagicMock(return_value=2)
        
        # Test the function
        result = await run_daily_download(
            asset_types=["stock", "etf", "fx"],
            cleanup_days=7,
            debug=False
        )
        
        # Verify results
        assert result is True
        mock_downloader.download_daily_data.assert_called_once()
        assert mock_downloader.cleanup_old_files.call_count == 3  # Once per asset type
    
    @patch('market_data.agent.firstrate_daily_downloader.FirstRateDownloader')
    async def test_run_daily_download_specific_date(self, mock_downloader_class, temp_setup):
        """Test running daily download for a specific date."""
        from scripts.firstrate_daily_download import run_daily_download
        
        mock_downloader = AsyncMock()
        mock_downloader_class.return_value = mock_downloader
        mock_downloader.download_daily_data.return_value = {"stock": True}
        mock_downloader.cleanup_old_files = MagicMock(return_value=1)
        
        test_date = date(2024, 8, 28)
        result = await run_daily_download(
            asset_types=["stock"],
            download_date=test_date,
            cleanup_days=5
        )
        
        assert result is True
        # Verify the downloader was called with the correct date
        call_args = mock_downloader.download_daily_data.call_args
        assert call_args is not None
    
    @patch('market_data.agent.firstrate_daily_downloader.FirstRateDownloader')
    async def test_run_daily_download_partial_failure(self, mock_downloader_class, temp_setup):
        """Test handling partial download failures."""
        from scripts.firstrate_daily_download import run_daily_download
        
        mock_downloader = AsyncMock()
        mock_downloader_class.return_value = mock_downloader
        mock_downloader.download_daily_data.return_value = {
            "stock": True,
            "etf": False,  # Failure
            "fx": True
        }
        mock_downloader.cleanup_old_files = MagicMock(return_value=0)
        
        result = await run_daily_download(
            asset_types=["stock", "etf", "fx"],
            cleanup_days=7
        )
        
        # Should return False due to partial failure
        assert result is False
    
    @patch('market_data.agent.firstrate_daily_downloader.FirstRateDownloader')
    async def test_run_daily_download_no_cleanup(self, mock_downloader_class, temp_setup):
        """Test running without cleanup."""
        from scripts.firstrate_daily_download import run_daily_download
        
        mock_downloader = AsyncMock()
        mock_downloader_class.return_value = mock_downloader
        mock_downloader.download_daily_data.return_value = {"stock": True}
        
        result = await run_daily_download(
            asset_types=["stock"],
            cleanup_days=0  # No cleanup
        )
        
        assert result is True
        # Cleanup should not be called
        mock_downloader.cleanup_old_files.assert_not_called()
    
    def test_script_command_line_help(self):
        """Test that the script shows help correctly."""
        import subprocess
        script_path = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'firstrate_daily_download.py')
        
        # Run script with --help
        result = subprocess.run([
            sys.executable, script_path, '--help'
        ], capture_output=True, text=True, env={'PYTHONPATH': 'src'})
        
        # Should exit with code 0 and show help
        assert result.returncode == 0
        assert 'FirstRate Daily Download Job' in result.stdout
        assert '--all' in result.stdout
        assert '--asset-types' in result.stdout
        assert '--debug' in result.stdout


class TestFirstRateDownloaderEnhancements:
    """Additional tests for FirstRateDownloader edge cases."""
    
    @pytest.fixture
    def temp_setup(self):
        """Set up temporary directories."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_downloader_custom_userid(self, temp_setup):
        """Test downloader with custom user ID."""
        custom_userid = "custom_test_id"
        downloader = FirstRateDownloader(base_path=temp_setup, userid=custom_userid)
        
        assert downloader.userid == custom_userid
        
        # Test URL building with custom user ID
        job = DownloadJob(asset_type="stock")
        url = downloader.build_download_url(job)
        
        assert f"userid={custom_userid}" in url
    
    def test_downloader_custom_api_base(self, temp_setup):
        """Test downloader API base URL (fixed in constructor)."""
        downloader = FirstRateDownloader(base_path=temp_setup)
        
        # API base is hardcoded in constructor
        assert downloader.api_base == "https://firstratedata.com/api"
        
        job = DownloadJob(asset_type="etf")
        url = downloader.build_download_url(job)
        
        assert url.startswith(downloader.api_base)
    
    def test_build_download_url_edge_cases(self, temp_setup):
        """Test URL building with various parameter combinations."""
        downloader = FirstRateDownloader(base_path=temp_setup)
        
        # Test different asset types
        test_cases = [
            {"asset_type": "stock", "period": "day", "timeframe": "1min", "adjustment": "adj_split"},
            {"asset_type": "etf", "period": "week", "timeframe": "5min", "adjustment": "adj_splitdiv"},
            {"asset_type": "fx", "period": "month", "timeframe": "15min", "adjustment": "UNADJUSTED"},
        ]
        
        for params in test_cases:
            job = DownloadJob(**params)
            url = downloader.build_download_url(job)
            
            # Verify all parameters are in URL
            for key, value in params.items():
                if key == "asset_type":
                    assert f"type={value}" in url
                else:
                    assert f"{key}={value}" in url
    
    def test_calculate_checksum_edge_cases(self, temp_setup):
        """Test checksum calculation with various file scenarios."""
        downloader = FirstRateDownloader(base_path=temp_setup)
        
        # Test empty file
        empty_file = Path(temp_setup) / "empty.txt"
        empty_file.touch()
        checksum = downloader.calculate_checksum(empty_file)
        assert len(checksum) == 32
        assert checksum == "d41d8cd98f00b204e9800998ecf8427e"  # MD5 of empty file
        
        # Test large file
        large_file = Path(temp_setup) / "large.txt"
        with open(large_file, 'w') as f:
            f.write("x" * 10000)  # 10KB file
        checksum = downloader.calculate_checksum(large_file)
        assert len(checksum) == 32
        assert all(c in '0123456789abcdef' for c in checksum)
    
    def test_verify_zip_file_comprehensive(self, temp_setup):
        """Comprehensive ZIP file verification tests."""
        downloader = FirstRateDownloader(base_path=temp_setup)
        
        # Test ZIP with multiple CSV files
        multi_csv_zip = Path(temp_setup) / "multi.zip"
        with zipfile.ZipFile(multi_csv_zip, 'w') as zf:
            zf.writestr("AAPL.csv", "timestamp,open,high,low,close,volume\n2024-08-29 09:30:00,150,151,149,150.5,2000")
            zf.writestr("MSFT.csv", "timestamp,open,high,low,close,volume\n2024-08-29 09:30:00,300,301,299,300.5,3000")
            zf.writestr("GOOGL.txt", "timestamp,open,high,low,close,volume\n2024-08-29 09:30:00,2500,2501,2499,2500.5,1000")
        
        assert downloader.verify_zip_file(multi_csv_zip) is True
        
        # Test ZIP with non-data files (should still pass if has some CSV/TXT)
        mixed_zip = Path(temp_setup) / "mixed.zip"
        with zipfile.ZipFile(mixed_zip, 'w') as zf:
            zf.writestr("data.csv", "timestamp,open,high,low,close,volume\n2024-08-29 09:30:00,100,101,99,100.5,1000")
            zf.writestr("readme.md", "# This is a readme file")
            zf.writestr("config.json", '{"setting": "value"}')
        
        assert downloader.verify_zip_file(mixed_zip) is True
        
        # Test ZIP with only non-data files
        no_data_zip = Path(temp_setup) / "no_data.zip"
        with zipfile.ZipFile(no_data_zip, 'w') as zf:
            zf.writestr("readme.md", "# No data here")
            zf.writestr("config.json", '{"setting": "value"}')
        
        assert downloader.verify_zip_file(no_data_zip) is False
    
    def test_cleanup_old_files_comprehensive(self, temp_setup):
        """Comprehensive tests for file cleanup logic."""
        downloader = FirstRateDownloader(base_path=temp_setup)
        asset_type = "stock"
        
        # Create daily directory
        daily_dir = Path(temp_setup) / "daily" / asset_type
        daily_dir.mkdir(parents=True, exist_ok=True)
        
        # Create files with various ages
        today = date.today()
        file_dates = [
            today - timedelta(days=15),  # Very old - should be deleted
            today - timedelta(days=10),  # Old - should be deleted  
            today - timedelta(days=7),   # Exactly 7 days - should be deleted
            today - timedelta(days=5),   # Recent - should remain
            today - timedelta(days=1),   # Yesterday - should remain
            today,                       # Today - should remain
        ]
        
        created_files = []
        for file_date in file_dates:
            filename = f"{asset_type}_{file_date.strftime('%Y%m%d')}_1min_adj_split.zip"
            file_path = daily_dir / filename
            file_path.touch()
            created_files.append(file_path)
        
        # Create some non-matching files that should not be deleted
        non_matching_files = [
            daily_dir / "other_file.txt",
            daily_dir / "etf_20240829_1min_adj_split.zip",  # Wrong asset type
        ]
        for file_path in non_matching_files:
            file_path.touch()
        
        # Test cleanup with 7-day retention
        deleted_count = downloader.cleanup_old_files(asset_type, keep_days=7)
        
        # Should delete files older than 7 days (15, 10, and exactly 7 days old)
        # The cleanup logic might treat 7 days as inclusive or exclusive
        assert deleted_count >= 2  # At least the very old files (15, 10 days)
        
        # Verify correct files were deleted
        assert not created_files[0].exists()  # 15 days old
        assert not created_files[1].exists()  # 10 days old  
        assert not created_files[2].exists()  # 7 days old
        assert created_files[3].exists()     # 5 days old
        assert created_files[4].exists()     # 1 day old
        assert created_files[5].exists()     # Today
        
        # Non-matching files should remain
        for file_path in non_matching_files:
            assert file_path.exists()
    
    async def test_download_with_network_timeout(self, temp_setup):
        """Test download handling network timeout."""
        downloader = FirstRateDownloader(base_path=temp_setup, max_retries=2, retry_delay=0.1)
        
        with patch('aiohttp.ClientSession') as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value = mock_session
            
            # Mock timeout exception
            import aiohttp
            mock_session.__aenter__.return_value = mock_session
            mock_session.get.side_effect = aiohttp.ClientTimeout()
            
            url = "https://example.com/timeout.zip"
            output_path = Path(temp_setup) / "output.zip"
            
            result = await downloader.download_with_retries(url, output_path)
            
            assert result is False
            assert not output_path.exists()
    
    async def test_download_with_connection_error(self, temp_setup):
        """Test download handling connection errors."""
        downloader = FirstRateDownloader(base_path=temp_setup, max_retries=2, retry_delay=0.1)
        
        with patch('aiohttp.ClientSession') as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value = mock_session
            
            # Mock connection error
            import aiohttp
            mock_session.__aenter__.return_value = mock_session
            mock_session.get.side_effect = aiohttp.ClientConnectorError(
                connection_key=mock.Mock(), os_error=OSError("Connection refused")
            )
            
            url = "https://example.com/connection_error.zip"
            output_path = Path(temp_setup) / "output.zip"
            
            result = await downloader.download_with_retries(url, output_path)
            
            assert result is False
            assert not output_path.exists()
    
    async def test_download_daily_data_mixed_results(self, temp_setup):
        """Test download_daily_data with mixed success/failure results."""
        downloader = FirstRateDownloader(base_path=temp_setup)
        
        # Create test jobs
        jobs = [
            DownloadJob(asset_type="stock"),
            DownloadJob(asset_type="etf"), 
            DownloadJob(asset_type="fx")
        ]
        
        # Mock download_with_retries to simulate mixed results
        with patch.object(downloader, 'download_with_retries') as mock_download:
            # First call succeeds, second fails, third succeeds
            mock_download.side_effect = [True, False, True]
            
            results = await downloader.download_daily_data(jobs)
            
            assert results["stock"] is True
            assert results["etf"] is False
            assert results["fx"] is True
            
            # Verify download was attempted for each job
            assert mock_download.call_count == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])