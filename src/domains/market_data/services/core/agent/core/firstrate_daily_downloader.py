#!/usr/bin/env python3
"""
FirstRate Daily Data Downloader

Downloads daily 1-minute bar data for stocks, ETFs, and FX from FirstRate API.
Stores data in daily format under /mnt/d/ats-data/firstrate-data/daily/{type}/

Features:
- Downloads data for stocks, ETFs, and FX
- Automatic retry with exponential backoff
- Progress tracking and logging
- Error handling and notification
- Checksum verification of downloaded files
"""

import logging
import asyncio
import aiohttp
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List
import tempfile
import shutil
import hashlib
import zipfile
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DownloadJob:
    """Configuration for a download job."""
    asset_type: str  # stock, etf, fx
    period: str = "day"
    timeframe: str = "1min"
    adjustment: str = "adj_split"
    output_dir: Optional[str] = None


class FirstRateDownloader:
    """Downloads daily data from FirstRate API."""

    def __init__(
        self,
        userid: str = "fg1LcNsv8kWWMJIt0caCFQ",
        base_path: str = "/mnt/d/ats-data/firstrate-data",
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.userid = userid
        self.base_path = Path(base_path)
        self.api_base = "https://firstratedata.com/api"
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Create directory structure
        for asset_type in ["stock", "etf", "fx"]:
            daily_dir = self.base_path / "daily" / asset_type
            daily_dir.mkdir(parents=True, exist_ok=True)

    def build_download_url(self, job: DownloadJob) -> str:
        """Build the API URL for downloading data."""
        params = {
            "type": job.asset_type,
            "period": job.period,
            "timeframe": job.timeframe,
            "adjustment": job.adjustment,
            "userid": self.userid
        }

        # Build query string
        query_params = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{self.api_base}/data_file?{query_params}"

        logger.debug(f"Built URL for {job.asset_type}: {url}")
        return url

    def get_output_path(self, job: DownloadJob, date_str: str) -> Path:
        """Get the output path for downloaded data."""
        if job.output_dir:
            output_dir = Path(job.output_dir)
        else:
            output_dir = self.base_path / "daily" / job.asset_type

        filename = f"{job.asset_type}_{date_str}_{job.timeframe}_{job.adjustment}.zip"
        return output_dir / filename

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def verify_zip_file(self, file_path: Path) -> bool:
        """Verify that the downloaded zip file is valid."""
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                # Test the zip file integrity
                bad_files = zf.testzip()
                if bad_files:
                    logger.error(f"Corrupted files in {file_path}: {bad_files}")
                    return False

                # Check if it contains CSV files
                csv_files = [f for f in zf.namelist() if f.endswith('.txt') or f.endswith('.csv')]
                if not csv_files:
                    logger.warning(f"No CSV/TXT files found in {file_path}")
                    return False

                logger.debug(f"Zip file {file_path} contains {len(csv_files)} data files")
                return True

        except zipfile.BadZipFile:
            logger.error(f"Invalid zip file: {file_path}")
            return False
        except Exception as e:
            logger.error(f"Error verifying zip file {file_path}: {e}")
            return False

    async def download_with_retries(self, url: str, output_path: Path) -> bool:
        """Download file with retry logic."""
        temp_file = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"Downloading {url} (attempt {attempt + 1}/{self.max_retries + 1})")

                # Create temporary file
                temp_dir = output_path.parent
                temp_dir.mkdir(parents=True, exist_ok=True)

                with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as tmp:
                    temp_file = Path(tmp.name)

                # Download with aiohttp for better async support
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            # Stream download to temporary file
                            with open(temp_file, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    f.write(chunk)

                            # Verify download
                            if temp_file.exists() and temp_file.stat().st_size > 0:
                                if self.verify_zip_file(temp_file):
                                    # Move to final location
                                    shutil.move(str(temp_file), str(output_path))

                                    checksum = self.calculate_checksum(output_path)
                                    file_size = output_path.stat().st_size

                                    logger.info(f"✅ Downloaded {output_path.name}: {file_size:,} bytes, checksum: {checksum[:8]}...")
                                    return True
                                else:
                                    logger.error(f"Downloaded file failed verification: {temp_file}")
                            else:
                                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file}")

                        elif response.status == 404:
                            logger.warning(f"Data not available (404): {url}")
                            return False  # Don't retry 404s

                        elif response.status == 429:
                            logger.warning(f"Rate limited (429), will retry: {url}")

                        else:
                            logger.error(f"HTTP {response.status}: {await response.text()}")

            except asyncio.TimeoutError:
                logger.error(f"Timeout downloading {url}")
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")

            # Clean up temp file on failure
            if temp_file and temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass

            # Exponential backoff before retry
            if attempt < self.max_retries:
                wait_time = self.retry_delay * (2 ** attempt)
                logger.info(f"Waiting {wait_time:.1f}s before retry...")
                await asyncio.sleep(wait_time)

        logger.error(f"❌ Failed to download after {self.max_retries + 1} attempts: {url}")
        return False

    async def download_daily_data(
        self,
        jobs: List[DownloadJob],
        date_override: Optional[date] = None
    ) -> Dict[str, bool]:
        """Download daily data for all specified asset types."""
        download_date = date_override or date.today()
        date_str = download_date.strftime('%Y%m%d')

        logger.info(f"🚀 Starting FirstRate daily download for {date_str}")
        logger.info(f"📊 Asset types: {[job.asset_type for job in jobs]}")

        results = {}

        for job in jobs:
            logger.info(f"📥 Downloading {job.asset_type} data...")

            url = self.build_download_url(job)
            output_path = self.get_output_path(job, date_str)

            # Skip if file already exists and is valid
            if output_path.exists():
                if self.verify_zip_file(output_path):
                    logger.info(f"✅ {job.asset_type} data already exists and is valid: {output_path.name}")
                    results[job.asset_type] = True
                    continue
                else:
                    logger.warning(f"⚠️ Existing file is invalid, re-downloading: {output_path.name}")
                    output_path.unlink()

            # Download the data
            success = await self.download_with_retries(url, output_path)
            results[job.asset_type] = success

            if success:
                logger.info(f"✅ {job.asset_type} download completed successfully")
            else:
                logger.error(f"❌ {job.asset_type} download failed")

        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)

        logger.info(f"🎉 Daily download completed: {successful}/{total} successful")

        return results

    def cleanup_old_files(self, asset_type: str, keep_days: int = 7) -> int:
        """Clean up old daily files, keeping only the last N days."""
        daily_dir = self.base_path / "daily" / asset_type
        if not daily_dir.exists():
            return 0

        cutoff_date = date.today() - timedelta(days=keep_days)
        deleted_count = 0

        for file_path in daily_dir.glob(f"{asset_type}_*.zip"):
            try:
                # Extract date from filename: {asset_type}_YYYYMMDD_...
                parts = file_path.stem.split('_')
                if len(parts) >= 2:
                    date_str = parts[1]
                    file_date = datetime.strptime(date_str, '%Y%m%d').date()

                    if file_date < cutoff_date:
                        file_path.unlink()
                        deleted_count += 1
                        logger.debug(f"Deleted old file: {file_path.name}")

            except (ValueError, IndexError):
                logger.warning(f"Cannot parse date from filename: {file_path.name}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

        if deleted_count > 0:
            logger.info(f"🧹 Cleaned up {deleted_count} old {asset_type} files (keeping {keep_days} days)")

        return deleted_count


async def main():
    """Main function for running daily downloads."""
    import argparse

    parser = argparse.ArgumentParser(description="FirstRate Daily Data Downloader")
    parser.add_argument("--asset-types", nargs="+", default=["stock", "etf", "fx"],
                       choices=["stock", "etf", "fx"], help="Asset types to download")
    parser.add_argument("--date", help="Download date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--cleanup-days", type=int, default=7,
                       help="Keep this many days of files, delete older ones")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Parse date if provided
    download_date = None
    if args.date:
        try:
            download_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date} (use YYYY-MM-DD)")
            return 1

    # Create download jobs
    jobs = [DownloadJob(asset_type=asset_type) for asset_type in args.asset_types]

    # Create downloader and run
    downloader = FirstRateDownloader()

    try:
        # Download daily data
        results = await downloader.download_daily_data(jobs, download_date)

        # Cleanup old files
        if args.cleanup_days > 0:
            for asset_type in args.asset_types:
                downloader.cleanup_old_files(asset_type, args.cleanup_days)

        # Return success/failure
        return 0 if all(results.values()) else 1

    except Exception as e:
        logger.error(f"💥 Daily download failed: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))