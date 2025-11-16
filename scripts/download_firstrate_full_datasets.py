#!/usr/bin/env python3
"""
FirstRate Full Dataset Downloader

Downloads complete historical datasets for 1-minute dividend/split adjusted stock files
from FirstRate Data API and organizes them under /mnt/d/ats-data/firstrate-data/full/

Usage:
    python3 scripts/download_firstrate_full_datasets.py
"""

import os
import sys
import logging
import asyncio
import aiohttp
from datetime import datetime
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
    """Configuration for a full dataset download job."""
    dataset_name: str
    url: str
    description: str
    output_filename: str

class FirstRateFullDownloader:
    """Downloads full historical datasets from FirstRate API."""

    def __init__(
        self,
        userid: str = "fg1LcNsv8kWWMJIt0caCFQ",
        base_path: str = "/mnt/d/ats-data/firstrate-data/full",
        max_retries: int = 3,
        retry_delay: float = 2.0
    ):
        self.userid = userid
        self.base_path = Path(base_path)
        self.api_base = "https://firstratedata.com/c/main"
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Create directory structure
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Created download directory: {self.base_path}")

    def get_full_datasets(self, asset_types: List[str] = ['stock'], timeframes: List[str] = ['1min'], adjustments: List[str] = ['adj_split']) -> List[DownloadJob]:
        """Generate download URLs for complete datasets A-Z using proper FirstRate API."""
        logger.info(f"🔍 Generating full dataset URLs for asset types: {asset_types}, timeframes: {timeframes}, adjustments: {adjustments}")
        
        jobs = []
        alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        
        for asset_type in asset_types:
            for letter in alphabet:
                for timeframe in timeframes:
                    for adjustment in adjustments:
                        url = (f"https://firstratedata.com/api/data_file?"
                              f"type={asset_type}&period=full&ticker_range={letter}&"
                              f"timeframe={timeframe}&adjustment={adjustment}&userid={self.userid}")
                        
                        dataset_name = f"{asset_type}_{letter}_{timeframe}_{adjustment}"
                        description = f"Complete {timeframe} {asset_type} data (letter {letter}, {adjustment})"
                        filename = f"{asset_type}_{letter}_{timeframe}_{adjustment}.zip"
                        
                        jobs.append(DownloadJob(
                            dataset_name=dataset_name,
                            url=url,
                            description=description,
                            output_filename=filename
                        ))
        
        logger.info(f"📋 Generated {len(jobs)} dataset download URLs")
        return jobs

    def _generate_dataset_name(self, url: str, index: int) -> str:
        """Generate descriptive dataset name from URL."""
        url_lower = url.lower()
        
        if 'stock' in url_lower and '1min' in url_lower:
            return "stocks_1min_complete"
        elif 'stock' in url_lower and 'dividend' in url_lower:
            return "stocks_dividend_adjusted"
        elif 'stock' in url_lower and 'split' in url_lower:
            return "stocks_split_adjusted"
        elif 'stock' in url_lower:
            return "stocks_complete"
        elif '1min' in url_lower or 'minute' in url_lower:
            return "intraday_1min"
        elif 'continuous_contract' in url_lower:
            return "futures_continuous_contracts"
        elif 'company_profiles' in url_lower:
            return "company_profiles"
        else:
            return f"firstrate_dataset_{index}"
    
    def _generate_description(self, url: str) -> str:
        """Generate description from URL."""
        url_lower = url.lower()
        
        if 'stock' in url_lower and '1min' in url_lower:
            return "Complete 1-minute stock data (dividend/split adjusted)"
        elif 'stock' in url_lower and 'dividend' in url_lower:
            return "Stock data with dividend adjustments"
        elif 'stock' in url_lower and 'split' in url_lower:
            return "Stock data with split adjustments"
        elif 'stock' in url_lower:
            return "Complete stock historical data"
        elif 'continuous_contract' in url_lower:
            return "Futures continuous contract dates"
        elif 'company_profiles' in url_lower:
            return "Company profile data"
        else:
            return f"FirstRate dataset from portal"

    def get_full_dataset_jobs(self) -> List[DownloadJob]:
        """Get list of full dataset download jobs (legacy - use async version)."""
        # This is kept for backwards compatibility
        return []

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
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

                # Check if it contains data files
                data_files = [f for f in zf.namelist() if f.endswith(('.txt', '.csv', '.dat'))]
                if not data_files:
                    logger.warning(f"No data files found in {file_path}")
                    return False

                logger.info(f"✅ Zip file {file_path.name} contains {len(data_files)} data files")
                return True

        except zipfile.BadZipFile:
            logger.error(f"Invalid zip file: {file_path}")
            return False
        except Exception as e:
            logger.error(f"Error verifying zip file {file_path}: {e}")
            return False

    async def download_with_retries(self, url: str, output_path: Path, description: str) -> bool:
        """Download file with retry logic and progress tracking."""
        temp_file = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"🚀 Downloading {description} (attempt {attempt + 1}/{self.max_retries + 1})")
                logger.info(f"📡 URL: {url}")

                # Create temporary file
                temp_dir = output_path.parent
                temp_dir.mkdir(parents=True, exist_ok=True)

                with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as tmp:
                    temp_file = Path(tmp.name)

                # Download with aiohttp for better async support
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=3600)  # 1 hour timeout for large files
                ) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            total_size = int(response.headers.get('content-length', 0))
                            downloaded = 0

                            logger.info(f"📦 File size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")

                            # Stream download to temporary file with progress
                            with open(temp_file, 'wb') as f:
                                async for chunk in response.content.iter_chunked(32768):  # 32KB chunks
                                    f.write(chunk)
                                    downloaded += len(chunk)
                                    
                                    if total_size > 0:
                                        progress = (downloaded / total_size) * 100
                                        if downloaded % (10 * 1024 * 1024) == 0:  # Log every 10MB
                                            logger.info(f"📥 Progress: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)")

                            # Verify download
                            if temp_file.exists() and temp_file.stat().st_size > 0:
                                if self.verify_zip_file(temp_file):
                                    # Move to final location
                                    shutil.move(str(temp_file), str(output_path))

                                    checksum = self.calculate_checksum(output_path)
                                    file_size = output_path.stat().st_size

                                    logger.info(f"✅ Downloaded {output_path.name}: {file_size:,} bytes ({file_size / 1024 / 1024:.1f} MB)")
                                    logger.info(f"🔐 MD5 checksum: {checksum}")
                                    return True
                                else:
                                    logger.error(f"Downloaded file failed verification: {temp_file}")
                            else:
                                logger.error(f"Downloaded file is empty or doesn't exist: {temp_file}")

                        elif response.status == 404:
                            logger.error(f"❌ Dataset not found (404): {url}")
                            return False  # Don't retry 404s

                        elif response.status == 429:
                            logger.warning(f"⏱️ Rate limited (429), will retry: {url}")

                        else:
                            logger.error(f"❌ HTTP {response.status}: {await response.text()}")

            except asyncio.TimeoutError:
                logger.error(f"⏱️ Timeout downloading {url}")
            except Exception as e:
                logger.error(f"❌ Error downloading {url}: {e}")

            # Clean up temp file on failure
            if temp_file and temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass

            # Exponential backoff before retry
            if attempt < self.max_retries:
                wait_time = self.retry_delay * (2 ** attempt)
                logger.info(f"⏸️ Waiting {wait_time:.1f}s before retry...")
                await asyncio.sleep(wait_time)

        logger.error(f"💥 Failed to download after {self.max_retries + 1} attempts: {url}")
        return False

    async def download_full_datasets(self, asset_types: List[str] = None, timeframes: List[str] = None, adjustments: List[str] = None) -> Dict[str, bool]:
        """Download all full datasets A-Z for specified asset types."""
        # Default to stocks and ETFs with 1min split-adjusted data
        if asset_types is None:
            asset_types = ['stock', 'etf']
        if timeframes is None:
            timeframes = ['1min']
        if adjustments is None:
            adjustments = ['adj_split']
            
        logger.info(f"🚀 Starting FirstRate full dataset downloads")
        logger.info(f"📊 Asset Types: {asset_types}, Timeframes: {timeframes}, Adjustments: {adjustments}")
        
        jobs = self.get_full_datasets(asset_types, timeframes, adjustments)
        results = {}

        for job in jobs:
            logger.info(f"📦 Processing: {job.description}")
            
            output_path = self.base_path / job.output_filename
            
            # Check if file already exists and is valid
            if output_path.exists():
                if self.verify_zip_file(output_path):
                    file_size = output_path.stat().st_size
                    logger.info(f"✅ {job.dataset_name} already exists and is valid: {output_path.name} ({file_size:,} bytes)")
                    results[job.dataset_name] = True
                    continue
                else:
                    logger.warning(f"⚠️ Existing file is invalid, re-downloading: {output_path.name}")
                    output_path.unlink()

            # Download the dataset
            success = await self.download_with_retries(job.url, output_path, job.description)
            results[job.dataset_name] = success

            if success:
                logger.info(f"✅ {job.dataset_name} download completed successfully")
            else:
                logger.error(f"❌ {job.dataset_name} download failed")

        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)

        logger.info(f"🎉 Full dataset downloads completed: {successful}/{total} successful")

        return results

async def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(description="Download FirstRate full stock datasets")
    parser.add_argument("--userid", type=str, default="fg1LcNsv8kWWMJIt0caCFQ",
                       help="FirstRate user ID")
    parser.add_argument("--output-dir", type=str, default="/mnt/d/ats-data/firstrate-data/full",
                       help="Output directory for downloaded files")
    parser.add_argument("--asset-types", nargs="+", default=["stock", "etf"],
                       choices=["stock", "etf"],
                       help="Asset types to download (default: stock etf)")
    parser.add_argument("--timeframes", nargs="+", default=["1min"],
                       choices=["1min", "5min", "30min", "1hour", "1day"],
                       help="Timeframes to download (default: 1min)")
    parser.add_argument("--adjustments", nargs="+", default=["adj_split"],
                       choices=["adj_split", "adj_splitdiv", "UNADJUSTED"],
                       help="Adjustment types to download (default: adj_split)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create downloader and run
    downloader = FirstRateFullDownloader(
        userid=args.userid,
        base_path=args.output_dir
    )

    try:
        results = await downloader.download_full_datasets(
            asset_types=args.asset_types,
            timeframes=args.timeframes,
            adjustments=args.adjustments
        )
        
        # Return success/failure
        return 0 if all(results.values()) else 1

    except Exception as e:
        logger.error(f"💥 Download failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))