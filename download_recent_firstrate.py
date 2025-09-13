#!/usr/bin/env python3
"""
Download Recent FirstRate Data
Quick script to download the most recent FirstRate data for testing
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')

from infrastructure.vendor.firstrate.adapters.firstrate_daily_downloader import FirstRateDownloader, DownloadJob

async def download_recent_data():
    """Download recent FirstRate data"""
    print("🚀 Downloading Recent FirstRate Data")
    print(f"📅 Current date: {date.today()}")
    
    # Create downloader
    downloader = FirstRateDownloader()
    
    # Create download jobs for recent days
    jobs = [DownloadJob(asset_type="stock")]
    
    # Download last 5 days to ensure we get recent data
    for days_ago in range(5):
        download_date = date.today() - timedelta(days=days_ago)
        print(f"📥 Attempting download for {download_date}")
        
        try:
            results = await downloader.download_daily_data(jobs, download_date)
            if results.get("stock", False):
                print(f"✅ Downloaded data for {download_date}")
            else:
                print(f"❌ No data for {download_date}")
        except Exception as e:
            print(f"⚠️ Error downloading {download_date}: {e}")
    
    # Check what files we now have
    data_path = Path("/mnt/d/ats-data/firstrate-data/daily/stock")
    if data_path.exists():
        recent_files = sorted([f for f in data_path.glob("*.zip") if f.stat().st_mtime > datetime.now().timestamp() - 7*24*3600])
        print(f"📁 Recent files downloaded: {len(recent_files)}")
        for f in recent_files[-10:]:  # Show last 10
            mod_time = datetime.fromtimestamp(f.stat().st_mtime)
            print(f"   • {f.name} (modified: {mod_time})")

if __name__ == "__main__":
    asyncio.run(download_recent_data())