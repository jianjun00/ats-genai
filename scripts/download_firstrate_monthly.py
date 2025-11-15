#!/usr/bin/env python3
"""
FirstRate Monthly Data Downloader

Downloads past 30 days of minute bar data for stocks and ETFs from FirstRate API.
This replaces the problematic daily download approach with proper monthly downloads.
"""

import asyncio
import aiohttp
import logging
from pathlib import Path
from datetime import datetime
import tempfile
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FirstRateMonthlyDownloader:
    def __init__(self):
        self.userid = "fg1LcNsv8kWWMJIt0caCFQ"
        self.base_url = "https://firstratedata.com/api/data_file"
        self.base_path = Path("/mnt/d/ats-data/firstrate-data/monthly")
        
    async def download_monthly_data(self, asset_type: str) -> bool:
        """Download 30-day data for the specified asset type."""
        
        # Build URL for monthly data
        params = {
            "type": asset_type,
            "period": "month", 
            "timeframe": "1min",
            "adjustment": "adj_split",
            "userid": self.userid
        }
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{self.base_url}?{query_string}"
        
        # Create output directory
        output_dir = self.base_path / asset_type
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Output filename with current month
        current_month = datetime.now().strftime('%Y%m')
        filename = f"{asset_type}_{current_month}_month_1min_adj_split.zip"
        output_path = output_dir / filename
        
        logger.info(f"🚀 Starting {asset_type} monthly download...")
        logger.info(f"📥 URL: {url}")
        logger.info(f"💾 Output: {output_path}")
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3600)) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        file_size = response.headers.get('content-length')
                        if file_size:
                            logger.info(f"📊 File size: {int(file_size) / 1024 / 1024:.1f} MB")
                        
                        # Download to temporary file first
                        with tempfile.NamedTemporaryFile(delete=False, dir=output_dir) as temp_file:
                            temp_path = Path(temp_file.name)
                            
                            downloaded = 0
                            async for chunk in response.content.iter_chunked(8192):
                                temp_file.write(chunk)
                                downloaded += len(chunk)
                                
                                # Show progress every 10MB
                                if downloaded % (10 * 1024 * 1024) == 0:
                                    mb_downloaded = downloaded / 1024 / 1024
                                    logger.info(f"📈 Downloaded: {mb_downloaded:.1f} MB")
                        
                        # Move to final location
                        shutil.move(str(temp_path), str(output_path))
                        
                        final_size = output_path.stat().st_size
                        logger.info(f"✅ {asset_type} download completed: {final_size / 1024 / 1024:.1f} MB")
                        return True
                        
                    else:
                        logger.error(f"❌ HTTP {response.status}: {await response.text()}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Download failed for {asset_type}: {e}")
            return False

async def main():
    """Download monthly data for stocks and ETFs."""
    downloader = FirstRateMonthlyDownloader()
    
    logger.info("🚀 Starting FirstRate monthly data download...")
    
    # Download both asset types
    tasks = [
        downloader.download_monthly_data("stock"),
        downloader.download_monthly_data("etf")
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check results
    success_count = sum(1 for result in results if result is True)
    total_count = len(results)
    
    logger.info(f"🎉 Download completed: {success_count}/{total_count} successful")
    
    # List downloaded files
    monthly_dir = Path("/mnt/d/ats-data/firstrate-data/monthly")
    if monthly_dir.exists():
        for asset_dir in monthly_dir.iterdir():
            if asset_dir.is_dir():
                files = list(asset_dir.glob("*.zip"))
                logger.info(f"📁 {asset_dir.name}: {len(files)} files")
                for file_path in files:
                    file_size = file_path.stat().st_size / 1024 / 1024
                    logger.info(f"   📄 {file_path.name}: {file_size:.1f} MB")

if __name__ == "__main__":
    asyncio.run(main())