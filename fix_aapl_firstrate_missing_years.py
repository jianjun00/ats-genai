#!/usr/bin/env python3
"""
Fix missing AAPL FirstRate data for 2000-2004 and 2024-2025
Reprocess the failed months to get complete 26-year dataset
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from market_data.agent.firstrate_adapter import FirstRateAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AAPLFirstRateReprocessor:
    """Reprocess failed AAPL FirstRate months"""
    
    def __init__(self):
        self.adapter = FirstRateAdapter("/data/firstrate-data")
        self.output_path = Path("/data/minute-bars/firstrate")
        self.minute_manager = FileBasedMinuteManager(str(self.output_path))
        self.checkpoint_file = "firstrate_monthly_production.json"
        
    def load_checkpoint(self):
        """Load the checkpoint file to identify failed months"""
        try:
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return {"failed_months": {}, "completed_months": {}}
    
    def save_checkpoint(self, checkpoint_data):
        """Save updated checkpoint file"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.info("✅ Checkpoint saved")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def get_aapl_missing_months(self):
        """Get list of AAPL months that are actually missing from filesystem"""
        # Check what years we actually have on filesystem
        aapl_path = Path("/data/minute-bars/firstrate/AAPL")
        existing_years = set()
        
        if aapl_path.exists():
            for year_dir in aapl_path.iterdir():
                if year_dir.is_dir() and year_dir.name.isdigit():
                    existing_years.add(int(year_dir.name))
        
        logger.info(f"📁 Found existing AAPL data for years: {sorted(existing_years)}")
        
        # Define the complete expected range (2000-2025)
        all_years = set(range(2000, 2026))
        missing_years = all_years - existing_years
        
        logger.info(f"❌ Missing AAPL data for years: {sorted(missing_years)}")
        
        # Generate months for missing years only
        missing_months = []
        for year in missing_years:
            if year == 2025:
                # Only process months up to current month for 2025
                months = range(1, 9)  # Jan-Aug 2025
            else:
                months = range(1, 13)  # All 12 months
                
            for month in months:
                missing_months.append(f"{year}-{month:02d}")
        
        logger.info(f"📋 Need to process {len(missing_months)} missing AAPL months")
        
        # Group by year for reporting
        by_year = {}
        for month_str in missing_months:
            year = month_str.split('-')[0]
            if year not in by_year:
                by_year[year] = []
            by_year[year].append(month_str)
        
        for year, months in sorted(by_year.items()):
            logger.info(f"   📅 {year}: {len(months)} months ({', '.join(months)})")
            
        return missing_months
    
    async def reprocess_month(self, year_month: str):
        """Reprocess a specific month of AAPL data"""
        try:
            year, month = map(int, year_month.split('-'))
            logger.info(f"🔄 Reprocessing AAPL {year}-{month:02d}...")
            
            # Get symbol inventory for AAPL
            inventory = self.adapter.get_symbol_inventory('stock')
            if 'AAPL' not in inventory:
                logger.error("❌ AAPL not found in symbol inventory")
                return False
                
            aapl_info = inventory['AAPL']
            zip_file = Path(aapl_info['zip_files'][0])
            
            # Process minute data for this specific month
            tick_count = 0
            month_start = date(year, month, 1)
            
            # Get next month for filtering
            if month == 12:
                next_month_start = date(year + 1, 1, 1)
            else:
                next_month_start = date(year, month + 1, 1)
            
            logger.info(f"   📦 Processing from {zip_file.name}")
            logger.info(f"   📅 Date range: {month_start} to {next_month_start}")
            
            for tick in self.adapter.process_minute_data_from_zip(zip_file, 'AAPL'):
                # Filter for specific month
                tick_date = tick.timestamp.date()
                if not (month_start <= tick_date < next_month_start):
                    continue
                    
                # Store the tick
                await self.minute_manager.store_minute_bar(
                    symbol=tick.symbol,
                    timestamp=tick.timestamp,
                    open_price=tick.open,
                    high_price=tick.high,
                    low_price=tick.low,
                    close_price=tick.close,
                    volume=tick.volume,
                    vendor="firstrate"
                )
                
                tick_count += 1
                if tick_count % 5000 == 0:
                    logger.info(f"   📈 Processed {tick_count:,} ticks for {year_month}")
            
            logger.info(f"✅ {year_month}: Successfully processed {tick_count:,} ticks")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error reprocessing {year_month}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def reprocess_all_missing_months(self):
        """Reprocess all missing AAPL months"""
        missing_months = self.get_aapl_missing_months()
        
        if not missing_months:
            logger.info("✅ No missing months to reprocess")
            return
        
        logger.info(f"🚀 Starting reprocessing of {len(missing_months)} missing months")
        start_time = datetime.now()
        
        # Load checkpoint
        checkpoint = self.load_checkpoint()
        
        processed_count = 0
        failed_count = 0
        
        for month_str in missing_months:  # Process all missing months
            success = await self.reprocess_month(month_str)
            
            if success:
                # Move from failed to completed
                if "completed_months" not in checkpoint:
                    checkpoint["completed_months"] = {}
                if "AAPL" not in checkpoint["completed_months"]:
                    checkpoint["completed_months"]["AAPL"] = []
                    
                checkpoint["completed_months"]["AAPL"].append(month_str)
                checkpoint["failed_months"]["AAPL"].remove(month_str)
                processed_count += 1
                
                # Save checkpoint after each successful month
                self.save_checkpoint(checkpoint)
            else:
                failed_count += 1
        
        elapsed = datetime.now() - start_time
        logger.info(f"🎉 Reprocessing completed!")
        logger.info(f"   ✅ Successfully processed: {processed_count} months")
        logger.info(f"   ❌ Still failed: {failed_count} months")  
        logger.info(f"   ⏱️  Total time: {elapsed}")

async def main():
    """Main reprocessing function"""
    logger.info("🚀 Starting AAPL FirstRate missing years reprocessing...")
    
    reprocessor = AAPLFirstRateReprocessor()
    await reprocessor.reprocess_all_missing_months()
    
    logger.info("🔍 Final verification...")
    
    # Check final coverage
    aapl_path = Path("/data/minute-bars/firstrate/AAPL")
    if aapl_path.exists():
        years = sorted([d for d in aapl_path.iterdir() if d.is_dir() and d.name.isdigit()])
        logger.info(f"📊 Final AAPL FirstRate coverage:")
        logger.info(f"   📅 Years: {years[0].name} - {years[-1].name}")
        logger.info(f"   📁 Total years: {len(years)}")
    else:
        logger.error(f"❌ AAPL directory not found: {aapl_path}")

if __name__ == "__main__":
    asyncio.run(main())