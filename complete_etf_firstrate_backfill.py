#!/usr/bin/env python3
"""
Complete ETF FirstRate backfill for available ETFs: GLD, IWM
Based on successful AAPL/TSLA processing method
"""
import sys
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

from market_data.agent.firstrate_adapter import FirstRateAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager, MinuteBar

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def process_etf_complete_backfill():
    """Process complete ETF backfill for GLD and IWM"""
    logger.info("🚀 Processing ETF complete backfill: GLD (2004-2025), IWM (2000-2025)...")
    
    # Initialize components
    adapter = FirstRateAdapter("/data/firstrate-data")
    output_path = Path("/data/minute-bars/firstrate")
    minute_manager = FileBasedMinuteManager(str(output_path))
    
    # ETF symbol configurations
    etf_configs = {
        'GLD': {
            'category': 'etf',
            'zip_file': 'etf_G_full_1min_adjsplitdiv_ranbdtj.zip',
            'start_year': 2004,  # GLD inception: 2004-11-18
            'start_month': 11,
            'start_day': 18
        },
        'IWM': {
            'category': 'etf', 
            'zip_file': 'etf_I_full_1min_adjsplitdiv_dneo8aj.zip',
            'start_year': 2000,  # IWM inception: 2000-05-26
            'start_month': 5,
            'start_day': 26
        }
    }
    
    # Find ETF zip files
    etf_zip_files = {}
    for symbol, config in etf_configs.items():
        zip_files = adapter.get_available_zip_files(config['category'])
        
        for zip_file in zip_files:
            if zip_file.name == config['zip_file']:
                etf_zip_files[symbol] = zip_file
                logger.info(f"📦 Found {symbol} in {zip_file.name}")
                break
        
        if symbol not in etf_zip_files:
            logger.error(f"❌ {symbol} zip file not found: {config['zip_file']}")
            return
    
    total_records_all = 0
    
    # Process each ETF
    for symbol in etf_configs:
        config = etf_configs[symbol]
        zip_file = etf_zip_files[symbol]
        
        logger.info(f"📅 Processing {symbol} from {config['start_year']}-{config['start_month']:02d}-{config['start_day']:02d}...")
        
        # Get date range for this ETF
        min_date, max_date = adapter.get_date_range_for_symbol(zip_file, symbol)
        logger.info(f"   Available data: {min_date} to {max_date}")
        
        total_records = 0
        
        # Process year by year
        for year in range(config['start_year'], 2026):  # Through 2025
            logger.info(f"📅 Processing {symbol} year {year}...")
            year_records = 0
            
            # Determine month range for the year
            if year == config['start_year']:
                # Start from inception month/day
                months = range(config['start_month'], 13)
            elif year == 2025:
                # Process up to current month (August 2025)
                months = range(1, 9)
            else:
                months = range(1, 13)  # All 12 months
            
            for month in months:
                month_str = f"{year}-{month:02d}"
                logger.info(f"🔄 Processing {symbol} {month_str}...")
                
                # Define date range for the month
                start_date = date(year, month, 1)
                
                # Special handling for inception month
                if year == config['start_year'] and month == config['start_month']:
                    start_date = date(config['start_year'], config['start_month'], config['start_day'])
                
                if month == 12:
                    end_date = date(year + 1, 1, 1)
                    from datetime import timedelta
                    end_date = end_date - timedelta(days=1)  # Last day of December
                else:
                    end_date = date(year, month + 1, 1)
                    from datetime import timedelta
                    end_date = end_date - timedelta(days=1)  # Last day of month
                
                # Get ticks for this month and collect into MinuteBar objects
                month_bars = []
                tick_generator = adapter.process_minute_data_from_zip(
                    zip_file, symbol, start_date, end_date
                )
                
                for tick in tick_generator:
                    # Convert tick to MinuteBar
                    bar = MinuteBar(
                        symbol=tick.symbol,
                        timestamp=tick.timestamp,
                        open=tick.open,
                        high=tick.high,
                        low=tick.low,
                        close=tick.close,
                        volume=tick.volume,
                        vendor="firstrate"
                    )
                    month_bars.append(bar)
                    
                    if len(month_bars) % 10000 == 0:
                        logger.info(f"   📈 Collected {len(month_bars):,} ticks for {symbol} {month_str}")
                
                # Store all bars for this month
                if month_bars:
                    result = await minute_manager.store_minute_data(symbol, month_bars)
                    month_records = len(month_bars)
                    year_records += month_records
                    logger.info(f"   ✅ {symbol} {month_str}: {month_records:,} records stored")
                else:
                    logger.info(f"   ⚠️ {symbol} {month_str}: No data found")
            
            total_records += year_records
            logger.info(f"🎯 {symbol} year {year} complete: {year_records:,} records")
        
        total_records_all += total_records
        logger.info(f"🏆 {symbol} complete: {total_records:,} total records")
    
    logger.info(f"🎉 ETF backfill complete!")
    logger.info(f"📊 Total records processed: {total_records_all:,}")
    
    # Final verification
    for symbol in etf_configs:
        etf_path = output_path / symbol
        if etf_path.exists():
            years = sorted([d.name for d in etf_path.iterdir() if d.is_dir() and d.name.isdigit()])
            config = etf_configs[symbol]
            expected_years = 2025 - config['start_year'] + 1
            logger.info(f"📁 {symbol}: {years}")
            logger.info(f"🏆 {symbol}: {len(years)} years (Expected: {expected_years} from {config['start_year']}-2025)")
        else:
            logger.error(f"❌ {symbol} directory not found: {etf_path}")

if __name__ == "__main__":
    asyncio.run(process_etf_complete_backfill())