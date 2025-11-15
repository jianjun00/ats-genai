#!/usr/bin/env python3
"""
FirstRate Monthly Data Processor

Processes downloaded monthly ZIP files into standard minute bar parquet files.
Converts the 30-day CSV files into monthly parquet files organized by symbol.
"""

import zipfile
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import tempfile
import shutil
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FirstRateMonthlyProcessor:
    def __init__(self):
        self.monthly_data_path = Path("/mnt/d/ats-data/firstrate-data/monthly")
        self.output_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
        
    def process_monthly_file(self, zip_path: Path, asset_type: str) -> int:
        """Process a monthly ZIP file into minute bar parquet files."""
        
        logger.info(f"🚀 Processing {asset_type} monthly file: {zip_path.name}")
        
        symbols_processed = 0
        records_processed = 0
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            logger.info(f"📊 Found {len(file_list)} symbol files in archive")
            
            for file_name in file_list:
                if not file_name.endswith('_month_1min_adjsplit.txt'):
                    continue
                    
                # Extract symbol name
                symbol = file_name.replace('_month_1min_adjsplit.txt', '')
                
                try:
                    # Read CSV data from zip
                    with zip_ref.open(file_name) as csv_file:
                        # Read CSV data
                        df = pd.read_csv(
                            csv_file, 
                            names=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
                            parse_dates=['timestamp']
                        )
                        
                        if df.empty:
                            logger.debug(f"   ⚠️ {symbol}: No data")
                            continue
                            
                        # Add vendor and additional columns
                        df['symbol'] = symbol
                        df['vendor'] = 'firstrate'
                        df['vwap'] = df['close']  # Use close as proxy for VWAP
                        df['trade_count'] = 0     # Not available from FirstRate
                        df['quality_score'] = 1.0
                        
                        # Convert timezone to UTC (FirstRate data is in EDT/EST)
                        # Use 'America/New_York' instead of 'US/Eastern' for better compatibility
                        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('America/New_York').dt.tz_convert('UTC')
                        
                        # Group by month and save
                        df['year_month'] = df['timestamp'].dt.to_period('M')
                        
                        for period, month_data in df.groupby('year_month'):
                            year = period.year
                            month = period.month
                            
                            # Create output directory structure
                            symbol_dir = self.output_path / symbol[0] / symbol / str(year) / f"{month:02d}"
                            symbol_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Output filename
                            output_file = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
                            
                            # Prepare final DataFrame
                            month_data = month_data.drop('year_month', axis=1)
                            month_data = month_data.sort_values('timestamp').reset_index(drop=True)
                            
                            # Save as parquet
                            month_data.to_parquet(output_file, index=False)
                            
                            logger.debug(f"   ✅ {symbol} {year}-{month:02d}: {len(month_data)} records")
                            records_processed += len(month_data)
                        
                        symbols_processed += 1
                        
                        if symbols_processed % 100 == 0:
                            logger.info(f"   📈 Progress: {symbols_processed} symbols processed")
                            
                except Exception as e:
                    logger.error(f"   ❌ Error processing {symbol}: {e}")
                    continue
        
        logger.info(f"✅ {asset_type} processing completed: {symbols_processed} symbols, {records_processed:,} records")
        return symbols_processed
    
    def process_all_monthly_files(self):
        """Process all available monthly files."""
        
        logger.info("🚀 Starting FirstRate monthly data processing...")
        
        total_symbols = 0
        
        for asset_type in ['stock', 'etf']:
            asset_dir = self.monthly_data_path / asset_type
            
            if not asset_dir.exists():
                logger.warning(f"⚠️ No {asset_type} directory found: {asset_dir}")
                continue
                
            # Find monthly ZIP files
            zip_files = list(asset_dir.glob("*_month_1min_adj_split.zip"))
            
            if not zip_files:
                logger.warning(f"⚠️ No monthly ZIP files found for {asset_type}")
                continue
                
            logger.info(f"📁 Found {len(zip_files)} {asset_type} monthly files")
            
            for zip_path in zip_files:
                symbols_count = self.process_monthly_file(zip_path, asset_type)
                total_symbols += symbols_count
        
        logger.info(f"🎉 Processing completed: {total_symbols} total symbols processed")
        
        # Show coverage summary
        self.show_coverage_summary()
    
    def show_coverage_summary(self):
        """Show summary of processed data coverage."""
        
        logger.info("📊 Coverage Summary:")
        
        # Count symbols and months
        symbol_count = defaultdict(int)
        month_count = defaultdict(set)
        
        for symbol_dir in self.output_path.rglob("*/"):
            if symbol_dir.name.isdigit() and len(symbol_dir.name) == 4:  # Year directory
                symbol = symbol_dir.parent.name
                year = symbol_dir.name
                
                for month_dir in symbol_dir.iterdir():
                    if month_dir.is_dir() and month_dir.name.isdigit():
                        month = month_dir.name
                        
                        parquet_files = list(month_dir.glob("*.parquet"))
                        if parquet_files:
                            symbol_count[symbol] += 1
                            month_count[f"{year}-{month}"].add(symbol)
        
        logger.info(f"   📈 Total symbols with data: {len(symbol_count)}")
        logger.info(f"   📅 Total months covered: {len(month_count)}")
        
        # Show most recent months
        recent_months = sorted(month_count.keys(), reverse=True)[:3]
        for month in recent_months:
            symbol_count_month = len(month_count[month])
            logger.info(f"   📊 {month}: {symbol_count_month} symbols")

def main():
    """Main processing function."""
    processor = FirstRateMonthlyProcessor()
    processor.process_all_monthly_files()

if __name__ == "__main__":
    main()