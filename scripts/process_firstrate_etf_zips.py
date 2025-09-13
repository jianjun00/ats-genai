#!/usr/bin/env python3
"""
FirstRate ETF Daily Zip Processor

Processes downloaded FirstRate daily ETF zip files into monthly parquet files.
Converts the raw daily zip downloads into the parquet format expected by the unified adapter.

This script bridges the gap between:
1. Downloaded daily zip files (from FirstRate API)
2. Monthly parquet files (expected by unified adapter)
"""

import os
import sys
import asyncio
import json
import argparse
import logging
import zipfile
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional
from pathlib import Path
import time
from collections import defaultdict
import io

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

logger = logging.getLogger(__name__)

# Critical ETFs to prioritize
CRITICAL_ETFS = [
    'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'XLK', 'XLF', 'XLE', 
    'XLV', 'XLI', 'XLP', 'XLY', 'XLU', 'XLB', 'XLRE'
]

class FirstRateETFZipProcessor:
    """Processes FirstRate daily ETF zip files into monthly parquet files"""

    def __init__(
        self,
        zip_input_path: str = "/mnt/d/ats-data/firstrate-data/daily/etf",
        parquet_output_path: str = "/mnt/d/ats-data/minute-bars/firstrate",
        checkpoint_file: str = "firstrate_etf_zip_processor.json"
    ):
        self.zip_input_path = Path(zip_input_path)
        self.parquet_output_path = Path(parquet_output_path)
        self.checkpoint_file = Path(checkpoint_file)

        # Create output directory
        self.parquet_output_path.mkdir(parents=True, exist_ok=True)

        # Load checkpoint
        self.checkpoint_data = self.load_checkpoint()

    def load_checkpoint(self) -> Dict:
        """Load processing checkpoint"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"📝 Loaded checkpoint: {data.get('zip_files_processed', 0)} zip files processed")
                    return data
            except Exception as e:
                logger.error(f"❌ Failed to load checkpoint: {e}")

        return {
            'zip_files_processed': {},
            'etf_data_extracted': {},
            'parquet_files_created': {},
            'last_run': None,
            'stats': {
                'zip_files_scanned': 0,
                'etf_records_extracted': 0,
                'parquet_files_written': 0,
                'etfs_updated': 0
            }
        }

    def save_checkpoint(self):
        """Save current processing state"""
        self.checkpoint_data['last_run'] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")

    def get_available_zip_files(self) -> List[Path]:
        """Get list of available ETF zip files to process"""
        if not self.zip_input_path.exists():
            logger.warning(f"⚠️ ETF zip input path does not exist: {self.zip_input_path}")
            return []

        zip_files = list(self.zip_input_path.glob("etf_*_1min_adj_split.zip"))
        zip_files.sort()  # Process chronologically
        
        logger.info(f"📁 Found {len(zip_files)} ETF zip files to process")
        return zip_files

    def extract_etf_data_from_zip(self, zip_path: Path, target_etfs: Set[str]) -> Dict[str, List[Dict]]:
        """Extract ETF data from a single zip file"""
        etf_data = defaultdict(list)
        zip_date = self.extract_date_from_filename(zip_path.name)
        
        if not zip_date:
            logger.warning(f"⚠️ Could not extract date from {zip_path.name}")
            return {}

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Get list of ETF files in the zip
                all_files = zf.namelist()
                etf_files = [f for f in all_files 
                            if any(f.startswith(f'{etf}_') for etf in target_etfs)]
                
                logger.debug(f"📊 Processing {len(etf_files)} ETF files from {zip_path.name}")
                
                for etf_file in etf_files:
                    # Extract ETF symbol from filename
                    etf_symbol = etf_file.split('_')[0]
                    
                    if etf_symbol in target_etfs:
                        try:
                            # Read the ETF data file
                            with zf.open(etf_file) as f:
                                content = f.read().decode('utf-8')
                                
                                # Parse CSV data
                                lines = content.strip().split('\n')
                                for line in lines:
                                    if line and not line.startswith('#'):
                                        parts = line.split(',')
                                        if len(parts) >= 6:  # timestamp, open, high, low, close, volume
                                            try:
                                                record = {
                                                    'symbol': etf_symbol,
                                                    'timestamp': pd.to_datetime(parts[0]),
                                                    'open': float(parts[1]),
                                                    'high': float(parts[2]),
                                                    'low': float(parts[3]),
                                                    'close': float(parts[4]),
                                                    'volume': int(float(parts[5]))
                                                }
                                                etf_data[etf_symbol].append(record)
                                            except (ValueError, IndexError) as e:
                                                logger.debug(f"⚠️ Skipping invalid line in {etf_file}: {line[:50]}...")
                                                continue
                                
                        except Exception as e:
                            logger.warning(f"⚠️ Error processing {etf_file}: {e}")
                            continue

        except Exception as e:
            logger.error(f"❌ Error reading zip file {zip_path}: {e}")
            return {}

        # Log extraction results
        total_records = sum(len(records) for records in etf_data.values())
        if total_records > 0:
            logger.info(f"✅ Extracted {total_records:,} records for {len(etf_data)} ETFs from {zip_path.name}")
        else:
            logger.debug(f"📭 No ETF data found in {zip_path.name}")

        return dict(etf_data)

    def extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract date from FirstRate zip filename"""
        try:
            # Format: etf_20250909_1min_adj_split.zip
            parts = filename.split('_')
            if len(parts) >= 2:
                date_str = parts[1]  # 20250909
                if len(date_str) == 8:
                    year = int(date_str[:4])
                    month = int(date_str[4:6])
                    day = int(date_str[6:8])
                    return date(year, month, day)
        except Exception as e:
            logger.debug(f"Error parsing date from {filename}: {e}")
        return None

    def group_data_by_month(self, all_etf_data: Dict[str, List[Dict]]) -> Dict[str, Dict[str, List[Dict]]]:
        """Group ETF data by symbol and month"""
        monthly_data = defaultdict(lambda: defaultdict(list))
        
        for symbol, records in all_etf_data.items():
            for record in records:
                timestamp = record['timestamp']
                month_key = f"{timestamp.year}-{timestamp.month:02d}"
                monthly_data[symbol][month_key].append(record)
        
        return dict(monthly_data)

    def write_etf_parquet_file(self, symbol: str, year: int, month: int, records: List[Dict]) -> bool:
        """Write ETF records to monthly parquet file"""
        try:
            # Create output directory structure: /parquet_output_path/SYMBOL_FIRST_LETTER/SYMBOL/YYYY/MM/
            symbol_dir = self.parquet_output_path / symbol[0] / symbol / str(year) / f"{month:02d}"
            symbol_dir.mkdir(parents=True, exist_ok=True)
            
            # Create parquet file path
            parquet_file = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
            
            # Convert records to DataFrame
            df = pd.DataFrame(records)
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Check if file already exists and merge if needed
            if parquet_file.exists():
                try:
                    existing_df = pd.read_parquet(parquet_file)
                    
                    # Combine and deduplicate
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['symbol', 'timestamp'])
                    combined_df = combined_df.sort_values('timestamp')
                    
                    logger.info(f"🔄 Merging {len(df)} new records with {len(existing_df)} existing records for {symbol} {year}-{month:02d}")
                    df = combined_df
                    
                except Exception as e:
                    logger.warning(f"⚠️ Could not merge with existing file {parquet_file}: {e}")
            
            # Write parquet file
            df.to_parquet(parquet_file, index=False)
            
            # Create metadata file
            metadata_file = symbol_dir / f".{symbol}_{year}_{month:02d}.metadata.json"
            metadata = {
                'symbol': symbol,
                'year': year,
                'month': month,
                'records': len(df),
                'date_range': [df['timestamp'].min().isoformat(), df['timestamp'].max().isoformat()],
                'created': datetime.now().isoformat(),
                'source': 'firstrate_daily_zip_processor'
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"✅ Created {parquet_file} with {len(df):,} records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to write parquet file for {symbol} {year}-{month:02d}: {e}")
            return False

    def process_all_zip_files(self, target_etfs: Optional[Set[str]] = None) -> Dict:
        """Process all available zip files for target ETFs"""
        if target_etfs is None:
            target_etfs = set(CRITICAL_ETFS)
        
        logger.info(f"🚀 Starting ETF zip file processing for {len(target_etfs)} ETFs")
        logger.info(f"🎯 Target ETFs: {', '.join(sorted(target_etfs))}")
        
        zip_files = self.get_available_zip_files()
        if not zip_files:
            logger.warning("⚠️ No zip files found to process")
            return self.checkpoint_data
        
        # Collect all data across all zip files by ETF
        all_etf_data = defaultdict(list)
        
        for i, zip_path in enumerate(zip_files, 1):
            zip_key = zip_path.name
            
            # Skip if already processed
            if zip_key in self.checkpoint_data['zip_files_processed']:
                logger.debug(f"⏭️ Skipping already processed {zip_key}")
                continue
            
            logger.info(f"📈 Progress: {i}/{len(zip_files)} - Processing {zip_key}")
            
            # Extract data from this zip file
            zip_etf_data = self.extract_etf_data_from_zip(zip_path, target_etfs)
            
            # Accumulate data
            for symbol, records in zip_etf_data.items():
                all_etf_data[symbol].extend(records)
            
            # Mark zip as processed
            self.checkpoint_data['zip_files_processed'][zip_key] = True
            self.checkpoint_data['stats']['zip_files_scanned'] += 1
            
            # Save checkpoint periodically
            if i % 5 == 0:
                self.save_checkpoint()
        
        # Group data by month and write parquet files
        if all_etf_data:
            logger.info("📊 Grouping data by month and writing parquet files...")
            monthly_data = self.group_data_by_month(dict(all_etf_data))
            
            for symbol in sorted(monthly_data.keys()):
                symbol_months = monthly_data[symbol]
                
                for month_key in sorted(symbol_months.keys()):
                    year, month = map(int, month_key.split('-'))
                    records = symbol_months[month_key]
                    
                    if self.write_etf_parquet_file(symbol, year, month, records):
                        self.checkpoint_data['stats']['parquet_files_written'] += 1
                        self.checkpoint_data['stats']['etf_records_extracted'] += len(records)
                
                self.checkpoint_data['stats']['etfs_updated'] += 1
                logger.info(f"✅ Completed processing for {symbol}")
        
        # Final statistics
        logger.info("🎉 ETF zip file processing completed!")
        logger.info(f"📊 Statistics:")
        logger.info(f"   • Zip files processed: {self.checkpoint_data['stats']['zip_files_scanned']}")
        logger.info(f"   • ETFs updated: {self.checkpoint_data['stats']['etfs_updated']}")
        logger.info(f"   • Parquet files written: {self.checkpoint_data['stats']['parquet_files_written']}")
        logger.info(f"   • Total records extracted: {self.checkpoint_data['stats']['etf_records_extracted']:,}")
        
        self.save_checkpoint()
        return self.checkpoint_data


def main():
    """Command line interface"""
    parser = argparse.ArgumentParser(
        description="FirstRate ETF Daily Zip Processor", 
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all critical ETFs
  PYTHONPATH=src python scripts/process_firstrate_etf_zips.py

  # Process specific ETFs only
  PYTHONPATH=src python scripts/process_firstrate_etf_zips.py --etfs SPY,QQQ,IWM

  # Debug mode
  PYTHONPATH=src python scripts/process_firstrate_etf_zips.py --debug
        """
    )
    
    # Options
    parser.add_argument("--etfs", help="Comma-separated list of ETFs to process (default: critical ETFs)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--checkpoint-file", default="firstrate_etf_zip_processor.json",
                       help="Checkpoint file for resumable processing")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'firstrate_etf_zip_processor_{datetime.now().strftime("%Y%m%d_%H%M")}.log')
        ]
    )
    
    # Determine target ETFs
    if args.etfs:
        target_etfs = set(etf.strip().upper() for etf in args.etfs.split(','))
    else:
        target_etfs = set(CRITICAL_ETFS)
    
    # Create processor
    processor = FirstRateETFZipProcessor(checkpoint_file=args.checkpoint_file)
    
    # Process zip files
    try:
        result = processor.process_all_zip_files(target_etfs)
        
        print(f"\n✅ ETF zip processing completed successfully!")
        print(f"📊 Final stats: {result['stats']}")
        return 0
            
    except KeyboardInterrupt:
        print("\n🛑 ETF processing interrupted by user")
        processor.save_checkpoint()
        print("💾 Checkpoint saved - resume with same command")
        return 1
        
    except Exception as e:
        print(f"\n❌ ETF processing failed: {e}")
        processor.save_checkpoint()
        logging.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())