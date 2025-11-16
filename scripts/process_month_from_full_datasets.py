#!/usr/bin/env python3
"""
Process specific month data from FirstRate full datasets

Extracts minute bar data for any specified month from downloaded full dataset zip files
and merges with existing monthly parquet files.

Usage:
    python3 scripts/process_month_from_full_datasets.py --zip-file stock_A_1min_adj_split.zip --year 2024 --month 10
    python3 scripts/process_month_from_full_datasets.py --zip-file stock_A_1min_adj_split.zip --year 2025 --month 11 --symbols AAPL,AMD,ADBE
"""

import os
import sys
import logging
import argparse
import zipfile
import io
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional
import pandas as pd
from collections import defaultdict

logger = logging.getLogger(__name__)

class FirstRateMonthProcessor:
    """Process specific month data from FirstRate full dataset files"""
    
    def __init__(
        self,
        zip_file_path: str,
        target_year: int,
        target_month: int,
        output_path: str = "/mnt/d/ats-data/minute-bars/firstrate"
    ):
        self.zip_file_path = Path(zip_file_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Target month configuration
        self.target_year = target_year
        self.target_month = target_month
        
        # Calculate month date range
        if target_month == 12:
            next_year = target_year + 1
            next_month = 1
        else:
            next_year = target_year
            next_month = target_month + 1
            
        from datetime import timedelta
        self.start_date = date(target_year, target_month, 1)
        self.end_date = date(next_year, next_month, 1) - timedelta(days=1)
        
        self.stats = {
            'symbols_processed': 0,
            'symbols_with_target_data': 0,
            'total_target_records': 0,
            'files_created': 0,
            'files_updated': 0,
            'processing_errors': []
        }
        
        logger.info(f"📁 Zip file: {self.zip_file_path}")
        logger.info(f"💾 Output path: {self.output_path}")
        logger.info(f"🎯 Target: {target_year}-{target_month:02d} ({self.start_date} to {self.end_date})")
    
    def get_symbols_from_zip(self) -> List[str]:
        """Extract symbol list from the zip file"""
        symbols = []
        try:
            with zipfile.ZipFile(self.zip_file_path, 'r') as zf:
                for filename in zf.namelist():
                    if filename.endswith('_full_1min_adjsplit.txt'):
                        symbol = filename.split('_')[0].upper()
                        if 1 <= len(symbol) <= 5 and symbol.isalpha():
                            symbols.append(symbol)
        except Exception as e:
            logger.error(f"Error reading zip file: {e}")
            return []
        
        symbols = sorted(list(set(symbols)))
        logger.info(f"📊 Found {len(symbols)} symbols in zip file")
        return symbols
    
    def extract_month_data_for_symbol(self, symbol: str) -> Optional[pd.DataFrame]:
        """Extract target month data for a specific symbol"""
        try:
            with zipfile.ZipFile(self.zip_file_path, 'r') as zf:
                txt_file = f"{symbol}_full_1min_adjsplit.txt"
                if txt_file not in zf.namelist():
                    logger.debug(f"File not found: {txt_file}")
                    return None
                
                with zf.open(txt_file) as f:
                    content = f.read()
                    if len(content) < 50:
                        logger.debug(f"Empty or too small file: {txt_file}")
                        return None
                    
                    # Parse CSV data
                    df = pd.read_csv(
                        io.BytesIO(content),
                        header=None,
                        names=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    )
                    
                    # Convert timestamp and filter to target month
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    month_df = df[
                        (df['timestamp'].dt.year == self.target_year) &
                        (df['timestamp'].dt.month == self.target_month)
                    ]
                    
                    if len(month_df) == 0:
                        logger.debug(f"No {self.target_year}-{self.target_month:02d} data for {symbol}")
                        return None
                    
                    # Add metadata columns
                    month_df['symbol'] = symbol
                    month_df['vendor'] = 'firstrate'
                    month_df['vwap'] = month_df['close']  # Simple approximation
                    month_df['trade_count'] = 0
                    month_df['quality_score'] = 1.0
                    
                    # Remove timezone info if present
                    if month_df['timestamp'].dt.tz is not None:
                        month_df['timestamp'] = month_df['timestamp'].dt.tz_localize(None)
                    
                    logger.info(f"✅ {symbol}: Found {len(month_df)} {self.target_year}-{self.target_month:02d} records")
                    return month_df
                    
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            self.stats['processing_errors'].append(f"{symbol}: {str(e)}")
            return None
    
    def merge_with_existing_data(self, symbol: str, month_df: pd.DataFrame) -> bool:
        """Merge month data with existing monthly parquet file and regenerate metadata"""
        try:
            # Construct parquet file path
            symbol_dir = (self.output_path / symbol[0] / symbol / 
                         str(self.target_year) / f"{self.target_month:02d}")
            parquet_path = symbol_dir / f"{symbol}_{self.target_year}_{self.target_month:02d}.parquet"
            
            if parquet_path.exists():
                # Load existing data and merge
                existing_df = pd.read_parquet(parquet_path)
                
                # Ensure timestamp compatibility
                if existing_df['timestamp'].dt.tz is not None:
                    existing_df['timestamp'] = existing_df['timestamp'].dt.tz_localize(None)
                
                # Merge and deduplicate
                merged_df = pd.concat([existing_df, month_df], ignore_index=True)
                merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='last')
                merged_df = merged_df.sort_values('timestamp')
                
                new_count = len(merged_df) - len(existing_df)
                logger.info(f"📝 {symbol}: Merged {new_count} new records (was {len(existing_df)}, now {len(merged_df)})")
                
                self.stats['files_updated'] += 1
            else:
                # Create new file
                merged_df = month_df.sort_values('timestamp')
                logger.info(f"📝 {symbol}: Created new file with {len(merged_df)} records")
                self.stats['files_created'] += 1
            
            # Ensure directory exists and save file + metadata atomically
            symbol_dir.mkdir(parents=True, exist_ok=True)
            self._save_parquet_with_metadata(parquet_path, merged_df)
            
            self.stats['total_target_records'] += len(month_df)
            return True
            
        except Exception as e:
            logger.error(f"Error merging data for {symbol}: {e}")
            self.stats['processing_errors'].append(f"{symbol} merge: {str(e)}")
            return False
    
    def _save_parquet_with_metadata(self, parquet_path: Path, df: pd.DataFrame) -> None:
        """Save parquet file and generate metadata atomically"""
        import json
        from datetime import datetime
        
        # Save parquet file
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        
        # Generate and save metadata
        metadata_path = parquet_path.with_suffix('.parquet.metadata.json')
        metadata = {
            'file_path': parquet_path.name,
            'created_at': datetime.now().isoformat(),
            'file_size_bytes': parquet_path.stat().st_size,
            'records_count': len(df),
            'symbols': df['symbol'].unique().tolist(),
            'date_range': {
                'start': df['timestamp'].dt.date.min().isoformat(),
                'end': df['timestamp'].dt.date.max().isoformat()
            },
            'vendor': 'firstrate',
            'last_updated': datetime.now().isoformat()
        }
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.debug(f"💾 Saved {parquet_path.name} with {len(df):,} records and updated metadata")
    
    def process_all_symbols(self, symbols: List[str] = None, limit: Optional[int] = None) -> dict:
        """Process all symbols or specific symbol list"""
        if symbols is None:
            symbols = self.get_symbols_from_zip()
        
        if limit:
            symbols = symbols[:limit]
            logger.info(f"🔢 Limited to {limit} symbols")
        
        logger.info(f"🚀 Processing {len(symbols)} symbols for {self.target_year}-{self.target_month:02d} data...")
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"🔄 Processing {symbol} ({i}/{len(symbols)})")
            
            # Extract target month data
            month_df = self.extract_month_data_for_symbol(symbol)
            self.stats['symbols_processed'] += 1
            
            if month_df is not None and len(month_df) > 0:
                # Merge with existing data
                if self.merge_with_existing_data(symbol, month_df):
                    self.stats['symbols_with_target_data'] += 1
            
            if i % 25 == 0:
                logger.info(f"📊 Progress: {i}/{len(symbols)} symbols, "
                           f"{self.stats['symbols_with_target_data']} with {self.target_year}-{self.target_month:02d} data")
        
        return self.stats
    
    def print_summary(self):
        """Print processing summary"""
        logger.info(f"🎉 {self.target_year}-{self.target_month:02d} processing completed!")
        logger.info(f"📊 Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"📈 Symbols with {self.target_year}-{self.target_month:02d} data: {self.stats['symbols_with_target_data']}")
        logger.info(f"📝 {self.target_year}-{self.target_month:02d} records processed: {self.stats['total_target_records']:,}")
        logger.info(f"📄 Files created: {self.stats['files_created']}")
        logger.info(f"📄 Files updated: {self.stats['files_updated']}")
        
        if self.stats['processing_errors']:
            logger.warning(f"⚠️  Processing errors: {len(self.stats['processing_errors'])}")
            for error in self.stats['processing_errors'][:5]:  # Show first 5
                logger.warning(f"  {error}")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Process specific month data from FirstRate full datasets")
    parser.add_argument("--zip-file", required=True, help="Path to FirstRate full dataset zip file")
    parser.add_argument("--year", type=int, required=True, help="Target year (e.g., 2024)")
    parser.add_argument("--month", type=int, required=True, help="Target month (1-12)")
    parser.add_argument("--output-path", default="/mnt/d/ats-data/minute-bars/firstrate",
                       help="Output directory for processed files")
    parser.add_argument("--symbols", help="Specific symbols to process (comma-separated)")
    parser.add_argument("--limit", type=int, help="Limit number of symbols for testing")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Validate month
    if not (1 <= args.month <= 12):
        logger.error(f"❌ Invalid month: {args.month} (must be 1-12)")
        return 1
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Validate zip file exists
    zip_path = Path(args.zip_file)
    if not zip_path.is_absolute():
        zip_path = Path("/mnt/d/ats-data/firstrate-data/full") / zip_path
    
    if not zip_path.exists():
        logger.error(f"❌ Zip file not found: {zip_path}")
        return 1
    
    # Create processor
    processor = FirstRateMonthProcessor(
        zip_file_path=str(zip_path),
        target_year=args.year,
        target_month=args.month,
        output_path=args.output_path
    )
    
    # Parse symbols if provided
    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',')]
        logger.info(f"🎯 Processing specific symbols: {symbols}")
    
    try:
        # Process symbols
        stats = processor.process_all_symbols(symbols=symbols, limit=args.limit)
        
        # Print summary
        processor.print_summary()
        
        return 0 if stats['symbols_with_target_data'] > 0 else 1
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())