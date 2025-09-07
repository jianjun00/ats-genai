#!/usr/bin/env python3
"""
Process FirstRate Zip Files to Minute Bars

Simple script to extract and process FirstRate stock zip files into minute bar parquet files.
Bypasses complex configuration and runs directly on host.
"""

import os
import sys
import zipfile
import pandas as pd
# import pyarrow.parquet as pq  # Not available on host
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List
import time
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleFirstRateProcessor:
    def __init__(self):
        self.stock_data_path = Path("/data/firstrate-data/stock")
        self.output_path = Path("/data/minute-bars/firstrate")
        self.processed_count = 0
        self.error_count = 0

    def process_letter_zip(self, letter: str, target_symbols: list = None, limit_symbols: int = 50):
        """Process a single letter zip file."""

        zip_pattern = f"stock_{letter}_full_*.zip"
        zip_files = list(self.stock_data_path.glob(zip_pattern))

        if not zip_files:
            logger.warning(f"⚠️  No zip file found for letter {letter}")
            return 0

        zip_file = zip_files[0]
        logger.info(f"📦 Processing {letter}: {zip_file.name} ({zip_file.stat().st_size / 1024**3:.1f} GB)")

        # Create output directory
        letter_output = self.output_path / letter
        letter_output.mkdir(parents=True, exist_ok=True)

        processed_symbols = 0

        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                # Get list of TXT files in zip (FirstRate format)
                txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                logger.info(f"   📄 Found {len(txt_files)} TXT files in zip")

                # Filter txt_files for target symbols if specified
                files_to_process = txt_files[:limit_symbols]  # Process limited number for testing
                if target_symbols:
                    # Look for target symbols first
                    target_files = []
                    other_files = []
                    for txt_file in txt_files:
                        symbol = Path(txt_file).stem.split('_')[0]
                        if symbol in target_symbols:
                            target_files.append(txt_file)
                        else:
                            other_files.append(txt_file)
                    # Process target symbols first, then others up to limit
                    files_to_process = target_files + other_files[:max(0, limit_symbols - len(target_files))]
                    logger.info(f"   🎯 Found {len(target_files)} target symbols: {[Path(f).stem.split('_')[0] for f in target_files]}")

                for txt_file in files_to_process:
                    try:
                        # Extract symbol from filename (e.g., MSFT_full_1min_adjsplitdiv.txt -> MSFT)
                        symbol = Path(txt_file).stem.split('_')[0]
                        if not symbol or symbol in ['metadata', 'readme']:
                            continue

                        logger.info(f"   📈 Processing symbol: {symbol}")

                        # Read TXT data from zip (CSV format)
                        with zf.open(txt_file) as f:
                            # Try to read first few lines to understand format
                            content = f.read()
                            # FirstRate format: timestamp,open,high,low,close,volume (no header)
                            df = pd.read_csv(io.BytesIO(content), header=None,
                                           names=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

                            if len(df) > 0:
                                # Create symbol directory
                                symbol_dir = letter_output / symbol
                                symbol_dir.mkdir(parents=True, exist_ok=True)

                                # Convert to monthly parquet files (simplified)
                                if 'timestamp' in df.columns:
                                    # Basic processing - save as single parquet file for now
                                    output_file = symbol_dir / f"{symbol}_sample.parquet"

                                    # Save sample of data
                                    sample_df = df.head(1000) if len(df) > 1000 else df
                                    sample_df.to_parquet(output_file, engine='auto')

                                    logger.info(f"   ✅ Saved {len(sample_df)} records to {output_file.name}")
                                    processed_symbols += 1
                                else:
                                    logger.warning(f"   ⚠️  No timestamp column found in {symbol}")
                            else:
                                logger.warning(f"   ⚠️  Empty data for {symbol}")

                    except Exception as e:
                        logger.error(f"   ❌ Error processing {txt_file}: {e}")
                        self.error_count += 1
                        continue

                    # Small delay between symbols
                    time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Error processing zip {zip_file}: {e}")
            return 0

        logger.info(f"✅ Completed {letter}: processed {processed_symbols} symbols")
        self.processed_count += processed_symbols
        return processed_symbols

    def check_priority_symbols(self):
        """Check if priority symbols were processed."""

        priority_symbols = {
            'M': ['MSFT', 'META', 'MA'],
            'N': ['NVDA', 'NFLX'],
            'V': ['V'],
            'P': ['PFE'],
            'U': ['UNH']
        }

        logger.info("🔍 Checking priority symbols:")

        for letter, symbols in priority_symbols.items():
            letter_dir = self.output_path / letter
            if letter_dir.exists():
                for symbol in symbols:
                    symbol_dir = letter_dir / symbol
                    if symbol_dir.exists():
                        parquet_files = list(symbol_dir.glob("*.parquet"))
                        if parquet_files:
                            logger.info(f"   ✅ {symbol}: {len(parquet_files)} parquet files")
                        else:
                            logger.info(f"   ❌ {symbol}: Directory exists but no parquet files")
                    else:
                        logger.info(f"   ❌ {symbol}: Directory does not exist")
            else:
                logger.info(f"   ❌ Letter {letter}: Directory does not exist")

def main():
    """Main processing function."""

    logger.info("🚀 Starting FirstRate Zip Processing...")

    processor = SimpleFirstRateProcessor()

    # Priority letters for major missing symbols
    priority_letters = ['B', 'C', 'K', 'L', 'O', 'W', 'X']  # Process remaining major symbols

    # Target specific priority symbols we need to process
    target_symbols = {
        'B': ['BRK.A', 'BRK.B'],  # Berkshire Hathaway (with dots in file names)
        'C': ['CVX', 'CRM'],
        'K': ['KO'],
        'L': ['LLY'],
        'O': ['ORCL'],
        'W': ['WMT', 'WFC'],
        'X': ['XOM']
    }

    logger.info(f"📋 Processing priority letters: {', '.join(priority_letters)}")

    total_processed = 0

    for letter in priority_letters:
        logger.info(f"🔄 Starting letter {letter}...")
        letter_targets = target_symbols.get(letter, [])
        processed = processor.process_letter_zip(letter, target_symbols=letter_targets, limit_symbols=50)  # Process more symbols per letter
        total_processed += processed

        if processed > 0:
            logger.info(f"✅ Letter {letter} completed: {processed} symbols processed")
        else:
            logger.warning(f"⚠️  Letter {letter}: No symbols processed")

        # Brief pause between letters
        time.sleep(2)

    logger.info("📊 Final Results:")
    logger.info(f"   Total symbols processed: {total_processed}")
    logger.info(f"   Processing errors: {processor.error_count}")

    # Check priority symbols
    processor.check_priority_symbols()

    logger.info("✅ FirstRate zip processing completed!")

if __name__ == "__main__":
    main()