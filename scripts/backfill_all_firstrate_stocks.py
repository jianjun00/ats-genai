#!/usr/bin/env python3
"""
Comprehensive FirstRate Minute Bar Backfill for All Stocks

Processes all 26 letter zip files (A-Z) to extract complete minute bar data
for all available stocks in the FirstRate dataset.
"""

import zipfile
import pandas as pd
from pathlib import Path
import logging
import io
import time
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveFirstRateProcessor:
    def __init__(self):
        self.stock_data_path = Path("/data/firstrate-data/stock")
        self.output_path = Path("/data/minute-bars/firstrate")
        self.checkpoint_file = Path("/data/firstrate_backfill_checkpoint.json")

        # Statistics tracking
        self.stats = {
            'letters_processed': 0,
            'symbols_processed': 0,
            'symbols_with_data': 0,
            'total_records': 0,
            'errors': 0,
            'start_time': datetime.now().isoformat(),
            'processed_letters': [],
            'failed_symbols': []
        }

        # Load checkpoint if exists
        self.load_checkpoint()

    def load_checkpoint(self):
        """Load processing checkpoint."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.stats.update(checkpoint.get('stats', {}))
                    logger.info(f"📝 Loaded checkpoint: {len(self.stats.get('processed_letters', []))} letters completed")
            except Exception as e:
                logger.error(f"❌ Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        """Save current processing state."""
        try:
            checkpoint_data = {
                'stats': self.stats,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")

    def process_letter_comprehensive(self, letter: str, resume: bool = True):
        """Process all stocks for a single letter comprehensively."""

        # Skip if already processed and resuming
        if resume and letter in self.stats.get('processed_letters', []):
            logger.info(f"⏭️  Letter {letter} already processed, skipping")
            return 0

        zip_pattern = f"stock_{letter}_full_*.zip"
        zip_files = list(self.stock_data_path.glob(zip_pattern))

        if not zip_files:
            logger.warning(f"⚠️  No zip file found for letter {letter}")
            return 0

        zip_file = zip_files[0]
        file_size_gb = zip_file.stat().st_size / 1024**3
        logger.info(f"📦 Processing {letter}: {zip_file.name} ({file_size_gb:.1f} GB)")

        # Create output directory
        letter_output = self.output_path / letter
        letter_output.mkdir(parents=True, exist_ok=True)

        processed_symbols = 0
        symbols_with_data = 0
        total_records = 0

        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                logger.info(f"   📄 Found {len(txt_files)} TXT files in {letter} zip")

                for i, txt_file in enumerate(txt_files, 1):
                    try:
                        # Extract symbol from filename
                        symbol = Path(txt_file).stem.split('_')[0]
                        if not symbol or symbol in ['metadata', 'readme']:
                            continue

                        # Progress logging every 50 symbols
                        if i % 50 == 0:
                            logger.info(f"   📈 Progress {letter}: {i}/{len(txt_files)} symbols ({i/len(txt_files)*100:.1f}%)")

                        # Read TXT data from zip
                        with zf.open(txt_file) as f:
                            content = f.read()

                            # Skip empty files
                            if len(content) < 100:  # Less than 100 bytes likely empty
                                continue

                            try:
                                df = pd.read_csv(io.BytesIO(content), header=None,
                                               names=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            except Exception as e:
                                logger.debug(f"   ⚠️  Failed to parse CSV for {symbol}: {e}")
                                continue

                            if len(df) > 0:
                                # Create symbol directory
                                symbol_dir = letter_output / symbol
                                symbol_dir.mkdir(parents=True, exist_ok=True)

                                # Save complete dataset
                                output_file = symbol_dir / f"{symbol}_complete.parquet"
                                df.to_parquet(output_file, engine='auto')

                                record_count = len(df)
                                total_records += record_count
                                symbols_with_data += 1

                                # Log only significant datasets (>1000 records) to avoid spam
                                if record_count > 1000:
                                    date_range = f"{df['timestamp'].min()} to {df['timestamp'].max()}"
                                    logger.debug(f"   ✅ {symbol}: {record_count:,} records ({date_range})")

                            processed_symbols += 1

                    except Exception as e:
                        logger.error(f"   ❌ Error processing {txt_file}: {e}")
                        self.stats['failed_symbols'].append(f"{letter}/{symbol}")
                        self.stats['errors'] += 1
                        continue

                    # Brief pause every 100 symbols to prevent overwhelming the system
                    if i % 100 == 0:
                        time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Error processing zip {zip_file}: {e}")
            return 0

        # Update statistics
        self.stats['letters_processed'] += 1
        self.stats['symbols_processed'] += processed_symbols
        self.stats['symbols_with_data'] += symbols_with_data
        self.stats['total_records'] += total_records
        self.stats['processed_letters'].append(letter)

        logger.info(f"✅ Letter {letter} completed: {processed_symbols} symbols processed, {symbols_with_data} with data, {total_records:,} total records")

        # Save checkpoint after each letter
        self.save_checkpoint()

        return processed_symbols

    def run_comprehensive_backfill(self, resume: bool = True):
        """Run comprehensive backfill for all letters A-Z."""

        logger.info("🚀 Starting Comprehensive FirstRate Minute Bar Backfill")
        logger.info(f"📊 Data source: {self.stock_data_path}")
        logger.info(f"💾 Output path: {self.output_path}")
        logger.info(f"📝 Checkpoint: {self.checkpoint_file}")

        # All letters A-Z
        all_letters = [chr(i) for i in range(ord('A'), ord('Z') + 1)]

        if resume:
            remaining_letters = [l for l in all_letters if l not in self.stats.get('processed_letters', [])]
            logger.info(f"📋 Resuming: {len(remaining_letters)} letters remaining: {remaining_letters}")
        else:
            remaining_letters = all_letters
            # Reset stats if not resuming
            self.stats = {
                'letters_processed': 0,
                'symbols_processed': 0,
                'symbols_with_data': 0,
                'total_records': 0,
                'errors': 0,
                'start_time': datetime.now().isoformat(),
                'processed_letters': [],
                'failed_symbols': []
            }
            logger.info(f"📋 Full backfill: Processing all {len(all_letters)} letters")

        # Process each letter
        for i, letter in enumerate(remaining_letters, 1):
            logger.info(f"🔄 Starting letter {letter} ({i}/{len(remaining_letters)})")

            try:
                processed = self.process_letter_comprehensive(letter, resume=resume)

                if processed > 0:
                    # Calculate progress
                    total_completed = len(self.stats['processed_letters'])
                    progress_pct = (total_completed / len(all_letters)) * 100

                    logger.info(f"✅ Letter {letter} completed ({total_completed}/26 letters, {progress_pct:.1f}% complete)")
                    logger.info(f"📊 Running totals: {self.stats['symbols_with_data']:,} symbols with data, {self.stats['total_records']:,} total records")
                else:
                    logger.warning(f"⚠️  Letter {letter}: No symbols processed")

            except Exception as e:
                logger.error(f"❌ Failed to process letter {letter}: {e}")
                self.stats['errors'] += 1
                continue

            # Pause between letters to prevent system overload
            if i < len(remaining_letters):  # Don't pause after last letter
                logger.info(f"⏸️  Brief pause before next letter...")
                time.sleep(2)

        # Final summary
        elapsed_time = (datetime.now() - datetime.fromisoformat(self.stats['start_time'])).total_seconds()

        logger.info("🎉 Comprehensive FirstRate backfill completed!")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Letters processed: {self.stats['letters_processed']}/26")
        logger.info(f"   Symbols processed: {self.stats['symbols_processed']:,}")
        logger.info(f"   Symbols with data: {self.stats['symbols_with_data']:,}")
        logger.info(f"   Total records: {self.stats['total_records']:,}")
        logger.info(f"   Processing errors: {self.stats['errors']}")
        logger.info(f"   Elapsed time: {elapsed_time:.1f} seconds ({elapsed_time/3600:.1f} hours)")

        if self.stats['failed_symbols']:
            logger.info(f"⚠️  Failed symbols ({len(self.stats['failed_symbols'])}): {self.stats['failed_symbols'][:10]}...")

        # Save final checkpoint
        self.save_checkpoint()

        return self.stats

def main():
    """Main execution function."""
    logger.info("🚀 FirstRate Comprehensive Minute Bar Backfill Starting...")

    processor = ComprehensiveFirstRateProcessor()

    try:
        # Run comprehensive backfill with resume capability
        final_stats = processor.run_comprehensive_backfill(resume=True)

        logger.info("✅ Backfill process completed successfully!")
        logger.info(f"📋 Final results: {final_stats}")

    except KeyboardInterrupt:
        logger.info("🛑 Backfill interrupted by user")
        processor.save_checkpoint()
        logger.info("💾 Checkpoint saved - use resume=True to continue")

    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        processor.save_checkpoint()
        raise

if __name__ == "__main__":
    main()