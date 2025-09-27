#!/usr/bin/env python3
"""
FirstRate Delisted/Archive Minute Bar Backfill

Processes FirstRate archive zip files containing delisted symbols to extract
complete historical minute bar data for stocks no longer trading.

Usage:
    PYTHONPATH=src python -m domains.workflow.firstrate.backfill.delisted
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

class FirstRateDelistedProcessor:
    def __init__(self):
        self.archive_data_path = Path("/mnt/d/ats-data/firstrate-data/archive")
        self.output_path = Path("/mnt/d/ats-data/minute-bars/firstrate-delisted")
        self.checkpoint_file = Path("firstrate_delisted_backfill_checkpoint.json")

        self.stats = {
            'archives_processed': 0,
            'symbols_processed': 0,
            'symbols_with_data': 0,
            'total_records': 0,
            'errors': 0,
            'start_time': datetime.now().isoformat(),
            'processed_archives': [],
            'failed_symbols': []
        }

        self.load_checkpoint()

    def load_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.stats.update(checkpoint.get('stats', {}))
                    logger.info(f"📝 Loaded checkpoint: {len(self.stats.get('processed_archives', []))} archives completed")
            except Exception as e:
                logger.error(f"❌ Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            checkpoint_data = {
                'stats': self.stats,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")

    def process_archive_file(self, archive_file: Path, resume: bool = True):
        archive_name = archive_file.stem

        if resume and archive_name in self.stats.get('processed_archives', []):
            logger.info(f"⏭️  Archive {archive_name} already processed, skipping")
            return 0

        file_size_gb = archive_file.stat().st_size / 1024**3
        logger.info(f"📦 Processing archive: {archive_file.name} ({file_size_gb:.1f} GB)")

        archive_output = self.output_path / archive_name
        archive_output.mkdir(parents=True, exist_ok=True)

        processed_symbols = 0
        symbols_with_data = 0
        total_records = 0

        try:
            with zipfile.ZipFile(archive_file, 'r') as zf:
                txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                logger.info(f"   📄 Found {len(txt_files)} TXT files in {archive_name}")

                for i, txt_file in enumerate(txt_files, 1):
                    try:
                        symbol = Path(txt_file).stem.split('_')[0]
                        if not symbol or symbol in ['metadata', 'readme']:
                            continue

                        if i % 100 == 0:
                            logger.info(f"   📈 Progress {archive_name}: {i}/{len(txt_files)} symbols ({i/len(txt_files)*100:.1f}%)")

                        with zf.open(txt_file) as f:
                            content = f.read()

                            if len(content) < 100:
                                continue

                            try:
                                df = pd.read_csv(io.BytesIO(content), header=None,
                                               names=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            except Exception as e:
                                logger.debug(f"   ⚠️  Failed to parse CSV for {symbol}: {e}")
                                continue

                            if len(df) > 0:
                                first_letter = symbol[0].upper() if symbol else 'UNKNOWN'
                                letter_dir = archive_output / first_letter
                                letter_dir.mkdir(parents=True, exist_ok=True)

                                symbol_dir = letter_dir / symbol
                                symbol_dir.mkdir(parents=True, exist_ok=True)

                                output_file = symbol_dir / f"{symbol}_delisted.parquet"
                                df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)

                                record_count = len(df)
                                total_records += record_count
                                symbols_with_data += 1

                                if record_count > 10000:
                                    date_range = f"{df['timestamp'].min()} to {df['timestamp'].max()}"
                                    logger.info(f"   ✅ {symbol}: {record_count:,} records ({date_range})")

                            processed_symbols += 1

                    except Exception as e:
                        logger.error(f"   ❌ Error processing {txt_file}: {e}")
                        self.stats['failed_symbols'].append(f"{archive_name}/{symbol}")
                        self.stats['errors'] += 1
                        continue

                    if i % 200 == 0:
                        time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Error processing archive {archive_file}: {e}")
            return 0

        self.stats['archives_processed'] += 1
        self.stats['symbols_processed'] += processed_symbols
        self.stats['symbols_with_data'] += symbols_with_data
        self.stats['total_records'] += total_records
        self.stats['processed_archives'].append(archive_name)

        logger.info(f"✅ Archive {archive_name} completed: {processed_symbols} symbols processed, {symbols_with_data} with data, {total_records:,} total records")

        self.save_checkpoint()

        return processed_symbols

    def run_delisted_backfill(self, resume: bool = True):
        logger.info("🚀 Starting FirstRate Delisted/Archive Minute Bar Backfill")
        logger.info(f"📊 Archive source: {self.archive_data_path}")
        logger.info(f"💾 Output path: {self.output_path}")
        logger.info(f"📝 Checkpoint: {self.checkpoint_file}")

        archive_files = list(self.archive_data_path.glob("archive_*.zip"))
        archive_files.sort()

        if not archive_files:
            logger.error("❌ No archive files found in archive directory")
            return self.stats

        logger.info(f"📁 Found {len(archive_files)} archive files:")
        total_size_gb = 0
        for af in archive_files:
            size_gb = af.stat().st_size / 1024**3
            total_size_gb += size_gb
            logger.info(f"   📦 {af.name}: {size_gb:.1f} GB")
        logger.info(f"   📊 Total archive size: {total_size_gb:.1f} GB")

        if resume:
            remaining_archives = [af for af in archive_files if af.stem not in self.stats.get('processed_archives', [])]
            logger.info(f"📋 Resuming: {len(remaining_archives)} archives remaining")
        else:
            remaining_archives = archive_files
            self.stats = {
                'archives_processed': 0,
                'symbols_processed': 0,
                'symbols_with_data': 0,
                'total_records': 0,
                'errors': 0,
                'start_time': datetime.now().isoformat(),
                'processed_archives': [],
                'failed_symbols': []
            }
            logger.info(f"📋 Full backfill: Processing all {len(archive_files)} archives")

        for i, archive_file in enumerate(remaining_archives, 1):
            logger.info(f"🔄 Starting archive {archive_file.name} ({i}/{len(remaining_archives)})")

            try:
                processed = self.process_archive_file(archive_file, resume=resume)

                if processed > 0:
                    total_completed = len(self.stats['processed_archives'])
                    progress_pct = (total_completed / len(archive_files)) * 100

                    logger.info(f"✅ Archive {archive_file.name} completed ({total_completed}/{len(archive_files)} archives, {progress_pct:.1f}% complete)")
                    logger.info(f"📊 Running totals: {self.stats['symbols_with_data']:,} symbols with data, {self.stats['total_records']:,} total records")
                else:
                    logger.warning(f"⚠️  Archive {archive_file.name}: No symbols processed")

            except Exception as e:
                logger.error(f"❌ Failed to process archive {archive_file.name}: {e}")
                self.stats['errors'] += 1
                continue

            if i < len(remaining_archives):
                logger.info(f"⏸️  Brief pause before next archive...")
                time.sleep(3)

        elapsed_time = (datetime.now() - datetime.fromisoformat(self.stats['start_time'])).total_seconds()

        logger.info("🎉 FirstRate delisted/archive backfill completed!")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Archives processed: {self.stats['archives_processed']}/{len(archive_files)}")
        logger.info(f"   Symbols processed: {self.stats['symbols_processed']:,}")
        logger.info(f"   Symbols with data: {self.stats['symbols_with_data']:,}")
        logger.info(f"   Total records: {self.stats['total_records']:,}")
        logger.info(f"   Processing errors: {self.stats['errors']}")
        logger.info(f"   Elapsed time: {elapsed_time:.1f} seconds ({elapsed_time/3600:.1f} hours)")

        if self.stats['failed_symbols']:
            logger.info(f"⚠️  Failed symbols ({len(self.stats['failed_symbols'])}): {self.stats['failed_symbols'][:10]}...")

        self.save_checkpoint()

        return self.stats

def main():
    logger.info("🚀 FirstRate Delisted/Archive Minute Bar Backfill Starting...")

    processor = FirstRateDelistedProcessor()

    try:
        final_stats = processor.run_delisted_backfill(resume=True)

        logger.info("✅ Delisted backfill process completed successfully!")
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