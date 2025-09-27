#!/usr/bin/env python3
"""
Monitor FirstRate delisted backfill progress
"""

import json
from pathlib import Path
from datetime import datetime

def main():
    print(f"📊 FirstRate Delisted Backfill Monitor - {datetime.now().strftime('%H:%M:%S')}")

    # Check checkpoint file
    checkpoint_file = Path("/data/firstrate_delisted_backfill_checkpoint.json")
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)

        stats = checkpoint.get('stats', {})

        print(f"\n📝 Checkpoint Status:")
        print(f"   Archives processed: {len(stats.get('processed_archives', []))}/5")
        print(f"   Symbols processed: {stats.get('symbols_processed', 0):,}")
        print(f"   Symbols with data: {stats.get('symbols_with_data', 0):,}")
        print(f"   Total records: {stats.get('total_records', 0):,}")
        print(f"   Processing errors: {stats.get('errors', 0)}")
        print(f"   Last updated: {checkpoint.get('last_updated', 'Unknown')}")

        if stats.get('processed_archives'):
            print(f"   Completed archives: {', '.join(stats['processed_archives'])}")

        print("⚠️  No checkpoint file found yet")

    # Check output directory structure
    base_path = Path("/data/minute-bars/firstrate-delisted")
    if base_path.exists():
        archives = sorted([d.name for d in base_path.iterdir() if d.is_dir()])
        print(f"\n📁 Output Directory Structure:")
        print(f"   Archive directories: {len(archives)} - {', '.join(archives)}")

        # Sample the first archive directory to check progress
        if archives:
            first_archive = base_path / archives[0]
            if first_archive.exists():
                letters = sorted([d.name for d in first_archive.iterdir() if d.is_dir() and len(d.name) == 1])
                total_symbols = 0
                total_files = 0

                for letter in letters:
                    letter_path = first_archive / letter
                    if letter_path.exists():
                        symbol_dirs = [d for d in letter_path.iterdir() if d.is_dir()]
                        letter_files = list(letter_path.rglob("*_delisted.parquet"))
                        total_symbols += len(symbol_dirs)
                        total_files += len(letter_files)

                print(f"   {archives[0]}: {len(letters)} letters, {total_symbols} symbols, {total_files} parquet files")
    else:
        print("⚠️  Output directory not created yet")

if __name__ == "__main__":
    main()