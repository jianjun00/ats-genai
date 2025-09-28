#!/usr/bin/env python3
"""
Check FirstRate backfill progress and statistics
"""

import json
from pathlib import Path

def main():
    print("📊 Checking FirstRate Backfill Progress...")

    # Check checkpoint file
    checkpoint_file = Path("/data/firstrate_backfill_checkpoint.json")
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)

        stats = checkpoint.get('stats', {})

        print(f"\n📝 Checkpoint Status:")
        print(f"   Letters processed: {len(stats.get('processed_letters', []))}/26")
        print(f"   Symbols processed: {stats.get('symbols_processed', 0):,}")
        print(f"   Symbols with data: {stats.get('symbols_with_data', 0):,}")
        print(f"   Total records: {stats.get('total_records', 0):,}")
        print(f"   Processing errors: {stats.get('errors', 0)}")
        print(f"   Last updated: {checkpoint.get('last_updated', 'Unknown')}")

        if stats.get('processed_letters'):
            print(f"   Completed letters: {', '.join(stats['processed_letters'])}")

        print("⚠️  No checkpoint file found yet")

    # Check directory structure
    base_path = Path("/data/minute-bars/firstrate")
    if base_path.exists():
        letters = sorted([d.name for d in base_path.iterdir() if d.is_dir() and len(d.name) == 1])
        print(f"\n📁 Directory Structure:")
        print(f"   Available letters: {len(letters)} - {', '.join(letters)}")

        # Sample a few letters to check file counts
        for letter in letters[:3]:  # Check first 3 letters
            letter_path = base_path / letter
            if letter_path.exists():
                symbol_dirs = [d for d in letter_path.iterdir() if d.is_dir()]
                complete_files = list(letter_path.rglob("*_complete.parquet"))
                sample_files = list(letter_path.rglob("*_sample.parquet"))

                print(f"   {letter}: {len(symbol_dirs)} symbols, {len(complete_files)} complete files, {len(sample_files)} sample files")

if __name__ == "__main__":
    main()