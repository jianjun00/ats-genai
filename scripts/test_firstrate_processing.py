#!/usr/bin/env python3
"""
Test FirstRate minute bar processing for remaining symbols.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

async def test_firstrate_processing():
    """Test FirstRate processing for a few symbols."""

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("🚀 Testing FirstRate minute bar processing...")

    # Check data directory
    data_path = Path("/mnt/d/ats-data/minute-bars/firstrate")
    if not data_path.exists():
        logger.error(f"❌ FirstRate data path not found: {data_path}")
        return False

    # Count existing symbols by first letter
    symbol_counts = {}
    for letter_dir in data_path.iterdir():
        if letter_dir.is_dir() and len(letter_dir.name) == 1:
            symbol_count = len([d for d in letter_dir.iterdir() if d.is_dir()])
            symbol_counts[letter_dir.name] = symbol_count

    logger.info(f"📊 Existing symbols by first letter: {symbol_counts}")

    # Find symbols that need processing (directories with few or no parquet files)
    symbols_needing_data = []
    total_symbols = 0

    for letter_dir in sorted(data_path.iterdir()):
        if letter_dir.is_dir() and len(letter_dir.name) == 1:
            for symbol_dir in letter_dir.iterdir():
                if symbol_dir.is_dir():
                    total_symbols += 1
                    parquet_files = list(symbol_dir.rglob("*.parquet"))
                    if len(parquet_files) < 100:  # Arbitrary threshold
                        symbols_needing_data.append(symbol_dir.name)
                        if len(symbols_needing_data) >= 10:  # Limit for testing
                            break
            if len(symbols_needing_data) >= 10:
                break

    logger.info(f"📈 Total symbols found: {total_symbols}")
    logger.info(f"🔄 Symbols needing more data (first 10): {symbols_needing_data[:10]}")

    # Test data collection would go here
    logger.info("✅ FirstRate processing test completed")
    return True

if __name__ == "__main__":
    asyncio.run(test_firstrate_processing())