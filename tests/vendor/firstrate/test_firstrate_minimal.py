#!/usr/bin/env python3
"""
Minimal FirstRate backfill test
"""

import sys
import asyncio
import logging
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

from domains.market_data.services.agent.firstrate_adapter import FirstRateAdapter
from storage.file_based_minute_manager import FileBasedMinuteManager

@pytest.mark.asyncio

async def test_simple_processing():
    """Simple processing test"""

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    logger.info("🚀 Starting FirstRate minimal test...")

    # Initialize components
    adapter = FirstRateAdapter("/data/firstrate-data")
    output_path = Path("/data/minute-bars/firstrate")
    output_path.mkdir(parents=True, exist_ok=True)

    minute_manager = FileBasedMinuteManager(str(output_path))

    # Get symbol inventory
    inventory = adapter.get_symbol_inventory('stock')
    logger.info(f"📊 Found {len(inventory)} symbols")

    # Test with first available symbol
    if not inventory:
        logger.error("❌ No symbols found!")
        return

    test_symbol = list(inventory.keys())[0]
    symbol_info = inventory[test_symbol]

    logger.info(f"🔸 Testing with symbol: {test_symbol}")
    logger.info(f"   ZIP files: {len(symbol_info['zip_files'])}")
    logger.info(f"   Date range: {symbol_info['date_range']}")

    # Process a small amount of data
    zip_file = Path(symbol_info['zip_files'][0])
    tick_count = 0

    try:
        logger.info(f"📦 Processing {zip_file.name}...")

        for tick in adapter.process_minute_data_from_zip(zip_file, test_symbol):
            # Store using minute manager
            await minute_manager.store_minute_bar(
                symbol=tick.symbol,
                timestamp=tick.timestamp,
                open_price=tick.open,
                high_price=tick.high,
                low_price=tick.low,
                close_price=tick.close,
                volume=tick.volume,
                vendor="firstrate"
            )

            tick_count += 1
            if tick_count % 1000 == 0:
                logger.info(f"   📈 Processed {tick_count} ticks...")

            # Limit to 5000 ticks for test
            if tick_count >= 5000:
                break

        logger.info(f"✅ {test_symbol}: Successfully processed {tick_count} ticks")

        # Verify files were created
        output_files = list(output_path.rglob("*.parquet"))
        logger.info(f"📁 Created {len(output_files)} parquet files")

        if output_files:
            sample_file = output_files[0]
            logger.info(f"📄 Sample file: {sample_file.relative_to(output_path)}")
            logger.info(f"📏 File size: {sample_file.stat().st_size:,} bytes")

        return tick_count

    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    result = asyncio.run(test_simple_processing())