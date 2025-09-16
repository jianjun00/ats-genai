#!/usr/bin/env python3
"""
Test FirstRate backfill with a small number of symbols
"""

import sys
import asyncio
import logging

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

# Import the processor
sys.path.insert(0, '/home/jianjun/ats-genai-data/scripts')
from populate_firstrate_minute_bars import FirstRateBackfillProcessor

@pytest.mark.asyncio

async def test_small_backfill():
    """Test backfill with just 2 symbols"""

    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    processor = FirstRateBackfillProcessor(
        data_path="/data/firstrate-data",
        output_path="/data/minute-bars/firstrate",
        checkpoint_file="test_checkpoint.json"
    )

    # Run with limited symbols
    result = await processor.run_backfill(
        asset_type='stock',
        symbols=['AAPL', 'MSFT'],  # Just test with these 2
        limit=2,
        resume=False
    )

    print(f"🎉 Test backfill completed!")
    print(f"📊 Stats: {result['processing_stats']}")

    return result

if __name__ == "__main__":
    result = asyncio.run(test_small_backfill())