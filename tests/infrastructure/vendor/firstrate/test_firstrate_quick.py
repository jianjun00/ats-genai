#!/usr/bin/env python3
"""
Quick test of fixed FirstRate processing
"""

import sys
import asyncio
import logging

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

# Import the fixed processor
sys.path.insert(0, '/home/jianjun/ats-genai-data/scripts')

from populate_firstrate_minute_bars import FirstRateBackfillProcessor

@pytest.mark.asyncio

async def test_quick():
    """Quick test with 1 symbol"""

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    processor = FirstRateBackfillProcessor(
        data_path="/data/firstrate-data",
        output_path="/data/minute-bars/firstrate",
        checkpoint_file="test_quick_checkpoint.json"
    )

    # Test with just 1 symbol, 1 month
    result = await processor.run_backfill(
        asset_type='stock',
        symbols=['AAPL'],  # Just test AAPL
        limit=1,
        resume=False
    )

    print(f"🎉 Quick test completed!")
    print(f"📊 Stats: {result['processing_stats']}")

    return result

if __name__ == "__main__":
    result = asyncio.run(test_quick())