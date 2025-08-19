#!/usr/bin/env python3
"""
Debug why stocks aren't being added to the universe despite qualifying
"""

import asyncio
import logging
from datetime import date
from config.environment import Environment
from universe.dynamic_modeling_universe import DynamicModelingUniverse

async def debug_universe_additions():
    """Debug the universe addition process"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize environment and universe
        env = Environment(env_type="dev")
        universe = DynamicModelingUniverse(env)
        
        logger.info("Initializing dynamic universe...")
        await universe.initialize()
        
        # Get qualifying stocks directly
        logger.info("Getting qualifying stocks...")
        qualifying_metrics = await universe._get_qualifying_stocks(date.today())
        
        logger.info(f"Total stocks returned: {len(qualifying_metrics)}")
        
        # Count how many actually qualify
        actually_qualifying = [m for m in qualifying_metrics if m.qualifies]
        logger.info(f"Stocks that actually qualify: {len(actually_qualifying)}")
        
        # Show top 10 qualifying stocks
        logger.info("Top 10 qualifying stocks:")
        for i, stock in enumerate(actually_qualifying[:10]):
            logger.info(f"  {i+1}. {stock.symbol}: Cap=${stock.avg_market_cap_millions:.1f}M, Vol=${stock.avg_dollar_volume_millions:.1f}M")
        
        # Show top 10 non-qualifying stocks (to see why they don't qualify)
        non_qualifying = [m for m in qualifying_metrics if not m.qualifies]
        logger.info(f"\nStocks that DON'T qualify: {len(non_qualifying)}")
        logger.info("First 5 non-qualifying stocks:")
        for i, stock in enumerate(non_qualifying[:5]):
            logger.info(f"  {i+1}. {stock.symbol}: Cap=${stock.avg_market_cap_millions:.1f}M (meets: {stock.meets_market_cap}), Vol=${stock.avg_dollar_volume_millions:.1f}M (meets: {stock.meets_volume})")
        
        # Check current universe
        current_stocks = await universe._get_current_universe_stocks()
        logger.info(f"\nCurrent universe stocks: {len(current_stocks)}")
        
        # Process additions manually to see what happens
        logger.info("\nTesting addition process...")
        current_instrument_ids = {stock.instrument_id for stock in current_stocks}
        qualifying_by_id = {m.instrument_id: m for m in qualifying_metrics if m.qualifies}
        
        potential_additions = []
        for instrument_id, metrics in qualifying_by_id.items():
            if instrument_id not in current_instrument_ids:
                potential_additions.append(metrics)
        
        logger.info(f"Potential additions (not already in universe): {len(potential_additions)}")
        
        # Check first few for re-entry eligibility
        logger.info("Checking re-entry eligibility for first 5 potential additions:")
        for i, metrics in enumerate(potential_additions[:5]):
            can_add = await universe._check_reentry_eligibility(metrics.instrument_id, date.today())
            logger.info(f"  {i+1}. {metrics.symbol} (ID: {metrics.instrument_id}): can_add={can_add}")
        
        await universe.close()
        
    except Exception as e:
        logger.error(f"Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_universe_additions())