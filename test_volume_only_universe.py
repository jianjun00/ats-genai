#!/usr/bin/env python3
"""
Test the dynamic universe with volume-only criteria since market cap data is not populated.
This creates a universe based purely on trading volume, which we know is working.
"""

import asyncio
import logging
from datetime import date
from config.environment import Environment
from universe.dynamic_modeling_universe import DynamicModelingUniverse

async def test_volume_only_universe():
    """Test universe creation with volume-only criteria"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize environment and universe
        env = Environment(env_type="dev")
        universe = DynamicModelingUniverse(env)
        
        # Modify universe to use volume-only criteria
        universe.universe_name = "dynamic_volume_only_100m"
        universe.min_market_cap_millions = 0  # Disable market cap requirement
        universe.min_dollar_volume_millions = 100  # Keep volume requirement
        
        logger.info("Initializing volume-only dynamic universe...")
        await universe.initialize()
        logger.info("✅ Universe initialization successful")
        
        # Test daily update
        logger.info("Running daily update...")
        summary = await universe.run_daily_update()
        
        logger.info("Update Summary:")
        logger.info(f"  Current stocks: {summary['current_count']}")
        logger.info(f"  Qualifying stocks: {summary['qualifying_count']}")
        logger.info(f"  Added: {len(summary['added'])}")
        logger.info(f"  Removed: {len(summary['removed'])}")
        logger.info(f"  Warned: {len(summary['warned'])}")
        
        # Show first 10 added stocks
        if summary['added']:
            logger.info("First 10 stocks added to universe:")
            for i, stock in enumerate(summary['added'][:10]):
                logger.info(f"  {i+1}. {stock['symbol']}: Vol=${stock['volume']:.1f}M")
        
        # Generate report
        logger.info("Generating universe report...")
        report = await universe.get_current_universe_report()
        
        # Save report
        report_file = f"volume_only_universe_report_{date.today().strftime('%Y%m%d')}.md"
        with open(report_file, 'w') as f:
            f.write(report)
        
        logger.info(f"✅ Report saved to {report_file}")
        
        # Close connections
        await universe.close()
        
        logger.info("✅ Volume-only universe creation test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Volume-only universe test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_volume_only_universe())
    exit(0 if success else 1)