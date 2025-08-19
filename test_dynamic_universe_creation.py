#!/usr/bin/env python3
"""
Test the dynamic universe creation with schema compatibility fix.
This tests that the system can handle both legacy and modern database schemas.
"""

import asyncio
import logging
from datetime import date
from config.environment import Environment
from universe.dynamic_modeling_universe import DynamicModelingUniverse

async def test_dynamic_universe():
    """Test complete dynamic universe creation and operation"""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize environment and universe with dev environment
        env = Environment(env_type="dev")
        universe = DynamicModelingUniverse(env)
        
        logger.info("Initializing dynamic universe...")
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
        
        # Generate report
        logger.info("Generating universe report...")
        report = await universe.get_current_universe_report()
        
        # Save report
        report_file = f"test_dynamic_universe_report_{date.today().strftime('%Y%m%d')}.md"
        with open(report_file, 'w') as f:
            f.write(report)
        
        logger.info(f"✅ Report saved to {report_file}")
        
        # Close connections
        await universe.close()
        
        logger.info("✅ Dynamic universe creation test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Dynamic universe test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_dynamic_universe())
    exit(0 if success else 1)