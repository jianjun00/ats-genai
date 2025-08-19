#!/usr/bin/env python3
"""
Test the dynamic universe with real computed market cap data.
This will use the proper market cap criteria now that we have computed values.
"""

import asyncio
import logging
from datetime import date
from config.environment import Environment
from universe.dynamic_modeling_universe import DynamicModelingUniverse

async def test_real_market_cap_universe():
    """Test universe creation with real market cap data"""
    
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
        
        # Use real criteria: >$400M market cap AND >$100M volume
        universe.universe_name = "dynamic_real_market_cap_400m_100m"
        universe.min_market_cap_millions = 400  # Real market cap threshold
        universe.min_dollar_volume_millions = 100  # Volume threshold
        
        logger.info("Initializing dynamic universe with real market cap criteria...")
        await universe.initialize()
        logger.info("✅ Universe initialization successful")
        
        # First, let's populate some market cap data for top stocks
        logger.info("Populating market cap data for top volume stocks...")
        await populate_sample_market_cap_data(env)
        
        # Test daily update
        logger.info("Running daily update with real market cap criteria...")
        summary = await universe.run_daily_update()
        
        logger.info("Update Summary:")
        logger.info(f"  Current stocks: {summary['current_count']}")
        logger.info(f"  Qualifying stocks: {summary['qualifying_count']}")
        logger.info(f"  Added: {len(summary['added'])}")
        logger.info(f"  Removed: {len(summary['removed'])}")
        logger.info(f"  Warned: {len(summary['warned'])}")
        
        # Show first 20 added stocks with their market caps
        if summary['added']:
            logger.info("First 20 stocks added to universe with real market cap:")
            for i, stock in enumerate(summary['added'][:20]):
                logger.info(f"  {i+1}. {stock['symbol']}: Cap=${stock['market_cap']:.0f}M, Vol=${stock['volume']:.0f}M")
        
        # Generate report
        logger.info("Generating universe report...")
        report = await universe.get_current_universe_report()
        
        # Save report
        report_file = f"real_market_cap_universe_report_{date.today().strftime('%Y%m%d')}.md"
        with open(report_file, 'w') as f:
            f.write(report)
        
        logger.info(f"✅ Report saved to {report_file}")
        
        # Close connections
        await universe.close()
        
        logger.info("✅ Real market cap universe creation test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Real market cap universe test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def populate_sample_market_cap_data(env: Environment):
    """Populate market cap data for top 50 high-volume stocks"""
    
    logger = logging.getLogger(__name__)
    
    # Get top 50 high-volume stocks from recent universe data
    top_volume_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA',
        'AVGO', 'JPM', 'UNH', 'JNJ', 'V', 'PG', 'MA', 'HD', 'CVX', 'LLY',
        'ABBV', 'PFE', 'KO', 'PEP', 'COST', 'WMT', 'DIS', 'ADBE', 'NFLX',
        'CRM', 'INTC', 'CSCO', 'VZ', 'T', 'XOM', 'AMD', 'QCOM', 'TXN',
        'HON', 'UPS', 'IBM', 'GE', 'CAT', 'BA', 'MMM', 'WFC', 'GS', 'MS',
        'C', 'BAC', 'ORCL', 'CL'
    ]
    
    logger.info(f"Populating market cap for {len(top_volume_symbols)} high-volume stocks...")
    
    # Use our market cap computation script
    import subprocess
    import os
    
    # Set environment variables
    env_vars = os.environ.copy()
    env_vars.update({
        'DB_HOST': 'localhost',
        'DB_PORT': '5433', 
        'DB_USER': 'postgres',
        'DB_PASSWORD': 'postgres',
        'DB_NAME': 'dev_db',
        'ENVIRONMENT': 'dev',
        'PYTHONPATH': 'src'
    })
    
    # Run the market cap computation
    symbols_str = ','.join(top_volume_symbols)
    cmd = [
        'python', 'src/secmaster/compute_market_cap_from_shares.py',
        '--environment', 'dev',
        '--symbols', symbols_str,
        '--days_back', '10',
        '--debug'
    ]
    
    try:
        result = subprocess.run(cmd, env=env_vars, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info("✅ Market cap data populated successfully")
        else:
            logger.warning(f"Market cap population had issues: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("Market cap population timed out")
    except Exception as e:
        logger.error(f"Failed to populate market cap data: {e}")


if __name__ == "__main__":
    success = asyncio.run(test_real_market_cap_universe())
    exit(0 if success else 1)