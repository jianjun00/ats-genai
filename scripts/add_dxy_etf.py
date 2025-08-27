#!/usr/bin/env python3
"""
Add Missing DXY ETF

Simple script to add the one missing critical ETF: DXY (US Dollar Index Bullish Fund)
"""

import os
import sys
import asyncio
import asyncpg
import logging

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def add_dxy_etf():
    """Add DXY ETF to dev_instruments"""
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with pool.acquire() as conn:
            # Check if DXY already exists
            existing = await conn.fetchrow("SELECT symbol FROM dev_instruments WHERE symbol = 'DXY'")
            
            if existing:
                logger.info("✅ DXY already exists in dev_instruments")
                return
            
            # Add DXY ETF
            await conn.execute("""
                INSERT INTO dev_instruments (symbol, name, exchange, type, active, currency, created_at, updated_at)
                VALUES ('DXY', 'Invesco DB US Dollar Index Bullish Fund', 'NYSE ARCA', 'ETF', true, 'USD', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """)
            
            logger.info("✅ Added DXY ETF to dev_instruments")
            
            # Verify addition
            final_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments WHERE symbol = 'DXY'")
            if final_count > 0:
                logger.info("✅ DXY ETF successfully verified in database")
            else:
                logger.error("❌ DXY ETF not found after insertion")
        
        await pool.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to add DXY ETF: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(add_dxy_etf())