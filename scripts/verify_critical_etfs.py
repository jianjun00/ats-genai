#!/usr/bin/env python3
"""
Verify Critical ETFs

Check that all critical ETFs from documentation are present in dev_instruments
"""

import sys
import asyncio
import logging

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def verify_critical_etfs():
    """Verify all critical ETFs are present"""
    
    # Critical ETFs from documentation
    critical_etfs = [
        # Priority 1 ETFs (Core Holdings)
        'SPY', 'QQQ', 'VTI', 'DIA',  # Broad Market
        'IWD', 'VTV', 'IWN', 'VBR',   # Value Factor
        'IWM', 'MTUM', 'VUG', 'IVV',  # Size and Growth
        'TLT', 'IEF', 'LQD',          # Fixed Income
        'SCHD',                       # Dividend
        'GLD', 'SLV',                 # Commodities
        
        # Priority 2 ETFs (Important Diversifiers)
        'HYG', 'JNK',                 # High Yield Bonds
        'UUP', 'USO', 'DBA', 'DXY',   # Alternative Assets & Currency
        'FXE', 'FXY',                 # Currency
        'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLU', 'XLP', 'XLY', 'XLB', 'XLRE',  # Sectors
        'VEA', 'VWO',                 # International
        
        # Priority 3 ETFs (Specialized)
        'SJNK', 'BKLN'                # High Yield Alternatives
    ]
    
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with pool.acquire() as conn:
            # Check each critical ETF
            present = []
            missing = []
            
            for symbol in critical_etfs:
                exists = await conn.fetchval(
                    "SELECT COUNT(*) FROM dev_instruments WHERE symbol = $1", symbol
                )
                if exists > 0:
                    present.append(symbol)
                else:
                    missing.append(symbol)
            
            # Get ETF details for present ones
            symbols_sql = "', '".join(present)
            etf_details = await conn.fetch(f"""
                SELECT symbol, name, exchange, type
                FROM dev_instruments 
                WHERE symbol IN ('{symbols_sql}')
                ORDER BY symbol
            """)
            
            # Get total ETF count
            total_etfs = await conn.fetchval(
                "SELECT COUNT(*) FROM dev_instruments WHERE type = 'ETF'"
            )
            
            logger.info("=" * 70)
            logger.info("🎯 CRITICAL ETF VERIFICATION RESULTS")
            logger.info("=" * 70)
            logger.info(f"Total critical ETFs required: {len(critical_etfs)}")
            logger.info(f"Critical ETFs present: {len(present)}")
            logger.info(f"Critical ETFs missing: {len(missing)}")
            logger.info(f"Coverage: {(len(present)/len(critical_etfs)*100):.1f}%")
            logger.info(f"Total ETFs in database: {total_etfs}")
            logger.info("")
            
            if missing:
                logger.info("❌ MISSING CRITICAL ETFs:")
                for symbol in missing:
                    logger.info(f"  ❌ {symbol}")
                logger.info("")
            
            logger.info("✅ PRESENT CRITICAL ETFs:")
            for row in etf_details:
                symbol = row['symbol']
                name = (row['name'] or '')[:45] + ('...' if len(row['name'] or '') > 45 else '')
                exchange = row['exchange'] or 'Unknown'
                etf_type = row['type'] or 'Unknown'
                logger.info(f"  ✅ {symbol:6} {name:50} {exchange:10} {etf_type}")
            
            logger.info("")
            if len(present) == len(critical_etfs):
                logger.info("🎉 ALL CRITICAL ETFs ARE PRESENT! Complete coverage achieved.")
            else:
                logger.warning(f"⚠️  {len(missing)} critical ETFs are still missing")
            
            logger.info("=" * 70)
        
        await pool.close()
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_critical_etfs())