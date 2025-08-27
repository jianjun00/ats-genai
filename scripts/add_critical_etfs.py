#!/usr/bin/env python3
"""
Add Critical ETFs to dev_instruments

Adds the critical ETFs mentioned in the documentation to the dev_instruments table.
These ETFs are essential for factor-based strategies, market exposure, and diversification.

Based on:
- docs/archive/mass-reduction-20250824/ml-platform/prd/PRD_ETF_Selection_Strategy.md
- docs/projects/30year-price-history/PRD_30_Year_Daily_Price_History.md

Environment Variables:
- DRY_RUN: Set to 'true' to preview what would be added without executing
"""

import os
import sys
import asyncio
import asyncpg
import logging
from datetime import datetime
from typing import List, Dict, Any

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CriticalETFPopulator:
    """Add critical ETFs to dev_instruments table"""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.start_time = datetime.now()
        
        # Define critical ETFs from documentation
        self.critical_etfs = [
            # Priority 1 ETFs (Core Holdings) - from PRD_ETF_Selection_Strategy.md
            
            # Broad Market Coverage
            {"symbol": "SPY", "name": "SPDR S&P 500 ETF", "exchange": "NYSE ARCA", "category": "Broad Market", "priority": 1},
            {"symbol": "QQQ", "name": "Invesco QQQ Trust", "exchange": "NASDAQ", "category": "Broad Market", "priority": 1},
            {"symbol": "VTI", "name": "Vanguard Total Stock Market ETF", "exchange": "NYSE ARCA", "category": "Broad Market", "priority": 1},
            {"symbol": "DIA", "name": "SPDR Dow Jones Industrial Average ETF", "exchange": "NYSE ARCA", "category": "Broad Market", "priority": 1},
            
            # Value Factor Exposure
            {"symbol": "IWD", "name": "iShares Russell 1000 Value ETF", "exchange": "NYSE ARCA", "category": "Value Factor", "priority": 1},
            {"symbol": "VTV", "name": "Vanguard Value ETF", "exchange": "NYSE ARCA", "category": "Value Factor", "priority": 1},
            {"symbol": "IWN", "name": "iShares Russell 2000 Value ETF", "exchange": "NYSE ARCA", "category": "Value Factor", "priority": 1},
            {"symbol": "VBR", "name": "Vanguard Small-Cap Value ETF", "exchange": "NYSE ARCA", "category": "Value Factor", "priority": 1},
            
            # Size and Factor Completion
            {"symbol": "IWM", "name": "iShares Russell 2000 ETF", "exchange": "NYSE ARCA", "category": "Small Cap", "priority": 1},
            {"symbol": "MTUM", "name": "iShares MSCI USA Momentum Factor ETF", "exchange": "NYSE ARCA", "category": "Momentum Factor", "priority": 1},
            
            # Growth Exposure
            {"symbol": "VUG", "name": "Vanguard Growth ETF", "exchange": "NYSE ARCA", "category": "Growth Factor", "priority": 1},
            {"symbol": "IVV", "name": "iShares Core S&P 500 ETF", "exchange": "NYSE ARCA", "category": "Broad Market", "priority": 1},
            
            # Fixed Income Core
            {"symbol": "TLT", "name": "iShares 20+ Year Treasury Bond ETF", "exchange": "NASDAQ", "category": "Fixed Income", "priority": 1},
            {"symbol": "IEF", "name": "iShares 7-10 Year Treasury Bond ETF", "exchange": "NASDAQ", "category": "Fixed Income", "priority": 1},
            {"symbol": "LQD", "name": "iShares iBoxx Investment Grade Corporate Bond ETF", "exchange": "NYSE ARCA", "category": "Fixed Income", "priority": 1},
            
            # Dividend Strategy
            {"symbol": "SCHD", "name": "Schwab US Dividend Equity ETF", "exchange": "NYSE ARCA", "category": "Dividend", "priority": 1},
            
            # Commodities
            {"symbol": "GLD", "name": "SPDR Gold Shares", "exchange": "NYSE ARCA", "category": "Commodities", "priority": 1},
            {"symbol": "SLV", "name": "iShares Silver Trust", "exchange": "NYSE ARCA", "category": "Commodities", "priority": 1},
            
            # Priority 2 ETFs (Important Diversifiers)
            
            # High Yield Bonds
            {"symbol": "HYG", "name": "iShares iBoxx High Yield Corporate Bond ETF", "exchange": "NYSE ARCA", "category": "High Yield Bond", "priority": 2},
            {"symbol": "JNK", "name": "SPDR Bloomberg High Yield Bond ETF", "exchange": "NYSE ARCA", "category": "High Yield Bond", "priority": 2},
            
            # Alternative Assets
            {"symbol": "UUP", "name": "Invesco DB US Dollar Index Bullish Fund", "exchange": "NYSE ARCA", "category": "Currency", "priority": 2},
            {"symbol": "USO", "name": "United States Oil Fund LP", "exchange": "NYSE ARCA", "category": "Commodities", "priority": 2},
            {"symbol": "DBA", "name": "Invesco DB Agriculture Fund", "exchange": "NYSE ARCA", "category": "Commodities", "priority": 2},
            
            # Currency ETFs - from PRD docs
            {"symbol": "DXY", "name": "Invesco DB US Dollar Index Bullish Fund", "exchange": "NYSE ARCA", "category": "Currency", "priority": 2},
            {"symbol": "FXE", "name": "Invesco CurrencyShares Euro Trust", "exchange": "NYSE ARCA", "category": "Currency", "priority": 2},
            {"symbol": "FXY", "name": "Invesco CurrencyShares Japanese Yen Trust", "exchange": "NYSE ARCA", "category": "Currency", "priority": 2},
            
            # Sector ETFs - SPDR Sector Suite
            {"symbol": "XLK", "name": "Technology Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLF", "name": "Financial Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLE", "name": "Energy Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLV", "name": "Health Care Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLI", "name": "Industrial Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLU", "name": "Utilities Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLP", "name": "Consumer Staples Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLY", "name": "Consumer Discretionary Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLB", "name": "Materials Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            {"symbol": "XLRE", "name": "Real Estate Select Sector SPDR Fund", "exchange": "NYSE ARCA", "category": "Sector", "priority": 2},
            
            # International Exposure
            {"symbol": "VEA", "name": "Vanguard FTSE Developed Markets ETF", "exchange": "NYSE ARCA", "category": "International", "priority": 2},
            {"symbol": "VWO", "name": "Vanguard FTSE Emerging Markets ETF", "exchange": "NYSE ARCA", "category": "International", "priority": 2},
            
            # Priority 3 ETFs (Specialized)
            {"symbol": "SJNK", "name": "SPDR Bloomberg Short Term High Yield Bond ETF", "exchange": "NYSE ARCA", "category": "High Yield Bond", "priority": 3},
            {"symbol": "BKLN", "name": "Invesco Senior Loan ETF", "exchange": "NYSE ARCA", "category": "High Yield Bond", "priority": 3},
        ]
        
        # Statistics tracking
        self.stats = {
            'total_etfs': len(self.critical_etfs),
            'already_exists': 0,
            'added_new': 0,
            'updated_existing': 0,
            'errors': 0
        }

    async def analyze_existing_etfs(self, pool: asyncpg.Pool) -> None:
        """Analyze existing ETFs in dev_instruments"""
        logger.info("📊 Analyzing existing ETFs in dev_instruments...")
        
        async with pool.acquire() as conn:
            # Get total instrument count
            total_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            
            # Get existing ETF count
            etf_count = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE type = 'ETF' OR name ILIKE '%ETF%' OR name ILIKE '%Fund%'
            """)
            
            # Get ETFs that would be added
            critical_symbols = [etf['symbol'] for etf in self.critical_etfs]
            symbols_sql = "', '".join(critical_symbols)
            existing_critical = await conn.fetch(f"""
                SELECT symbol, name, exchange, type 
                FROM dev_instruments 
                WHERE symbol IN ('{symbols_sql}')
                ORDER BY symbol
            """)
            
            # Get breakdown by category
            category_counts = {}
            for etf in self.critical_etfs:
                category = etf['category']
                category_counts[category] = category_counts.get(category, 0) + 1
            
            logger.info("=" * 70)
            logger.info("📈 ETF ANALYSIS SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Total instruments in dev_instruments: {total_count:,}")
            logger.info(f"Current ETF-like instruments: {etf_count:,}")
            logger.info(f"Critical ETFs to add: {self.stats['total_etfs']:,}")
            logger.info(f"Already exist: {len(existing_critical):,}")
            logger.info(f"New ETFs to add: {self.stats['total_etfs'] - len(existing_critical):,}")
            logger.info("")
            
            logger.info("📊 CRITICAL ETF CATEGORIES:")
            for category, count in sorted(category_counts.items()):
                logger.info(f"  ✅ {category:20} {count:3,} ETFs")
            
            logger.info("")
            logger.info("🔍 EXISTING CRITICAL ETFs:")
            for row in existing_critical:
                symbol = row['symbol']
                name = (row['name'] or '')[:40] + ('...' if len(row['name'] or '') > 40 else '')
                exchange = row['exchange'] or 'Unknown'
                etf_type = row['type'] or 'Unknown'
                logger.info(f"  ✅ {symbol:6} {name:45} {exchange:10} {etf_type}")
            
            logger.info("=" * 70)

    async def add_critical_etfs(self, pool: asyncpg.Pool) -> None:
        """Add critical ETFs to dev_instruments table"""
        logger.info("🚀 Adding critical ETFs to dev_instruments...")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN MODE - No actual additions will occur")
            self.stats['added_new'] = self.stats['total_etfs'] - self.stats['already_exists']
            return
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                for etf in self.critical_etfs:
                    try:
                        # Check if ETF already exists
                        existing = await conn.fetchrow(
                            "SELECT id, symbol, name FROM dev_instruments WHERE symbol = $1",
                            etf['symbol']
                        )
                        
                        if existing:
                            # Update existing ETF to ensure it's marked as ETF type
                            await conn.execute("""
                                UPDATE dev_instruments 
                                SET name = $2, exchange = $3, type = 'ETF', 
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE symbol = $1
                            """, etf['symbol'], etf['name'], etf['exchange'])
                            
                            self.stats['updated_existing'] += 1
                            logger.info(f"  🔄 Updated: {etf['symbol']} - {etf['name'][:50]}...")
                        else:
                            # Insert new ETF (simpler approach without ON CONFLICT)
                            await conn.execute("""
                                INSERT INTO dev_instruments (symbol, name, exchange, type, active, currency, created_at, updated_at)
                                VALUES ($1, $2, $3, 'ETF', true, 'USD', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """, etf['symbol'], etf['name'], etf['exchange'])
                            
                            self.stats['added_new'] += 1
                            logger.info(f"  ✅ Added: {etf['symbol']} - {etf['name'][:50]}...")
                    
                    except Exception as e:
                        self.stats['errors'] += 1
                        logger.error(f"  ❌ Error adding {etf['symbol']}: {e}")
        
        logger.info(f"✅ ETF addition completed: {self.stats['added_new']} new, {self.stats['updated_existing']} updated")

    async def verify_etf_additions(self, pool: asyncpg.Pool) -> None:
        """Verify the ETF additions"""
        logger.info("✅ Verifying ETF additions...")
        
        async with pool.acquire() as conn:
            # Get final ETF count
            total_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            etf_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments WHERE type = 'ETF'")
            
            # Get critical ETFs that now exist
            critical_symbols = [etf['symbol'] for etf in self.critical_etfs]
            symbols_sql = "', '".join(critical_symbols)
            critical_found = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE symbol IN ('{symbols_sql}')
            """)
            
            # Get breakdown by category
            critical_by_category = await conn.fetch(f"""
                SELECT 
                    symbol,
                    name,
                    exchange,
                    type
                FROM dev_instruments 
                WHERE symbol IN ('{symbols_sql}')
                ORDER BY symbol
            """)
            
            # Get sample of added ETFs (recent ones)
            sample_etfs = await conn.fetch("""
                SELECT symbol, name, exchange, type, created_at
                FROM dev_instruments 
                WHERE type = 'ETF'
                ORDER BY created_at DESC
                LIMIT 10
            """)
            
            logger.info("=" * 70)
            logger.info("✅ ETF ADDITION VERIFICATION")
            logger.info("=" * 70)
            logger.info(f"Total instruments: {total_instruments:,}")
            logger.info(f"Total ETFs: {etf_count:,}")
            logger.info(f"Critical ETFs found: {critical_found:,} / {self.stats['total_etfs']:,}")
            logger.info(f"Success rate: {(critical_found / self.stats['total_etfs'] * 100):.1f}%")
            logger.info("")
            
            logger.info("📊 CRITICAL ETF STATUS:")
            for etf_data in self.critical_etfs:
                symbol = etf_data['symbol']
                category = etf_data['category']
                priority = etf_data['priority']
                
                found = any(row['symbol'] == symbol for row in critical_by_category)
                status = "✅" if found else "❌"
                logger.info(f"  {status} {symbol:6} {category:20} Priority {priority}")
            
            logger.info("")
            logger.info("🆕 RECENTLY ADDED ETFs:")
            for row in sample_etfs[:5]:
                symbol = row['symbol']
                name = (row['name'] or '')[:40] + ('...' if len(row['name'] or '') > 40 else '')
                exchange = row['exchange'] or 'Unknown'
                logger.info(f"  ✅ {symbol:6} {name:45} {exchange}")
            
            logger.info("=" * 70)

    def log_summary(self):
        """Log final summary of ETF addition operation"""
        elapsed = datetime.now() - self.start_time
        
        logger.info("=" * 70)
        logger.info("🎉 CRITICAL ETF ADDITION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"⏱️  Total Time: {elapsed}")
        logger.info(f"🏃 Dry Run Mode: {self.dry_run}")
        logger.info("")
        logger.info("📊 OPERATION SUMMARY:")
        logger.info(f"  Total ETFs Processed: {self.stats['total_etfs']:,}")
        logger.info(f"  New ETFs Added: {self.stats['added_new']:,}")
        logger.info(f"  Existing ETFs Updated: {self.stats['updated_existing']:,}")
        logger.info(f"  Errors Encountered: {self.stats['errors']:,}")
        logger.info("")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN COMPLETED - No actual changes made")
        else:
            logger.info("✅ ADDITION COMPLETED - Critical ETFs now available")
            
        logger.info("=" * 70)

async def main():
    """Main execution function"""
    
    # Configuration
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    logger.info("🚀 Starting Critical ETF Addition")
    logger.info(f"🏃 Dry Run Mode: {dry_run}")
    
    if dry_run:
        logger.info("⚠️ DRY RUN MODE - No actual changes will be made")
    else:
        logger.info("⚠️ LIVE MODE - ETFs will be added to dev_instruments")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        populator = CriticalETFPopulator(dry_run=dry_run)
        
        # Step 1: Analyze existing ETFs
        await populator.analyze_existing_etfs(pool)
        
        # Step 2: Add critical ETFs
        await populator.add_critical_etfs(pool)
        
        # Step 3: Verify additions
        await populator.verify_etf_additions(pool)
        
        # Step 4: Log final summary
        populator.log_summary()
        
        await pool.close()
        return 0
        
    except Exception as e:
        logger.error(f"❌ ETF addition failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)