"""
ETF Instrument Population Strategy

Adds major ETFs to the ATS platform instrument universe for comprehensive
market coverage including equities, bonds, commodities, and currency exposure.

Based on ETF coverage research showing 100% daily and intraday data availability
across Polygon, Tiingo, and EODHD vendors.
"""

import asyncio
import asyncpg
from datetime import datetime
from typing import Dict, List


# Major ETFs with metadata for ATS platform
MAJOR_ETFS = [
    {
        "symbol": "SPY",
        "name": "SPDR S&P 500 ETF Trust",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Broad Market",
        "sector": "Equity",
        "description": "Tracks S&P 500 index - most liquid ETF",
        "expense_ratio": 0.0945,
        "aum_billions": 400.0,
        "inception_date": "1993-01-22",
        "priority": 1  # Highest priority
    },
    {
        "symbol": "QQQ",
        "name": "Invesco QQQ Trust Series 1",
        "exchange": "NASDAQ",
        "instrument_type": "ETF", 
        "category": "Technology",
        "sector": "Equity",
        "description": "Tracks Nasdaq-100 index - tech exposure",
        "expense_ratio": 0.20,
        "aum_billions": 200.0,
        "inception_date": "1999-03-10",
        "priority": 1
    },
    {
        "symbol": "GLD",
        "name": "SPDR Gold Shares",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Commodities",
        "sector": "Precious Metals", 
        "description": "Physical gold exposure - inflation hedge",
        "expense_ratio": 0.40,
        "aum_billions": 60.0,
        "inception_date": "2004-11-18",
        "priority": 1
    },
    {
        "symbol": "TLT",
        "name": "iShares 20+ Year Treasury Bond ETF",
        "exchange": "NASDAQ",
        "instrument_type": "ETF",
        "category": "Fixed Income",
        "sector": "Government Bonds",
        "description": "Long-term Treasury bonds - interest rate hedge",
        "expense_ratio": 0.15,
        "aum_billions": 45.0,
        "inception_date": "2002-07-22",
        "priority": 1
    },
    {
        "symbol": "UUP",
        "name": "Invesco DB US Dollar Index Bullish Fund",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Currency",
        "sector": "Dollar",
        "description": "US Dollar strength - currency hedge",
        "expense_ratio": 0.75,
        "aum_billions": 1.2,
        "inception_date": "2007-02-20",
        "priority": 2
    },
    {
        "symbol": "USO",
        "name": "United States Oil Fund LP",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Commodities", 
        "sector": "Energy",
        "description": "Crude oil exposure - energy sector hedge",
        "expense_ratio": 0.72,
        "aum_billions": 2.5,
        "inception_date": "2006-04-10",
        "priority": 2
    },
    {
        "symbol": "HYG",
        "name": "iShares iBoxx $ High Yield Corporate Bond ETF",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Fixed Income",
        "sector": "Corporate Bonds",
        "description": "High-yield corporate bonds - credit exposure",
        "expense_ratio": 0.49,
        "aum_billions": 15.0,
        "inception_date": "2007-04-04",
        "priority": 2
    },
    {
        "symbol": "JNK", 
        "name": "SPDR Bloomberg High Yield Bond ETF",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Fixed Income",
        "sector": "Corporate Bonds",
        "description": "Alternative high-yield bond exposure",
        "expense_ratio": 0.40,
        "aum_billions": 8.0,
        "inception_date": "2007-11-28",
        "priority": 3
    },
    {
        "symbol": "IEF",
        "name": "iShares 7-10 Year Treasury Bond ETF", 
        "exchange": "NASDAQ",
        "instrument_type": "ETF",
        "category": "Fixed Income",
        "sector": "Government Bonds",
        "description": "Intermediate Treasury bonds - duration exposure",
        "expense_ratio": 0.15,
        "aum_billions": 20.0,
        "inception_date": "2002-07-22",
        "priority": 2
    },
    {
        "symbol": "VTI",
        "name": "Vanguard Total Stock Market ETF",
        "exchange": "NYSE",
        "instrument_type": "ETF",
        "category": "Broad Market",
        "sector": "Equity",
        "description": "Total US stock market exposure",
        "expense_ratio": 0.03,
        "aum_billions": 350.0,
        "inception_date": "2001-05-24", 
        "priority": 1
    }
]


class ETFInstrumentPopulator:
    """Populates ATS platform with major ETF instruments"""
    
    def __init__(self, db_connection: asyncpg.Connection):
        self.conn = db_connection
        
    async def create_etf_table_if_needed(self):
        """Create ETF-specific table if it doesn't exist"""
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_etf_instruments (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) UNIQUE NOT NULL,
                name TEXT NOT NULL,
                exchange VARCHAR(20),
                instrument_type VARCHAR(50) DEFAULT 'ETF',
                category VARCHAR(50),
                sector VARCHAR(50),
                description TEXT,
                expense_ratio DECIMAL(6,4),
                aum_billions DECIMAL(10,2),
                inception_date DATE,
                priority INTEGER DEFAULT 3,
                is_active BOOLEAN DEFAULT true,
                data_sources TEXT[] DEFAULT ARRAY['polygon', 'tiingo', 'eodhd'],
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create indexes for performance
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_etf_symbol ON dev_etf_instruments(symbol)
        """)
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_etf_category ON dev_etf_instruments(category)
        """)
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_etf_priority ON dev_etf_instruments(priority)
        """)
        
    async def insert_etf_instruments(self) -> int:
        """Insert ETF instruments into database"""
        
        insert_query = """
            INSERT INTO dev_etf_instruments (
                symbol, name, exchange, instrument_type, category, sector,
                description, expense_ratio, aum_billions, inception_date, priority
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                category = EXCLUDED.category,
                sector = EXCLUDED.sector,
                description = EXCLUDED.description,
                expense_ratio = EXCLUDED.expense_ratio,
                aum_billions = EXCLUDED.aum_billions,
                inception_date = EXCLUDED.inception_date,
                priority = EXCLUDED.priority,
                updated_at = NOW()
        """
        
        inserted_count = 0
        
        for etf in MAJOR_ETFS:
            try:
                # Parse inception date
                inception_date = datetime.strptime(etf['inception_date'], '%Y-%m-%d').date()
                
                await self.conn.execute(
                    insert_query,
                    etf['symbol'], etf['name'], etf['exchange'], etf['instrument_type'],
                    etf['category'], etf['sector'], etf['description'],
                    etf['expense_ratio'], etf['aum_billions'], inception_date, etf['priority']
                )
                
                inserted_count += 1
                print(f"✅ {etf['symbol']}: {etf['name']} (Priority {etf['priority']})")
                
            except Exception as e:
                print(f"❌ {etf['symbol']}: Failed to insert - {e}")
                
        return inserted_count
        
    async def add_etfs_to_main_instruments_table(self) -> int:
        """Add ETFs to main dev_instruments table for data collection"""
        
        insert_query = """
            INSERT INTO dev_instruments (
                symbol, name, exchange, instrument_type, currency, 
                is_active, data_sources, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, 'USD', true, $5, NOW(), NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                instrument_type = EXCLUDED.instrument_type,
                data_sources = EXCLUDED.data_sources,
                updated_at = NOW()
        """
        
        added_count = 0
        
        for etf in MAJOR_ETFS:
            try:
                await self.conn.execute(
                    insert_query,
                    etf['symbol'], etf['name'], etf['exchange'], 
                    etf['instrument_type'], ['polygon', 'tiingo', 'eodhd']
                )
                
                added_count += 1
                print(f"📊 {etf['symbol']}: Added to main instruments table")
                
            except Exception as e:
                print(f"❌ {etf['symbol']}: Failed to add to main table - {e}")
                
        return added_count
        
    async def validate_etf_population(self) -> Dict:
        """Validate ETF population and return statistics"""
        
        # ETF-specific table stats
        etf_stats = await self.conn.fetchrow("""
            SELECT 
                COUNT(*) as total_etfs,
                COUNT(CASE WHEN priority = 1 THEN 1 END) as priority_1,
                COUNT(CASE WHEN priority = 2 THEN 1 END) as priority_2,
                COUNT(CASE WHEN priority = 3 THEN 1 END) as priority_3,
                COUNT(CASE WHEN category = 'Broad Market' THEN 1 END) as broad_market,
                COUNT(CASE WHEN category = 'Fixed Income' THEN 1 END) as fixed_income,
                COUNT(CASE WHEN category = 'Commodities' THEN 1 END) as commodities
            FROM dev_etf_instruments
        """)
        
        # Main instruments table stats
        main_stats = await self.conn.fetchval("""
            SELECT COUNT(*) FROM dev_instruments 
            WHERE instrument_type = 'ETF' AND symbol = ANY($1)
        """, [etf['symbol'] for etf in MAJOR_ETFS])
        
        # Category breakdown
        categories = await self.conn.fetch("""
            SELECT category, COUNT(*) as count
            FROM dev_etf_instruments
            GROUP BY category
            ORDER BY count DESC
        """)
        
        return {
            'etf_table_stats': dict(etf_stats) if etf_stats else {},
            'main_table_count': main_stats,
            'categories': [(cat['category'], cat['count']) for cat in categories]
        }
        
    async def run_etf_population(self) -> Dict:
        """Run complete ETF population process"""
        
        print("🎯 Starting ETF Instrument Population")
        print("=" * 50)
        
        # Step 1: Create ETF table
        print("📊 Step 1: Creating ETF instruments table...")
        await self.create_etf_table_if_needed()
        print("✅ ETF table ready")
        
        # Step 2: Insert ETF data
        print("\n📊 Step 2: Inserting ETF instrument data...")
        etf_count = await self.insert_etf_instruments()
        print(f"✅ Inserted {etf_count} ETF instruments")
        
        # Step 3: Add to main instruments table  
        print("\n📊 Step 3: Adding ETFs to main instruments table...")
        main_count = await self.add_etfs_to_main_instruments_table()
        print(f"✅ Added {main_count} ETFs to main instruments")
        
        # Step 4: Validate population
        print("\n📊 Step 4: Validating ETF population...")
        validation_stats = await self.validate_etf_population()
        
        print("\n" + "=" * 50)
        print("🎉 ETF POPULATION COMPLETE")
        print("=" * 50)
        
        etf_stats = validation_stats['etf_table_stats']
        print(f"📊 ETF Instruments Table: {etf_stats.get('total_etfs', 0)} total ETFs")
        print(f"   Priority 1 (Core): {etf_stats.get('priority_1', 0)}")
        print(f"   Priority 2 (Important): {etf_stats.get('priority_2', 0)}")
        print(f"   Priority 3 (Additional): {etf_stats.get('priority_3', 0)}")
        
        print(f"\n💼 Asset Class Distribution:")
        for category, count in validation_stats['categories']:
            print(f"   {category}: {count} ETFs")
            
        print(f"\n📈 Main Instruments Table: {validation_stats['main_table_count']} ETFs ready for data collection")
        
        return validation_stats


async def populate_etfs_via_dev_cli():
    """Populate ETFs using dev environment database connection"""
    
    try:
        # Connect to dev database (same as existing jobs)
        conn = await asyncpg.connect(
            host='postgres',  # K8s service name
            port=5432,
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        
        populator = ETFInstrumentPopulator(conn)
        results = await populator.run_etf_population()
        
        await conn.close()
        
        print("\n🎯 Next Steps:")
        print("1. Run 30-year price backfill for ETFs using existing jobs")
        print("2. Update checkpoint jobs to include ETF symbols") 
        print("3. Validate ETF data collection across all vendors")
        
        return results
        
    except Exception as e:
        print(f"💥 ETF population failed: {e}")
        raise


if __name__ == "__main__":
    # Run ETF population
    results = asyncio.run(populate_etfs_via_dev_cli())
    print("\n🎉 ETF Population Complete!")
    print("Use: python scripts/etf_population_strategy.py")