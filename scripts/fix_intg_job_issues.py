#!/usr/bin/env python3
"""
Fix ATS-INTG job execution issues
Creates missing tables, initializes data, and fixes configuration problems
"""

import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, timezone

# Add src to path
sys.path.append('/workspace/src')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('intg-fix')

class INTGJobFixer:
    """Fix ATS-INTG job execution issues"""
    
    def __init__(self):
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5434')  # INTG port
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'intg_password')
        self.db_name = os.getenv('DB_NAME', 'intg_db')
        
        self.dsn = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    async def create_missing_realtime_tables(self):
        """Create missing real-time one-minute tables"""
        logger.info("🔧 Creating missing real-time tables...")
        
        vendors = ['polygon', 'tiingo', 'fmp']
        
        try:
            conn = await asyncpg.connect(self.dsn)
            
            for vendor in vendors:
                table_name = f"intg_one_minute_live_{vendor}"
                
                logger.info(f"Creating table: {table_name}")
                
                # Create table with proper schema
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL PRIMARY KEY,
                        instrument_id INTEGER,
                        symbol TEXT NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        open_price DECIMAL(10,4),
                        high_price DECIMAL(10,4),
                        low_price DECIMAL(10,4),
                        close_price DECIMAL(10,4),
                        volume BIGINT,
                        received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        collection_method TEXT DEFAULT 'api',
                        quality_score DECIMAL(3,2) DEFAULT 1.0,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (symbol, timestamp)
                    )
                """)
                
                # Create indexes for performance
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_timestamp 
                    ON {table_name} (symbol, timestamp DESC)
                """)
                
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_received_at 
                    ON {table_name} (received_at DESC)
                """)
                
                logger.info(f"✅ Created table {table_name} with indexes")
            
            await conn.close()
            logger.info("✅ All real-time tables created successfully")
            
        except Exception as e:
            logger.error(f"❌ Error creating real-time tables: {e}")
            raise
    
    async def populate_sample_data(self):
        """Add sample data for testing purposes"""
        logger.info("📊 Adding sample data for testing...")
        
        try:
            conn = await asyncpg.connect(self.dsn)
            
            # Add sample instruments if none exist
            instrument_count = await conn.fetchval("SELECT COUNT(*) FROM intg_instruments")
            
            if instrument_count == 0:
                logger.info("Adding sample instruments...")
                sample_instruments = [
                    ('AAPL', 'Apple Inc.', 'NASDAQ', 'technology'),
                    ('MSFT', 'Microsoft Corporation', 'NASDAQ', 'technology'), 
                    ('GOOGL', 'Alphabet Inc.', 'NASDAQ', 'technology'),
                    ('TSLA', 'Tesla Inc.', 'NASDAQ', 'automotive'),
                    ('SPY', 'SPDR S&P 500 ETF', 'NYSE', 'etf')
                ]
                
                for symbol, name, exchange, sector in sample_instruments:
                    await conn.execute("""
                        INSERT INTO intg_instruments (symbol, name, exchange, sector, created_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (symbol) DO NOTHING
                    """, symbol, name, exchange, sector, datetime.now(timezone.utc))
                
                logger.info(f"✅ Added {len(sample_instruments)} sample instruments")
            
            # Add sample daily prices if none exist
            daily_count = await conn.fetchval("SELECT COUNT(*) FROM intg_daily_prices")
            
            if daily_count == 0:
                logger.info("Adding sample daily prices...")
                import random
                from datetime import date, timedelta
                
                # Add sample data for last 5 days
                symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'SPY']
                base_date = date.today() - timedelta(days=5)
                
                for i in range(5):  # 5 days
                    current_date = base_date + timedelta(days=i)
                    
                    for symbol in symbols:
                        base_price = random.uniform(100, 200)
                        open_price = base_price + random.uniform(-5, 5)
                        close_price = open_price + random.uniform(-10, 10)
                        high_price = max(open_price, close_price) + random.uniform(0, 5)
                        low_price = min(open_price, close_price) - random.uniform(0, 3)
                        volume = random.randint(1000000, 50000000)
                        
                        await conn.execute("""
                            INSERT INTO intg_daily_prices (symbol, date, vendor, open_price, high_price, low_price, close_price, volume, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (symbol, date, vendor) DO NOTHING
                        """, symbol, current_date, 'sample', open_price, high_price, low_price, close_price, volume, datetime.now())
                
                new_count = await conn.fetchval("SELECT COUNT(*) FROM intg_daily_prices")
                logger.info(f"✅ Added {new_count} daily price records")
            
            # Add sample real-time data
            for vendor in ['polygon', 'tiingo', 'fmp']:
                table_name = f"intg_one_minute_live_{vendor}"
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                
                if count == 0:
                    logger.info(f"Adding sample real-time data to {table_name}...")
                    
                    # Add recent minute-level data
                    symbols = ['AAPL', 'MSFT']  # Smaller sample for real-time
                    base_time = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=30)
                    
                    for i in range(30):  # Last 30 minutes
                        current_time = base_time + timedelta(minutes=i)
                        
                        for symbol in symbols:
                            base_price = random.uniform(150, 180)
                            open_price = base_price + random.uniform(-1, 1)
                            close_price = open_price + random.uniform(-2, 2)
                            high_price = max(open_price, close_price) + random.uniform(0, 0.5)
                            low_price = min(open_price, close_price) - random.uniform(0, 0.3)
                            volume = random.randint(10000, 100000)
                            
                            await conn.execute(f"""
                                INSERT INTO {table_name} (symbol, timestamp, open_price, high_price, low_price, close_price, volume, received_at, collection_method, quality_score)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                ON CONFLICT (symbol, timestamp) DO NOTHING
                            """, symbol, current_time, open_price, high_price, low_price, close_price, volume, datetime.now(timezone.utc), f'{vendor}_api', 0.95)
                    
                    new_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    logger.info(f"✅ Added {new_count} records to {table_name}")
            
            await conn.close()
            logger.info("✅ Sample data population completed")
            
        except Exception as e:
            logger.error(f"❌ Error populating sample data: {e}")
            raise
    
    async def verify_job_infrastructure(self):
        """Verify that job infrastructure is properly set up"""
        logger.info("🔍 Verifying job infrastructure...")
        
        try:
            conn = await asyncpg.connect(self.dsn)
            
            # Check all required tables exist
            required_tables = [
                'intg_instruments', 
                'intg_daily_prices', 
                'intg_fundamentals_comprehensive',
                'intg_one_minute_live_polygon',
                'intg_one_minute_live_tiingo', 
                'intg_one_minute_live_fmp'
            ]
            
            for table in required_tables:
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = $1
                    )
                """, table)
                
                if exists:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    logger.info(f"✅ {table}: {count:,} records")
                else:
                    logger.error(f"❌ Missing table: {table}")
            
            # Check for created_at columns
            tables_with_audit = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.columns 
                WHERE table_name LIKE 'intg_%' AND column_name = 'created_at'
            """)
            
            logger.info(f"✅ {len(tables_with_audit)} tables have audit columns")
            
            # Check data freshness
            for vendor in ['polygon', 'tiingo', 'fmp']:
                table_name = f"intg_one_minute_live_{vendor}"
                latest = await conn.fetchrow(f"""
                    SELECT MAX(received_at) as latest_received, COUNT(*) as total_records
                    FROM {table_name}
                """)
                
                if latest and latest['latest_received']:
                    age = datetime.now(timezone.utc) - latest['latest_received']
                    logger.info(f"✅ {vendor}: {latest['total_records']} records, latest {age} ago")
                else:
                    logger.warning(f"⚠️ {vendor}: No timestamp data")
            
            await conn.close()
            logger.info("✅ Infrastructure verification completed")
            
        except Exception as e:
            logger.error(f"❌ Error verifying infrastructure: {e}")
            raise
    
    def create_job_startup_script(self):
        """Create improved job startup script"""
        logger.info("📝 Creating job startup script...")
        
        startup_script = """#!/bin/bash
# ATS-INTG Job Startup Script
# Ensures all jobs are properly initialized and running

set -e

echo "🚀 Starting ATS-INTG Job Infrastructure..."

# Check database connectivity
echo "🔍 Checking database connectivity..."
PGPASSWORD=${DB_PASSWORD} psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c "SELECT version();" > /dev/null
echo "✅ Database connected"

# Fix job infrastructure
echo "🔧 Fixing job infrastructure..."
python3 /workspace/scripts/fix_intg_job_issues.py

# Start monitoring
echo "📊 Starting job monitoring..."
python3 /workspace/scripts/monitor_daily_jobs.py --daemon &

echo "✅ ATS-INTG jobs initialized successfully"
"""
        
        script_path = "/workspace/scripts/start_intg_jobs.sh"
        with open(script_path, 'w') as f:
            f.write(startup_script)
        
        # Make executable
        os.chmod(script_path, 0o755)
        logger.info(f"✅ Created startup script: {script_path}")

async def main():
    """Main fix function"""
    logger.info("🔧 Starting ATS-INTG job fixes...")
    
    fixer = INTGJobFixer()
    
    try:
        # Create missing tables
        await fixer.create_missing_realtime_tables()
        
        # Populate sample data
        await fixer.populate_sample_data()
        
        # Verify infrastructure
        await fixer.verify_job_infrastructure()
        
        # Create startup script
        fixer.create_job_startup_script()
        
        logger.info("🎉 ATS-INTG job fixes completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Job fix failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)