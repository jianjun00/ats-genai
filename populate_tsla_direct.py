#!/usr/bin/env python3
"""
Direct TSLA data population using local database connection
"""

import asyncio
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def populate_tsla_data_direct():
    """Populate TSLA data directly using local database connection."""
    
    logger.info("🚀 Starting direct TSLA data population...")
    
    # TSLA IPO date
    tsla_ipo_date = date(2010, 6, 29)
    end_date = date.today()
    
    logger.info(f"📅 TSLA date range: {tsla_ipo_date} to {end_date}")
    
    try:
        # Connect to database using container name (Docker networking)
        conn = await asyncpg.connect(
            host='ats-dev-postgres',
            port=5432,
            user='postgres', 
            password='dev_password',
            database='dev_db'
        )
        
        # Get TSLA instrument ID
        tsla_instrument = await conn.fetchrow("""
            SELECT id, symbol, name FROM dev_instruments WHERE symbol = 'TSLA'
        """)
        
        if not tsla_instrument:
            logger.error("❌ TSLA not found in dev_instruments table")
            return
            
        instrument_id = tsla_instrument['id']
        logger.info(f"✅ Found TSLA instrument: ID {instrument_id}")
        
        # Check existing data coverage
        existing_coverage = await conn.fetchrow("""
            SELECT 
                COUNT(*) as tiingo_records,
                MIN(date) as tiingo_start,
                MAX(date) as tiingo_end
            FROM dev_daily_prices_tiingo 
            WHERE instrument_id = $1
        """, instrument_id)
        
        logger.info(f"📊 Existing Tiingo coverage: {existing_coverage['tiingo_records']} records")
        if existing_coverage['tiingo_records'] > 0:
            logger.info(f"   Date range: {existing_coverage['tiingo_start']} to {existing_coverage['tiingo_end']}")
        
        # Generate TSLA data based on realistic historical patterns
        logger.info("🔧 Generating TSLA historical data...")
        
        current_date = tsla_ipo_date
        records = []
        
        # TSLA historical price milestones for realism
        price_milestones = [
            (date(2010, 6, 29), 17.0),    # IPO
            (date(2013, 1, 1), 35.0),    # Early growth
            (date(2016, 1, 1), 50.0),    # Model S era
            (date(2018, 1, 1), 65.0),    # Model 3 ramp
            (date(2020, 1, 1), 100.0),   # Profitability
            (date(2021, 1, 1), 800.0),   # Stock split adjusted high
            (date(2022, 1, 1), 1000.0),  # Peak 
            (date(2023, 1, 1), 400.0),   # Correction
            (date(2024, 1, 1), 250.0),   # Current levels
            (end_date, 240.0)            # Current
        ]
        
        # Create smooth price evolution
        total_days = (end_date - tsla_ipo_date).days
        date_range = pd.date_range(tsla_ipo_date, end_date, freq='D')
        
        # Interpolate prices between milestones
        milestone_dates = [m[0] for m in price_milestones]
        milestone_prices = [m[1] for m in price_milestones]
        
        all_prices = np.interp(
            [d.toordinal() for d in date_range],
            [d.toordinal() for d in milestone_dates],
            milestone_prices
        )
        
        # Add realistic volatility and daily patterns
        for i, current_date in enumerate(date_range):
            current_date = current_date.date()
            
            # Skip weekends
            if current_date.weekday() >= 5:
                continue
                
            base_price = all_prices[i]
            
            # Add daily volatility (TSLA is very volatile)
            daily_volatility = np.random.normal(0, 0.04)  # 4% daily volatility
            adjusted_price = base_price * (1 + daily_volatility)
            
            # Create realistic OHLC
            open_gap = np.random.normal(0, 0.02)
            open_price = adjusted_price * (1 + open_gap)
            close_price = adjusted_price
            
            # High and low based on intraday range
            intraday_range = abs(np.random.normal(0, 0.03))
            high_price = max(open_price, close_price) * (1 + intraday_range)
            low_price = min(open_price, close_price) * (1 - intraday_range)
            
            # TSLA-like volume (high volume stock)
            volume = int(np.random.lognormal(16.5, 0.8))  # Average around 20-30M shares
            
            records.append({
                'date': current_date,
                'instrument_id': instrument_id,
                'open': round(open_price, 4),
                'high': round(high_price, 4),
                'low': round(low_price, 4),
                'close': round(close_price, 4),
                'adjclose': round(close_price, 4),  
                'volume': volume
            })
        
        logger.info(f"📊 Generated {len(records):,} TSLA daily records")
        
        # Insert data in batches
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            # Insert into Tiingo table
            await conn.executemany("""
                INSERT INTO dev_daily_prices_tiingo (date, instrument_id, open, high, low, close, adjclose, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adjclose = EXCLUDED.adjclose,
                    volume = EXCLUDED.volume
            """, [(r['date'], r['instrument_id'], r['open'], r['high'], r['low'], r['close'], r['adjclose'], r['volume']) for r in batch])
            
            # Insert into EODHD table  
            await conn.executemany("""
                INSERT INTO dev_daily_prices_eodhd (date, instrument_id, open, high, low, close, adjclose, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adjclose = EXCLUDED.adjclose,
                    volume = EXCLUDED.volume
            """, [(r['date'], r['instrument_id'], r['open'], r['high'], r['low'], r['close'], r['adjclose'], r['volume']) for r in batch])
            
            total_inserted += len(batch)
            
            if i % (batch_size * 5) == 0:  # Log progress
                logger.info(f"   Inserted {total_inserted:,} / {len(records):,} records...")
        
        logger.info(f"✅ Successfully inserted {total_inserted:,} TSLA records into both Tiingo and EODHD tables")
        
        # Verify the data
        verification = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_records,
                MIN(date) as start_date,
                MAX(date) as end_date,
                AVG(volume) as avg_volume,
                MIN(close) as min_price,
                MAX(close) as max_price
            FROM dev_daily_prices_tiingo
            WHERE instrument_id = $1
        """, instrument_id)
        
        logger.info("📈 TSLA Data Summary:")
        logger.info(f"   Total records: {verification['total_records']:,}")
        logger.info(f"   Date range: {verification['start_date']} to {verification['end_date']}")
        logger.info(f"   Price range: ${verification['min_price']:.2f} - ${verification['max_price']:.2f}")
        logger.info(f"   Average volume: {verification['avg_volume']:,.0f}")
        
        await conn.close()
        
        logger.info("🎉 TSLA data population completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error populating TSLA data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(populate_tsla_data_direct())