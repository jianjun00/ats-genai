#!/usr/bin/env python3
"""
Critical ETF 30-Year Daily Price Backfill
Focused backfill for specific critical ETF symbols across all vendors
"""
import sys
sys.path.append('src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, date
import time

from core.shared.utils.vendor_api_keys import get_polygon_api_key, get_tiingo_api_key, get_eodhd_api_key
from core.shared.utils.backfill_framework import get_vendor_database_connection, VendorRateLimiters

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("critical_etf_backfill")

# Critical ETF symbols that are missing from vendors
CRITICAL_ETFS = {
    'polygon': ['DBA', 'EFA', 'EWZ', 'GLD', 'HYG', 'IEMG', 'IVV', 'IWM', 'JNK', 'LQD', 
                'SLV', 'SPXU', 'SPY', 'TIP', 'UNG', 'UPRO', 'USO', 'UUP', 'UVXY', 'VCIT',
                'VEA', 'VOO', 'VTI', 'VWO', 'VXX', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 
                'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY'],
    'eodhd': ['SLV', 'SPXU', 'SPY', 'TIP', 'UNG', 'UPRO', 'USO', 'UUP', 'VEA', 'VOO', 
              'VTI', 'VWO', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 
              'XLU', 'XLV', 'XLY'],
    'tiingo': []  # All critical ETFs already in Tiingo
}

class CriticalETFBackfiller:
    def __init__(self):
        self.start_date = date(1995, 1, 1)
        self.end_date = date(2025, 9, 13)
        
    async def get_instrument_id(self, conn, symbol):
        """Get instrument ID for a symbol"""
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'
        
        result = await conn.fetchrow(f"""
            SELECT id FROM {table_prefix}instrument 
            WHERE symbol = $1 AND active = true
        """, symbol)
        
        return result['id'] if result else None

    async def backfill_polygon_etfs(self):
        """Backfill missing critical ETFs for Polygon"""
        if not CRITICAL_ETFS['polygon']:
            logger.info("🎯 No missing ETFs for Polygon")
            return
            
        logger.info(f"🚀 Starting Polygon backfill for {len(CRITICAL_ETFS['polygon'])} critical ETFs")
        
        api_key = get_polygon_api_key()
        rate_limiter = VendorRateLimiters.polygon_free()
        conn = await get_vendor_database_connection()
        
        try:
            processed = 0
            inserted_total = 0
            
            for symbol in CRITICAL_ETFS['polygon']:
                instrument_id = await self.get_instrument_id(conn, symbol)
                if not instrument_id:
                    logger.warning(f"⚠️ Symbol {symbol} not found in instruments table")
                    continue
                
                logger.info(f"📈 Processing {symbol} (ID: {instrument_id})")
                
                # Check existing data
                env = os.getenv('ENV_TYPE', 'intg').lower()
                table_name = f'{env}_daily_prices_polygon'
                
                existing = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE instrument_id = $1
                """, instrument_id)
                
                if existing > 0:
                    logger.info(f"⏭️ Skipping {symbol} - already has {existing} records")
                    continue
                
                # Fetch from Polygon API
                url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{self.start_date}/{self.end_date}"
                params = {'adjusted': 'true', 'sort': 'asc', 'apikey': api_key}
                
                await rate_limiter.wait()
                
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get('status') != 'OK' or not data.get('results'):
                        logger.warning(f"⚠️ No data for {symbol}: {data.get('status', 'Unknown')}")
                        continue
                    
                    # Insert price data
                    rows = []
                    for result in data['results']:
                        price_date = date.fromtimestamp(result['t'] / 1000)
                        rows.append((
                            price_date,
                            symbol,
                            float(result['o']),  # open
                            float(result['h']),  # high
                            float(result['l']),  # low
                            float(result['c']),  # close
                            int(result['v']),    # volume
                            instrument_id
                        ))
                    
                    if rows:
                        inserted = await conn.executemany(f"""
                            INSERT INTO {table_name} 
                            (date, symbol, open, high, low, close, volume, instrument_id, created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
                            ON CONFLICT (date, instrument_id) DO UPDATE SET
                                symbol = EXCLUDED.symbol,
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                updated_at = NOW()
                        """, rows)
                        
                        inserted_count = len(rows)
                        inserted_total += inserted_count
                        processed += 1
                        
                        logger.info(f"✅ {symbol}: Inserted {inserted_count} records")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {symbol}: {e}")
                    continue
            
            logger.info(f"🎉 Polygon backfill complete: {processed} ETFs processed, {inserted_total} total records")
            
        finally:
            await conn.close()

    async def backfill_eodhd_etfs(self):
        """Backfill missing critical ETFs for EODHD"""
        if not CRITICAL_ETFS['eodhd']:
            logger.info("🎯 No missing ETFs for EODHD")
            return
            
        logger.info(f"🚀 Starting EODHD backfill for {len(CRITICAL_ETFS['eodhd'])} critical ETFs")
        
        api_key = get_eodhd_api_key()
        rate_limiter = VendorRateLimiters.eodhd()
        conn = await get_vendor_database_connection()
        
        try:
            processed = 0
            inserted_total = 0
            
            for symbol in CRITICAL_ETFS['eodhd']:
                instrument_id = await self.get_instrument_id(conn, symbol)
                if not instrument_id:
                    logger.warning(f"⚠️ Symbol {symbol} not found in instruments table")
                    continue
                
                logger.info(f"📈 Processing {symbol} (ID: {instrument_id})")
                
                # Check existing data
                env = os.getenv('ENV_TYPE', 'intg').lower()
                table_name = f'{env}_daily_prices_eodhd'
                
                existing = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE instrument_id = $1
                """, instrument_id)
                
                if existing > 0:
                    logger.info(f"⏭️ Skipping {symbol} - already has {existing} records")
                    continue
                
                # Fetch from EODHD API
                url = f"https://eodhistoricaldata.com/api/eod/{symbol}.US"
                params = {
                    'api_token': api_key,
                    'from': self.start_date.isoformat(),
                    'to': self.end_date.isoformat(),
                    'fmt': 'json'
                }
                
                await rate_limiter.wait()
                
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    if not data:
                        logger.warning(f"⚠️ No data for {symbol}")
                        continue
                    
                    # Insert price data
                    rows = []
                    for result in data:
                        price_date = datetime.strptime(result['date'], '%Y-%m-%d').date()
                        rows.append((
                            price_date,
                            symbol,
                            float(result['open']),
                            float(result['high']),
                            float(result['low']),
                            float(result['close']),
                            int(result['volume']),
                            instrument_id
                        ))
                    
                    if rows:
                        inserted = await conn.executemany(f"""
                            INSERT INTO {table_name} 
                            (date, symbol, open, high, low, close, volume, instrument_id, created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
                            ON CONFLICT (date, instrument_id) DO UPDATE SET
                                symbol = EXCLUDED.symbol,
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                updated_at = NOW()
                        """, rows)
                        
                        inserted_count = len(rows)
                        inserted_total += inserted_count
                        processed += 1
                        
                        logger.info(f"✅ {symbol}: Inserted {inserted_count} records")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {symbol}: {e}")
                    continue
            
            logger.info(f"🎉 EODHD backfill complete: {processed} ETFs processed, {inserted_total} total records")
            
        finally:
            await conn.close()

async def main():
    backfiller = CriticalETFBackfiller()
    
    # Run backfills for missing ETFs
    await backfiller.backfill_polygon_etfs()
    await backfiller.backfill_eodhd_etfs()
    
    logger.info("🎉 Critical ETF backfill complete for all vendors!")

if __name__ == "__main__":
    asyncio.run(main())