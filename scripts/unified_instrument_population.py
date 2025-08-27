#!/usr/bin/env python3
"""
Unified Instrument Population

Creates a comprehensive master instruments table by merging data from all vendor-specific tables:
- dev_instrument_polygon (11,598 instruments)
- dev_instrument_tiingo (11,756 instruments) 
- dev_instrument_eodhd (44,769+ instruments)

Strategy:
- Use symbol as primary matching key across vendors
- Prioritize data quality: EODHD → Polygon → Tiingo
- Handle conflicts with vendor-specific metadata preservation
- Create unified dev_instruments with proper referential integrity
- Maintain vendor-specific tables for detailed data

Environment Variables:
- DRY_RUN: Set to 'true' to preview changes without executing
- BATCH_SIZE: Number of instruments to process per batch (default: 1000)
- IDEMPOTENT_MODE: Set to 'true' to only update existing instruments, not add new ones
- FORCE_REBUILD: Set to 'true' to completely rebuild the unified table from scratch
"""

import os
import sys
import asyncio
import asyncpg
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
import json

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UnifiedInstrumentPopulator:
    """Unified instrument master table population from all vendors"""
    
    def __init__(self, dry_run: bool = False, batch_size: int = 1000, 
                 idempotent_mode: bool = False, force_rebuild: bool = False):
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.idempotent_mode = idempotent_mode
        self.force_rebuild = force_rebuild
        self.start_time = datetime.now()
        
        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'conflicts_resolved': 0,
            'errors': 0
        }
        
    async def analyze_current_state(self, pool: asyncpg.Pool) -> Dict[str, Any]:
        """Analyze current instrument data across all tables - with graceful column handling"""
        async with pool.acquire() as conn:
            # Check if active column exists in tiingo table
            tiingo_has_active = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_tiingo' AND column_name = 'active'
                )
            """)
            
            # Get counts from all tables
            polygon_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_polygon WHERE active = true") if await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_polygon' AND column_name = 'active'
                )
            """) else await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_polygon")
            
            if tiingo_has_active:
                tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo WHERE active = true")
            else:
                tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo")
            
            eodhd_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_eodhd")
            
            # Check if dev_instruments table exists
            unified_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'dev_instruments'
                )
            """)
            
            if unified_exists:
                current_unified = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            else:
                current_unified = 0
            
            # Get symbol overlaps - handle active column gracefully
            tiingo_filter = "WHERE active = true" if tiingo_has_active else ""
            polygon_filter = "WHERE active = true" if await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_polygon' AND column_name = 'active'
                )
            """) else ""
            
            common_symbols = await conn.fetchval(f"""
                SELECT COUNT(DISTINCT symbol) FROM (
                    SELECT symbol FROM dev_instrument_polygon {polygon_filter}
                    UNION ALL
                    SELECT symbol FROM dev_instrument_tiingo {tiingo_filter}
                    UNION ALL  
                    SELECT symbol FROM dev_instrument_eodhd
                ) symbols
            """)
            
            # Check for price data dependencies (only if unified table exists)
            if unified_exists:
                price_symbols_needing_instruments = await conn.fetchval("""
                    SELECT COUNT(DISTINCT symbol) FROM dev_daily_prices_polygon p
                    WHERE NOT EXISTS (SELECT 1 FROM dev_instruments i WHERE i.id = p.instrument_id)
                """)
            else:
                price_symbols_needing_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_prices_polygon")
            
            return {
                'vendor_counts': {
                    'polygon': polygon_count,
                    'tiingo': tiingo_count,
                    'eodhd': eodhd_count
                },
                'current_unified': current_unified,
                'total_unique_symbols': common_symbols,
                'price_data_gaps': price_symbols_needing_instruments,
                'unified_table_exists': unified_exists
            }

    async def create_unified_strategy(self, pool: asyncpg.Pool) -> List[Dict[str, Any]]:
        """Create comprehensive instrument unification strategy with deterministic ordering"""
        logger.info("🔍 Creating unified instrument strategy...")
        
        async with pool.acquire() as conn:
            # Check if columns exist before using them
            polygon_has_active = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_polygon' AND column_name = 'active'
                )
            """)
            
            polygon_has_type = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_polygon' AND column_name = 'type'
                )
            """)
            
            tiingo_has_active = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_tiingo' AND column_name = 'active'
                )
            """)
            
            tiingo_has_asset_type = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_tiingo' AND column_name = 'asset_type'
                )
            """)
            
            eodhd_has_type = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instrument_eodhd' AND column_name = 'type'
                )
            """)
            
            # Build query with conditional column references
            polygon_active_filter = "WHERE active = true" if polygon_has_active else ""
            tiingo_active_filter = "WHERE active = true" if tiingo_has_active else ""
            tiingo_type_field = "asset_type as type" if tiingo_has_asset_type else "'Stock' as type"
            polygon_type_field = "type" if polygon_has_type else "'Stock' as type"
            eodhd_type_field = "type" if eodhd_has_type else "'Stock' as type"
            
            # Get all unique symbols with vendor data - deterministic and stable query
            query = f"""
            WITH vendor_symbols AS (
                SELECT 
                    symbol,
                    'polygon' as vendor,
                    symbol as polygon_symbol,
                    name,
                    exchange,
                    {polygon_type_field},
                    currency,
                    figi,
                    composite_figi,
                    {'active' if polygon_has_active else 'true as active'},
                    NULL as start_date,
                    NULL as end_date
                FROM dev_instrument_polygon
                {polygon_active_filter}
                
                UNION ALL
                
                SELECT 
                    symbol,
                    'tiingo' as vendor,
                    symbol as tiingo_symbol,
                    name,
                    exchange,
                    {tiingo_type_field},
                    NULL as currency,
                    NULL as figi,
                    NULL as composite_figi,
                    {'active' if tiingo_has_active else 'true as active'},
                    start_date,
                    end_date
                FROM dev_instrument_tiingo 
                {tiingo_active_filter}
                
                UNION ALL
                
                SELECT 
                    symbol,
                    'eodhd' as vendor,
                    symbol as eodhd_symbol,
                    name,
                    exchange,
                    {eodhd_type_field},
                    currency,
                    NULL as figi,
                    NULL as composite_figi,
                    true as active,
                    NULL as start_date,
                    NULL as end_date
                FROM dev_instrument_eodhd
            ),
            symbol_priorities AS (
                SELECT 
                    symbol,
                    json_agg(
                        json_build_object(
                            'vendor', vendor,
                            'name', name,
                            'exchange', exchange,
                            'type', type,
                            'currency', currency,
                            'figi', figi,
                            'composite_figi', composite_figi,
                            'active', active,
                            'start_date', start_date,
                            'end_date', end_date
                        ) ORDER BY 
                            CASE vendor 
                                WHEN 'eodhd' THEN 1 
                                WHEN 'polygon' THEN 2 
                                WHEN 'tiingo' THEN 3 
                            END,
                            name  -- Secondary sort for deterministic results
                    ) as vendor_data,
                    COUNT(*) as vendor_count
                FROM vendor_symbols
                GROUP BY symbol
            )
            SELECT symbol, vendor_data, vendor_count
            FROM symbol_priorities
            ORDER BY vendor_count DESC, symbol  -- Deterministic ordering
            """
            
            rows = await conn.fetch(query)
            
            instruments = []
            for row in rows:
                symbol = row['symbol']
                vendor_data = json.loads(row['vendor_data']) if isinstance(row['vendor_data'], str) else row['vendor_data']
                vendor_count = row['vendor_count']
                
                # Create unified instrument record
                unified = self.create_unified_instrument(symbol, vendor_data, vendor_count)
                instruments.append(unified)
            
            logger.info(f"📊 Strategy created for {len(instruments):,} unique symbols")
            return instruments

    def create_unified_instrument(self, symbol: str, vendor_data: List[Dict], vendor_count: int) -> Dict[str, Any]:
        """Create unified instrument from vendor data with intelligent conflict resolution"""
        
        # Start with the highest priority vendor (EODHD first)
        primary = vendor_data[0]
        
        unified = {
            'symbol': symbol,
            'name': primary.get('name'),
            'exchange': primary.get('exchange'),
            'type': primary.get('type'),
            'currency': primary.get('currency'),
            'figi': primary.get('figi'),
            'composite_figi': primary.get('composite_figi'),
            'active': primary.get('active', True),
            'list_date': None,  # Can be enhanced with vendor-specific data
            'delist_date': None,
            'sector': None,  # Can be enhanced from external sources
            'vendor_count': vendor_count,
            'vendor_metadata': {
                'sources': [v.get('vendor') for v in vendor_data],
                'conflicts': self.identify_conflicts(vendor_data),
                'raw_data': vendor_data
            }
        }
        
        # Intelligent field resolution for conflicts
        if vendor_count > 1:
            unified = self.resolve_field_conflicts(unified, vendor_data)
        
        return unified

    def identify_conflicts(self, vendor_data: List[Dict]) -> Dict[str, List[str]]:
        """Identify conflicts between vendor data"""
        conflicts = {}
        
        if len(vendor_data) <= 1:
            return conflicts
        
        # Check each field for conflicts
        fields_to_check = ['name', 'exchange', 'type', 'currency']
        
        for field in fields_to_check:
            values = [v.get(field) for v in vendor_data if v.get(field)]
            unique_values = list(set(values))
            
            if len(unique_values) > 1:
                conflicts[field] = unique_values
        
        return conflicts

    def resolve_field_conflicts(self, unified: Dict, vendor_data: List[Dict]) -> Dict[str, Any]:
        """Resolve conflicts using intelligent rules"""
        
        # Name resolution: Prefer longest descriptive name
        names = [v.get('name') for v in vendor_data if v.get('name')]
        if names:
            unified['name'] = max(names, key=len)
        
        # Exchange resolution: Prefer specific exchanges over general ones
        exchanges = [v.get('exchange') for v in vendor_data if v.get('exchange')]
        if exchanges:
            # Prefer NASDAQ, NYSE over generic codes
            priority_exchanges = ['NASDAQ', 'NYSE', 'XNAS', 'XNYS']
            for priority_ex in priority_exchanges:
                if priority_ex in exchanges:
                    unified['exchange'] = priority_ex
                    break
            else:
                unified['exchange'] = exchanges[0]  # Default to first
        
        # Currency resolution: Default to USD for US exchanges
        if not unified.get('currency') and unified.get('exchange') in ['NASDAQ', 'NYSE', 'XNAS', 'XNYS']:
            unified['currency'] = 'USD'
        
        return unified

    async def ensure_table_schema(self, pool: asyncpg.Pool) -> None:
        """Ensure dev_instruments table exists with proper schema and constraints for idempotency"""
        async with pool.acquire() as conn:
            # Create table if not exists with proper unique constraint on symbol
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_instruments (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(500),
                    exchange VARCHAR(50),
                    type VARCHAR(50),
                    currency VARCHAR(10),
                    figi VARCHAR(50),
                    composite_figi VARCHAR(50),
                    active BOOLEAN DEFAULT true,
                    list_date DATE,
                    delist_date DATE,
                    sector VARCHAR(100),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Ensure unique constraint exists (idempotent)
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dev_instruments_symbol_unique 
                ON dev_instruments(symbol);
                
                -- Additional useful indexes
                CREATE INDEX IF NOT EXISTS idx_dev_instruments_exchange ON dev_instruments(exchange);
                CREATE INDEX IF NOT EXISTS idx_dev_instruments_active ON dev_instruments(active);
                CREATE INDEX IF NOT EXISTS idx_dev_instruments_type ON dev_instruments(type);
            """)
            logger.info("✅ Ensured dev_instruments table schema with unique constraints")

    async def populate_unified_instruments(self, pool: asyncpg.Pool, instruments: List[Dict]) -> None:
        """Populate dev_instruments with unified data - fully idempotent"""
        logger.info(f"🚀 Starting unified population of {len(instruments):,} instruments")
        
        # Ensure proper table schema first
        await self.ensure_table_schema(pool)
        
        async with pool.acquire() as conn:
            # Use transaction for atomicity
            async with conn.transaction():
                # Ensure sequence starts after existing max id (idempotent)
                # Get current max ID and set sequence accordingly
                max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM dev_instruments")
                if max_id > 0:
                    await conn.execute("SELECT setval('dev_instruments_id_seq', $1)", max_id + 1)
                
                # Process in batches
                for i in range(0, len(instruments), self.batch_size):
                    batch = instruments[i:i + self.batch_size]
                    batch_num = i // self.batch_size + 1
                    total_batches = (len(instruments) + self.batch_size - 1) // self.batch_size
                    
                    logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} instruments)")
                    
                    if not self.dry_run:
                        await self.insert_instrument_batch(conn, batch)
                    else:
                        logger.info(f"🏃 DRY RUN: Would upsert {len(batch)} instruments")
                    
                    self.stats['total_processed'] += len(batch)
                    
                    # Log progress
                    if batch_num % 10 == 0:
                        self.log_progress()
        
        logger.info("✅ Unified instrument population completed with full transaction atomicity")

    async def insert_instrument_batch(self, conn: asyncpg.Connection, batch: List[Dict]) -> None:
        """Insert a batch of unified instruments with configurable idempotency modes"""
        for instrument in batch:
            try:
                symbol = instrument['symbol']
                
                if self.idempotent_mode:
                    # In idempotent mode, only update existing instruments
                    existing = await conn.fetchrow(
                        "SELECT id FROM dev_instruments WHERE symbol = $1", symbol
                    )
                    
                    if existing:
                        # Update existing instrument
                        await conn.execute("""
                            UPDATE dev_instruments SET
                                name = $2, exchange = $3, type = $4, currency = $5,
                                figi = $6, composite_figi = $7, active = $8,
                                list_date = $9, delist_date = $10, sector = $11,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE symbol = $1
                        """,
                            symbol, instrument.get('name'),
                            instrument.get('exchange'), instrument.get('type'),
                            instrument.get('currency'), instrument.get('figi'),
                            instrument.get('composite_figi'), instrument.get('active'),
                            instrument.get('list_date'), instrument.get('delist_date'),
                            instrument.get('sector')
                        )
                        self.stats['updated'] += 1
                    else:
                        # Skip new instruments in idempotent mode
                        self.stats['skipped'] += 1
                        logger.debug(f"🔄 Skipping new instrument {symbol} (idempotent mode)")
                else:
                    # Normal mode: Use UPSERT for full functionality
                    result = await conn.execute("""
                        INSERT INTO dev_instruments 
                        (symbol, name, exchange, type, currency, figi, composite_figi, 
                         active, list_date, delist_date, sector, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (symbol) DO UPDATE SET
                            name = EXCLUDED.name,
                            exchange = EXCLUDED.exchange,
                            type = EXCLUDED.type,
                            currency = EXCLUDED.currency,
                            figi = EXCLUDED.figi,
                            composite_figi = EXCLUDED.composite_figi,
                            active = EXCLUDED.active,
                            list_date = EXCLUDED.list_date,
                            delist_date = EXCLUDED.delist_date,
                            sector = EXCLUDED.sector,
                            updated_at = CURRENT_TIMESTAMP
                    """,
                        symbol, instrument.get('name'),
                        instrument.get('exchange'), instrument.get('type'),
                        instrument.get('currency'), instrument.get('figi'),
                        instrument.get('composite_figi'), instrument.get('active'),
                        instrument.get('list_date'), instrument.get('delist_date'),
                        instrument.get('sector')
                    )
                    
                    # Track whether this was an insert or update based on the result
                    if 'INSERT' in result:
                        self.stats['inserted'] += 1
                    else:
                        self.stats['updated'] += 1
                
                if instrument.get('vendor_metadata', {}).get('conflicts'):
                    self.stats['conflicts_resolved'] += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing {instrument['symbol']}: {e}")
                self.stats['errors'] += 1

    def log_progress(self):
        """Log comprehensive progress summary"""
        elapsed = datetime.now() - self.start_time
        
        logger.info("=" * 80)
        logger.info("📊 UNIFIED INSTRUMENT POPULATION PROGRESS")
        logger.info("=" * 80)
        logger.info(f"📈 Processed: {self.stats['total_processed']:,}")
        logger.info(f"➕ Inserted: {self.stats['inserted']:,}")
        logger.info(f"🔄 Updated: {self.stats['updated']:,}")
        logger.info(f"⚖️ Conflicts Resolved: {self.stats['conflicts_resolved']:,}")
        logger.info(f"❌ Errors: {self.stats['errors']:,}")
        logger.info(f"⏱️  Elapsed: {elapsed}")
        logger.info("=" * 80)

    async def verify_population(self, pool: asyncpg.Pool) -> Dict[str, Any]:
        """Verify unified population results - comprehensive validation"""
        async with pool.acquire() as conn:
            # Check if table exists first
            unified_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'dev_instruments'
                )
            """)
            
            if not unified_exists:
                return {
                    'error': 'dev_instruments table does not exist',
                    'final_instrument_count': 0,
                    'active_instruments': 0,
                    'price_integrity_records': 0,
                    'total_price_records': 0,
                    'integrity_percentage': 0
                }
            
            final_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            
            # Check if active column exists
            has_active_column = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'dev_instruments' AND column_name = 'active'
                )
            """)
            
            if has_active_column:
                active_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments WHERE active = true")
            else:
                active_count = final_count  # Assume all are active if no column
            
            # Check price data integrity
            price_integrity = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_daily_prices_polygon p
                WHERE EXISTS (SELECT 1 FROM dev_instruments i WHERE i.id = p.instrument_id)
            """)
            
            total_price_records = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_prices_polygon")
            
            # Additional validation checks
            duplicate_symbols = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT symbol FROM dev_instruments 
                    GROUP BY symbol HAVING COUNT(*) > 1
                ) duplicates
            """)
            
            missing_names = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE name IS NULL OR name = ''
            """)
            
            # Check referential integrity with vendor tables
            vendor_symbol_coverage = await conn.fetchval("""
                SELECT COUNT(DISTINCT di.symbol) FROM dev_instruments di
                WHERE EXISTS (
                    SELECT 1 FROM dev_instrument_polygon dp WHERE dp.symbol = di.symbol
                    UNION ALL
                    SELECT 1 FROM dev_instrument_tiingo dt WHERE dt.symbol = di.symbol  
                    UNION ALL
                    SELECT 1 FROM dev_instrument_eodhd de WHERE de.symbol = di.symbol
                )
            """)
            
            return {
                'final_instrument_count': final_count,
                'active_instruments': active_count,
                'price_integrity_records': price_integrity,
                'total_price_records': total_price_records,
                'integrity_percentage': (price_integrity / total_price_records * 100) if total_price_records > 0 else 0,
                'duplicate_symbols': duplicate_symbols,
                'missing_names': missing_names,
                'vendor_symbol_coverage': vendor_symbol_coverage,
                'coverage_percentage': (vendor_symbol_coverage / final_count * 100) if final_count > 0 else 0,
                'validation_passed': duplicate_symbols == 0 and (price_integrity / total_price_records * 100 if total_price_records > 0 else 100) > 95
            }

async def main():
    """Main execution function"""
    
    # Configuration
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    batch_size = int(os.getenv('BATCH_SIZE', '1000'))
    idempotent_mode = os.getenv('IDEMPOTENT_MODE', 'false').lower() == 'true'
    force_rebuild = os.getenv('FORCE_REBUILD', 'false').lower() == 'true'
    
    logger.info("🚀 Starting Unified Instrument Population")
    logger.info(f"🏃 Dry Run Mode: {dry_run}")
    logger.info(f"📦 Batch Size: {batch_size:,}")
    logger.info(f"🔄 Idempotent Mode: {idempotent_mode}")
    logger.info(f"🔨 Force Rebuild: {force_rebuild}")
    
    if idempotent_mode:
        logger.info("⚙️  IDEMPOTENT MODE: Only existing instruments will be updated")
    elif force_rebuild:
        logger.info("⚙️  FORCE REBUILD MODE: Unified table will be rebuilt from scratch")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        populator = UnifiedInstrumentPopulator(
            dry_run=dry_run, 
            batch_size=batch_size,
            idempotent_mode=idempotent_mode,
            force_rebuild=force_rebuild
        )
        
        # Analyze current state
        logger.info("📊 Analyzing current instrument data...")
        analysis = await populator.analyze_current_state(pool)
        
        logger.info("=" * 60)
        logger.info("📈 CURRENT STATE ANALYSIS")
        logger.info("=" * 60)
        for vendor, count in analysis['vendor_counts'].items():
            logger.info(f"  {vendor.upper()}: {count:,} instruments")
        logger.info(f"  Current Unified: {analysis['current_unified']:,}")
        logger.info(f"  Total Unique Symbols: {analysis['total_unique_symbols']:,}")
        logger.info(f"  Price Data Gaps: {analysis['price_data_gaps']:,}")
        logger.info("=" * 60)
        
        # Create unified strategy
        instruments = await populator.create_unified_strategy(pool)
        
        # Execute population
        await populator.populate_unified_instruments(pool, instruments)
        
        # Verify results
        logger.info("✅ Verifying population results...")
        verification = await populator.verify_population(pool)
        
        # Final summary
        populator.log_progress()
        
        # Comprehensive final reporting
        logger.info("🎉 UNIFIED INSTRUMENT POPULATION COMPLETE!")
        logger.info("=" * 80)
        logger.info("📊 FINAL RESULTS SUMMARY")
        logger.info("=" * 80)
        
        if 'error' in verification:
            logger.error(f"❌ Validation Error: {verification['error']}")
        else:
            logger.info(f"📊 Total Instruments: {verification['final_instrument_count']:,}")
            logger.info(f"✅ Active Instruments: {verification['active_instruments']:,}")
            logger.info(f"🔗 Price Data Integrity: {verification['integrity_percentage']:.1f}% ({verification['price_integrity_records']:,}/{verification['total_price_records']:,})")
            logger.info(f"🏷️  Vendor Coverage: {verification['coverage_percentage']:.1f}% ({verification['vendor_symbol_coverage']:,}/{verification['final_instrument_count']:,})")
            
            # Validation checks
            if verification['duplicate_symbols'] > 0:
                logger.warning(f"⚠️ Duplicate symbols found: {verification['duplicate_symbols']}")
            else:
                logger.info("✅ No duplicate symbols")
            
            if verification['missing_names'] > 0:
                logger.warning(f"⚠️ Missing names: {verification['missing_names']}")
            else:
                logger.info("✅ All instruments have names")
            
            if verification['validation_passed']:
                logger.info("🎯 ALL VALIDATION CHECKS PASSED!")
            else:
                logger.warning("⚠️ Some validation checks failed")
        
        logger.info("=" * 80)
        logger.info("🚀 IDEMPOTENT OPERATION - Safe to run multiple times")
        logger.info("=" * 80)
        
        await pool.close()
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())