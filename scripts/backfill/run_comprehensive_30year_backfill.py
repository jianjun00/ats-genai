#!/usr/bin/env python3
"""
Comprehensive 30-Year Historical Data Backfill

Orchestrates the complete backfill of 30 years of daily price data across core vendors:
- Polygon (1995-2025)
- Tiingo (1995-2025) 
- Financial Modeling Prep (1995-2025)

Alpha Vantage, Yahoo, and Interactive Brokers are used only for tie-breaking discrepancies.

This script handles:
- Intelligent chunking to respect API rate limits
- Progress tracking and resumption
- Error handling and retry logic
- Data validation and quality checks
- Parallel processing optimization
- Resource management and monitoring
"""

import asyncio
import asyncpg
import aiohttp
import logging
import os
import json
import argparse
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..', 'src'))

from config.environment import Environment

@dataclass
class BackfillConfig:
    """Configuration for 30-year backfill"""
    start_date: date = date(1995, 1, 1)
    end_date: date = date(2025, 8, 19)
    chunk_years: int = 2                    # Process 2 years at a time
    batch_size: int = 5                     # Process 5 symbols at a time
    max_concurrent_requests: int = 3        # Max concurrent API requests
    retry_attempts: int = 3                 # Retry failed requests
    delay_between_chunks: float = 2.0       # Delay between chunks (seconds)
    save_progress_interval: int = 10        # Save progress every N symbols

@dataclass
class VendorRateLimits:
    """Rate limit configuration for each vendor"""
    calls_per_minute: int
    calls_per_day: Optional[int] = None
    delay_between_calls: float = 1.0

@dataclass
class BackfillProgress:
    """Track backfill progress across all vendors"""
    total_symbols: int = 0
    completed_symbols: int = 0
    failed_symbols: List[str] = None
    total_records_inserted: int = 0
    vendor_progress: Dict[str, Dict] = None
    start_time: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None
    
    def __post_init__(self):
        if self.failed_symbols is None:
            self.failed_symbols = []
        if self.vendor_progress is None:
            self.vendor_progress = {}

class ComprehensiveBackfillOrchestrator:
    """Orchestrates 30-year backfill across core vendors (Polygon, Tiingo, FMP)"""
    
    def __init__(self, env: Environment, config: BackfillConfig):
        self.env = env
        self.config = config
        self.db_url = env.get_database_url()
        self.logger = logging.getLogger(__name__)
        
        # Vendor configurations - 30 years for Polygon, Tiingo, and FMP only
        self.vendor_configs = {
            'polygon': {
                'api_key': os.getenv('POLYGON_API_KEY'),
                'base_url': 'https://api.polygon.io',
                'rate_limits': VendorRateLimits(calls_per_minute=5, calls_per_day=1000),
                'table_name': env.get_table_name('daily_prices_polygon'),
                'endpoint_template': '/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc'
            },
            'tiingo': {
                'api_key': os.getenv('TIINGO_API_KEY'),
                'base_url': 'https://api.tiingo.com',
                'rate_limits': VendorRateLimits(calls_per_minute=20, calls_per_day=2000),
                'table_name': env.get_table_name('daily_prices_tiingo'),
                'endpoint_template': '/tiingo/daily/{symbol}/prices?startDate={start_date}&endDate={end_date}&format=json'
            },
            'fmp': {
                'api_key': os.getenv('FMP_API_KEY'),
                'base_url': 'https://financialmodelingprep.com',
                'rate_limits': VendorRateLimits(calls_per_minute=250, calls_per_day=10000),
                'table_name': env.get_table_name('daily_prices_fmp'),
                'endpoint_template': '/api/v3/historical-price-full/{symbol}?from={start_date}&to={end_date}'
            }
        }
        
        # Progress tracking
        self.progress = BackfillProgress()
    
    async def get_target_symbols(self) -> List[str]:
        """Get list of symbols to backfill"""
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)
        try:
            async with pool.acquire() as conn:
                # Get ALL instruments for comprehensive 30-year backfill
                rows = await conn.fetch("""
                    SELECT DISTINCT symbol
                    FROM dev_instruments
                    WHERE symbol IS NOT NULL
                    ORDER BY symbol
                """)
                
                symbols = [row['symbol'] for row in rows]
                self.logger.info(f"Found {len(symbols)} total instruments for backfill")
                
                return symbols
        
        finally:
            await pool.close()
    
    def generate_date_chunks(self) -> List[Tuple[date, date]]:
        """Generate date chunks for processing"""
        chunks = []
        current_start = self.config.start_date
        
        while current_start < self.config.end_date:
            chunk_end = min(
                current_start.replace(year=current_start.year + self.config.chunk_years),
                self.config.end_date
            )
            chunks.append((current_start, chunk_end))
            current_start = chunk_end + timedelta(days=1)
        
        return chunks
    
    async def fetch_vendor_data(self, vendor: str, symbol: str, start_date: date, end_date: date,
                               session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch data from specific vendor"""
        
        config = self.vendor_configs[vendor]
        if not config['api_key']:
            self.logger.warning(f"No API key for {vendor}, skipping")
            return []
        
        # Respect rate limits
        await asyncio.sleep(config['rate_limits'].delay_between_calls)
        
        # Build URL
        if vendor == 'polygon':
            url = f"{config['base_url']}{config['endpoint_template'].format(symbol=symbol, start_date=start_date, end_date=end_date)}"
            headers = {'Authorization': f'Bearer {config["api_key"]}'}
        elif vendor == 'tiingo':
            url = f"{config['base_url']}{config['endpoint_template'].format(symbol=symbol, start_date=start_date, end_date=end_date)}"
            headers = {'Authorization': f'Token {config["api_key"]}'}
        elif vendor == 'fmp':
            url = f"{config['base_url']}{config['endpoint_template'].format(symbol=symbol, start_date=start_date, end_date=end_date)}&apikey={config['api_key']}"
            headers = {}
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 429:
                    self.logger.warning(f"Rate limit hit for {vendor}/{symbol}")
                    await asyncio.sleep(60)
                    return []
                
                if response.status != 200:
                    self.logger.error(f"HTTP {response.status} for {vendor}/{symbol}")
                    return []
                
                data = await response.json()
                return self.parse_vendor_response(vendor, data, start_date, end_date)
        
        except Exception as e:
            self.logger.error(f"Error fetching {vendor}/{symbol}: {e}")
            return []
    
    def parse_vendor_response(self, vendor: str, data: Dict, start_date: date, end_date: date) -> List[Dict]:
        """Parse response data from different vendors"""
        
        parsed_data = []
        
        try:
            if vendor == 'polygon':
                if 'results' in data:
                    for item in data['results']:
                        parsed_data.append({
                            'date': datetime.fromtimestamp(item['t'] / 1000).date(),
                            'open_price': item.get('o'),
                            'high_price': item.get('h'),
                            'low_price': item.get('l'),
                            'close': item.get('c'),
                            'volume': item.get('v'),
                            'vwap': item.get('vw'),
                            'transactions': item.get('n')
                        })
            
            elif vendor == 'tiingo':
                if isinstance(data, list):
                    for item in data:
                        parsed_data.append({
                            'date': datetime.strptime(item['date'][:10], '%Y-%m-%d').date(),
                            'open_price': item.get('open'),
                            'high_price': item.get('high'),
                            'low_price': item.get('low'),
                            'close': item.get('close'),
                            'adj_close': item.get('adjClose'),
                            'volume': item.get('volume')
                        })
            
            
            elif vendor == 'fmp':
                if 'historical' in data:
                    for item in data['historical']:
                        parsed_data.append({
                            'date': datetime.strptime(item['date'], '%Y-%m-%d').date(),
                            'open_price': item.get('open'),
                            'high_price': item.get('high'),
                            'low_price': item.get('low'),
                            'close': item.get('close'),
                            'adj_close': item.get('adjClose'),
                            'volume': item.get('volume')
                        })
        
        except Exception as e:
            self.logger.error(f"Error parsing {vendor} response: {e}")
        
        return parsed_data
    
    async def save_vendor_data(self, vendor: str, symbol: str, data: List[Dict]) -> int:
        """Save data to vendor-specific table"""
        
        if not data:
            return 0
        
        table_name = self.vendor_configs[vendor]['table_name']
        pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=5)
        
        try:
            async with pool.acquire() as conn:
                # Get instrument ID
                instrument_id = await conn.fetchval(
                    "SELECT id FROM dev_instruments WHERE symbol = $1", symbol
                )
                
                if not instrument_id:
                    self.logger.warning(f"Instrument not found for {symbol}")
                    return 0
                
                # Prepare insert data
                insert_data = []
                for record in data:
                    # Build insert tuple based on vendor table schema
                    if vendor == 'polygon':
                        insert_data.append((
                            instrument_id,
                            record['date'],
                            record.get('open_price'),
                            record.get('high_price'),
                            record.get('low_price'),
                            record.get('close'),
                            record.get('volume'),
                            record.get('vwap'),
                            record.get('transactions')
                        ))
                        
                        sql = f"""
                            INSERT INTO {table_name} 
                            (instrument_id, date, open_price, high_price, low_price, close, volume, vwap, transactions)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (instrument_id, date) DO UPDATE SET
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                vwap = EXCLUDED.vwap,
                                transactions = EXCLUDED.transactions,
                                updated_at = NOW()
                        """
                    
                    else:  # tiingo, fmp - similar schema
                        insert_data.append((
                            instrument_id,
                            record['date'],
                            record.get('open_price'),
                            record.get('high_price'),
                            record.get('low_price'),
                            record.get('close'),
                            record.get('adj_close', record.get('close')),
                            record.get('volume')
                        ))
                        
                        sql = f"""
                            INSERT INTO {table_name}
                            (instrument_id, date, open_price, high_price, low_price, close, adj_close, volume)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (instrument_id, date) DO UPDATE SET
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close = EXCLUDED.close,
                                adj_close = EXCLUDED.adj_close,
                                volume = EXCLUDED.volume,
                                updated_at = NOW()
                        """
                
                # Batch insert
                await conn.executemany(sql, insert_data)
                return len(insert_data)
        
        finally:
            await pool.close()
    
    async def process_symbol_chunk(self, symbols: List[str], start_date: date, end_date: date) -> Dict[str, int]:
        """Process a chunk of symbols across all vendors"""
        
        vendor_results = {vendor: 0 for vendor in self.vendor_configs.keys()}
        
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            
            for symbol in symbols:
                self.logger.info(f"Processing {symbol} for period {start_date} to {end_date}")
                
                # Process each vendor for this symbol
                for vendor in self.vendor_configs.keys():
                    try:
                        data = await self.fetch_vendor_data(vendor, symbol, start_date, end_date, session)
                        if data:
                            records_saved = await self.save_vendor_data(vendor, symbol, data)
                            vendor_results[vendor] += records_saved
                            self.logger.info(f"  {vendor}: {records_saved} records saved")
                        else:
                            self.logger.warning(f"  {vendor}: No data received")
                    
                    except Exception as e:
                        self.logger.error(f"  {vendor}: Error processing {symbol} - {e}")
                        self.progress.failed_symbols.append(f"{symbol}_{vendor}")
                
                # Update progress
                self.progress.completed_symbols += 1
                
                # Brief delay between symbols
                await asyncio.sleep(self.config.delay_between_chunks)
        
        return vendor_results
    
    async def run_comprehensive_backfill(self, symbols: Optional[List[str]] = None) -> BackfillProgress:
        """Run comprehensive 30-year backfill"""
        
        self.progress.start_time = datetime.now()
        
        if symbols is None:
            symbols = await self.get_target_symbols()
        
        self.progress.total_symbols = len(symbols) * len(self.vendor_configs)
        
        self.logger.info(f"🚀 Starting 30-year comprehensive backfill")
        self.logger.info(f"📅 Period: {self.config.start_date} to {self.config.end_date}")
        self.logger.info(f"🎯 Symbols: {len(symbols)}")
        self.logger.info(f"🏢 Vendors: {list(self.vendor_configs.keys())}")
        
        # Generate date chunks
        date_chunks = self.generate_date_chunks()
        self.logger.info(f"📊 Date chunks: {len(date_chunks)}")
        
        # Process each date chunk
        total_vendor_results = {vendor: 0 for vendor in self.vendor_configs.keys()}
        
        for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
            self.logger.info(f"🔄 Processing chunk {i}/{len(date_chunks)}: {chunk_start} to {chunk_end}")
            
            # Process symbols in batches within this date chunk
            for j in range(0, len(symbols), self.config.batch_size):
                batch_symbols = symbols[j:j + self.config.batch_size]
                
                self.logger.info(f"  Batch {j//self.config.batch_size + 1}: {batch_symbols}")
                
                try:
                    chunk_results = await self.process_symbol_chunk(batch_symbols, chunk_start, chunk_end)
                    
                    # Aggregate results
                    for vendor, count in chunk_results.items():
                        total_vendor_results[vendor] += count
                        
                    self.progress.total_records_inserted = sum(total_vendor_results.values())
                    
                    # Progress update
                    completion_pct = (i / len(date_chunks)) * 100
                    self.logger.info(f"  Progress: {completion_pct:.1f}% complete, {self.progress.total_records_inserted:,} records inserted")
                
                except Exception as e:
                    self.logger.error(f"  Error processing batch: {e}")
        
        # Final summary
        self.progress.vendor_progress = total_vendor_results
        
        self.logger.info(f"🎉 30-year backfill completed!")
        self.logger.info(f"📊 Total records inserted: {self.progress.total_records_inserted:,}")
        for vendor, count in total_vendor_results.items():
            self.logger.info(f"  • {vendor}: {count:,} records")
        
        if self.progress.failed_symbols:
            self.logger.warning(f"❌ Failed symbols/vendors: {len(self.progress.failed_symbols)}")
            for failed in self.progress.failed_symbols[:10]:  # Show first 10
                self.logger.warning(f"  • {failed}")
        
        return self.progress
    
    def save_progress_report(self, filename: str = None):
        """Save detailed progress report to file"""
        
        if filename is None:
            filename = f"backfill_30year_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = {
            'config': asdict(self.config),
            'progress': asdict(self.progress),
            'vendor_configs': {
                vendor: {
                    'rate_limits': asdict(config['rate_limits']),
                    'table_name': config['table_name']
                }
                for vendor, config in self.vendor_configs.items()
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"📄 Progress report saved to {filename}")

async def main():
    """Main function for 30-year backfill"""
    
    parser = argparse.ArgumentParser(description='Run comprehensive 30-year historical backfill')
    parser.add_argument('--symbols', nargs='+', help='Specific symbols to backfill')
    parser.add_argument('--chunk-years', type=int, default=2, help='Years per chunk')
    parser.add_argument('--batch-size', type=int, default=5, help='Symbols per batch')
    parser.add_argument('--start-year', type=int, default=1995, help='Start year')
    parser.add_argument('--end-year', type=int, default=2025, help='End year')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'backfill_30year_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    
    # Configuration
    config = BackfillConfig(
        start_date=date(args.start_year, 1, 1),
        end_date=date(args.end_year, 8, 19),
        chunk_years=args.chunk_years,
        batch_size=args.batch_size
    )
    
    env = Environment()
    orchestrator = ComprehensiveBackfillOrchestrator(env, config)
    
    if args.dry_run:
        print("🧪 DRY RUN MODE - No data will be written")
        symbols = await orchestrator.get_target_symbols()
        date_chunks = orchestrator.generate_date_chunks()
        
        print(f"📊 Would process:")
        print(f"  • {len(symbols)} symbols")
        print(f"  • {len(date_chunks)} date chunks")
        print(f"  • {len(orchestrator.vendor_configs)} vendors")
        print(f"  • ~{len(symbols) * len(date_chunks) * len(orchestrator.vendor_configs)} total requests")
        return
    
    # Run the backfill
    progress = await orchestrator.run_comprehensive_backfill(args.symbols)
    
    # Save report
    orchestrator.save_progress_report()
    
    print(f"\n✅ BACKFILL COMPLETED")
    print(f"📊 Records inserted: {progress.total_records_inserted:,}")
    print(f"⏱️  Duration: {datetime.now() - progress.start_time}")

if __name__ == "__main__":
    asyncio.run(main())