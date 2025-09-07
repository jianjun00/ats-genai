"""
Unified Daily Price Pipeline

Main pipeline for validating and unifying daily prices from multiple vendors.
This pipeline processes instruments over a date range and creates unified price records.
"""

import asyncio
import asyncpg
import logging
import argparse
import sys
from datetime import date, timedelta
from typing import List, Optional, Dict
import json
import os
import subprocess

from domains.market_data.services.eod.unified_daily_price_validator import UnifiedDailyPriceValidator, ValidationStatus
from shared.utils.environment import Environment


class UnifiedDailyPricePipeline:
    """
    Main pipeline for processing unified daily prices
    """
    
    def __init__(self, environment: Environment):
        self.env = environment
        self.logger = logging.getLogger(__name__)
        self.validator = UnifiedDailyPriceValidator(environment)
        self.conn: Optional[asyncpg.Connection] = None
        
        # Pipeline configuration
        self.batch_size = 100  # Process symbols in batches
        self.max_concurrent = 5  # Max concurrent symbol processing
        
    async def connect(self):
        """Establish database connection"""
        if self.conn is None:
            self.conn = await asyncpg.connect(self.env.get_database_url())
            await self.validator.connect()
            self.logger.info("✅ Connected to database for pipeline processing")
    
    async def disconnect(self):
        """Close database connections"""
        if self.conn:
            await self.conn.close()
            self.conn = None
        await self.validator.disconnect()
    
    async def create_run_record(self, run_type: str, parameters: Dict) -> int:
        """
        Create a run record to track this pipeline execution
        """
        try:
            # Get git information
            git_commit = self._get_git_commit()
            git_branch = self._get_git_branch()
            
            # Get command line
            command_line = " ".join(sys.argv)
            
            query = """
                INSERT INTO dev_runs (
                    run_type, status, command_line, git_commit_hash, git_branch,
                    environment, parameters, created_by
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """
            
            run_id = await self.conn.fetchval(
                query,
                run_type,
                'running',
                command_line,
                git_commit,
                git_branch,
                'dev',
                json.dumps(parameters),
                'unified_price_pipeline'
            )
            
            self.logger.info(f"📋 Created run record: {run_id}")
            return run_id
            
        except Exception as e:
            self.logger.error(f"Error creating run record: {e}")
            raise
    
    async def update_run_record(self, run_id: int, status: str, results: Dict = None, error_message: str = None):
        """
        Update run record with completion status and results
        """
        try:
            query = """
                UPDATE dev_runs 
                SET status = $1, end_time = now(), results = $2, error_message = $3
                WHERE id = $4
            """
            
            await self.conn.execute(
                query,
                status,
                json.dumps(results) if results else None,
                error_message,
                run_id
            )
            
            self.logger.info(f"📋 Updated run {run_id} with status: {status}")
            
        except Exception as e:
            self.logger.error(f"Error updating run record: {e}")
    
    def _get_git_commit(self) -> Optional[str]:
        """Get current git commit hash"""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True, text=True, cwd=os.path.dirname(__file__)
            )
            return result.stdout.strip() if result.returncode == 0 else None
        except:
            return None
    
    def _get_git_branch(self) -> Optional[str]:
        """Get current git branch"""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True, text=True, cwd=os.path.dirname(__file__)
            )
            return result.stdout.strip() if result.returncode == 0 else None
        except:
            return None
    
    async def get_validation_status_id(self, status: ValidationStatus) -> int:
        """Get validation status ID from enum"""
        query = "SELECT id FROM dev_price_validation_status WHERE code = $1"
        row = await self.conn.fetchrow(query, status.value)
        if not row:
            raise ValueError(f"Unknown validation status: {status.value}")
        return row['id']
    
    async def store_unified_price(self, unified_price, run_id: int):
        """
        Store unified price record in the database
        """
        try:
            # Get validation status ID
            validation_status_id = await self.get_validation_status_id(unified_price.validation_result.status)
            
            # Insert unified price
            insert_query = """
                INSERT INTO dev_daily_prices (
                    instrument_id, date, open_price, high_price, low_price, close, adj_close, volume,
                    validation_status_id, run_id, primary_vendor, secondary_vendors, vendor_count,
                    price_variance, statistical_score, confidence_score,
                    polygon_price, tiingo_price, fmp_price, alphavantage_price, yfinance_price,
                    validation_notes, rejection_reason
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                ON CONFLICT (instrument_id, date) 
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume = EXCLUDED.volume,
                    validation_status_id = EXCLUDED.validation_status_id,
                    run_id = EXCLUDED.run_id,
                    primary_vendor = EXCLUDED.primary_vendor,
                    secondary_vendors = EXCLUDED.secondary_vendors,
                    vendor_count = EXCLUDED.vendor_count,
                    price_variance = EXCLUDED.price_variance,
                    statistical_score = EXCLUDED.statistical_score,
                    confidence_score = EXCLUDED.confidence_score,
                    polygon_price = EXCLUDED.polygon_price,
                    tiingo_price = EXCLUDED.tiingo_price,
                    fmp_price = EXCLUDED.fmp_price,
                    alphavantage_price = EXCLUDED.alphavantage_price,
                    yfinance_price = EXCLUDED.yfinance_price,
                    validation_notes = EXCLUDED.validation_notes,
                    rejection_reason = EXCLUDED.rejection_reason,
                    updated_at = now()
                RETURNING id
            """
            
            # Extract vendor prices for audit trail
            polygon_price = unified_price.vendor_prices.get('polygon')
            tiingo_price = unified_price.vendor_prices.get('tiingo')
            fmp_price = unified_price.vendor_prices.get('fmp')
            alphavantage_price = unified_price.vendor_prices.get('alphavantage')
            yfinance_price = unified_price.vendor_prices.get('yfinance')
            
            unified_id = await self.conn.fetchval(
                insert_query,
                unified_price.instrument_id,
                unified_price.date,
                unified_price.open_price,
                unified_price.high_price,
                unified_price.low_price,
                unified_price.close,
                unified_price.adj_close,
                unified_price.volume,
                validation_status_id,
                run_id,
                unified_price.primary_vendor,
                unified_price.secondary_vendors,
                unified_price.vendor_count,
                unified_price.validation_result.price_variance,
                unified_price.validation_result.statistical_score,
                unified_price.validation_result.confidence_score,
                polygon_price,
                tiingo_price,
                fmp_price,
                alphavantage_price,
                yfinance_price,
                unified_price.validation_result.validation_notes,
                unified_price.validation_result.rejection_reason
            )
            
            return unified_id
            
        except Exception as e:
            self.logger.error(f"Error storing unified price: {e}")
            raise
    
    async def get_symbols_to_process(self, symbols: List[str] = None, limit: int = None) -> List[str]:
        """
        Get list of symbols to process
        """
        if symbols:
            return symbols[:limit] if limit else symbols
        
        # Get symbols from active universe membership
        query = """
            SELECT DISTINCT i.symbol 
            FROM dev_instruments i
            JOIN dev_universe_membership um ON i.symbol = um.symbol
            WHERE um.end_at IS NULL OR um.end_at >= CURRENT_DATE
            ORDER BY i.symbol
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        rows = await self.conn.fetch(query)
        return [row['symbol'] for row in rows]
    
    async def get_missing_dates_for_symbol(self, symbol: str, start_date: date, end_date: date) -> List[date]:
        """
        Get dates that are missing unified prices for a symbol
        """
        try:
            # Get instrument_id
            instrument_id = await self.validator._get_instrument_id(symbol)
            if not instrument_id:
                return []
            
            # Check which dates already have unified prices
            query = """
                WITH date_series AS (
                    SELECT generate_series($2::date, $3::date, '1 day'::interval)::date as check_date
                ),
                business_days AS (
                    SELECT check_date
                    FROM date_series
                    WHERE EXTRACT(dow FROM check_date) NOT IN (0, 6)  -- Exclude weekends
                ),
                existing_prices AS (
                    SELECT date
                    FROM dev_daily_prices
                    WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
                )
                SELECT bd.check_date
                FROM business_days bd
                LEFT JOIN existing_prices ep ON bd.check_date = ep.date
                WHERE ep.date IS NULL
                ORDER BY bd.check_date
            """
            
            rows = await self.conn.fetch(query, instrument_id, start_date, end_date)
            return [row['check_date'] for row in rows]
            
        except Exception as e:
            self.logger.error(f"Error getting missing dates for {symbol}: {e}")
            return []
    
    async def process_symbol_date(self, symbol: str, target_date: date, run_id: int) -> Dict:
        """
        Process a single symbol for a specific date
        """
        result = {
            'symbol': symbol,
            'date': target_date.isoformat(),
            'success': False,
            'status': None,
            'confidence': None,
            'price': None,
            'error': None
        }
        
        try:
            unified_price = await self.validator.validate_and_unify_price(symbol, target_date)
            
            if unified_price and unified_price.validation_result.is_valid:
                # Store valid unified price
                unified_id = await self.store_unified_price(unified_price, run_id)
                
                result.update({
                    'success': True,
                    'status': unified_price.validation_result.status.value,
                    'confidence': unified_price.validation_result.confidence_score,
                    'price': float(unified_price.close),
                    'unified_id': unified_id,
                    'vendor_count': unified_price.vendor_count,
                    'primary_vendor': unified_price.primary_vendor
                })
                
            elif unified_price and not unified_price.validation_result.is_valid:
                # Store invalid price with validation details
                unified_id = await self.store_unified_price(unified_price, run_id)
                
                result.update({
                    'success': False,
                    'status': unified_price.validation_result.status.value,
                    'confidence': unified_price.validation_result.confidence_score,
                    'price': float(unified_price.close),
                    'unified_id': unified_id,
                    'error': unified_price.validation_result.rejection_reason
                })
            else:
                result['error'] = 'No price data available from any vendor'
                
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Error processing {symbol} on {target_date}: {e}")
        
        return result
    
    async def run_pipeline(self, 
                          start_date: date, 
                          end_date: date = None,
                          symbols: List[str] = None,
                          limit: int = None,
                          skip_existing: bool = True) -> Dict:
        """
        Run the unified daily price pipeline
        """
        if end_date is None:
            end_date = start_date
            
        self.logger.info(f"🚀 Starting unified daily price pipeline")
        self.logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        # Create run record
        parameters = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'symbols': symbols,
            'limit': limit,
            'skip_existing': skip_existing
        }
        
        run_id = await self.create_run_record('daily_price_unification', parameters)
        
        try:
            # Get symbols to process
            symbols_to_process = await self.get_symbols_to_process(symbols, limit)
            self.logger.info(f"📊 Processing {len(symbols_to_process)} symbols")
            
            # Process symbols in batches
            total_processed = 0
            total_successful = 0
            total_failed = 0
            all_results = []
            
            for i in range(0, len(symbols_to_process), self.batch_size):
                batch_symbols = symbols_to_process[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (len(symbols_to_process) + self.batch_size - 1) // self.batch_size
                
                self.logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)")
                
                # Process each symbol in the batch
                for symbol in batch_symbols:
                    symbol_results = []
                    
                    # Get missing dates for this symbol
                    if skip_existing:
                        missing_dates = await self.get_missing_dates_for_symbol(symbol, start_date, end_date)
                    else:
                        # Process all dates in range
                        missing_dates = []
                        current_date = start_date
                        while current_date <= end_date:
                            # Skip weekends
                            if current_date.weekday() < 5:
                                missing_dates.append(current_date)
                            current_date += timedelta(days=1)
                    
                    if not missing_dates:
                        self.logger.debug(f"⏭️  {symbol}: No missing dates to process")
                        continue
                    
                    self.logger.info(f"🔄 {symbol}: Processing {len(missing_dates)} dates")
                    
                    # Process each date for this symbol
                    for target_date in missing_dates:
                        result = await self.process_symbol_date(symbol, target_date, run_id)
                        symbol_results.append(result)
                        
                        if result['success']:
                            total_successful += 1
                            self.logger.info(f"✅ {symbol} {target_date}: ${result['price']:.2f} "
                                           f"({result['status']}, conf: {result['confidence']:.2f})")
                        else:
                            total_failed += 1
                            self.logger.warning(f"❌ {symbol} {target_date}: {result.get('error', 'Unknown error')}")
                        
                        total_processed += 1
                    
                    all_results.extend(symbol_results)
                
                # Log batch progress
                self.logger.info(f"✅ Batch {batch_num}/{total_batches} completed")
            
            # Calculate final results
            results = {
                'total_processed': total_processed,
                'successful': total_successful,
                'failed': total_failed,
                'success_rate': (total_successful / total_processed) if total_processed > 0 else 0,
                'symbols_count': len(symbols_to_process),
                'date_range': f"{start_date} to {end_date}",
                'run_id': run_id,
                'details': all_results
            }
            
            # Update run record with success
            await self.update_run_record(run_id, 'completed', results)
            
            self.logger.info(f"🎉 Pipeline completed successfully!")
            self.logger.info(f"📊 Results: {total_successful} successful, {total_failed} failed "
                           f"({results['success_rate']:.1%} success rate)")
            
            return results
            
        except Exception as e:
            # Update run record with failure
            await self.update_run_record(run_id, 'failed', error_message=str(e))
            self.logger.error(f"💥 Pipeline failed: {e}")
            raise


async def main():
    """Main entry point for the pipeline"""
    parser = argparse.ArgumentParser(description="Unified Daily Price Pipeline")
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD), defaults to start-date')
    parser.add_argument('--symbols', help='Comma-separated list of symbols')
    parser.add_argument('--limit', type=int, help='Limit number of symbols to process')
    parser.add_argument('--no-skip-existing', action='store_true', 
                       help='Process all dates, not just missing ones')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse arguments
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date) if args.end_date else start_date
    symbols = args.symbols.split(',') if args.symbols else None
    skip_existing = not args.no_skip_existing
    
    # Initialize pipeline
    env = Environment()
    pipeline = UnifiedDailyPricePipeline(env)
    
    try:
        await pipeline.connect()
        
        results = await pipeline.run_pipeline(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            limit=args.limit,
            skip_existing=skip_existing
        )
        
        print(f"✅ Pipeline completed: {results['successful']}/{results['total_processed']} successful")
        
    finally:
        await pipeline.disconnect()


if __name__ == "__main__":
    asyncio.run(main())