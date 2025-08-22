#!/usr/bin/env python3
"""
Comprehensive Data Backfill Flyte Workflow

Clean, efficient Flyte workflow for 30-year historical data backfill that:
- Uses existing base Docker images (no package installation)
- Leverages run_dev infrastructure 
- Supports both daily and minute data across all vendors (Polygon, Tiingo, FMP)
- Uses dynamic code upload to pre-configured containers

Usage with run_dev:
    python scripts/flyte/comprehensive_data_backfill_workflow.py --data-type daily --years 30
    python scripts/flyte/comprehensive_data_backfill_workflow.py --data-type minute --years 5
"""

import asyncio
import argparse
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

from flytekit import task, workflow, dynamic, ImageSpec
from flytekit.types.file import FlyteFile

# Use existing base image - no package installation needed
BASE_IMAGE = ImageSpec(
    name="ats-base",
    registry="existing",  # Use existing registry
    base_image="ats-base:latest"  # Reference existing base image
)

@dataclass
class BackfillConfig:
    """Configuration for comprehensive backfill operations"""
    data_type: str  # 'daily' or 'minute'
    vendors: List[str]  # ['polygon', 'tiingo', 'fmp']
    start_date: date
    end_date: date
    symbols: List[str]
    chunk_size: int = 50  # Symbols per chunk
    
@task(container_image=BASE_IMAGE)
def get_symbols_for_backfill(universe_type: str = "sp500_major") -> List[str]:
    """Get symbols for backfill using existing infrastructure"""
    # This task runs in the existing base image with all packages pre-installed
    import asyncio
    import asyncpg
    
    async def fetch_symbols():
        # Use existing database connection from run_dev infrastructure
        db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            
            if universe_type == "sp500_major":
                query = """
                    SELECT DISTINCT symbol 
                    FROM dev_instruments 
                    WHERE symbol IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO', 
                                   'WMT', 'LLY', 'JPM', 'UNH', 'XOM', 'V', 'PG', 'MA', 'JNJ', 'HD',
                                   'COST', 'ABBV', 'NFLX', 'BAC', 'CRM', 'KO', 'WFC', 'CVX', 'AMD',
                                   'ADBE', 'LIN', 'MRK', 'DIS', 'PEP', 'TMO', 'VZ', 'ACN', 'CSCO')
                    ORDER BY symbol
                """
            else:
                query = """
                    SELECT DISTINCT symbol 
                    FROM dev_instruments 
                    WHERE symbol IS NOT NULL
                    ORDER BY symbol
                    LIMIT 100
                """
            
            rows = await conn.fetch(query)
            symbols = [row['symbol'] for row in rows]
            await conn.close()
            
            return symbols
            
        except Exception as e:
            print(f"Error fetching symbols: {e}")
            # Fallback list
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'WMT', 'JPM', 'UNH']
    
    return asyncio.run(fetch_symbols())

@task(container_image=BASE_IMAGE)
def run_daily_backfill_chunk(symbols: List[str], start_date: str, end_date: str, vendors: List[str]) -> Dict:
    """Run daily price backfill for a chunk of symbols"""
    # This runs in existing base image - all packages already available
    import asyncio
    import sys
    import os
    
    # Add src to path (already configured in base image)
    sys.path.insert(0, '/app/src')
    
    async def backfill_chunk():
        from scripts.backfill.run_comprehensive_30year_backfill import ComprehensiveBackfillOrchestrator, BackfillConfig
        from config.environment import Environment
        from datetime import date
        
        # Convert string dates back to date objects
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        
        config = BackfillConfig(
            start_date=start,
            end_date=end,
            chunk_years=1,
            batch_size=len(symbols),
            vendors=vendors
        )
        
        env = Environment()
        orchestrator = ComprehensiveBackfillOrchestrator(env, config)
        
        # Run backfill for this specific chunk
        results = await orchestrator.run_comprehensive_backfill(symbols)
        
        return {
            'symbols_processed': len(symbols),
            'total_records': results.total_records_inserted,
            'vendor_results': results.vendor_progress,
            'chunk_start': start_date,
            'chunk_end': end_date
        }
    
    return asyncio.run(backfill_chunk())

@task(container_image=BASE_IMAGE)
def run_minute_backfill_chunk(symbols: List[str], start_date: str, end_date: str, vendors: List[str]) -> Dict:
    """Run minute price backfill for a chunk of symbols"""
    # This runs in existing base image - all packages already available
    import asyncio
    import sys
    
    # Add src to path (already configured in base image)
    sys.path.insert(0, '/app/src')
    
    async def backfill_minute_chunk():
        # Use existing minute backfill infrastructure
        from market_data.backfill.enhanced_minute_backfill_orchestrator import EnhancedMinuteBackfillOrchestrator, EnhancedBackfillConfig
        from config.environment import Environment
        from datetime import date
        
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        
        config = EnhancedBackfillConfig(
            start_date=start,
            end_date=end,
            symbols=symbols,
            vendors=vendors,
            batch_size=2,  # Small batches for minute data
            enable_checkpointing=True
        )
        
        env = Environment()
        orchestrator = EnhancedMinuteBackfillOrchestrator(env, config)
        
        # Run minute backfill
        results = await orchestrator.run_backfill()
        
        return {
            'symbols_processed': len(symbols),
            'total_records': results.get('total_records', 0),
            'chunk_start': start_date,
            'chunk_end': end_date,
            'data_type': 'minute'
        }
    
    return asyncio.run(backfill_minute_chunk())

@dynamic(container_image=BASE_IMAGE)
def parallel_backfill_execution(config: BackfillConfig) -> List[Dict]:
    """Execute parallel backfill across multiple chunks"""
    
    # Get symbols
    symbols = get_symbols_for_backfill()
    
    # Create symbol chunks
    symbol_chunks = []
    for i in range(0, len(symbols), config.chunk_size):
        chunk = symbols[i:i + config.chunk_size]
        symbol_chunks.append(chunk)
    
    # Create date ranges
    if config.data_type == 'daily':
        # Larger date ranges for daily data
        chunk_months = 12
    else:
        # Smaller date ranges for minute data
        chunk_months = 1
    
    date_ranges = []
    current_date = config.start_date
    while current_date < config.end_date:
        chunk_end = min(
            current_date.replace(year=current_date.year + (chunk_months // 12),
                               month=current_date.month + (chunk_months % 12)),
            config.end_date
        )
        date_ranges.append((current_date.isoformat(), chunk_end.isoformat()))
        current_date = chunk_end + timedelta(days=1)
    
    # Execute backfill tasks in parallel
    results = []
    for symbols_chunk in symbol_chunks:
        for start_date, end_date in date_ranges:
            if config.data_type == 'daily':
                result = run_daily_backfill_chunk(
                    symbols=symbols_chunk,
                    start_date=start_date,
                    end_date=end_date,
                    vendors=config.vendors
                )
            else:
                result = run_minute_backfill_chunk(
                    symbols=symbols_chunk,
                    start_date=start_date,
                    end_date=end_date,
                    vendors=config.vendors
                )
            results.append(result)
    
    return results

@workflow
def comprehensive_data_backfill_workflow(
    data_type: str = "daily",
    years: int = 30,
    vendors: List[str] = ["polygon", "tiingo", "fmp"]
) -> Dict:
    """
    Comprehensive Data Backfill Workflow
    
    Efficiently orchestrates historical data backfill using existing infrastructure:
    - No package installation (uses existing base Docker images)
    - Leverages run_dev database connections
    - Supports both daily and minute data
    - Parallel execution with dynamic chunking
    """
    
    # Calculate date range
    end_date = date.today()
    start_date = date(end_date.year - years, 1, 1)
    
    if data_type == "minute" and years > 10:
        # Limit minute data to reasonable timeframe
        start_date = date(end_date.year - 5, 1, 1)
    
    config = BackfillConfig(
        data_type=data_type,
        vendors=vendors,
        start_date=start_date,
        end_date=end_date,
        symbols=[],  # Will be populated dynamically
        chunk_size=20 if data_type == "minute" else 50
    )
    
    # Execute parallel backfill
    results = parallel_backfill_execution(config=config)
    
    # Aggregate results
    total_records = sum(r.get('total_records', 0) for r in results)
    total_symbols = sum(r.get('symbols_processed', 0) for r in results)
    
    return {
        'backfill_type': f"{data_type}_data_backfill",
        'total_records_inserted': total_records,
        'total_symbols_processed': total_symbols,
        'vendors_used': vendors,
        'date_range': f"{start_date} to {end_date}",
        'chunks_completed': len(results),
        'infrastructure': 'run_dev + existing_base_docker + flyte'
    }

def main():
    """Main function for running via run_dev"""
    parser = argparse.ArgumentParser(description='Comprehensive Data Backfill via Flyte')
    parser.add_argument('--data-type', choices=['daily', 'minute'], default='daily')
    parser.add_argument('--years', type=int, default=30)
    parser.add_argument('--vendors', nargs='+', default=['polygon', 'tiingo', 'fmp'])
    
    args = parser.parse_args()
    
    # This will be executed via run_dev infrastructure
    print(f"🚀 Starting {args.data_type} backfill for {args.years} years")
    print(f"📊 Vendors: {args.vendors}")
    print("✅ Using existing infrastructure - no package installation needed!")
    
    # The workflow will be executed by Flyte using existing base Docker images
    return comprehensive_data_backfill_workflow(
        data_type=args.data_type,
        years=args.years,
        vendors=args.vendors
    )

if __name__ == "__main__":
    main()