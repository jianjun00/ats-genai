#!/usr/bin/env python3
"""
Bulk S/R Event Backfill for S&P 500 Stocks

Generate S/R events for multiple stocks from 2025-01-01 until now.
Processes stocks in batches and stores events in database.

Usage:
    python scripts/bulk_sr_backfill.py --start-date 2025-01-01 --batch-size 10
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import traceback

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import the processor from the same directory
import importlib.util
script_path = os.path.join(os.path.dirname(__file__), 'simple_sr_backfill.py')
spec = importlib.util.spec_from_file_location("simple_sr_backfill", script_path)
simple_sr_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(simple_sr_module)
SimpleSRBackfillProcessor = simple_sr_module.SimpleSRBackfillProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_available_symbols(data_path: str = '/mnt/d/ats-data/minute-bars/firstrate') -> List[str]:
    """Get list of available stock symbols from data directory"""

    symbols = []
    data_path = Path(data_path)

    # Scan each letter directory for stock symbols
    for letter_dir in data_path.iterdir():
        if letter_dir.is_dir() and len(letter_dir.name) == 1 and letter_dir.name.isalpha():
            # Look for directories that look like stock symbols (3-5 uppercase letters)
            for symbol_dir in letter_dir.iterdir():
                if (symbol_dir.is_dir() and
                    3 <= len(symbol_dir.name) <= 5 and
                    symbol_dir.name.isupper() and
                    symbol_dir.name.isalpha()):

                    # Check if symbol has 2025 data
                    if (symbol_dir / '2025').exists():
                        symbols.append(symbol_dir.name)

    return sorted(symbols)

def get_sp500_symbols() -> List[str]:
    """Get a subset of major S&P 500 symbols for testing"""

    # Major S&P 500 stocks that are likely to be in the data
    major_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NFLX',
        'NVDA', 'BRK', 'UNH', 'JNJ', 'JPM', 'V', 'PG', 'HD', 'DIS',
        'MA', 'PYPL', 'VZ', 'ADBE', 'INTC', 'CRM', 'T', 'PFE', 'WMT',
        'KO', 'PEP', 'ABT', 'TMO', 'COST', 'AVGO', 'DHR', 'MRK', 'TXN',
        'LIN', 'ACN', 'XOM', 'WFC', 'MDT', 'BAC', 'ORCL', 'NEE', 'CVX'
    ]

    return major_symbols

async def process_symbol_batch(symbols: List[str], start_date: datetime,
                              end_date: datetime, batch_num: int) -> Dict:
    """Process a batch of symbols"""

    logger.info(f"🚀 Processing batch {batch_num}: {len(symbols)} symbols")

    results = {
        'batch_num': batch_num,
        'symbols_processed': 0,
        'total_events': 0,
        'errors': 0,
        'symbol_results': {}
    }

    processor = SimpleSRBackfillProcessor()

    try:
        await processor.initialize()

        for i, symbol in enumerate(symbols, 1):
            logger.info(f"📊 Processing {symbol} ({i}/{len(symbols)} in batch {batch_num})")

            try:
                result = await processor.run_backfill(symbol, start_date, end_date, limit=50)

                if result['success']:
                    results['symbols_processed'] += 1
                    results['total_events'] += result['events_processed']
                    results['symbol_results'][symbol] = result['events_processed']

                    logger.info(f"✅ {symbol}: {result['events_processed']} events")
                else:
                    results['errors'] += 1
                    results['symbol_results'][symbol] = f"ERROR: {result['error']}"
                    logger.warning(f"⚠️  {symbol}: {result['error']}")

            except Exception as e:
                results['errors'] += 1
                results['symbol_results'][symbol] = f"EXCEPTION: {str(e)}"
                logger.error(f"❌ {symbol}: {e}")

    except Exception as e:
        logger.error(f"❌ Batch {batch_num} initialization failed: {e}")
        results['errors'] = len(symbols)

    finally:
        await processor.close()

    return results

async def main():
    """Main function for bulk S/R backfill"""

    parser = argparse.ArgumentParser(description="Bulk S/R Event Backfill for S&P 500")
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: today)')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of symbols per batch')
    parser.add_argument('--max-symbols', type=int, help='Maximum number of symbols to process')
    parser.add_argument('--use-available', action='store_true', help='Use all available symbols instead of S&P 500 list')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')

    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()
    except ValueError as e:
        logger.error(f"❌ Invalid date format: {e}")
        return

    logger.info("🚀 Starting Bulk S/R Event Backfill")
    logger.info(f"📅 Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"📦 Batch size: {args.batch_size}")

    # Get symbol list
    if args.use_available:
        logger.info("🔍 Scanning for available symbols...")
        symbols = get_available_symbols()
        logger.info(f"📊 Found {len(symbols)} available symbols")
    else:
        symbols = get_sp500_symbols()
        logger.info(f"📊 Using {len(symbols)} major S&P 500 symbols")

    # Apply max symbols limit
    if args.max_symbols:
        symbols = symbols[:args.max_symbols]
        logger.info(f"🔢 Limited to {len(symbols)} symbols")

    # Process in batches
    batch_size = args.batch_size
    total_batches = (len(symbols) + batch_size - 1) // batch_size

    logger.info(f"🚀 Processing {total_batches} batches of {batch_size} symbols each")

    overall_results = {
        'total_symbols': len(symbols),
        'symbols_processed': 0,
        'total_events': 0,
        'errors': 0,
        'batch_results': []
    }

    start_time = datetime.now()

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(symbols))
        batch_symbols = symbols[start_idx:end_idx]

        logger.info(f"\n📦 Starting batch {batch_num + 1}/{total_batches}")

        try:
            batch_result = await process_symbol_batch(
                batch_symbols, start_date, end_date, batch_num + 1
            )

            # Update overall results
            overall_results['symbols_processed'] += batch_result['symbols_processed']
            overall_results['total_events'] += batch_result['total_events']
            overall_results['errors'] += batch_result['errors']
            overall_results['batch_results'].append(batch_result)

            logger.info(f"✅ Batch {batch_num + 1} complete: "
                       f"{batch_result['symbols_processed']}/{len(batch_symbols)} symbols, "
                       f"{batch_result['total_events']} events, "
                       f"{batch_result['errors']} errors")

        except Exception as e:
            logger.error(f"❌ Batch {batch_num + 1} failed: {e}")
            overall_results['errors'] += len(batch_symbols)

    # Final summary
    processing_time = (datetime.now() - start_time).total_seconds()

    logger.info("\n🎉 Bulk S/R Backfill Complete!")
    logger.info(f"📊 Processed: {overall_results['symbols_processed']}/{overall_results['total_symbols']} symbols")
    logger.info(f"🎯 Total events generated: {overall_results['total_events']}")
    logger.info(f"❌ Errors: {overall_results['errors']}")
    logger.info(f"⏱️  Total time: {processing_time:.1f} seconds")

    # Success rate
    success_rate = (overall_results['symbols_processed'] / overall_results['total_symbols']) * 100
    logger.info(f"📈 Success rate: {success_rate:.1f}%")

    # Events per symbol average
    if overall_results['symbols_processed'] > 0:
        avg_events = overall_results['total_events'] / overall_results['symbols_processed']
        logger.info(f"📊 Average events per symbol: {avg_events:.1f}")

    # Show top performers
    all_symbol_results = {}
    for batch_result in overall_results['batch_results']:
        all_symbol_results.update(batch_result.get('symbol_results', {}))

    # Get symbols with most events (filter out errors)
    successful_symbols = {k: v for k, v in all_symbol_results.items() if isinstance(v, int)}
    if successful_symbols:
        top_symbols = sorted(successful_symbols.items(), key=lambda x: x[1], reverse=True)[:10]
        logger.info("\n🏆 Top 10 symbols by events generated:")
        for symbol, events in top_symbols:
            logger.info(f"  {symbol}: {events} events")

if __name__ == "__main__":
    asyncio.run(main())