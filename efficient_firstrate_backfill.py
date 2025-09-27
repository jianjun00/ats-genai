#!/usr/bin/env python3
"""
Efficient FirstRate 30-Day Backfill
Targets major symbols first, then processes the rest efficiently
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from pathlib import Path
import logging

# Add src to path
sys.path.insert(0, 'src')

from infrastructure.vendor.firstrate.adapters.firstrate_minute_adapter import FirstRateMinuteAdapter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_priority_backfill():
    """Run backfill for priority symbols first"""
    
    # Major market symbols - highest priority
    major_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 
        'SPY', 'QQQ', 'VTI', 'IWM', 'XLF', 'XLK', 'XLE', 'XLI', 'XLV', 'XLY'
    ]
    
    # Additional popular symbols
    popular_symbols = [
        'BABA', 'BRK.B', 'JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'PFE',
        'DIS', 'ADBE', 'CRM', 'NFLX', 'KO', 'PEP', 'T', 'VZ', 'INTC', 'CSCO'
    ]
    
    start_time = datetime.now()
    logger.info("🚀 EFFICIENT FIRSTRATE 30-DAY BACKFILL")
    logger.info("="*60)
    
    async with FirstRateMinuteAdapter() as adapter:
        # Check available files
        files = adapter.get_recent_firstrate_files(30)
        logger.info(f"📁 Found {len(files)} FirstRate files for past 30 days")
        
        all_results = []
        
        # Phase 1: Major symbols (highest priority)
        logger.info(f"🎯 PHASE 1: Processing {len(major_symbols)} major symbols")
        results1 = await adapter.incremental_backfill_to_files(
            symbols=major_symbols,
            days_back=30,
            output_path='/mnt/d/ats-data/minute-bars/firstrate'
        )
        all_results.append(('Major Symbols', results1))
        logger.info(f"✅ Phase 1: {len(results1.get('symbols_processed', []))}/{len(major_symbols)} processed, "
                   f"{results1.get('files_written', 0)} files written")
        logger.info(f"📈 PHASE 2: Processing {len(popular_symbols)} popular symbols")
        results2 = await adapter.incremental_backfill_to_files(
            symbols=popular_symbols,
            days_back=30,
            output_path='/mnt/d/ats-data/minute-bars/firstrate'
        )
        all_results.append(('Popular Symbols', results2))
        logger.info(f"✅ Phase 2: {len(results2.get('symbols_processed', []))}/{len(popular_symbols)} processed, "
                   f"{results2.get('files_written', 0)} files written")
        logger.info(f"🔤 PHASE 3: Sample processing across alphabet")
        
        # Get a representative sample from each letter
        alphabet_sample = []
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            # Add 3-5 symbols per letter that are commonly traded
            if letter == 'A': alphabet_sample.extend(['AMD', 'ABNB', 'ADSK', 'AXP'])
            elif letter == 'B': alphabet_sample.extend(['BAC', 'BMY', 'BA', 'BIIB']) 
            elif letter == 'C': alphabet_sample.extend(['CAT', 'CVX', 'CRM', 'COST'])
            elif letter == 'D': alphabet_sample.extend(['DIS', 'DOW', 'DHR'])
            elif letter == 'F': alphabet_sample.extend(['F', 'FB', 'FDX'])
            elif letter == 'G': alphabet_sample.extend(['GE', 'GM', 'GILD'])
            elif letter == 'I': alphabet_sample.extend(['IBM', 'INTC', 'INTU'])
            elif letter == 'M': alphabet_sample.extend(['MMM', 'MRK', 'MCD'])
            elif letter == 'N': alphabet_sample.extend(['NVDA', 'NFLX', 'NKE'])
            elif letter == 'O': alphabet_sample.extend(['ORCL', 'OXY'])
            elif letter == 'P': alphabet_sample.extend(['PG', 'PFE', 'PYPL'])
            elif letter == 'S': alphabet_sample.extend(['SNOW', 'SHOP', 'SQ'])
            elif letter == 'T': alphabet_sample.extend(['TSLA', 'TXN', 'TMO'])
            elif letter == 'U': alphabet_sample.extend(['UBER', 'UNH'])
            elif letter == 'W': alphabet_sample.extend(['WMT', 'WFC'])
            elif letter == 'Z': alphabet_sample.extend(['ZM', 'ZNGA'])
        
        # Remove duplicates and symbols already processed
        processed_symbols = set()
        for _, result in all_results:
            processed_symbols.update(result.get('symbols_processed', []))
        
        alphabet_sample = [s for s in set(alphabet_sample) if s not in processed_symbols]
        
        logger.info(f"🎯 Phase 3: Processing {len(alphabet_sample)} alphabet sample symbols")
        results3 = await adapter.incremental_backfill_to_files(
            symbols=alphabet_sample,
            days_back=30,
            output_path='/mnt/d/ats-data/minute-bars/firstrate'
        )
        all_results.append(('Alphabet Sample', results3))
        logger.info(f"✅ Phase 3: {len(results3.get('symbols_processed', []))}/{len(alphabet_sample)} processed, "
                   f"{results3.get('files_written', 0)} files written")
        duration = datetime.now() - start_time
        total_processed = sum(len(result.get('symbols_processed', [])) for _, result in all_results)
        total_files_written = sum(result.get('files_written', 0) for _, result in all_results)
        total_files_skipped = sum(result.get('files_skipped', 0) for _, result in all_results)
        
        logger.info("="*60)
        logger.info("🏁 EFFICIENT BACKFILL COMPLETE")
        logger.info("="*60)
        logger.info(f"⏱️ Total duration: {duration}")
        logger.info(f"📊 Total symbols processed: {total_processed}")
        logger.info(f"📄 Files written (updated): {total_files_written}")
        logger.info(f"⏭️ Files skipped (no changes): {total_files_skipped}")
        
        logger.info("\n📋 Phase Results:")
        for phase_name, result in all_results:
            processed = len(result.get('symbols_processed', []))
            written = result.get('files_written', 0)
            skipped = result.get('files_skipped', 0)
            logger.info(f"  {phase_name}: {processed} processed, {written} written, {skipped} skipped")
        
        if total_files_written > 0:
            logger.info(f"\n🎯 SUCCESS: Updated {total_files_written} files with recent data!")
            
            # Show some examples of updated files
            logger.info("\n📁 Sample updated files:")
            sample_symbols = (major_symbols + popular_symbols)[:10]
            for symbol in sample_symbols:
                file_path = f"/mnt/d/ats-data/minute-bars/firstrate/{symbol[0]}/{symbol}/2025/09/{symbol}_2025_09.parquet"
                if Path(file_path).exists():
                    mod_time = datetime.fromtimestamp(Path(file_path).stat().st_mtime)
                    logger.info(f"  ✅ {symbol}: {file_path} (modified: {mod_time})")

if __name__ == "__main__":
    asyncio.run(run_priority_backfill())