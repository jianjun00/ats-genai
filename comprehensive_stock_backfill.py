#!/usr/bin/env python3
"""
Comprehensive FirstRate Stock Backfill
Processes ALL available stock symbols from FirstRate ZIP archives from 2000-2025

Usage:
    python3 scripts/run_dev.py run --script comprehensive_stock_backfill.py
    python3 scripts/run_dev.py run --script comprehensive_stock_backfill.py --limit 100
    python3 scripts/run_dev.py run --script comprehensive_stock_backfill.py --resume
"""

import os
import sys
import asyncio
import json
import argparse
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Tuple
from pathlib import Path
from collections import defaultdict
import time
import zipfile
import subprocess

# Add src to Python path 
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

logger = logging.getLogger(__name__)


class ComprehensiveStockBackfillManager:
    """Manages comprehensive FirstRate stock backfill from all ZIP archives"""
    
    def __init__(
        self,
        data_path: str = "/data/firstrate-data/stock",
        output_path: str = "/data/minute-bars/firstrate", 
        checkpoint_file: str = "firstrate_comprehensive_stock_backfill.json"
    ):
        self.data_path = Path(data_path)
        self.output_path = Path(output_path)
        self.checkpoint_file = Path(checkpoint_file)
        
        # Track processing state
        self.available_symbols: Set[str] = set()
        self.processed_symbols: Set[str] = set()
        self.remaining_symbols: Set[str] = set()
        
        # Checkpoint data
        self.checkpoint_data = {
            "started_at": None,
            "completed_symbols": {},
            "failed_symbols": {},
            "zip_file_inventory": {},
            "processing_stats": {
                "total_symbols_discovered": 0,
                "total_symbols_processed": 0,
                "total_symbols_failed": 0,
                "total_records_written": 0,
                "total_processing_time": 0
            }
        }
        
    def load_checkpoint(self) -> None:
        """Load existing checkpoint data"""
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'r') as f:
                    self.checkpoint_data = json.load(f)
                    logger.info(f"Loaded checkpoint with {len(self.checkpoint_data.get('completed_symbols', {}))} completed symbols")
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
            
    def save_checkpoint(self) -> None:
        """Save current checkpoint data"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2, default=str)
                logger.info(f"Checkpoint saved: {len(self.checkpoint_data.get('completed_symbols', {}))} completed symbols")
        except Exception as e:
            logger.error(f"Could not save checkpoint: {e}")
    
    def discover_available_symbols(self) -> None:
        """Discover all available stock symbols from ZIP files"""
        logger.info(f"🔍 Discovering symbols in {self.data_path}")
        
        zip_inventory = {}
        all_symbols = set()
        
        for zip_file in self.data_path.glob("*.zip"):
            try:
                symbols_in_zip = set()
                logger.info(f"📦 Scanning {zip_file.name}...")
                
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    for file_name in zf.namelist():
                        if file_name.endswith('.txt') and '_' in file_name:
                            # Extract symbol from filename like "AAPL_20241201.txt"
                            symbol = file_name.split('_')[0]
                            if len(symbol) >= 1 and symbol.isalpha():
                                symbols_in_zip.add(symbol)
                                all_symbols.add(symbol)
                
                zip_inventory[str(zip_file)] = {
                    "symbols": sorted(list(symbols_in_zip)),
                    "symbol_count": len(symbols_in_zip),
                    "file_size": zip_file.stat().st_size,
                    "last_modified": zip_file.stat().st_mtime
                }
                
                logger.info(f"  Found {len(symbols_in_zip)} symbols in {zip_file.name}")
                
            except Exception as e:
                logger.error(f"Error scanning {zip_file}: {e}")
                
        self.available_symbols = all_symbols
        self.checkpoint_data["zip_file_inventory"] = zip_inventory
        self.checkpoint_data["processing_stats"]["total_symbols_discovered"] = len(all_symbols)
        
        logger.info(f"✅ Discovered {len(all_symbols)} total stock symbols from {len(zip_inventory)} ZIP files")
        
    def discover_processed_symbols(self) -> None:
        """Discover what symbols are already processed"""
        logger.info(f"🔍 Checking already processed symbols in {self.output_path}")
        
        processed = set()
        
        if self.output_path.exists():
            for symbol_dir in self.output_path.iterdir():
                if symbol_dir.is_dir() and symbol_dir.name.isupper() and len(symbol_dir.name) <= 5:
                    # Check if symbol has meaningful data (not just empty directories)
                    has_data = False
                    for year_dir in symbol_dir.iterdir():
                        if year_dir.is_dir() and year_dir.name.isdigit():
                            for month_dir in year_dir.iterdir():
                                if month_dir.is_dir():
                                    parquet_files = list(month_dir.glob("*.parquet"))
                                    if parquet_files:
                                        has_data = True
                                        break
                            if has_data:
                                break
                    
                    if has_data:
                        processed.add(symbol_dir.name)
        
        self.processed_symbols = processed
        logger.info(f"✅ Found {len(processed)} already processed symbols")
        
    def calculate_remaining_work(self) -> None:
        """Calculate remaining symbols to process"""
        completed_from_checkpoint = set(self.checkpoint_data.get("completed_symbols", {}).keys())
        self.remaining_symbols = self.available_symbols - self.processed_symbols - completed_from_checkpoint
        
        logger.info(f"📊 Processing Status:")
        logger.info(f"  Available symbols: {len(self.available_symbols)}")
        logger.info(f"  Already processed: {len(self.processed_symbols)}")
        logger.info(f"  Completed in checkpoint: {len(completed_from_checkpoint)}")
        logger.info(f"  Remaining to process: {len(self.remaining_symbols)}")
        
    def create_processing_batches(self, limit: int = None) -> List[List[str]]:
        """Create processing batches for parallel execution"""
        symbols_to_process = sorted(list(self.remaining_symbols))
        
        if limit:
            symbols_to_process = symbols_to_process[:limit]
            logger.info(f"🔢 Limited to {limit} symbols for processing")
        
        # Create batches of 50 symbols each for manageable checkpoint sizes
        batch_size = 50
        batches = []
        
        for i in range(0, len(symbols_to_process), batch_size):
            batch = symbols_to_process[i:i + batch_size]
            batches.append(batch)
            
        logger.info(f"📦 Created {len(batches)} processing batches")
        return batches
        
    def execute_backfill_batch(self, symbols: List[str], batch_num: int, total_batches: int) -> bool:
        """Execute backfill for a batch of symbols"""
        symbols_str = ",".join(symbols)
        
        logger.info(f"🚀 Processing batch {batch_num}/{total_batches}: {len(symbols)} symbols")
        logger.info(f"   Symbols: {symbols_str}")
        
        try:
            # Use existing populate script with specific symbols
            cmd = [
                'python3', 
                'scripts/populate_firstrate_minute_bars.py',
                '--asset-type', 'stock',
                '--symbols', symbols_str,
                '--checkpoint-file', f'batch_{batch_num}_checkpoint.json',
                '--debug'
            ]
            
            start_time = time.time()
            
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd='/workspace'  # Docker container working directory
            )
            
            processing_time = time.time() - start_time
            
            if result.returncode == 0:
                # Success - update checkpoint
                for symbol in symbols:
                    self.checkpoint_data["completed_symbols"][symbol] = {
                        "processed_at": datetime.now().isoformat(),
                        "processing_time": processing_time / len(symbols),
                        "batch_number": batch_num
                    }
                    
                self.checkpoint_data["processing_stats"]["total_symbols_processed"] += len(symbols)
                self.checkpoint_data["processing_stats"]["total_processing_time"] += processing_time
                
                logger.info(f"✅ Batch {batch_num} completed successfully in {processing_time:.1f}s")
                return True
                
            else:
                # Failure - log and track
                error_msg = result.stderr or result.stdout or "Unknown error"
                
                for symbol in symbols:
                    self.checkpoint_data["failed_symbols"][symbol] = {
                        "failed_at": datetime.now().isoformat(), 
                        "error": error_msg[:500],  # Truncate long errors
                        "batch_number": batch_num
                    }
                    
                self.checkpoint_data["processing_stats"]["total_symbols_failed"] += len(symbols)
                
                logger.error(f"❌ Batch {batch_num} failed: {error_msg[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Exception processing batch {batch_num}: {e}")
            return False
        
    def run_comprehensive_backfill(self, limit: int = None, resume: bool = False) -> None:
        """Run the comprehensive stock backfill process"""
        
        if not resume or not self.checkpoint_data.get("started_at"):
            self.checkpoint_data["started_at"] = datetime.now().isoformat()
        
        logger.info("🚀 Starting Comprehensive FirstRate Stock Backfill")
        logger.info(f"   Data Path: {self.data_path}")
        logger.info(f"   Output Path: {self.output_path}")
        logger.info(f"   Checkpoint: {self.checkpoint_file}")
        
        # Discovery phase
        self.discover_available_symbols()
        self.discover_processed_symbols() 
        self.calculate_remaining_work()
        
        if not self.remaining_symbols:
            logger.info("✅ No remaining symbols to process - backfill is complete!")
            return
            
        # Create and process batches
        batches = self.create_processing_batches(limit=limit)
        
        successful_batches = 0
        failed_batches = 0
        
        for i, batch in enumerate(batches, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Batch {i}/{len(batches)}")
            logger.info(f"{'='*60}")
            
            success = self.execute_backfill_batch(batch, i, len(batches))
            
            if success:
                successful_batches += 1
            else:
                failed_batches += 1
                
            # Save checkpoint after each batch
            self.save_checkpoint()
            
            # Brief pause between batches
            if i < len(batches):
                time.sleep(2)
        
        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("🏁 Comprehensive Stock Backfill Complete")
        logger.info(f"{'='*60}")
        logger.info(f"✅ Successful batches: {successful_batches}")
        logger.info(f"❌ Failed batches: {failed_batches}")
        logger.info(f"📊 Total symbols processed: {self.checkpoint_data['processing_stats']['total_symbols_processed']}")
        logger.info(f"📊 Total symbols failed: {self.checkpoint_data['processing_stats']['total_symbols_failed']}")
        logger.info(f"⏱️  Total processing time: {self.checkpoint_data['processing_stats']['total_processing_time']:.1f}s")


def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(description="Comprehensive FirstRate Stock Backfill")
    parser.add_argument('--limit', type=int, help='Limit number of symbols to process (for testing)')
    parser.add_argument('--resume', action='store_true', help='Resume from existing checkpoint')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('comprehensive_stock_backfill.log')
        ]
    )
    
    # Create and run backfill manager
    manager = ComprehensiveStockBackfillManager()
    manager.load_checkpoint()
    manager.run_comprehensive_backfill(limit=args.limit, resume=args.resume)


if __name__ == "__main__":
    main()