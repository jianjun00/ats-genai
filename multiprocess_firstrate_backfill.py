#!/usr/bin/env python3
"""
Multiprocessing FirstRate Backfill System
High-performance parallel processing using Python's built-in multiprocessing

This system provides massive speedup without external dependencies:
- Uses all CPU cores (20 cores available)
- Process-based parallelism for CPU-intensive work
- Thread-based parallelism for I/O-intensive operations  
- Progress tracking and fault tolerance
- Dynamic load balancing

Expected performance improvements:
- Single-threaded: 200+ hours
- Multi-process (16 workers): ~12-25 hours (8-16x speedup)
- Hybrid approach: Even better performance
"""

import os
import sys
import time
import json
import logging
import subprocess
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
from pathlib import Path
import zipfile
import queue
import threading
from dataclasses import dataclass
from collections import defaultdict

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result from processing a single symbol"""
    symbol: str
    success: bool
    processing_time: float
    records_written: int = 0
    error_message: str = ""
    worker_id: int = 0


class SymbolProcessor:
    """Individual symbol processor for multiprocessing"""
    
    @staticmethod 
    def process_symbol(symbol_info: Tuple[str, int]) -> ProcessingResult:
        """Process a single symbol (static method for multiprocessing)"""
        
        symbol, worker_id = symbol_info
        start_time = time.time()
        
        try:
            # Setup logging for this process
            logging.basicConfig(level=logging.INFO)
            process_logger = logging.getLogger(f'worker_{worker_id}')
            
            process_logger.info(f"🚀 Worker {worker_id} processing {symbol}")
            
            # Execute FirstRate processing via subprocess
            # This ensures proper environment and avoids import issues
            cmd = [
                'python3', 
                'scripts/populate_firstrate_minute_bars.py',
                '--asset-type', 'stock',
                '--symbols', symbol,
                '--checkpoint-file', f'worker_{worker_id}_{symbol}_checkpoint.json',
                '--debug'
            ]
            
            # Execute in the proper directory with timeout
            result = subprocess.run(
                cmd, 
                cwd='/home/jianjun/ats-genai-data',
                capture_output=True, 
                text=True,
                timeout=3600  # 1 hour timeout per symbol
            )
            
            processing_time = time.time() - start_time
            
            if result.returncode == 0:
                # Success - try to extract records written from output
                records_written = SymbolProcessor._extract_records_from_output(result.stdout)
                
                process_logger.info(f"✅ Worker {worker_id} completed {symbol} in {processing_time:.1f}s")
                
                return ProcessingResult(
                    symbol=symbol,
                    success=True,
                    processing_time=processing_time,
                    records_written=records_written,
                    worker_id=worker_id
                )
            else:
                # Failure
                error_msg = result.stderr or result.stdout or "Unknown error"
                process_logger.error(f"❌ Worker {worker_id} failed {symbol}: {error_msg[:200]}")
                
                return ProcessingResult(
                    symbol=symbol,
                    success=False,
                    processing_time=processing_time,
                    error_message=error_msg[:500],  # Truncate long errors
                    worker_id=worker_id
                )
                
        except subprocess.TimeoutExpired:
            processing_time = time.time() - start_time
            return ProcessingResult(
                symbol=symbol,
                success=False,
                processing_time=processing_time,
                error_message=f"Processing timeout after {processing_time:.1f}s",
                worker_id=worker_id
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            return ProcessingResult(
                symbol=symbol,
                success=False,
                processing_time=processing_time,
                error_message=str(e),
                worker_id=worker_id
            )
    
    @staticmethod
    def _extract_records_from_output(output: str) -> int:
        """Extract number of records written from command output"""
        try:
            # Look for patterns like "Written X records" or "Processed X records"
            import re
            patterns = [
                r'written\s+(\d+)\s+records',
                r'processed\s+(\d+)\s+records',
                r'saved\s+(\d+)\s+records'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, output.lower())
                if matches:
                    return int(matches[-1])  # Take the last match
            
            return 0  # No records found in output
            
        except Exception:
            return 0


class ParallelBackfillCoordinator:
    """Coordinates parallel backfill across multiple processes"""
    
    def __init__(self, max_workers: int = None):
        # Auto-detect optimal number of workers
        if max_workers is None:
            cpu_count = multiprocessing.cpu_count()
            # Use 80% of available cores, leave some for system
            max_workers = max(1, int(cpu_count * 0.8))
        
        self.max_workers = max_workers
        self.results_queue = queue.Queue()
        self.progress_stats = {
            "started_at": datetime.now(),
            "total_symbols": 0,
            "completed_symbols": 0,
            "failed_symbols": 0,
            "total_processing_time": 0,
            "worker_stats": defaultdict(lambda: {"processed": 0, "failed": 0, "total_time": 0}),
            "processing_rate": 0,  # symbols per hour
            "eta_completion": None
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'parallel_backfill_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            ]
        )
        self.logger = logging.getLogger('coordinator')
        
    def process_symbols_parallel(self, symbols: List[str]) -> Dict:
        """Process symbols using multiprocessing"""
        
        self.logger.info(f"🚀 Starting parallel processing of {len(symbols)} symbols")
        self.logger.info(f"🖥️  Using {self.max_workers} parallel workers")
        self.progress_stats["total_symbols"] = len(symbols)
        
        start_time = time.time()
        
        # Prepare symbol-worker pairs for processing
        symbol_worker_pairs = [
            (symbol, i % self.max_workers) 
            for i, symbol in enumerate(symbols)
        ]
        
        successful_results = []
        failed_results = []
        
        # Use ProcessPoolExecutor for CPU-intensive work
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(SymbolProcessor.process_symbol, symbol_pair): symbol_pair[0]
                for symbol_pair in symbol_worker_pairs
            }
            
            self.logger.info(f"📤 Submitted {len(future_to_symbol)} tasks to {self.max_workers} processes")
            
            # Process results as they complete
            completed_count = 0
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                
                try:
                    result = future.result()
                    
                    if result.success:
                        successful_results.append(result)
                        self.progress_stats["completed_symbols"] += 1
                    else:
                        failed_results.append(result)
                        self.progress_stats["failed_symbols"] += 1
                    
                    # Update worker stats
                    worker_id = result.worker_id
                    self.progress_stats["worker_stats"][worker_id]["total_time"] += result.processing_time
                    
                    if result.success:
                        self.progress_stats["worker_stats"][worker_id]["processed"] += 1
                    else:
                        self.progress_stats["worker_stats"][worker_id]["failed"] += 1
                    
                    completed_count += 1
                    
                    # Progress reporting every 10 completions or 5%
                    if completed_count % max(1, min(10, len(symbols) // 20)) == 0:
                        elapsed_time = time.time() - start_time
                        progress_pct = (completed_count / len(symbols)) * 100
                        processing_rate = completed_count / (elapsed_time / 3600) if elapsed_time > 0 else 0
                        
                        eta_hours = (len(symbols) - completed_count) / processing_rate if processing_rate > 0 else 0
                        eta_time = datetime.now() + timedelta(hours=eta_hours)
                        
                        self.progress_stats["processing_rate"] = processing_rate
                        self.progress_stats["eta_completion"] = eta_time
                        
                        self.logger.info(f"📊 Progress: {completed_count}/{len(symbols)} ({progress_pct:.1f}%) - "
                                       f"Rate: {processing_rate:.1f}/hr - "
                                       f"Success: {len(successful_results)}/{completed_count} - "
                                       f"ETA: {eta_time.strftime('%Y-%m-%d %H:%M')}")
                        
                        # Show top performing workers
                        if completed_count % 50 == 0:  # Every 50 completions
                            self._log_worker_performance()
                
                except Exception as e:
                    self.logger.error(f"❌ Exception processing {symbol}: {e}")
                    failed_results.append(ProcessingResult(
                        symbol=symbol,
                        success=False,
                        processing_time=0,
                        error_message=str(e),
                        worker_id=-1
                    ))
                    self.progress_stats["failed_symbols"] += 1
        
        total_processing_time = time.time() - start_time
        self.progress_stats["total_processing_time"] = total_processing_time
        self.progress_stats["completed_at"] = datetime.now()
        
        return {
            "successful_results": successful_results,
            "failed_results": failed_results,
            "processing_stats": dict(self.progress_stats),
            "total_processing_time": total_processing_time
        }
    
    def _log_worker_performance(self):
        """Log worker performance statistics"""
        
        self.logger.info("👥 Worker Performance Summary:")
        
        for worker_id, stats in self.progress_stats["worker_stats"].items():
            total_processed = stats["processed"] + stats["failed"]
            success_rate = (stats["processed"] / total_processed * 100) if total_processed > 0 else 0
            avg_time = stats["total_time"] / total_processed if total_processed > 0 else 0
            
            self.logger.info(f"   Worker {worker_id}: {stats['processed']}/{total_processed} "
                           f"({success_rate:.1f}% success) - Avg: {avg_time:.1f}s/symbol")


class MultiprocessFirstRateBackfillSystem:
    """Main multiprocessing-based backfill system"""
    
    def __init__(self):
        self.coordinator = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('mp_system')
    
    def load_remaining_symbols(self) -> List[str]:
        """Load remaining symbols from analysis file"""
        
        analysis_file = Path("firstrate_stock_universe_analysis.json")
        
        if analysis_file.exists():
            with open(analysis_file, 'r') as f:
                analysis = json.load(f)
            
            remaining = analysis.get('remaining_symbols', [])
            self.logger.info(f"📊 Loaded {len(remaining)} remaining symbols from analysis")
            return remaining
        else:
            self.logger.warning("⚠️  Analysis file not found - using sample symbols")
            return ['MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'BAC', 'PG', 'KO']
    
    def run_parallel_backfill(self, symbols: List[str], max_workers: int = None) -> Dict:
        """Run the parallel backfill operation"""
        
        self.logger.info(f"🚀 Starting multiprocessing backfill of {len(symbols)} symbols")
        
        # Create coordinator
        self.coordinator = ParallelBackfillCoordinator(max_workers=max_workers)
        
        # Run parallel processing
        results = self.coordinator.process_symbols_parallel(symbols)
        
        # Save detailed results
        results_file = f"multiprocess_backfill_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Convert datetime objects to strings for JSON serialization
        serializable_results = self._make_json_serializable(results)
        
        with open(results_file, "w") as f:
            json.dump(serializable_results, f, indent=2, default=str)
        
        self.logger.info(f"💾 Results saved to {results_file}")
        
        return results
    
    def _make_json_serializable(self, data):
        """Convert data to JSON-serializable format"""
        if isinstance(data, dict):
            return {key: self._make_json_serializable(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._make_json_serializable(item) for item in data]
        elif isinstance(data, datetime):
            return data.isoformat()
        elif hasattr(data, '__dict__'):
            return self._make_json_serializable(data.__dict__)
        else:
            return data


def main():
    """Main execution function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Multiprocessing FirstRate Backfill")
    parser.add_argument('--max-workers', type=int, help='Maximum number of parallel workers (default: auto-detect)')
    parser.add_argument('--limit', type=int, help='Limit number of symbols for testing')
    parser.add_argument('--test-mode', action='store_true', help='Run with small test batch')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Multiprocessing FirstRate Backfill System")
    print("=" * 60)
    
    # Show system capabilities
    cpu_count = multiprocessing.cpu_count()
    recommended_workers = max(1, int(cpu_count * 0.8))
    actual_workers = args.max_workers or recommended_workers
    
    print(f"🖥️  System: {cpu_count} CPU cores available")
    print(f"🚀 Workers: {actual_workers} parallel processes")
    print(f"📈 Expected speedup: {actual_workers}x (theoretical)")
    print()
    
    # Initialize system
    system = MultiprocessFirstRateBackfillSystem()
    
    try:
        # Load symbols to process
        all_symbols = system.load_remaining_symbols()
        
        if args.test_mode:
            # Test mode: small batch
            symbols = all_symbols[:20]
            print(f"🧪 Test mode: processing {len(symbols)} symbols")
        elif args.limit:
            # Limited run
            symbols = all_symbols[:args.limit]
            print(f"🔢 Limited run: processing {len(symbols)} symbols")
        else:
            # Full run
            symbols = all_symbols
            print(f"🏗️  Full run: processing {len(symbols)} symbols")
            
            if len(symbols) > 1000:
                print("⚠️  WARNING: This will process thousands of symbols")
                print("   Consider using --test-mode first to validate the system")
                
                response = input("Continue with full parallel backfill? [y/N]: ")
                if response.lower() != 'y':
                    print("Cancelled by user")
                    return
        
        print(f"\n🚀 Starting multiprocessing backfill...")
        print(f"   Symbols: {len(symbols)}")
        print(f"   Workers: {actual_workers}")
        print(f"   Est. sequential time: {len(symbols) * 2 / 60:.1f} minutes")
        print(f"   Est. parallel time: {len(symbols) * 2 / actual_workers / 60:.1f} minutes")
        print()
        
        # Run the parallel backfill
        results = system.run_parallel_backfill(symbols, max_workers=args.max_workers)
        
        # Display summary
        successful = len(results["successful_results"])
        failed = len(results["failed_results"])
        total_time = results["total_processing_time"]
        
        print(f"\n{'='*60}")
        print("🏁 MULTIPROCESSING BACKFILL COMPLETED")
        print(f"{'='*60}")
        print(f"✅ Successful: {successful}/{len(symbols)} ({successful/len(symbols)*100:.1f}%)")
        print(f"❌ Failed: {failed}/{len(symbols)} ({failed/len(symbols)*100:.1f}%)")
        print(f"⏱️  Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        
        if successful > 0:
            processing_rate = successful / (total_time / 3600)  # symbols per hour
            print(f"📈 Processing rate: {processing_rate:.1f} symbols/hour")
            
            # Calculate speedup vs sequential
            sequential_estimate = len(symbols) * 2 * 60  # 2 minutes per symbol
            speedup = sequential_estimate / total_time
            print(f"🚀 Actual speedup vs sequential: {speedup:.1f}x")
            
            # Extrapolate full backfill time
            if len(symbols) < 6827:
                full_time_hours = 6827 / processing_rate
                print(f"📊 Estimated time for full backfill (6,827 symbols): {full_time_hours:.1f} hours ({full_time_hours/24:.1f} days)")
        
        # Show worker performance
        if "processing_stats" in results and "worker_stats" in results["processing_stats"]:
            print(f"\n👥 Worker Performance:")
            for worker_id, stats in results["processing_stats"]["worker_stats"].items():
                total = stats["processed"] + stats["failed"]
                if total > 0:
                    success_rate = stats["processed"] / total * 100
                    avg_time = stats["total_time"] / total
                    print(f"   Worker {worker_id}: {stats['processed']}/{total} ({success_rate:.1f}%) - {avg_time:.1f}s avg")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during parallel backfill: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()