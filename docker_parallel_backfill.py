#!/usr/bin/env python3
"""
Docker-Based Parallel FirstRate Backfill
Uses multiple Docker containers as parallel workers to achieve speedup

This approach:
- Launches multiple Docker containers simultaneously  
- Each container processes a subset of symbols
- Uses the existing proven populate_firstrate_minute_bars.py approach
- Works with existing infrastructure (no new dependencies)
- Can achieve 4-8x speedup on multi-core systems

Architecture:
- Main coordinator launches N Docker containers
- Each container gets 1/N of the total symbols
- Containers run independently with separate checkpoints
- Progress monitoring and result aggregation
"""

import os
import sys
import subprocess
import time
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pathlib import Path
from collections import defaultdict
import queue
import multiprocessing

logger = logging.getLogger(__name__)


class DockerWorkerManager:
    """Manages individual Docker container workers"""
    
    def __init__(self, worker_id: int, symbols: List[str], total_workers: int):
        self.worker_id = worker_id
        self.symbols = symbols
        self.total_workers = total_workers
        self.checkpoint_file = f"docker_worker_{worker_id}_checkpoint.json"
        self.log_file = f"/tmp/docker_worker_{worker_id}.log"
        
        self.process = None
        self.start_time = None
        self.stats = {
            "worker_id": worker_id,
            "symbols_assigned": len(symbols),
            "symbols_completed": 0,
            "status": "pending",  # pending, running, completed, failed
            "start_time": None,
            "end_time": None,
            "processing_time": 0,
            "error_message": None
        }
        
    def start_worker(self) -> bool:
        """Start the Docker worker container"""
        
        symbols_str = ",".join(self.symbols)
        
        # Docker command using run_dev pattern
        cmd = [
            'python3', 'scripts/run_dev.py', 'run',
            '--script', 'scripts/populate_firstrate_minute_bars.py',
            '--asset-type', 'stock',
            '--symbols', symbols_str,
            '--checkpoint-file', self.checkpoint_file,
            '--debug'
        ]
        
        logger.info(f"🚀 Starting Docker worker {self.worker_id} with {len(self.symbols)} symbols")
        logger.info(f"   Symbols: {symbols_str[:100]}{'...' if len(symbols_str) > 100 else ''}")
        logger.info(f"   Checkpoint: {self.checkpoint_file}")
        logger.info(f"   Log: {self.log_file}")
        
        try:
            # Start the subprocess
            self.process = subprocess.Popen(
                cmd,
                stdout=open(self.log_file, 'w'),
                stderr=subprocess.STDOUT,
                cwd='/home/jianjun/ats-genai-data'
            )
            
            self.start_time = time.time()
            self.stats.update({
                "status": "running",
                "start_time": datetime.now().isoformat(),
                "pid": self.process.pid
            })
            
            logger.info(f"✅ Worker {self.worker_id} started (PID: {self.process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start worker {self.worker_id}: {e}")
            self.stats.update({
                "status": "failed",
                "error_message": str(e)
            })
            return False
    
    def check_status(self) -> str:
        """Check worker status"""
        
        if not self.process:
            return "not_started"
        
        poll_result = self.process.poll()
        
        if poll_result is None:
            # Still running
            self.stats["status"] = "running"
            if self.start_time:
                self.stats["processing_time"] = time.time() - self.start_time
            return "running"
        
        elif poll_result == 0:
            # Completed successfully
            if self.stats["status"] != "completed":
                end_time = time.time()
                self.stats.update({
                    "status": "completed",
                    "end_time": datetime.now().isoformat(),
                    "processing_time": end_time - self.start_time if self.start_time else 0
                })
                logger.info(f"✅ Worker {self.worker_id} completed successfully")
            return "completed"
        
        else:
            # Failed
            if self.stats["status"] != "failed":
                end_time = time.time()
                self.stats.update({
                    "status": "failed", 
                    "end_time": datetime.now().isoformat(),
                    "processing_time": end_time - self.start_time if self.start_time else 0,
                    "exit_code": poll_result
                })
                logger.error(f"❌ Worker {self.worker_id} failed with exit code {poll_result}")
            return "failed"
    
    def get_progress_info(self) -> Dict:
        """Get detailed progress information"""
        
        # Try to read checkpoint file for progress
        checkpoint_path = Path(self.checkpoint_file)
        completed_symbols = 0
        
        try:
            if checkpoint_path.exists():
                with open(checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                    completed_symbols = len(checkpoint_data.get("completed_months", {}))
        except Exception:
            pass
        
        self.stats["symbols_completed"] = completed_symbols
        
        # Read recent log entries for additional info
        recent_log_lines = []
        try:
            if Path(self.log_file).exists():
                with open(self.log_file, 'r') as f:
                    lines = f.readlines()
                    recent_log_lines = lines[-5:]  # Last 5 lines
        except Exception:
            pass
        
        return {
            **self.stats,
            "progress_percentage": (completed_symbols / len(self.symbols)) * 100 if self.symbols else 0,
            "recent_log_lines": [line.strip() for line in recent_log_lines]
        }
    
    def terminate(self):
        """Terminate the worker process"""
        if self.process and self.process.poll() is None:
            logger.info(f"⚠️  Terminating worker {self.worker_id}")
            self.process.terminate()
            
            # Wait a bit, then kill if necessary
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning(f"Force killing worker {self.worker_id}")
                self.process.kill()


class DockerParallelCoordinator:
    """Coordinates multiple Docker container workers"""
    
    def __init__(self, num_workers: int = None):
        if num_workers is None:
            # Default to number of CPU cores / 4 (Docker containers are resource heavy)
            num_workers = max(1, multiprocessing.cpu_count() // 4)
        
        self.num_workers = num_workers
        self.workers: List[DockerWorkerManager] = []
        
        self.coordination_stats = {
            "started_at": None,
            "num_workers": num_workers,
            "total_symbols": 0,
            "total_completed": 0,
            "total_failed": 0,
            "worker_stats": {},
            "overall_progress": 0,
            "estimated_completion": None
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'docker_parallel_backfill_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            ]
        )
        self.logger = logging.getLogger('docker_coordinator')
    
    def distribute_symbols(self, symbols: List[str]) -> List[List[str]]:
        """Distribute symbols across workers"""
        
        symbols_per_worker = len(symbols) // self.num_workers
        remainder = len(symbols) % self.num_workers
        
        distributed = []
        start_idx = 0
        
        for i in range(self.num_workers):
            # Give extra symbol to first 'remainder' workers
            worker_symbol_count = symbols_per_worker + (1 if i < remainder else 0)
            end_idx = start_idx + worker_symbol_count
            
            worker_symbols = symbols[start_idx:end_idx]
            distributed.append(worker_symbols)
            
            start_idx = end_idx
            
            self.logger.info(f"Worker {i}: {len(worker_symbols)} symbols "
                           f"({worker_symbols[0] if worker_symbols else 'None'} - "
                           f"{worker_symbols[-1] if worker_symbols else 'None'})")
        
        return distributed
    
    def start_parallel_processing(self, symbols: List[str]) -> bool:
        """Start all worker containers"""
        
        self.logger.info(f"🚀 Starting Docker parallel processing")
        self.logger.info(f"   Total symbols: {len(symbols)}")
        self.logger.info(f"   Workers: {self.num_workers}")
        
        # Distribute symbols
        distributed_symbols = self.distribute_symbols(symbols)
        
        # Create workers
        self.workers = []
        for i, worker_symbols in enumerate(distributed_symbols):
            if worker_symbols:  # Only create workers with symbols
                worker = DockerWorkerManager(i, worker_symbols, self.num_workers)
                self.workers.append(worker)
        
        self.logger.info(f"📦 Created {len(self.workers)} workers")
        
        # Start all workers
        successful_starts = 0
        for worker in self.workers:
            if worker.start_worker():
                successful_starts += 1
            else:
                self.logger.error(f"Failed to start worker {worker.worker_id}")
        
        self.coordination_stats.update({
            "started_at": datetime.now().isoformat(),
            "total_symbols": len(symbols),
            "workers_started": successful_starts
        })
        
        if successful_starts == 0:
            self.logger.error("❌ No workers started successfully")
            return False
        
        self.logger.info(f"✅ Started {successful_starts}/{len(self.workers)} workers")
        return True
    
    def monitor_progress(self, report_interval: int = 30) -> Dict:
        """Monitor progress until completion"""
        
        self.logger.info(f"📊 Starting progress monitoring (updates every {report_interval}s)")
        
        start_time = time.time()
        last_report = 0
        
        while True:
            time.sleep(5)  # Check every 5 seconds
            
            # Check all worker statuses
            running_workers = 0
            completed_workers = 0
            failed_workers = 0
            
            total_symbols_completed = 0
            worker_progress_info = {}
            
            for worker in self.workers:
                status = worker.check_status()
                progress_info = worker.get_progress_info()
                worker_progress_info[worker.worker_id] = progress_info
                
                if status == "running":
                    running_workers += 1
                elif status == "completed":
                    completed_workers += 1
                elif status == "failed":
                    failed_workers += 1
                
                total_symbols_completed += progress_info["symbols_completed"]
            
            # Update coordination stats
            self.coordination_stats.update({
                "total_completed": total_symbols_completed,
                "overall_progress": (total_symbols_completed / self.coordination_stats["total_symbols"]) * 100,
                "worker_stats": worker_progress_info,
                "running_workers": running_workers,
                "completed_workers": completed_workers,
                "failed_workers": failed_workers
            })
            
            # Periodic reporting
            elapsed_time = time.time() - start_time
            if elapsed_time - last_report >= report_interval:
                self._log_progress_report()
                last_report = elapsed_time
            
            # Check if all workers are done
            if running_workers == 0:
                self.logger.info("🏁 All workers completed")
                break
        
        # Final stats
        total_time = time.time() - start_time
        self.coordination_stats.update({
            "completed_at": datetime.now().isoformat(),
            "total_processing_time": total_time
        })
        
        return self.coordination_stats
    
    def _log_progress_report(self):
        """Log detailed progress report"""
        
        stats = self.coordination_stats
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info("📊 DOCKER PARALLEL PROGRESS REPORT")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Overall Progress: {stats['total_completed']}/{stats['total_symbols']} "
                        f"({stats['overall_progress']:.1f}%)")
        self.logger.info(f"Workers: {stats.get('running_workers', 0)} running, "
                        f"{stats.get('completed_workers', 0)} completed, "
                        f"{stats.get('failed_workers', 0)} failed")
        
        # Individual worker progress
        self.logger.info("\n👥 Worker Status:")
        for worker_id, info in stats.get("worker_stats", {}).items():
            status_icon = {"running": "🔄", "completed": "✅", "failed": "❌", "pending": "⏳"}.get(info["status"], "❓")
            self.logger.info(f"   Worker {worker_id}: {status_icon} {info['symbols_completed']}/{info['symbols_assigned']} "
                           f"({info['progress_percentage']:.1f}%) - {info['status']}")
        
        # Processing rate estimate
        if stats["total_completed"] > 0 and "started_at" in stats:
            start_time = datetime.fromisoformat(stats["started_at"])
            elapsed_hours = (datetime.now() - start_time).total_seconds() / 3600
            processing_rate = stats["total_completed"] / elapsed_hours
            
            remaining_symbols = stats["total_symbols"] - stats["total_completed"]
            eta_hours = remaining_symbols / processing_rate if processing_rate > 0 else 0
            eta_time = datetime.now() + timedelta(hours=eta_hours)
            
            self.logger.info(f"\n📈 Performance:")
            self.logger.info(f"   Processing rate: {processing_rate:.1f} symbols/hour")
            self.logger.info(f"   ETA: {eta_time.strftime('%Y-%m-%d %H:%M')} ({eta_hours:.1f}h remaining)")
        
        self.logger.info(f"{'='*60}\n")
    
    def cleanup_workers(self):
        """Cleanup all worker processes"""
        self.logger.info("🧹 Cleaning up workers...")
        
        for worker in self.workers:
            try:
                worker.terminate()
            except Exception as e:
                self.logger.warning(f"Error terminating worker {worker.worker_id}: {e}")


def main():
    """Main execution function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Docker Parallel FirstRate Backfill")
    parser.add_argument('--num-workers', type=int, help='Number of Docker workers (default: CPU cores / 4)')
    parser.add_argument('--limit', type=int, help='Limit number of symbols for testing')
    parser.add_argument('--test-mode', action='store_true', help='Run with small test batch')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Docker Parallel FirstRate Backfill System")
    print("=" * 60)
    
    # Show system capabilities
    cpu_count = multiprocessing.cpu_count()
    recommended_workers = max(1, cpu_count // 4)
    actual_workers = args.num_workers or recommended_workers
    
    print(f"🖥️  System: {cpu_count} CPU cores available")
    print(f"🐳 Docker workers: {actual_workers}")
    print(f"📈 Expected speedup: {actual_workers}x")
    print()
    
    # Load symbols
    analysis_file = Path("firstrate_stock_universe_analysis.json")
    if analysis_file.exists():
        with open(analysis_file, 'r') as f:
            analysis = json.load(f)
        all_symbols = analysis.get('remaining_symbols', [])
    else:
        print("⚠️  Analysis file not found - using sample symbols")
        all_symbols = ['MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'BAC', 'PG', 'KO']
    
    # Select symbols based on mode
    if args.test_mode:
        symbols = all_symbols[:40]  # 40 symbols for testing
        print(f"🧪 Test mode: processing {len(symbols)} symbols")
    elif args.limit:
        symbols = all_symbols[:args.limit]
        print(f"🔢 Limited run: processing {len(symbols)} symbols")
    else:
        symbols = all_symbols
        print(f"🏗️  Full run: processing {len(symbols)} symbols")
        
        if len(symbols) > 1000:
            print("⚠️  WARNING: This will process thousands of symbols")
            print("   Consider using --test-mode first to validate the system")
            
            response = input("Continue with full Docker parallel backfill? [y/N]: ")
            if response.lower() != 'y':
                print("Cancelled by user")
                return
    
    print(f"\n🚀 Starting Docker parallel backfill...")
    print(f"   Symbols: {len(symbols)}")
    print(f"   Workers: {actual_workers}")
    print(f"   Est. sequential time: {len(symbols) * 2 / 60:.1f} minutes")
    print(f"   Est. parallel time: {len(symbols) * 2 / actual_workers / 60:.1f} minutes")
    print()
    
    # Create coordinator
    coordinator = DockerParallelCoordinator(num_workers=actual_workers)
    
    try:
        # Start parallel processing
        if not coordinator.start_parallel_processing(symbols):
            print("❌ Failed to start parallel processing")
            return
        
        print("🔄 Monitoring progress... (Ctrl+C to stop)")
        
        # Monitor until completion
        final_stats = coordinator.monitor_progress()
        
        # Display final results
        print(f"\n{'='*60}")
        print("🏁 DOCKER PARALLEL BACKFILL COMPLETED")
        print(f"{'='*60}")
        
        total_completed = final_stats["total_completed"]
        total_symbols = final_stats["total_symbols"]
        total_time = final_stats["total_processing_time"]
        
        print(f"✅ Completed: {total_completed}/{total_symbols} ({total_completed/total_symbols*100:.1f}%)")
        print(f"⏱️  Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        
        if total_completed > 0:
            processing_rate = total_completed / (total_time / 3600)
            sequential_estimate = total_symbols * 2 * 60  # 2 minutes per symbol
            speedup = sequential_estimate / total_time
            
            print(f"📈 Processing rate: {processing_rate:.1f} symbols/hour")
            print(f"🚀 Speedup vs sequential: {speedup:.1f}x")
            
            # Project full backfill time
            if total_symbols < 6827:
                full_time_hours = 6827 / processing_rate
                print(f"📊 Est. time for full backfill (6,827 symbols): {full_time_hours:.1f}h ({full_time_hours/24:.1f} days)")
        
        # Save results
        results_file = f"docker_parallel_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(final_stats, f, indent=2, default=str)
        
        print(f"💾 Results saved to {results_file}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user - cleaning up workers...")
    except Exception as e:
        print(f"\n❌ Error during parallel backfill: {e}")
        import traceback
        traceback.print_exc()
    finally:
        coordinator.cleanup_workers()


if __name__ == "__main__":
    main()