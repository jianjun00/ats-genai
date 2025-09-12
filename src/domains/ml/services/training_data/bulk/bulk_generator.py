#!/usr/bin/env python3
"""
Bulk Training Data Generator
Efficiently generates training data for multiple symbols and date ranges.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

@dataclass
class BulkGenerationTask:
    """Individual training data generation task."""
    symbol: str
    start_date: date
    end_date: date
    start_day_offset: int = 0
    end_day_offset: int = 0
    base_duration: str = '60m'
    environment: str = 'dev'

    def to_command_args(self, output_dir: str) -> List[str]:
        """Convert task to command line arguments."""
        return [
            'python3', 'src/domains/ml/services/training_data/runners/training_data_callback_runner.py',
            '--symbols', self.symbol,
            '--start-date', self.start_date.strftime('%Y-%m-%d'),
            '--end-date', self.end_date.strftime('%Y-%m-%d'),
            '--start-day-offset', str(self.start_day_offset),
            '--end-day-offset', str(self.end_day_offset),
            '--environment', self.environment,
            '--storage-format', 'arrayrecord',
            '--output-dir', output_dir,
            '--gin-config', 'config/training_data.gin',
            '--base-duration', self.base_duration
        ]

@dataclass
class BulkGenerationResult:
    """Result of a bulk generation task."""
    task: BulkGenerationTask
    success: bool
    duration_seconds: float
    dataset_id: Optional[str] = None
    error_message: Optional[str] = None
    records_generated: int = 0
    file_size_mb: float = 0.0

class BulkTrainingDataGenerator:
    """
    Bulk generator for training data across multiple symbols and date ranges.
    Supports parallel execution and progress tracking.
    """

    def __init__(self,
                 max_parallel_tasks: int = 3,
                 output_dir: str = '/data/training_data',
                 environment: str = 'dev'):
        self.max_parallel_tasks = max_parallel_tasks
        self.output_dir = output_dir
        self.environment = environment
        self.logger = logging.getLogger(__name__)

        # Environment variables for subprocess
        self.env_vars = {
            'DB_HOST': 'localhost',
            'DB_PORT': '3432' if environment == 'dev' else '4432',
            'DB_USER': 'postgres',
            'DB_PASSWORD': 'dev_password' if environment == 'dev' else 'intg_password',
            'DB_NAME': f'{environment}_db',
            'ENVIRONMENT_TYPE': environment,
            'PYTHONPATH': 'src'
        }

    def generate_monthly_tasks(self,
                             symbols: List[str],
                             start_date: date,
                             end_date: date,
                             start_day_offset: int = 5,
                             end_day_offset: int = 2,
                             base_duration: str = '60m') -> List[BulkGenerationTask]:
        """
        Generate tasks for monthly training data generation.

        Creates one task per symbol per month in the date range.
        """
        tasks = []

        for symbol in symbols:
            current_date = start_date.replace(day=1)  # Start of month

            while current_date <= end_date:
                # Calculate end of month
                if current_date.month == 12:
                    next_month = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    next_month = current_date.replace(month=current_date.month + 1)

                month_end = next_month - timedelta(days=1)

                # Don't go beyond the requested end date
                task_end_date = min(month_end, end_date)

                task = BulkGenerationTask(
                    symbol=symbol,
                    start_date=current_date,
                    end_date=task_end_date,
                    start_day_offset=start_day_offset,
                    end_day_offset=end_day_offset,
                    base_duration=base_duration,
                    environment=self.environment
                )

                tasks.append(task)

                # Move to next month
                current_date = next_month

        return tasks

    def generate_bulk_training_data(self,
                                  tasks: List[BulkGenerationTask],
                                  progress_callback: Optional[callable] = None) -> List[BulkGenerationResult]:
        """
        Execute bulk training data generation with parallel processing.

        Args:
            tasks: List of generation tasks to execute
            progress_callback: Optional callback for progress updates

        Returns:
            List of generation results
        """
        self.logger.info(f"Starting bulk generation of {len(tasks)} tasks")

        results = []

        with ThreadPoolExecutor(max_workers=self.max_parallel_tasks) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._execute_single_task, task): task
                for task in tasks
            }

            completed_count = 0

            # Process completed tasks
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                completed_count += 1

                try:
                    result = future.result()
                    results.append(result)

                    if result.success:
                        self.logger.info(f"✅ Completed {task.symbol} {task.start_date} -> {task.end_date} "
                                       f"({result.duration_seconds:.1f}s)")
                    else:
                        self.logger.error(f"❌ Failed {task.symbol} {task.start_date} -> {task.end_date}: "
                                        f"{result.error_message}")

                    # Progress callback
                    if progress_callback:
                        progress_callback(completed_count, len(tasks), result)

                except Exception as e:
                    self.logger.error(f"❌ Exception in task {task.symbol}: {e}")
                    results.append(BulkGenerationResult(
                        task=task,
                        success=False,
                        duration_seconds=0.0,
                        error_message=str(e)
                    ))

        self.logger.info(f"Bulk generation completed: {len([r for r in results if r.success])}/{len(results)} successful")
        return results

    def _execute_single_task(self, task: BulkGenerationTask) -> BulkGenerationResult:
        """Execute a single training data generation task."""
        start_time = time.time()

        try:
            # Build command
            cmd = task.to_command_args(self.output_dir)

            # Set environment variables
            import os
            env = os.environ.copy()
            env.update(self.env_vars)

            # Execute command
            result = subprocess.run(
                cmd,
                cwd='/home/jianjun/ats-genai-admin',  # Adjust path as needed
                env=env,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                # Parse output for dataset ID and stats
                dataset_id = self._extract_dataset_id(result.stdout)
                records_generated, file_size_mb = self._extract_stats(result.stdout)

                return BulkGenerationResult(
                    task=task,
                    success=True,
                    duration_seconds=duration,
                    dataset_id=dataset_id,
                    records_generated=records_generated,
                    file_size_mb=file_size_mb
                )
            else:
                return BulkGenerationResult(
                    task=task,
                    success=False,
                    duration_seconds=duration,
                    error_message=result.stderr or result.stdout
                )

        except subprocess.TimeoutExpired:
            return BulkGenerationResult(
                task=task,
                success=False,
                duration_seconds=time.time() - start_time,
                error_message="Task timed out after 30 minutes"
            )
        except Exception as e:
            return BulkGenerationResult(
                task=task,
                success=False,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )

    def _extract_dataset_id(self, stdout: str) -> Optional[str]:
        """Extract dataset ID from command output."""
        for line in stdout.split('\n'):
            if 'dataset_id:' in line:
                return line.split('dataset_id:')[1].strip()
        return None

    def _extract_stats(self, stdout: str) -> Tuple[int, float]:
        """Extract generation statistics from command output."""
        records = 0
        file_size = 0.0

        for line in stdout.split('\n'):
            if 'Generated' in line and 'records' in line:
                try:
                    records = int(line.split('Generated')[1].split('records')[0].strip())
                except:
                    pass
            if 'File size:' in line and 'MB' in line:
                try:
                    file_size = float(line.split('File size:')[1].split('MB')[0].strip())
                except:
                    pass

        return records, file_size

    def generate_summary_report(self, results: List[BulkGenerationResult]) -> str:
        """Generate a comprehensive summary report."""
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        total_duration = sum(r.duration_seconds for r in results)
        total_records = sum(r.records_generated for r in successful)
        total_size_mb = sum(r.file_size_mb for r in successful)

        symbols_processed = set(r.task.symbol for r in successful)

        report = f"""
🚀 Bulk Training Data Generation Summary
{'='*50}

📊 Overall Statistics:
  • Total Tasks:     {len(results)}
  • Successful:      {len(successful)} ({len(successful)/len(results)*100:.1f}%)
  • Failed:          {len(failed)} ({len(failed)/len(results)*100:.1f}%)

📈 Generation Results:
  • Symbols Processed: {len(symbols_processed)} ({', '.join(sorted(symbols_processed))})
  • Total Records:     {total_records:,}
  • Total Size:        {total_size_mb:.1f} MB
  • Total Duration:    {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)

⚡ Performance:
  • Avg Task Duration: {total_duration/len(results):.1f} seconds
  • Records/Second:     {total_records/total_duration:.0f} (overall)
  • Parallel Efficiency: {self.max_parallel_tasks} concurrent tasks
"""

        if failed:
            report += f"\n❌ Failed Tasks ({len(failed)}):\n"
            for result in failed[:5]:  # Show first 5 failures
                report += f"  • {result.task.symbol} {result.task.start_date}: {result.error_message[:100]}...\n"
            if len(failed) > 5:
                report += f"  • ... and {len(failed) - 5} more failures\n"

        return report

def demo_bulk_generation():
    """Demonstrate bulk training data generation."""
    generator = BulkTrainingDataGenerator(
        max_parallel_tasks=2,
        environment='dev'
    )

    # Create sample tasks for multiple symbols
    symbols = ['AAPL', 'TSLA', 'MSFT']
    start_date = date(2024, 6, 1)
    end_date = date(2024, 6, 30)

    tasks = generator.generate_monthly_tasks(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        start_day_offset=5,
        end_day_offset=2
    )

    print(f"📋 Generated {len(tasks)} tasks for bulk processing:")
    for task in tasks:
        print(f"  • {task.symbol}: {task.start_date} -> {task.end_date} "
              f"(collection window: -{task.start_day_offset}/+{task.end_day_offset} days)")

    # Progress callback
    def progress_callback(completed, total, result):
        progress = completed / total * 100
        print(f"🔄 Progress: {completed}/{total} ({progress:.1f}%) - "
              f"Latest: {result.task.symbol} {'✅' if result.success else '❌'}")

    # NOTE: Commenting out actual execution for demo
    # results = generator.generate_bulk_training_data(tasks, progress_callback)
    # report = generator.generate_summary_report(results)
    # print(report)

    print("\n💡 To execute bulk generation, uncomment the execution lines above")
    return tasks

if __name__ == "__main__":
    demo_bulk_generation()