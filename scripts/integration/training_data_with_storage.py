#!/usr/bin/env python3
"""
Training Data Generation with Advanced Storage Integration

Demonstrates the complete pipeline from training data generation to storage
using the TimeSeriesSequenceTrainingGenerator with SequenceStorageManager.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import Environment
from ml.training_data.timeseries_sequence_training_generator import (
    TimeSeriesSequenceTrainingGenerator,
    TrainingDataConfig,
    SequenceTrainingExample
)
from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
from state.universe_state_manager import UniverseStateManager


class TrainingDataPipeline:
    """Complete pipeline for generating and storing sequence training data."""
    
    def __init__(self, 
                 storage_base_path: str = "/data/training/sequences",
                 debug: bool = False):
        """
        Initialize the training data pipeline.
        
        Args:
            storage_base_path: Base path for storing training data
            debug: Enable debug logging
        """
        self.storage_base_path = storage_base_path
        self.debug = debug
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.env = Environment()
        self.setup_training_config()
        self.setup_storage_config()
        
        # Initialize managers
        self.universe_manager = UniverseStateManager(env=self.env)
        self.training_generator = TimeSeriesSequenceTrainingGenerator(
            env=self.env,
            config=self.training_config,
            universe_manager=self.universe_manager
        )
        self.storage_manager = SequenceStorageManager(
            base_path=self.storage_base_path,
            config=self.storage_config
        )
    
    def setup_logging(self):
        """Setup logging configuration."""
        level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def setup_training_config(self):
        """Setup training data configuration."""
        self.training_config = TrainingDataConfig(
            base_interval_minutes=1,
            training_interval_minutes=60,
            sequence_lengths={
                '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
                '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
                '1h': 24,   # Past 24 x 1-hour intervals (1 day)
                '1d': 20,   # Past 20 x daily intervals (4 weeks)
            },
            prediction_horizons={
                '1h': 6,    # Next 6 hours
                '1d': 5,    # Next 5 days
            },
            timeframes=['1m', '5m', '15m', '1h', '1d', '1w', '1M'],
            feature_types=[
                'ohlcv',
                'returns',
                'volatility',
                'volume_profile',
                'technical',
                'market_structure'
            ]
        )
    
    def setup_storage_config(self):
        """Setup storage configuration with optimal settings."""
        self.storage_config = StorageConfig(
            primary_format="riegeli",  # Use Riegeli for optimal ML performance
            compression_level=6,       # Balanced compression/speed
            chunk_size=1000,          # 1000 examples per file
            enable_indexing=True,     # Enable fast querying
            enable_checksums=True,    # Ensure data integrity
            buffer_size=64 * 1024 * 1024  # 64MB buffer
        )
    
    async def generate_and_store_training_data(self,
                                             symbols: List[str],
                                             start_date: date,
                                             end_date: date,
                                             min_examples_per_symbol: int = 50) -> Dict[str, Any]:
        """
        Generate training data and store it using the advanced storage system.
        
        Args:
            symbols: List of symbols to process
            start_date: Start date for training data
            end_date: End date for training data
            min_examples_per_symbol: Minimum examples required per symbol
            
        Returns:
            Dictionary with generation and storage statistics
        """
        self.logger.info(f"Starting training data pipeline for {len(symbols)} symbols")
        self.logger.info(f"Date range: {start_date} to {end_date}")
        self.logger.info(f"Storage path: {self.storage_base_path}")
        
        pipeline_start_time = datetime.now()
        
        # Step 1: Generate training data
        self.logger.info("🧬 Generating training data...")
        generation_start = datetime.now()
        
        training_examples = await self.training_generator.generate_training_dataset(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            min_examples_per_symbol=min_examples_per_symbol
        )
        
        generation_time = datetime.now() - generation_start
        
        if not training_examples:
            self.logger.error("No training examples generated!")
            return {'error': 'No training examples generated'}
        
        self.logger.info(f"✅ Generated {len(training_examples)} training examples in {generation_time}")
        
        # Step 2: Store training data using advanced storage system
        self.logger.info("💾 Storing training data with advanced storage system...")
        storage_start = datetime.now()
        
        # Create batch ID with timestamp
        batch_id = f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Store the training data
        storage_result = await self.storage_manager.save_sequence_batch(
            examples=training_examples,
            batch_id=batch_id
        )
        
        storage_time = datetime.now() - storage_start
        total_time = datetime.now() - pipeline_start_time
        
        self.logger.info(f"✅ Stored training data in {storage_time}")
        
        # Step 3: Generate comprehensive report
        report = self.create_pipeline_report(
            training_examples, storage_result, generation_time, storage_time, total_time
        )
        
        # Step 4: Demonstrate querying capabilities
        await self.demonstrate_querying(symbols[0] if symbols else 'AAPL', start_date, end_date)
        
        # Step 5: Get storage statistics
        storage_stats = self.storage_manager.get_storage_stats()
        report['storage_statistics'] = storage_stats
        
        self.logger.info("🎯 Training data pipeline completed successfully!")
        return report
    
    def create_pipeline_report(self,
                             examples: List[SequenceTrainingExample],
                             storage_result: Dict[str, Any],
                             generation_time: timedelta,
                             storage_time: timedelta,
                             total_time: timedelta) -> Dict[str, Any]:
        """Create comprehensive pipeline report."""
        
        # Analyze training examples
        symbol_counts = {}
        for example in examples:
            symbol_counts[example.symbol] = symbol_counts.get(example.symbol, 0) + 1
        
        # Calculate performance metrics
        generation_rate = len(examples) / generation_time.total_seconds()
        storage_rate = len(examples) / storage_time.total_seconds()
        
        report = {
            'pipeline_summary': {
                'total_examples': len(examples),
                'symbols': list(symbol_counts.keys()),
                'symbol_distribution': symbol_counts,
                'generation_time_seconds': generation_time.total_seconds(),
                'storage_time_seconds': storage_time.total_seconds(),
                'total_time_seconds': total_time.total_seconds(),
                'generation_rate_examples_per_second': generation_rate,
                'storage_rate_examples_per_second': storage_rate
            },
            'training_data_details': {
                'config': {
                    'base_interval_minutes': self.training_config.base_interval_minutes,
                    'training_interval_minutes': self.training_config.training_interval_minutes,
                    'sequence_lengths': self.training_config.sequence_lengths,
                    'prediction_horizons': self.training_config.prediction_horizons,
                    'timeframes': self.training_config.timeframes,
                    'feature_types': self.training_config.feature_types
                }
            },
            'storage_details': {
                'batch_id': storage_result.get('batch_id'),
                'storage_format': self.storage_config.primary_format,
                'compression_level': self.storage_config.compression_level,
                'files_created': {
                    'sequence_file': storage_result.get('sequence_file'),
                    'metadata_file': storage_result.get('metadata_file')
                },
                'performance': {
                    'sequence_stats': storage_result.get('sequence_stats'),
                    'metadata_stats': storage_result.get('metadata_stats')
                }
            }
        }
        
        return report
    
    async def demonstrate_querying(self, symbol: str, start_date: date, end_date: date):
        """Demonstrate the querying capabilities of the storage system."""
        self.logger.info(f"🔍 Demonstrating querying capabilities for {symbol}...")
        
        try:
            # Query examples by symbol and date range
            query_start = datetime.now()
            metadata_results = await self.storage_manager.query_by_symbol(
                symbol=symbol,
                start_date=datetime.combine(start_date, datetime.min.time()),
                end_date=datetime.combine(end_date, datetime.max.time())
            )
            query_time = datetime.now() - query_start
            
            self.logger.info(f"📊 Query completed in {query_time}")
            self.logger.info(f"   Found {len(metadata_results)} examples for {symbol}")
            
            if metadata_results:
                sample_metadata = metadata_results[0]
                self.logger.info(f"   Sample result: ID={sample_metadata.example_id}, "
                               f"timestamp={sample_metadata.prediction_timestamp}")
            
        except Exception as e:
            self.logger.error(f"Query demonstration failed: {e}")
    
    def print_report(self, report: Dict[str, Any]):
        """Print a human-readable report."""
        print("\n" + "="*80)
        print("TRAINING DATA PIPELINE REPORT")
        print("="*80)
        
        summary = report['pipeline_summary']
        print(f"\n📊 Pipeline Summary:")
        print(f"   Total Examples: {summary['total_examples']:,}")
        print(f"   Symbols: {', '.join(summary['symbols'])}")
        print(f"   Generation Time: {summary['generation_time_seconds']:.2f} seconds")
        print(f"   Storage Time: {summary['storage_time_seconds']:.2f} seconds")
        print(f"   Total Time: {summary['total_time_seconds']:.2f} seconds")
        print(f"   Generation Rate: {summary['generation_rate_examples_per_second']:.1f} examples/sec")
        print(f"   Storage Rate: {summary['storage_rate_examples_per_second']:.1f} examples/sec")
        
        storage = report['storage_details']
        print(f"\n💾 Storage Details:")
        print(f"   Format: {storage['storage_format'].upper()}")
        print(f"   Batch ID: {storage['batch_id']}")
        print(f"   Compression Level: {storage['compression_level']}")
        
        if 'sequence_stats' in storage['performance']:
            seq_stats = storage['performance']['sequence_stats']
            print(f"   File Size: {seq_stats.get('file_size', 0) / (1024*1024):.2f} MB")
            print(f"   Compression Ratio: {seq_stats.get('compression_ratio', 1.0):.3f}")
        
        if 'storage_statistics' in report:
            stats = report['storage_statistics']
            print(f"\n📈 Overall Storage Statistics:")
            print(f"   Total Size: {stats['total_size_mb']} MB")
            print(f"   Sequence Files: {stats['sequence_files']}")
            print(f"   Metadata Files: {stats['metadata_files']}")
            print(f"   Index Files: {stats['index_files']}")
        
        print("\n" + "="*80)
        print("✨ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*80)
        
        print("\n💡 Next Steps:")
        print("   1. Use the stored data for ML model training")
        print("   2. Query specific date ranges or symbols as needed")
        print("   3. Monitor storage performance and adjust chunk sizes")
        print("   4. Scale up to larger datasets with the same pipeline")


async def main():
    """Main entry point for the integration demonstration."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Training data generation with advanced storage")
    parser.add_argument('--symbols', nargs='+', default=['AAPL', 'TSLA'],
                       help='Symbols to generate training data for')
    parser.add_argument('--start-date', default='2024-01-15',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2024-01-16',
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--storage-path', default='/tmp/training_sequences',
                       help='Base path for storing training data')
    parser.add_argument('--min-examples', type=int, default=10,
                       help='Minimum examples per symbol')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Create and run pipeline
    pipeline = TrainingDataPipeline(
        storage_base_path=args.storage_path,
        debug=args.debug
    )
    
    try:
        report = await pipeline.generate_and_store_training_data(
            symbols=args.symbols,
            start_date=start_date,
            end_date=end_date,
            min_examples_per_symbol=args.min_examples
        )
        
        if 'error' in report:
            print(f"❌ Pipeline failed: {report['error']}")
            return 1
        
        # Print comprehensive report
        pipeline.print_report(report)
        
        # Save report to file
        import json
        report_file = Path(args.storage_path) / "pipeline_report.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n📄 Detailed report saved to: {report_file}")
        return 0
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        if args.debug:
            raise
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)