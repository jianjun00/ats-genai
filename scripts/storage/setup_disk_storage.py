#!/usr/bin/env python3
"""
Setup Disk Storage for 1-Minute Data in /home/jianjun/ats

Creates the directory structure and initializes the hybrid storage system
for managing 1-minute financial data with optimal performance and storage efficiency.
"""

import os
import sys
import asyncio
import asyncpg
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List
import argparse
import logging

# Add src to path
sys.path.append('/home/jianjun/ats-genai/src')

from storage.hybrid_minute_data_manager import (
    HybridMinuteDataManager, 
    StorageConfig,
    create_hybrid_manager,
    migrate_existing_data
)
from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter
from config.environment import env

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DiskStorageSetup:
    """Setup and initialize disk storage for 1-minute data."""
    
    def __init__(self, base_path: str = "/home/jianjun/ats"):
        self.base_path = Path(base_path)
        self.data_path = self.base_path / "data"
        self.minute_data_path = self.data_path / "STK" / "1min"
        
    def create_directory_structure(self):
        """Create the complete directory structure for 1-minute data."""
        logger.info(f"Creating directory structure at {self.minute_data_path}")
        
        # Main directories
        directories = [
            self.minute_data_path / "hot",     # Database cache data
            self.minute_data_path / "warm",    # Recent uncompressed data
            self.minute_data_path / "cold",    # Compressed historical data
            self.minute_data_path / "archive", # Long-term archive
            self.minute_data_path / "metadata", # Metadata and catalogs
            self.minute_data_path / "backups",  # Backup storage
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created: {directory}")
        
        # Create README files
        self._create_readme_files()
        
        logger.info("Directory structure created successfully")
    
    def _create_readme_files(self):
        """Create README files explaining the storage structure."""
        
        main_readme = self.minute_data_path / "README.md"
        main_readme.write_text("""# 1-Minute Financial Data Storage
        
This directory contains high-frequency 1-minute OHLCV data organized for optimal 
performance and storage efficiency.

## Structure

- `hot/` - Recent data (last 30 days) cached from database
- `warm/` - Recent data (30-90 days) in uncompressed Parquet files
- `cold/` - Historical data (>90 days) in compressed Parquet files  
- `archive/` - Long-term archive (>2 years) with maximum compression
- `metadata/` - Data catalogs, schemas, and metadata
- `backups/` - Backup storage for critical data

## Data Format

All data is stored in Parquet format with the following schema:
- Partitioned by: symbol/year/month
- Compression: Snappy (warm), LZ4 (cold), GZIP (archive)
- File naming: {symbol}_{YYYY}_{MM}.parquet

## Access Patterns

- Recent data (hot): Query database for fastest access
- Historical data (warm/cold): Query Parquet files directly
- Unified access: Use HybridMinuteDataManager for seamless queries

## Vendors

Primary vendor: Polygon.io (1-minute US stocks and crypto)
Backup vendor: Interactive Brokers (live data)

## Data Quality

All data includes quality scores and validation flags.
Technical indicators are pre-calculated and stored.
""")
        
        # Tier-specific README files
        tier_descriptions = {
            "hot": "Database cache for recent data (last 30 days)\nFastest query performance",
            "warm": "Uncompressed Parquet files (30-90 days old)\nBalance of speed and storage",
            "cold": "Compressed Parquet files (>90 days old)\nOptimized for storage efficiency",
            "archive": "Maximum compression for long-term storage\nRarely accessed historical data"
        }
        
        for tier, description in tier_descriptions.items():
            tier_readme = self.minute_data_path / tier / "README.md"
            tier_readme.write_text(f"# {tier.title()} Storage Tier\n\n{description}\n")
    
    def create_sample_config(self) -> StorageConfig:
        """Create optimized storage configuration."""
        return StorageConfig(
            base_data_path=str(self.minute_data_path),
            hot_data_days=30,           # 1 month in database
            warm_data_days=90,          # 3 months uncompressed
            cold_data_days=730,         # 2 years compressed
            partition_by="year_month",  # Monthly partitions
            compression="snappy",       # Good balance of speed/size
            batch_size=10000,          # Large batches for efficiency
            max_concurrent_files=4,     # Parallel I/O
            memory_limit_mb=4096       # 4GB memory limit
        )
    
    def analyze_existing_data(self) -> Dict[str, Any]:
        """Analyze existing data in /home/jianjun/ats."""
        logger.info("Analyzing existing data structure...")
        
        analysis = {
            "futures_data": {"symbols": 0, "files": 0, "size_gb": 0},
            "event_data": {"files": 0, "size_gb": 0},
            "total_size_gb": 0,
            "recommendations": []
        }
        
        # Analyze futures data
        futures_path = self.data_path / "FUT" / "30min"
        if futures_path.exists():
            symbols = list(futures_path.iterdir())
            analysis["futures_data"]["symbols"] = len([s for s in symbols if s.is_dir()])
            
            total_size = 0
            total_files = 0
            for symbol_dir in symbols:
                if symbol_dir.is_dir():
                    for file in symbol_dir.rglob("*.parquet"):
                        total_size += file.stat().st_size
                        total_files += 1
            
            analysis["futures_data"]["files"] = total_files
            analysis["futures_data"]["size_gb"] = total_size / (1024**3)
            analysis["total_size_gb"] += analysis["futures_data"]["size_gb"]
        
        # Analyze event data
        event_path = self.data_path / "event"
        if event_path.exists():
            total_size = 0
            total_files = 0
            for file in event_path.rglob("*.csv"):
                total_size += file.stat().st_size
                total_files += 1
            
            analysis["event_data"]["files"] = total_files
            analysis["event_data"]["size_gb"] = total_size / (1024**3)
            analysis["total_size_gb"] += analysis["event_data"]["size_gb"]
        
        # Generate recommendations
        if analysis["futures_data"]["size_gb"] > 5:
            analysis["recommendations"].append(
                "Large futures dataset detected - consider migrating to new 1-minute format"
            )
        
        if analysis["total_size_gb"] > 10:
            analysis["recommendations"].append(
                "Significant data volume - hybrid storage architecture recommended"
            )
        
        logger.info(f"Analysis complete: {analysis['total_size_gb']:.1f} GB total")
        return analysis
    
    async def test_storage_performance(self, db_url: str) -> Dict[str, Any]:
        """Test storage performance with sample data."""
        logger.info("Testing storage performance...")
        
        config = self.create_sample_config()
        manager = await create_hybrid_manager(db_url, config)
        
        # Generate test data
        test_data = self._generate_test_data("TEST", 1000)  # 1000 minute bars
        
        # Test storage performance
        start_time = datetime.now()
        
        try:
            # Test hot storage (database)
            hot_result = await manager.store_minute_data("TEST", test_data[:500], force_tier="hot")
            hot_time = (datetime.now() - start_time).total_seconds()
            
            # Test cold storage (disk)
            cold_start = datetime.now()
            cold_result = await manager.store_minute_data("TEST", test_data[500:], force_tier="cold")
            cold_time = (datetime.now() - cold_start).total_seconds()
            
            # Test query performance
            query_start = datetime.now()
            result_df = await manager.query_minute_data(
                "TEST",
                test_data[0]['timestamp'],
                test_data[-1]['timestamp']
            )
            query_time = (datetime.now() - query_start).total_seconds()
            
            performance = {
                "hot_storage": {
                    "records": hot_result['stored_hot'],
                    "time_seconds": hot_time,
                    "records_per_second": hot_result['stored_hot'] / hot_time if hot_time > 0 else 0
                },
                "cold_storage": {
                    "records": cold_result['stored_cold'],
                    "time_seconds": cold_time,
                    "records_per_second": cold_result['stored_cold'] / cold_time if cold_time > 0 else 0
                },
                "query_performance": {
                    "records_returned": len(result_df),
                    "time_seconds": query_time,
                    "records_per_second": len(result_df) / query_time if query_time > 0 else 0
                }
            }
            
            logger.info(f"Performance test complete: {performance}")
            return performance
            
        finally:
            await manager.close()
    
    def _generate_test_data(self, symbol: str, count: int) -> List[Dict[str, Any]]:
        """Generate test minute data."""
        data = []
        start_time = datetime.now() - timedelta(minutes=count)
        
        price = 100.0
        for i in range(count):
            timestamp = start_time + timedelta(minutes=i)
            
            # Simple random walk
            change = (hash(f"{symbol}_{i}") % 200 - 100) / 10000  # -1% to +1%
            price *= (1 + change)
            
            data.append({
                'symbol': symbol,
                'timestamp': timestamp,
                'open': price,
                'high': price * 1.005,
                'low': price * 0.995,
                'close': price,
                'volume': 1000 + (hash(f"{symbol}_{i}") % 5000),
                'vwap': price,
                'trade_count': 10 + (hash(f"{symbol}_{i}") % 50),
                'vendor': 'test',
                'quality_score': 0.95
            })
        
        return data


async def main():
    parser = argparse.ArgumentParser(description="Setup disk storage for 1-minute data")
    parser.add_argument("--base-path", default="/home/jianjun/ats", 
                       help="Base path for data storage")
    parser.add_argument("--db-url", 
                       default="postgresql://postgres:postgres@localhost:5433/dev_db",
                       help="Database URL for testing")
    parser.add_argument("--create-dirs", action="store_true",
                       help="Create directory structure")
    parser.add_argument("--analyze", action="store_true",
                       help="Analyze existing data")
    parser.add_argument("--test-performance", action="store_true",
                       help="Test storage performance")
    parser.add_argument("--migrate-data", action="store_true",
                       help="Migrate existing futures data")
    
    args = parser.parse_args()
    
    setup = DiskStorageSetup(args.base_path)
    
    print("=" * 80)
    print("DISK STORAGE SETUP FOR 1-MINUTE DATA")
    print("=" * 80)
    
    if args.create_dirs:
        print("\n1. Creating directory structure...")
        setup.create_directory_structure()
        print("✓ Directory structure created")
    
    if args.analyze:
        print("\n2. Analyzing existing data...")
        analysis = setup.analyze_existing_data()
        
        print(f"\nFutures Data:")
        print(f"  Symbols: {analysis['futures_data']['symbols']}")
        print(f"  Files: {analysis['futures_data']['files']}")
        print(f"  Size: {analysis['futures_data']['size_gb']:.1f} GB")
        
        print(f"\nEvent Data:")
        print(f"  Files: {analysis['event_data']['files']}")
        print(f"  Size: {analysis['event_data']['size_gb']:.1f} GB")
        
        print(f"\nTotal Size: {analysis['total_size_gb']:.1f} GB")
        
        if analysis['recommendations']:
            print("\nRecommendations:")
            for rec in analysis['recommendations']:
                print(f"  • {rec}")
    
    if args.test_performance:
        print("\n3. Testing storage performance...")
        try:
            performance = await setup.test_storage_performance(args.db_url)
            
            print(f"\nHot Storage (Database):")
            print(f"  Records: {performance['hot_storage']['records']}")
            print(f"  Time: {performance['hot_storage']['time_seconds']:.2f}s")
            print(f"  Rate: {performance['hot_storage']['records_per_second']:.0f} records/sec")
            
            print(f"\nCold Storage (Disk):")
            print(f"  Records: {performance['cold_storage']['records']}")
            print(f"  Time: {performance['cold_storage']['time_seconds']:.2f}s")
            print(f"  Rate: {performance['cold_storage']['records_per_second']:.0f} records/sec")
            
            print(f"\nQuery Performance:")
            print(f"  Records: {performance['query_performance']['records_returned']}")
            print(f"  Time: {performance['query_performance']['time_seconds']:.2f}s")
            print(f"  Rate: {performance['query_performance']['records_per_second']:.0f} records/sec")
            
        except Exception as e:
            print(f"Error testing performance: {e}")
            print("Make sure database is running and accessible")
    
    if args.migrate_data:
        print("\n4. Migrating existing data...")
        try:
            config = setup.create_sample_config()
            manager = await create_hybrid_manager(args.db_url, config)
            
            # Create symbol mapping for futures -> stocks
            symbol_mapping = {
                'ES': 'SPY',    # S&P 500 futures -> SPY ETF
                'NQ': 'QQQ',    # NASDAQ futures -> QQQ ETF  
                'YM': 'DIA',    # Dow futures -> DIA ETF
                'GC': 'GLD',    # Gold futures -> GLD ETF
                'CL': 'USO',    # Oil futures -> USO ETF
            }
            
            migrated = await migrate_existing_data(
                manager, 
                args.base_path + "/data", 
                symbol_mapping
            )
            
            print(f"\nMigration Results:")
            print(f"  Symbols: {migrated['symbols']}")
            print(f"  Files: {migrated['files']}")
            print(f"  Records: {migrated['records']:,}")
            
            await manager.close()
            
        except Exception as e:
            print(f"Error migrating data: {e}")
    
    print("\n5. Storage Configuration Summary")
    print("-" * 50)
    config = setup.create_sample_config()
    print(f"Base Path: {config.base_data_path}")
    print(f"Hot Data: {config.hot_data_days} days (database)")
    print(f"Warm Data: {config.warm_data_days} days (uncompressed)")
    print(f"Cold Data: {config.cold_data_days} days (compressed)")
    print(f"Partitioning: {config.partition_by}")
    print(f"Compression: {config.compression}")
    
    print("\n6. Vendor Recommendations")
    print("-" * 50)
    print("Primary: Polygon.io")
    print("  • Best 1-minute data coverage")
    print("  • High quality US stocks + crypto")
    print("  • 20+ years historical data")
    print("  • Real-time updates")
    print("  • Cost: $99/month premium")
    
    print("\nBackup: Interactive Brokers")
    print("  • Free with trading account")
    print("  • Global multi-asset coverage") 
    print("  • Limited historical (5 years)")
    print("  • Excellent for live data")
    
    print("\n7. Next Steps")
    print("-" * 50)
    print("1. Set up Polygon.io API key")
    print("2. Start with small symbol set (10-50 symbols)")
    print("3. Run daily data ingestion job")
    print("4. Monitor storage usage and performance")
    print("5. Scale to larger symbol universe")
    print("6. Set up automated archival process")
    
    print(f"\n✓ Storage setup complete at {args.base_path}")


if __name__ == "__main__":
    asyncio.run(main())