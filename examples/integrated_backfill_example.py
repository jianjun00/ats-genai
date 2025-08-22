#!/usr/bin/env python3
"""
Integrated Backfill Example

Demonstrates how to use the enhanced hybrid storage manager to:
1. Analyze gaps in existing parquet data structure at /home/jianjun/ats/data
2. Identify missing data periods in both parquet files and database 
3. Backfill only the missing data using Polygon and Tiingo APIs
4. Store results in both existing parquet structure and database

This integrates with the discovered existing data infrastructure.
"""

import os
import sys
import asyncio
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.hybrid_minute_data_manager import (
    StorageConfig, 
    run_integrated_gap_analysis_and_backfill,
    create_integrated_hybrid_manager
)

# Mock adapters for demonstration (replace with real adapters)
class MockPolygonAdapter:
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def fetch_minute_bars_async(self, symbol: str, start_date: datetime, end_date: datetime):
        """Mock fetch - returns empty list for demo."""
        return []

class MockTiingoAdapter:
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def fetch_minute_bars_async(self, symbol: str, start_date: datetime, end_date: datetime):
        """Mock fetch - returns empty list for demo."""
        return []

async def demo_gap_analysis_only():
    """Demonstrate gap analysis without actual backfill."""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger("integrated_backfill_demo")
    
    logger.info("🎯 INTEGRATED BACKFILL DEMO - Gap Analysis Only")
    logger.info("=" * 60)
    
    # Configuration matching existing data structure
    config = StorageConfig(
        base_data_path="/home/jianjun/ats/data/STK/1min",
        existing_parquet_path="/home/jianjun/ats/data/STK/1min/cold",
        database_url="postgresql://postgres:postgres@localhost:5433/dev_db"
    )
    
    # Symbols to analyze (major tech stocks that should exist in data)
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
    
    # Date range to check (recent period)
    end_date = date.today()
    start_date = end_date - timedelta(days=90)  # Last 3 months
    
    logger.info(f"📊 Configuration:")
    logger.info(f"   Symbols: {symbols}")
    logger.info(f"   Date range: {start_date} to {end_date}")
    logger.info(f"   Existing parquet path: {config.existing_parquet_path}")
    logger.info(f"   Database: {config.database_url}")
    logger.info("")
    
    try:
        # Create hybrid manager
        async with await create_integrated_hybrid_manager(config=config) as manager:
            logger.info("🔍 Starting gap analysis...")
            
            # Analyze data gaps
            gaps = await manager.analyze_data_gaps(symbols, start_date, end_date)
            
            logger.info(f"📋 Gap Analysis Results:")
            logger.info(f"   Total gaps found: {len(gaps)}")
            
            if gaps:
                # Group gaps by symbol
                gaps_by_symbol = {}
                for gap in gaps:
                    if gap.symbol not in gaps_by_symbol:
                        gaps_by_symbol[gap.symbol] = []
                    gaps_by_symbol[gap.symbol].append(gap)
                
                logger.info(f"   Affected symbols: {len(gaps_by_symbol)}")
                logger.info("")
                
                # Show detailed gap information
                for symbol, symbol_gaps in gaps_by_symbol.items():
                    total_estimated_bars = sum(gap.estimated_bars for gap in symbol_gaps)
                    logger.info(f"   {symbol}: {len(symbol_gaps)} gaps, ~{total_estimated_bars:,} missing bars")
                    
                    for i, gap in enumerate(symbol_gaps[:3]):  # Show first 3 gaps
                        logger.info(f"     Gap {i+1}: {gap.start_date} to {gap.end_date} ({gap.source}) - {gap.estimated_bars} bars")
                    
                    if len(symbol_gaps) > 3:
                        logger.info(f"     ... and {len(symbol_gaps) - 3} more gaps")
                    logger.info("")
                
                # Estimate total backfill work
                total_estimated_bars = sum(gap.estimated_bars for gap in gaps)
                logger.info(f"📈 Backfill Estimate:")
                logger.info(f"   Total missing bars: ~{total_estimated_bars:,}")
                logger.info(f"   Estimated API calls: ~{len(gaps)}")
                logger.info(f"   Estimated time: ~{len(gaps) * 0.5 / 60:.1f} minutes")
                
            else:
                logger.info("✅ No gaps found - all data appears complete!")
            
            # Get storage statistics
            logger.info("")
            logger.info("📊 Current Storage Statistics:")
            stats = await manager.get_storage_stats()
            
            if 'hot_storage' in stats:
                hot_stats = stats['hot_storage']
                logger.info(f"   Database records: {hot_stats.get('total_records', 'N/A'):,}")
                logger.info(f"   Database symbols: {hot_stats.get('unique_symbols', 'N/A')}")
                if hot_stats.get('table_size'):
                    size_mb = hot_stats['table_size'] / (1024 * 1024)
                    logger.info(f"   Database size: {size_mb:.1f} MB")
            
            if 'cold_storage' in stats:
                cold_stats = stats['cold_storage']
                logger.info(f"   Parquet files: {cold_stats.get('total_files', 'N/A')}")
                logger.info(f"   Parquet symbols: {cold_stats.get('total_symbols', 'N/A')}")
                logger.info(f"   Parquet size: {cold_stats.get('total_size_mb', 0):.1f} MB")
    
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

async def demo_full_backfill():
    """Demonstrate full gap analysis and backfill (with mock adapters)."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger("full_backfill_demo")
    
    logger.info("🚀 FULL INTEGRATED BACKFILL DEMO")
    logger.info("=" * 60)
    logger.info("NOTE: Using mock adapters - no actual data will be fetched")
    logger.info("")
    
    # Configuration
    config = StorageConfig(
        base_data_path="/home/jianjun/ats/data/STK/1min",
        existing_parquet_path="/home/jianjun/ats/data/STK/1min/cold",
        database_url="postgresql://postgres:postgres@localhost:5433/dev_db"
    )
    
    # Smaller test set for demo
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    end_date = date.today()
    start_date = end_date - timedelta(days=30)  # Last month
    
    try:
        # Create mock adapters (replace with real ones for actual backfill)
        async with MockPolygonAdapter("mock_key") as polygon_adapter:
            async with MockTiingoAdapter("mock_key") as tiingo_adapter:
                
                # Run complete integrated workflow
                results = await run_integrated_gap_analysis_and_backfill(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    polygon_adapter=polygon_adapter,
                    tiingo_adapter=tiingo_adapter,
                    config=config
                )
                
                logger.info("🎉 Demo completed successfully!")
                logger.info(f"   Duration: {results['duration']}")
                logger.info(f"   Gaps found: {results['total_gaps_found']}")
                logger.info(f"   Gaps filled: {results['total_gaps_filled']}")
    
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

def show_usage():
    """Show usage information."""
    print("Integrated Backfill Example")
    print("")
    print("Usage:")
    print("  python integrated_backfill_example.py [mode]")
    print("")
    print("Modes:")
    print("  gap-analysis   - Analyze data gaps only (safe, no changes)")
    print("  full-demo      - Full backfill demo with mock adapters")
    print("  help           - Show this help")
    print("")
    print("Examples:")
    print("  python integrated_backfill_example.py gap-analysis")
    print("  python integrated_backfill_example.py full-demo")

async def main():
    """Main entry point."""
    
    # Check command line arguments
    mode = sys.argv[1] if len(sys.argv) > 1 else "gap-analysis"
    
    if mode == "gap-analysis":
        await demo_gap_analysis_only()
    elif mode == "full-demo":
        await demo_full_backfill()
    elif mode == "help":
        show_usage()
    else:
        print(f"Unknown mode: {mode}")
        show_usage()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())