#!/usr/bin/env python3
"""
Demo Multi-Vendor 1-Minute Bars Collection

Demonstrates the multi-vendor data collection system with simulated data
when API keys are not available. Shows the full workflow and output format.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DemoMinuteBar:
    """Demo minute bar for simulation."""
    def __init__(self, symbol: str, timestamp: datetime, vendor: str):
        self.symbol = symbol
        self.timestamp = timestamp
        self.vendor = vendor
        
        # Generate realistic OHLCV data
        base_price = 150.0 + random.uniform(-50, 50)  # Base price around $150
        volatility = random.uniform(0.5, 2.0)
        
        self.open = base_price + random.uniform(-volatility, volatility)
        self.high = max(self.open, base_price + random.uniform(0, volatility))
        self.low = min(self.open, base_price - random.uniform(0, volatility))
        self.close = base_price + random.uniform(-volatility, volatility)
        self.volume = random.randint(1000, 100000)


class DemoVendorAdapter:
    """Demo vendor adapter that generates simulated data."""
    
    def __init__(self, vendor_name: str, success_rate: float = 0.9):
        self.vendor_name = vendor_name
        self.success_rate = success_rate
    
    async def fetch_multiple_symbols_async(
        self, 
        symbols: List[str], 
        start_date: datetime, 
        end_date: datetime,
        **kwargs
    ) -> Dict[str, List[DemoMinuteBar]]:
        """Simulate fetching data for multiple symbols."""
        
        logger.info(f"🔄 [{self.vendor_name.upper()}] Fetching data for {len(symbols)} symbols...")
        
        # Simulate API delay
        await asyncio.sleep(random.uniform(1, 3))
        
        results = {}
        
        for symbol in symbols:
            # Simulate some symbols failing
            if random.random() > self.success_rate:
                logger.warning(f"⚠️  [{self.vendor_name.upper()}] Failed to fetch data for {symbol}")
                results[symbol] = []
                continue
            
            # Generate minute bars for the date range
            bars = []
            current_time = start_date.replace(hour=9, minute=30)  # Market open
            end_time = min(end_date, start_date + timedelta(days=1)).replace(hour=16, minute=0)  # Market close
            
            # Generate bars for market hours only
            while current_time < end_time:
                if current_time.weekday() < 5:  # Monday-Friday only
                    bar = DemoMinuteBar(symbol, current_time, self.vendor_name)
                    bars.append(bar)
                
                current_time += timedelta(minutes=1)
                
                # Stop after reasonable amount for demo
                if len(bars) >= 100:  # Limit demo data
                    break
            
            results[symbol] = bars
            logger.info(f"✅ [{self.vendor_name.upper()}] Generated {len(bars)} bars for {symbol}")
        
        return results
    
    def validate_data_quality(self, bars: List[DemoMinuteBar]) -> Dict[str, Any]:
        """Simulate data quality validation."""
        if not bars:
            return {"valid": False, "reason": "No data"}
        
        # Simulate quality metrics
        quality_score = random.uniform(0.8, 1.0)
        gaps = random.randint(0, len(bars) // 20)
        
        return {
            "valid": quality_score > 0.7,
            "total_bars": len(bars),
            "time_gaps": gaps,
            "gap_details": [],
            "price_outliers": random.randint(0, 3),
            "zero_volume_bars": 0,
            "avg_volume": sum(bar.volume for bar in bars) / len(bars),
            "data_completeness": (len(bars) - gaps) / len(bars) if bars else 0,
            "quality_score": quality_score,
            "vendor": self.vendor_name
        }


async def demo_collection():
    """Demonstrate the multi-vendor data collection process."""
    
    print("🚀 Multi-Vendor 1-Minute Data Collection Demo")
    print("=" * 60)
    
    # Configuration
    symbols = ["AAPL", "MSFT", "GOOGL"]
    vendors = ["polygon", "tiingo", "fmp", "eodhd"]
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()
    
    print(f"📊 Target Symbols: {', '.join(symbols)}")
    print(f"🏪 Vendors: {', '.join(vendors)}")
    print(f"📅 Date Range: {start_date.date()} to {end_date.date()}")
    print(f"💾 Storage: File-based (simulated)")
    print()
    
    # Initialize demo adapters
    adapters = {
        vendor: DemoVendorAdapter(vendor, success_rate=0.8 + random.uniform(0, 0.2))
        for vendor in vendors
    }
    
    # Statistics tracking
    stats = {
        'total_symbols': len(symbols),
        'successful_symbols': 0,
        'failed_symbols': 0,
        'total_bars': 0,
        'vendor_stats': {vendor: {'bars': 0, 'symbols': 0} for vendor in vendors}
    }
    
    # Collect data from each vendor
    all_results = {}
    
    for vendor_name, adapter in adapters.items():
        logger.info(f"🔄 Processing vendor: {vendor_name.upper()}")
        
        try:
            # Simulate data collection
            vendor_data = await adapter.fetch_multiple_symbols_async(
                symbols, start_date, end_date
            )
            
            vendor_summary = {
                'symbols_processed': 0,
                'total_bars': 0,
                'quality_metrics': {}
            }
            
            # Process each symbol's data
            for symbol, bars in vendor_data.items():
                if bars:
                    # Validate quality
                    quality_metrics = adapter.validate_data_quality(bars)
                    
                    # Simulate storage
                    logger.info(f"💾 [{vendor_name.upper()}] Storing {len(bars)} bars for {symbol}")
                    
                    # Update stats
                    stats['vendor_stats'][vendor_name]['bars'] += len(bars)
                    stats['vendor_stats'][vendor_name]['symbols'] += 1
                    stats['total_bars'] += len(bars)
                    
                    vendor_summary['symbols_processed'] += 1
                    vendor_summary['total_bars'] += len(bars)
                    vendor_summary['quality_metrics'][symbol] = quality_metrics
            
            all_results[vendor_name] = vendor_summary
            logger.info(f"✅ [{vendor_name.upper()}] Completed: {vendor_summary['symbols_processed']} symbols, {vendor_summary['total_bars']} bars")
            
        except Exception as e:
            logger.error(f"❌ [{vendor_name.upper()}] Error: {e}")
            all_results[vendor_name] = {'status': 'failed', 'error': str(e)}
    
    # Print final summary
    print("\n" + "="*60)
    print("📊 MULTI-VENDOR DATA COLLECTION SUMMARY")
    print("="*60)
    print(f"Total Symbols Requested: {stats['total_symbols']}")
    print(f"Total Bars Collected: {stats['total_bars']:,}")
    
    print(f"\n📈 Vendor Breakdown:")
    for vendor, vendor_stats in stats['vendor_stats'].items():
        if vendor_stats['bars'] > 0:
            print(f"  {vendor.upper():<10}: {vendor_stats['bars']:,} bars from {vendor_stats['symbols']} symbols")
    
    # Show quality metrics summary
    print(f"\n🔍 Data Quality Summary:")
    for vendor_name, results in all_results.items():
        if 'quality_metrics' in results:
            avg_quality = sum(
                metrics.get('quality_score', 0) 
                for metrics in results['quality_metrics'].values()
            ) / max(len(results['quality_metrics']), 1)
            
            avg_completeness = sum(
                metrics.get('data_completeness', 0) 
                for metrics in results['quality_metrics'].values()
            ) / max(len(results['quality_metrics']), 1)
            
            print(f"  {vendor_name.upper():<10}: Quality: {avg_quality:.2f}, Completeness: {avg_completeness:.2%}")
    
    print(f"\n📁 File Storage Structure (simulated):")
    print("  /data/minute_bars/")
    for symbol in symbols:
        print(f"    ├── {symbol}/")
        print(f"    │   ├── 2024-08.parquet  # Monthly file")
        print(f"    │   └── metadata.json")
    
    print(f"\n🎯 Collection Process:")
    print("  ✅ Multi-vendor data fetching")
    print("  ✅ Data quality validation") 
    print("  ✅ Overlap detection and resolution")
    print("  ✅ File-based monthly storage")
    print("  ✅ Comprehensive reporting")
    
    print("\n✅ Demo completed successfully!")
    
    # Show example usage commands
    print(f"\n💡 Usage Examples:")
    print("# Collect real data with API keys:")
    print("export POLYGON_API_KEY='your_key'")
    print("export TIINGO_API_KEY='your_key'") 
    print("export FMP_API_KEY='your_key'")
    print("export EODHD_API_KEY='your_key'")
    print()
    print("python scripts/populate_minute_bars_multi_vendor.py --symbols AAPL,MSFT --days 30")
    print("python scripts/populate_minute_bars_multi_vendor.py --symbols-file sample_symbols.txt --storage database")
    
    return all_results


if __name__ == "__main__":
    asyncio.run(demo_collection())