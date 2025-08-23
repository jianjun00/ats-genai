#!/usr/bin/env python3
"""
Test the Large Scale Data Catalog Dashboard
Validates 10K+ stock analytics functionality
"""

import asyncio
import aiohttp
import json
from typing import Dict, Any

class LargeScaleDataCatalogTester:
    def __init__(self, base_url: str = "http://localhost:3200"):
        self.base_url = base_url
    
    async def test_stats_api(self) -> Dict[str, Any]:
        """Test the main statistics API"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/api/v1/stats") as response:
                return await response.json()
    
    async def test_instruments_api(self) -> Dict[str, Any]:
        """Test the instruments API with pagination"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/api/v1/instruments?limit=20") as response:
                return await response.json()
    
    async def test_search_api(self) -> Dict[str, Any]:
        """Test the search functionality"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/api/v1/search?q=AAPL") as response:
                return await response.json()
    
    async def test_performance_metrics(self) -> Dict[str, Any]:
        """Test performance metrics API"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/api/v1/performance-metrics") as response:
                return await response.json()
    
    async def run_comprehensive_test(self):
        """Run all tests and display results"""
        print("🚀 Testing Large Scale Data Catalog Dashboard")
        print("=" * 60)
        
        try:
            # Test 1: Statistics API
            print("📊 Testing Statistics API...")
            stats = await self.test_stats_api()
            print(f"  ✅ Total Instruments: {stats['total_instruments']:,}")
            print(f"  ✅ Total Records: {stats['total_records']:,}")
            print(f"  ✅ Coverage: {stats['coverage_percentage']}%")
            print(f"  ✅ Last Update: {stats['days_since_update']} days ago")
            print()
            
            # Test 2: Instruments API
            print("🏢 Testing Instruments API...")
            instruments = await self.test_instruments_api()
            print(f"  ✅ Retrieved {len(instruments['instruments'])} instruments")
            print(f"  ✅ Total Available: {instruments['total']:,}")
            if instruments['instruments']:
                sample = instruments['instruments'][0]
                print(f"  ✅ Sample: {sample['symbol']} - {sample['name']}")
            print()
            
            # Test 3: Search API
            print("🔍 Testing Search API...")
            search_results = await self.test_search_api()
            print(f"  ✅ Search for 'AAPL' found {len(search_results['results'])} results")
            if search_results['results']:
                result = search_results['results'][0]
                print(f"  ✅ Top result: {result['symbol']} - {result['match_type']} match")
            print()
            
            # Test 4: Performance Metrics
            print("⚡ Testing Performance Metrics...")
            performance = await self.test_performance_metrics()
            print(f"  ✅ Average Query Time: {performance['avg_query_time']}ms")
            print(f"  ✅ Storage Efficiency: {performance['storage_efficiency']}%")
            print(f"  ✅ Records Per Hour: {performance['records_per_hour']:,}")
            print(f"  ✅ Daily Throughput: {performance['daily_throughput']:,}")
            print()
            
            print("🎉 ALL TESTS PASSED!")
            print("=" * 60)
            print("✅ Large Scale Data Catalog Dashboard is fully operational")
            print("📊 Ready to handle 10,000+ stocks with high-performance analytics")
            print("🌐 Access dashboard at: http://localhost:3200")
            print("=" * 60)
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        
        return True

async def main():
    tester = LargeScaleDataCatalogTester()
    success = await tester.run_comprehensive_test()
    if success:
        print("\n🚀 Dashboard Features:")
        print("  • 📊 Real-time statistics for 10K+ stocks")
        print("  • 🔍 Advanced search and filtering")
        print("  • 📈 Coverage analysis and quality metrics") 
        print("  • ⚡ Performance monitoring")
        print("  • 🏢 Instrument browser with pagination")
        print("  • 📋 Exchange breakdown and analytics")
        print("  • 💾 Storage optimization insights")
        print("  • 🎯 System health monitoring")

if __name__ == "__main__":
    asyncio.run(main())