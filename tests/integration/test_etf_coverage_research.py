"""
ETF Coverage Research Test

Tests daily price and 1-minute intraday data availability for major ETFs
across Polygon, Tiingo, and EODHD vendors.

Major ETFs to test:
- SPY (S&P 500 ETF)
- QQQ (Nasdaq 100 ETF)  
- GLD (Gold ETF)
- TLT (20+ Year Treasury Bond ETF)
- UUP (US Dollar Index ETF - for "dollar" exposure)
- USO (Oil ETF)
- HYG (High Yield Corporate Bond ETF)
- JNK (High Yield Bond ETF - alternative)
- IEF (7-10 Year Treasury Bond ETF - alternative)
"""

import asyncio
import pytest
import aiohttp
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os


# Major ETFs to test
MAJOR_ETFS = [
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF", "category": "Broad Market"},
    {"symbol": "QQQ", "name": "Invesco QQQ Trust", "category": "Technology"},
    {"symbol": "GLD", "name": "SPDR Gold Shares", "category": "Gold"},
    {"symbol": "TLT", "name": "iShares 20+ Year Treasury Bond ETF", "category": "Long-term Bonds"},
    {"symbol": "UUP", "name": "Invesco DB US Dollar Index Bullish Fund", "category": "US Dollar"},
    {"symbol": "USO", "name": "United States Oil Fund", "category": "Oil/Energy"},
    {"symbol": "HYG", "name": "iShares iBoxx $ High Yield Corporate Bond ETF", "category": "High Yield Bonds"},
    {"symbol": "JNK", "name": "SPDR Bloomberg High Yield Bond ETF", "category": "High Yield Bonds Alt"},
    {"symbol": "IEF", "name": "iShares 7-10 Year Treasury Bond ETF", "category": "Medium-term Bonds"},
    {"symbol": "VTI", "name": "Vanguard Total Stock Market ETF", "category": "Total Market"},
]


class ETFCoverageResearcher:
    """Research ETF data coverage across multiple vendors"""
    
    def __init__(self):
        # API keys from environment
        self.polygon_key = os.getenv('POLYGON_API_KEY', 'test_api_key_placeholder')
        self.tiingo_key = os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5')
        self.eodhd_key = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')
        
        # Test date ranges
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=30)  # Last 30 days
        
        # Results storage
        self.results = {
            'polygon': {'daily': {}, 'intraday': {}},
            'tiingo': {'daily': {}, 'intraday': {}}, 
            'eodhd': {'daily': {}, 'intraday': {}}
        }

    async def test_polygon_daily_etfs(self, session: aiohttp.ClientSession) -> Dict:
        """Test Polygon daily ETF data availability"""
        print("🔍 Testing Polygon Daily ETF Coverage...")
        
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        daily_results = {}
        
        for etf in MAJOR_ETFS:
            symbol = etf['symbol']
            
            try:
                # Daily aggregates endpoint
                url = f"{base_url}/{symbol}/range/1/day/{self.start_date}/{self.end_date}"
                params = {
                    'adjusted': 'true',
                    'sort': 'asc',
                    'limit': 50,
                    'apikey': self.polygon_key
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        api_status = data.get('status', '')
                        
                        if api_status in ['OK', 'DELAYED']:
                            results = data.get('results', [])
                            daily_results[symbol] = {
                                'available': True,
                                'records': len(results),
                                'status': api_status,
                                'latest_date': None,
                                'sample_data': results[0] if results else None
                            }
                            
                            if results:
                                # Convert timestamp to date
                                latest_ts = results[-1]['t'] / 1000
                                daily_results[symbol]['latest_date'] = datetime.fromtimestamp(latest_ts).date()
                                
                            print(f"✅ {symbol}: {len(results)} daily records (status: {api_status})")
                        else:
                            daily_results[symbol] = {
                                'available': False,
                                'error': f"API Status: {api_status}",
                                'records': 0
                            }
                            print(f"❌ {symbol}: API Error - {api_status}")
                    else:
                        error_text = await response.text()
                        daily_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}: {error_text}",
                            'records': 0
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                daily_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'records': 0
                }
                print(f"💥 {symbol}: Request failed - {e}")
                
            # Rate limiting
            await asyncio.sleep(0.5)
            
        return daily_results

    async def test_polygon_intraday_etfs(self, session: aiohttp.ClientSession) -> Dict:
        """Test Polygon 1-minute intraday ETF data availability"""
        print("🔍 Testing Polygon 1-Minute Intraday ETF Coverage...")
        
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        intraday_results = {}
        
        # Test just yesterday for intraday to avoid too much data
        test_date = self.end_date - timedelta(days=1)
        
        for etf in MAJOR_ETFS[:5]:  # Test first 5 ETFs for intraday
            symbol = etf['symbol']
            
            try:
                # 1-minute aggregates endpoint  
                url = f"{base_url}/{symbol}/range/1/minute/{test_date}/{test_date}"
                params = {
                    'adjusted': 'true',
                    'sort': 'asc',
                    'limit': 100,  # Limit to avoid rate limits
                    'apikey': self.polygon_key
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        api_status = data.get('status', '')
                        
                        if api_status in ['OK', 'DELAYED']:
                            results = data.get('results', [])
                            intraday_results[symbol] = {
                                'available': True,
                                'records': len(results),
                                'status': api_status,
                                'test_date': test_date,
                                'sample_data': results[0] if results else None
                            }
                            print(f"✅ {symbol}: {len(results)} 1-min records (status: {api_status})")
                        else:
                            intraday_results[symbol] = {
                                'available': False,
                                'error': f"API Status: {api_status}",
                                'records': 0
                            }
                            print(f"❌ {symbol}: API Error - {api_status}")
                    else:
                        error_text = await response.text()
                        intraday_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}: {error_text}",
                            'records': 0
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                intraday_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'records': 0
                }
                print(f"💥 {symbol}: Request failed - {e}")
                
            # Rate limiting for intraday
            await asyncio.sleep(1.0)
            
        return intraday_results

    async def test_tiingo_daily_etfs(self, session: aiohttp.ClientSession) -> Dict:
        """Test Tiingo daily ETF data availability"""
        print("🔍 Testing Tiingo Daily ETF Coverage...")
        
        daily_results = {}
        
        for etf in MAJOR_ETFS:
            symbol = etf['symbol']
            
            try:
                # Tiingo daily prices endpoint
                url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
                params = {
                    'startDate': self.start_date.strftime('%Y-%m-%d'),
                    'endDate': self.end_date.strftime('%Y-%m-%d'),
                    'token': self.tiingo_key
                }
                
                headers = {
                    'Content-Type': 'application/json'
                }
                
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if isinstance(data, list) and data:
                            daily_results[symbol] = {
                                'available': True,
                                'records': len(data),
                                'latest_date': data[-1]['date'][:10] if data else None,
                                'sample_data': data[0]
                            }
                            print(f"✅ {symbol}: {len(data)} daily records")
                        else:
                            daily_results[symbol] = {
                                'available': False,
                                'error': "Empty response",
                                'records': 0
                            }
                            print(f"📭 {symbol}: No data returned")
                    else:
                        error_text = await response.text()
                        daily_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}: {error_text}",
                            'records': 0
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                daily_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'records': 0
                }
                print(f"💥 {symbol}: Request failed - {e}")
                
            # Rate limiting
            await asyncio.sleep(0.3)
            
        return daily_results

    async def test_tiingo_intraday_etfs(self, session: aiohttp.ClientSession) -> Dict:
        """Test Tiingo intraday ETF data availability"""
        print("🔍 Testing Tiingo Intraday ETF Coverage...")
        
        intraday_results = {}
        
        # Test just yesterday for intraday
        test_date = self.end_date - timedelta(days=1)
        
        for etf in MAJOR_ETFS[:5]:  # Test first 5 ETFs
            symbol = etf['symbol']
            
            try:
                # Tiingo intraday endpoint
                url = f"https://api.tiingo.com/iex/{symbol}/prices"
                params = {
                    'startDate': test_date.strftime('%Y-%m-%d'),
                    'endDate': test_date.strftime('%Y-%m-%d'),
                    'resampleFreq': '1min',
                    'token': self.tiingo_key
                }
                
                headers = {
                    'Content-Type': 'application/json'
                }
                
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if isinstance(data, list) and data:
                            intraday_results[symbol] = {
                                'available': True,
                                'records': len(data),
                                'test_date': test_date,
                                'sample_data': data[0]
                            }
                            print(f"✅ {symbol}: {len(data)} 1-min records")
                        else:
                            intraday_results[symbol] = {
                                'available': False,
                                'error': "Empty response",
                                'records': 0
                            }
                            print(f"📭 {symbol}: No intraday data")
                    else:
                        error_text = await response.text()
                        intraday_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}: {error_text}",
                            'records': 0
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                intraday_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'records': 0
                }
                print(f"💥 {symbol}: Request failed - {e}")
                
            # Rate limiting
            await asyncio.sleep(0.5)
            
        return intraday_results

    async def test_eodhd_daily_etfs(self, session: aiohttp.ClientSession) -> Dict:
        """Test EODHD daily ETF data availability"""
        print("🔍 Testing EODHD Daily ETF Coverage...")
        
        daily_results = {}
        
        for etf in MAJOR_ETFS:
            symbol = etf['symbol']
            
            try:
                # EODHD end-of-day endpoint
                url = f"https://eodhd.com/api/eod/{symbol}.US"
                params = {
                    'from': self.start_date.strftime('%Y-%m-%d'),
                    'to': self.end_date.strftime('%Y-%m-%d'),
                    'period': 'd',
                    'fmt': 'json',
                    'api_token': self.eodhd_key
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if isinstance(data, list) and data:
                            daily_results[symbol] = {
                                'available': True,
                                'records': len(data),
                                'latest_date': data[-1]['date'] if data else None,
                                'sample_data': data[0]
                            }
                            print(f"✅ {symbol}: {len(data)} daily records")
                        else:
                            daily_results[symbol] = {
                                'available': False,
                                'error': "Empty response",
                                'records': 0
                            }
                            print(f"📭 {symbol}: No data returned")
                    else:
                        error_text = await response.text()
                        daily_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}: {error_text}",
                            'records': 0
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                daily_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'records': 0
                }
                print(f"💥 {symbol}: Request failed - {e}")
                
            # Rate limiting
            await asyncio.sleep(0.2)
            
        return daily_results

    async def test_eodhd_intraday_etfs(self, session: aiohttp.ClientSession) -> Dict:
        """Test EODHD intraday ETF data availability"""
        print("🔍 Testing EODHD Intraday ETF Coverage...")
        
        intraday_results = {}
        
        # Test just yesterday for intraday
        test_date = self.end_date - timedelta(days=1)
        
        for etf in MAJOR_ETFS[:5]:  # Test first 5 ETFs
            symbol = etf['symbol']
            
            try:
                # EODHD intraday endpoint
                url = f"https://eodhd.com/api/intraday/{symbol}.US"
                params = {
                    'interval': '1m',
                    'from': int(datetime.combine(test_date, datetime.min.time()).timestamp()),
                    'to': int(datetime.combine(test_date, datetime.max.time()).timestamp()),
                    'fmt': 'json',
                    'api_token': self.eodhd_key
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if isinstance(data, list) and data:
                            intraday_results[symbol] = {
                                'available': True,
                                'records': len(data),
                                'test_date': test_date,
                                'sample_data': data[0]
                            }
                            print(f"✅ {symbol}: {len(data)} 1-min records")
                        else:
                            intraday_results[symbol] = {
                                'available': False,
                                'error': "Empty response",
                                'records': 0
                            }
                            print(f"📭 {symbol}: No intraday data")
                    else:
                        error_text = await response.text()
                        intraday_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}: {error_text}",
                            'records': 0
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                intraday_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'records': 0
                }
                print(f"💥 {symbol}: Request failed - {e}")
                
            # Rate limiting  
            await asyncio.sleep(0.5)
            
        return intraday_results

    async def run_comprehensive_coverage_test(self) -> Dict:
        """Run comprehensive ETF coverage test across all vendors"""
        
        print("🎯 Starting Comprehensive ETF Coverage Research")
        print("=" * 60)
        
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # Test Polygon
            print("\n📊 POLYGON API TESTING")
            print("-" * 30)
            self.results['polygon']['daily'] = await self.test_polygon_daily_etfs(session)
            self.results['polygon']['intraday'] = await self.test_polygon_intraday_etfs(session)
            
            # Test Tiingo
            print("\n📊 TIINGO API TESTING")
            print("-" * 30)
            self.results['tiingo']['daily'] = await self.test_tiingo_daily_etfs(session)
            self.results['tiingo']['intraday'] = await self.test_tiingo_intraday_etfs(session)
            
            # Test EODHD
            print("\n📊 EODHD API TESTING")
            print("-" * 30)
            self.results['eodhd']['daily'] = await self.test_eodhd_daily_etfs(session)
            self.results['eodhd']['intraday'] = await self.test_eodhd_intraday_etfs(session)
            
        return self.results

    def generate_coverage_report(self) -> str:
        """Generate comprehensive coverage report"""
        
        report = []
        report.append("=" * 80)
        report.append("🎯 ETF DATA COVERAGE RESEARCH REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary by vendor
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            report.append(f"📊 {vendor.upper()} COVERAGE SUMMARY")
            report.append("-" * 40)
            
            daily_data = self.results[vendor]['daily']
            intraday_data = self.results[vendor]['intraday']
            
            daily_available = sum(1 for etf in daily_data.values() if etf.get('available', False))
            daily_total = len(daily_data)
            
            intraday_available = sum(1 for etf in intraday_data.values() if etf.get('available', False))
            intraday_total = len(intraday_data)
            
            report.append(f"Daily Coverage: {daily_available}/{daily_total} ETFs ({daily_available/daily_total*100 if daily_total > 0 else 0:.1f}%)")
            report.append(f"Intraday Coverage: {intraday_available}/{intraday_total} ETFs ({intraday_available/intraday_total*100 if intraday_total > 0 else 0:.1f}%)")
            report.append("")
        
        # Detailed ETF breakdown
        report.append("📋 DETAILED ETF COVERAGE BREAKDOWN")
        report.append("-" * 50)
        
        for etf in MAJOR_ETFS:
            symbol = etf['symbol']
            name = etf['name']
            category = etf['category']
            
            report.append(f"\n🎯 {symbol} - {name} ({category})")
            report.append("   " + "-" * (len(symbol) + len(name) + len(category) + 6))
            
            # Daily coverage
            report.append("   Daily Price Coverage:")
            for vendor in ['polygon', 'tiingo', 'eodhd']:
                daily_data = self.results[vendor]['daily']
                if symbol in daily_data:
                    data = daily_data[symbol]
                    if data.get('available', False):
                        records = data.get('records', 0)
                        latest = data.get('latest_date', 'N/A')
                        status = data.get('status', 'OK')
                        report.append(f"     ✅ {vendor.capitalize()}: {records} records (latest: {latest}, status: {status})")
                    else:
                        error = data.get('error', 'Unknown error')
                        report.append(f"     ❌ {vendor.capitalize()}: {error}")
                else:
                    report.append(f"     ⚪ {vendor.capitalize()}: Not tested")
            
            # Intraday coverage
            report.append("   1-Minute Intraday Coverage:")
            for vendor in ['polygon', 'tiingo', 'eodhd']:
                intraday_data = self.results[vendor]['intraday']
                if symbol in intraday_data:
                    data = intraday_data[symbol]
                    if data.get('available', False):
                        records = data.get('records', 0)
                        test_date = data.get('test_date', 'N/A')
                        report.append(f"     ✅ {vendor.capitalize()}: {records} records (date: {test_date})")
                    else:
                        error = data.get('error', 'Unknown error')
                        report.append(f"     ❌ {vendor.capitalize()}: {error}")
                else:
                    report.append(f"     ⚪ {vendor.capitalize()}: Not tested")
        
        # Recommendations
        report.append("\n" + "=" * 60)
        report.append("📝 RECOMMENDATIONS")
        report.append("=" * 60)
        
        # Find best vendor for daily data
        daily_coverage = {}
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            daily_data = self.results[vendor]['daily']
            daily_coverage[vendor] = sum(1 for etf in daily_data.values() if etf.get('available', False))
        
        best_daily_vendor = max(daily_coverage, key=daily_coverage.get)
        report.append(f"🏆 Best Daily Data Coverage: {best_daily_vendor.upper()} ({daily_coverage[best_daily_vendor]} ETFs)")
        
        # Find best vendor for intraday data
        intraday_coverage = {}
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            intraday_data = self.results[vendor]['intraday']
            intraday_coverage[vendor] = sum(1 for etf in intraday_data.values() if etf.get('available', False))
        
        if any(intraday_coverage.values()):
            best_intraday_vendor = max(intraday_coverage, key=intraday_coverage.get)
            report.append(f"🏆 Best Intraday Data Coverage: {best_intraday_vendor.upper()} ({intraday_coverage[best_intraday_vendor]} ETFs)")
        else:
            report.append("⚠️ No vendors tested for intraday data yet")
        
        report.append("")
        report.append("💡 Next Steps:")
        report.append("1. Update instrument population scripts to include ETFs")
        report.append("2. Modify data collection jobs to handle ETF symbols")
        report.append("3. Add ETF-specific validation to existing pipelines")
        report.append("4. Consider ETF-specific data retention policies")
        report.append("")
        
        return "\n".join(report)


# Test fixture
@pytest.fixture
def etf_researcher():
    """ETF coverage researcher fixture"""
    return ETFCoverageResearcher()


class TestETFCoverageResearch:
    """Test suite for ETF coverage research"""

    @pytest.mark.asyncio
    async def test_comprehensive_etf_coverage(self, etf_researcher):
        """Test comprehensive ETF coverage across all vendors"""
        
        # Run the comprehensive test
        results = await etf_researcher.run_comprehensive_coverage_test()
        
        # Generate and print report
        report = etf_researcher.generate_coverage_report()
        print("\n" + report)
        
        # Basic assertions
        assert 'polygon' in results
        assert 'tiingo' in results
        assert 'eodhd' in results
        
        # Check that we tested the major ETFs
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            daily_data = results[vendor]['daily']
            
            # Should have tested at least some major ETFs
            major_symbols = [etf['symbol'] for etf in MAJOR_ETFS]
            tested_symbols = list(daily_data.keys())
            
            # Check overlap
            overlap = set(major_symbols) & set(tested_symbols)
            assert len(overlap) > 0, f"{vendor} should have tested some major ETFs"
        
        # Save results for future reference
        print(f"\n📊 Research completed! Tested {len(MAJOR_ETFS)} major ETFs across 3 vendors.")
        print("Results show which vendors provide the best coverage for ETF trading data.")
        
        return results


if __name__ == "__main__":
    async def main():
        researcher = ETFCoverageResearcher()
        results = await researcher.run_comprehensive_coverage_test()
        report = researcher.generate_coverage_report()
        print(report)
        return results
    
    # Run the test
    results = asyncio.run(main())
    print("\n🎉 ETF Coverage Research Complete!")
    print("Run with: PYTHONPATH=src pytest tests/integration/test_etf_coverage_research.py -v -s")