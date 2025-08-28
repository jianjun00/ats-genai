"""
Momentum and Small-Cap ETF Coverage Research

Tests data availability for:
1. IWM - iShares Russell 2000 ETF (small-cap exposure)
2. Popular momentum ETFs for systematic strategies

Momentum ETF Categories:
- Factor-based momentum ETFs
- Sector momentum ETFs  
- Multi-factor ETFs with momentum exposure
"""

import asyncio
import pytest
import aiohttp
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
import os


# Additional ETFs to research
ADDITIONAL_ETFS = [
    # Small-Cap Coverage (Requested)
    {
        "symbol": "IWM",
        "name": "iShares Russell 2000 ETF", 
        "exchange": "NYSE",
        "category": "Small Cap",
        "sector": "Equity",
        "description": "Russell 2000 small-cap stocks - completes size spectrum",
        "expense_ratio": 0.19,
        "aum_billions": 65.0,
        "priority": 1,  # High priority for size diversification
        "rationale": "Essential for complete market cap coverage (large/mid/small)"
    },
    
    # Pure Momentum ETFs
    {
        "symbol": "MTUM", 
        "name": "iShares MSCI USA Momentum Factor ETF",
        "exchange": "NYSE",
        "category": "Factor",
        "sector": "Momentum",
        "description": "Pure momentum factor exposure - systematic momentum strategy",
        "expense_ratio": 0.15,
        "aum_billions": 18.0,
        "priority": 1,  # Top momentum choice
        "rationale": "Best pure-play momentum ETF with strong track record"
    },
    
    {
        "symbol": "PDP",
        "name": "Invesco DWA Momentum ETF", 
        "exchange": "NASDAQ",
        "category": "Factor",
        "sector": "Momentum",
        "description": "Dorsey Wright momentum methodology - relative strength",
        "expense_ratio": 0.63,
        "aum_billions": 2.8,
        "priority": 2,
        "rationale": "Alternative momentum methodology (relative strength vs factor)"
    },
    
    # Multi-Factor with Strong Momentum Component
    {
        "symbol": "QUAL",
        "name": "iShares MSCI USA Quality Factor ETF",
        "exchange": "NYSE", 
        "category": "Factor",
        "sector": "Quality",
        "description": "Quality factor with momentum characteristics",
        "expense_ratio": 0.15,
        "aum_billions": 22.0,
        "priority": 2,
        "rationale": "Quality often has momentum characteristics, good diversifier"
    },
    
    {
        "symbol": "VMOT",
        "name": "Alpha Architect Value Momentum Trend ETF",
        "exchange": "NYSE",
        "category": "Factor",
        "sector": "Multi-Factor",
        "description": "Combines value and momentum factors",
        "expense_ratio": 0.79,
        "aum_billions": 0.3,
        "priority": 3,
        "rationale": "Academic-based approach combining value and momentum"
    },
    
    # Sector Momentum Options
    {
        "symbol": "XLK",
        "name": "Technology Select Sector SPDR Fund",
        "exchange": "NYSE",
        "category": "Sector",
        "sector": "Technology", 
        "description": "Tech sector - historically high momentum characteristics",
        "expense_ratio": 0.10,
        "aum_billions": 55.0,
        "priority": 2,
        "rationale": "Tech sector natural momentum play, complements QQQ"
    },
    
    {
        "symbol": "XLI",
        "name": "Industrial Select Sector SPDR Fund",
        "exchange": "NYSE",
        "category": "Sector", 
        "sector": "Industrial",
        "description": "Industrial sector with cyclical momentum",
        "expense_ratio": 0.10,
        "aum_billions": 28.0,
        "priority": 3,
        "rationale": "Industrial momentum for economic cycle exposure"
    },
    
    # International Momentum
    {
        "symbol": "IMTM",
        "name": "iShares MSCI Intl Momentum Factor ETF",
        "exchange": "NYSE",
        "category": "International",
        "sector": "Momentum",
        "description": "International developed market momentum",
        "expense_ratio": 0.30,
        "aum_billions": 1.8,
        "priority": 3,
        "rationale": "International diversification for momentum strategies"
    }
]


class MomentumETFResearcher:
    """Research momentum and additional ETF coverage"""
    
    def __init__(self):
        # API keys
        self.polygon_key = os.getenv('POLYGON_API_KEY', 'test_api_key_placeholder')
        self.tiingo_key = os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5')  
        self.eodhd_key = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')
        
        # Test periods
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=30)
        
        self.results = {
            'polygon': {'daily': {}, 'intraday': {}},
            'tiingo': {'daily': {}, 'intraday': {}},
            'eodhd': {'daily': {}, 'intraday': {}}
        }

    async def test_polygon_coverage(self, session: aiohttp.ClientSession, etfs: List[Dict]) -> Dict:
        """Test Polygon coverage for additional ETFs"""
        print("🔍 Testing Polygon Coverage for Additional ETFs...")
        
        daily_results = {}
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        
        for etf in etfs:
            symbol = etf['symbol']
            
            try:
                # Daily data
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
                                'category': etf['category'],
                                'priority': etf['priority'],
                                'rationale': etf['rationale']
                            }
                            print(f"✅ {symbol}: {len(results)} records ({etf['category']}, Priority {etf['priority']})")
                        else:
                            daily_results[symbol] = {
                                'available': False,
                                'error': f"API Status: {api_status}",
                                'category': etf['category'],
                                'priority': etf['priority']
                            }
                            print(f"❌ {symbol}: API Error - {api_status}")
                    else:
                        daily_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}",
                            'category': etf['category'], 
                            'priority': etf['priority']
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                daily_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'category': etf['category'],
                    'priority': etf['priority']
                }
                print(f"💥 {symbol}: {e}")
                
            await asyncio.sleep(0.5)  # Rate limiting
            
        return daily_results

    async def test_tiingo_coverage(self, session: aiohttp.ClientSession, etfs: List[Dict]) -> Dict:
        """Test Tiingo coverage for additional ETFs"""
        print("🔍 Testing Tiingo Coverage for Additional ETFs...")
        
        daily_results = {}
        
        for etf in etfs:
            symbol = etf['symbol']
            
            try:
                url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
                params = {
                    'startDate': self.start_date.strftime('%Y-%m-%d'),
                    'endDate': self.end_date.strftime('%Y-%m-%d'),
                    'token': self.tiingo_key
                }
                
                headers = {'Content-Type': 'application/json'}
                
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if isinstance(data, list) and data:
                            daily_results[symbol] = {
                                'available': True,
                                'records': len(data),
                                'category': etf['category'],
                                'priority': etf['priority'],
                                'rationale': etf['rationale']
                            }
                            print(f"✅ {symbol}: {len(data)} records ({etf['category']}, Priority {etf['priority']})")
                        else:
                            daily_results[symbol] = {
                                'available': False,
                                'error': "Empty response",
                                'category': etf['category'],
                                'priority': etf['priority']
                            }
                            print(f"📭 {symbol}: No data")
                    else:
                        daily_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}",
                            'category': etf['category'],
                            'priority': etf['priority']
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                daily_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'category': etf['category'],
                    'priority': etf['priority']
                }
                print(f"💥 {symbol}: {e}")
                
            await asyncio.sleep(0.3)
            
        return daily_results

    async def test_eodhd_coverage(self, session: aiohttp.ClientSession, etfs: List[Dict]) -> Dict:
        """Test EODHD coverage for additional ETFs"""
        print("🔍 Testing EODHD Coverage for Additional ETFs...")
        
        daily_results = {}
        
        for etf in etfs:
            symbol = etf['symbol']
            
            try:
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
                                'category': etf['category'],
                                'priority': etf['priority'],
                                'rationale': etf['rationale']
                            }
                            print(f"✅ {symbol}: {len(data)} records ({etf['category']}, Priority {etf['priority']})")
                        else:
                            daily_results[symbol] = {
                                'available': False,
                                'error': "Empty response",
                                'category': etf['category'],
                                'priority': etf['priority']
                            }
                            print(f"📭 {symbol}: No data")
                    else:
                        daily_results[symbol] = {
                            'available': False,
                            'error': f"HTTP {response.status}",
                            'category': etf['category'],
                            'priority': etf['priority']
                        }
                        print(f"❌ {symbol}: HTTP {response.status}")
                        
            except Exception as e:
                daily_results[symbol] = {
                    'available': False,
                    'error': str(e),
                    'category': etf['category'],
                    'priority': etf['priority']
                }
                print(f"💥 {symbol}: {e}")
                
            await asyncio.sleep(0.2)
            
        return daily_results

    async def run_momentum_etf_research(self) -> Dict:
        """Run comprehensive momentum ETF research"""
        
        print("🎯 Momentum and Small-Cap ETF Research")
        print("=" * 50)
        
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # Test all vendors
            print("\n📊 POLYGON API TESTING")
            print("-" * 30)
            self.results['polygon']['daily'] = await self.test_polygon_coverage(session, ADDITIONAL_ETFS)
            
            print("\n📊 TIINGO API TESTING") 
            print("-" * 30)
            self.results['tiingo']['daily'] = await self.test_tiingo_coverage(session, ADDITIONAL_ETFS)
            
            print("\n📊 EODHD API TESTING")
            print("-" * 30)
            self.results['eodhd']['daily'] = await self.test_eodhd_coverage(session, ADDITIONAL_ETFS)
            
        return self.results

    def generate_momentum_report(self) -> str:
        """Generate momentum ETF research report"""
        
        report = []
        report.append("=" * 80)
        report.append("🚀 MOMENTUM & SMALL-CAP ETF RESEARCH REPORT") 
        report.append("=" * 80)
        report.append("")
        
        # Vendor coverage summary
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            daily_data = self.results[vendor]['daily']
            available_count = sum(1 for etf in daily_data.values() if etf.get('available', False))
            total_count = len(daily_data)
            
            report.append(f"📊 {vendor.upper()} Coverage: {available_count}/{total_count} ETFs ({available_count/total_count*100 if total_count > 0 else 0:.1f}%)")
        
        report.append("")
        report.append("📋 DETAILED ETF ANALYSIS BY CATEGORY")
        report.append("-" * 50)
        
        # Group by category and priority
        categories = {}
        for etf in ADDITIONAL_ETFS:
            category = etf['category']
            if category not in categories:
                categories[category] = []
            categories[category].append(etf)
        
        for category, etfs in categories.items():
            report.append(f"\n🎯 {category.upper()} CATEGORY")
            report.append("   " + "-" * (len(category) + 10))
            
            # Sort by priority
            etfs_sorted = sorted(etfs, key=lambda x: x['priority'])
            
            for etf in etfs_sorted:
                symbol = etf['symbol']
                name = etf['name']
                priority = etf['priority']
                rationale = etf['rationale']
                
                report.append(f"\n   💎 {symbol} - {name} (Priority {priority})")
                report.append(f"      💡 {rationale}")
                
                # Coverage by vendor
                coverage_line = "      📊 Coverage: "
                coverage_items = []
                
                for vendor in ['polygon', 'tiingo', 'eodhd']:
                    daily_data = self.results[vendor]['daily']
                    if symbol in daily_data:
                        data = daily_data[symbol]
                        if data.get('available', False):
                            records = data.get('records', 0)
                            coverage_items.append(f"{vendor.capitalize()}({records})")
                        else:
                            coverage_items.append(f"{vendor.capitalize()}(❌)")
                    else:
                        coverage_items.append(f"{vendor.capitalize()}(?)")
                
                coverage_line += " | ".join(coverage_items)
                report.append(coverage_line)
        
        # Recommendations
        report.append("\n" + "=" * 60)
        report.append("📝 MOMENTUM ETF RECOMMENDATIONS")
        report.append("=" * 60)
        
        # Categorize by priority
        priority_1 = [etf for etf in ADDITIONAL_ETFS if etf['priority'] == 1]
        priority_2 = [etf for etf in ADDITIONAL_ETFS if etf['priority'] == 2]
        priority_3 = [etf for etf in ADDITIONAL_ETFS if etf['priority'] == 3]
        
        if priority_1:
            report.append("\n🏆 MUST-HAVE ETFs (Priority 1):")
            for etf in priority_1:
                symbol = etf['symbol']
                # Check coverage across vendors
                total_coverage = 0
                for vendor in ['polygon', 'tiingo', 'eodhd']:
                    daily_data = self.results[vendor]['daily']
                    if symbol in daily_data and daily_data[symbol].get('available', False):
                        total_coverage += 1
                
                coverage_status = "✅ Excellent" if total_coverage == 3 else f"⚠️ {total_coverage}/3 vendors"
                report.append(f"   • {symbol}: {etf['name']} - {coverage_status}")
                report.append(f"     {etf['rationale']}")
        
        if priority_2:
            report.append("\n📊 RECOMMENDED ETFs (Priority 2):")
            for etf in priority_2:
                symbol = etf['symbol']
                total_coverage = 0
                for vendor in ['polygon', 'tiingo', 'eodhd']:
                    daily_data = self.results[vendor]['daily']
                    if symbol in daily_data and daily_data[symbol].get('available', False):
                        total_coverage += 1
                
                coverage_status = "✅ Excellent" if total_coverage == 3 else f"⚠️ {total_coverage}/3 vendors"
                report.append(f"   • {symbol}: {etf['name']} - {coverage_status}")
        
        if priority_3:
            report.append("\n🔧 OPTIONAL ETFs (Priority 3):")
            for etf in priority_3:
                symbol = etf['symbol']
                total_coverage = 0
                for vendor in ['polygon', 'tiingo', 'eodhd']:
                    daily_data = self.results[vendor]['daily']
                    if symbol in daily_data and daily_data[symbol].get('available', False):
                        total_coverage += 1
                
                coverage_status = "✅ Available" if total_coverage >= 2 else f"⚠️ Limited ({total_coverage}/3)"
                report.append(f"   • {symbol}: {etf['name']} - {coverage_status}")
        
        # Final recommendations
        report.append("\n💡 IMPLEMENTATION RECOMMENDATIONS:")
        report.append("")
        report.append("1. **IWM (Priority 1)**: Essential for small-cap exposure completion")
        report.append("2. **MTUM (Priority 1)**: Best pure momentum factor ETF")  
        report.append("3. **XLK (Priority 2)**: Tech sector momentum complement to QQQ")
        report.append("4. **QUAL (Priority 2)**: Quality factor with momentum characteristics")
        report.append("5. **PDP (Priority 2)**: Alternative momentum methodology for diversification")
        report.append("")
        report.append("🎯 These additions provide:")
        report.append("   • Complete market cap coverage (large/mid/small via IWM)")
        report.append("   • Pure momentum factor exposure (MTUM)")
        report.append("   • Sector momentum opportunities (XLK)")
        report.append("   • Multi-factor diversification (QUAL)")
        report.append("   • International momentum potential (IMTM)")
        
        return "\n".join(report)


# Test class
class TestMomentumETFResearch:
    """Test suite for momentum ETF research"""
    
    @pytest.mark.asyncio
    async def test_momentum_etf_coverage(self):
        """Test momentum ETF coverage across vendors"""
        
        researcher = MomentumETFResearcher()
        results = await researcher.run_momentum_etf_research()
        
        # Generate report
        report = researcher.generate_momentum_report()
        print("\n" + report)
        
        # Basic validations
        assert 'polygon' in results
        assert 'tiingo' in results  
        assert 'eodhd' in results
        
        # Check that we tested the key ETFs
        key_etfs = ['IWM', 'MTUM', 'XLK']
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            daily_data = results[vendor]['daily']
            for etf_symbol in key_etfs:
                assert etf_symbol in daily_data, f"{etf_symbol} should be tested in {vendor}"
        
        print(f"\n🎉 Tested {len(ADDITIONAL_ETFS)} additional ETFs across 3 vendors!")
        print("Research shows which momentum ETFs have best data coverage.")
        
        return results


if __name__ == "__main__":
    async def main():
        researcher = MomentumETFResearcher()
        results = await researcher.run_momentum_etf_research()
        report = researcher.generate_momentum_report()
        print(report)
        return results
    
    results = asyncio.run(main())
    print("\n🚀 Momentum ETF Research Complete!")
    print("Run with: PYTHONPATH=src pytest tests/integration/test_momentum_etf_research.py -v -s")