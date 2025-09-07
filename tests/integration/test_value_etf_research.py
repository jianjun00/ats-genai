"""
Value ETF Coverage Research

Tests data availability for:
1. Core value ETFs (large-cap, small-cap)
2. Multi-factor ETFs with value tilts
3. Sector value opportunities

Value ETF Categories:
- Pure value factor ETFs
- Size-based value ETFs (large-cap, small-cap)
- Multi-factor ETFs with value exposure
"""

import asyncio
import pytest
import aiohttp
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
import os


# Value ETFs to research
VALUE_ETFS = [
    # Core Value ETFs (Priority 1)
    {
        "symbol": "IWD",
        "name": "iShares Russell 1000 Value ETF",
        "exchange": "NYSE",
        "category": "Large Cap Value",
        "sector": "Value Factor",
        "description": "Russell 1000 large-cap value stocks - core value exposure",
        "expense_ratio": 0.19,
        "aum_billions": 65.0,
        "priority": 1,
        "rationale": "Most liquid large-cap value ETF with excellent track record"
    },

    {
        "symbol": "VTV",
        "name": "Vanguard Value ETF",
        "exchange": "NYSE",
        "category": "Large Cap Value",
        "sector": "Value Factor",
        "description": "Vanguard's large-cap value offering - ultra-low cost",
        "expense_ratio": 0.04,
        "aum_billions": 95.0,
        "priority": 1,
        "rationale": "Lowest cost large-cap value ETF from Vanguard"
    },

    # Small-Cap Value
    {
        "symbol": "IWN",
        "name": "iShares Russell 2000 Value ETF",
        "exchange": "NYSE",
        "category": "Small Cap Value",
        "sector": "Value Factor",
        "description": "Russell 2000 small-cap value stocks - completes value spectrum",
        "expense_ratio": 0.24,
        "aum_billions": 15.0,
        "priority": 1,
        "rationale": "Essential small-cap value exposure to complement large-cap"
    },

    {
        "symbol": "VBR",
        "name": "Vanguard Small-Cap Value ETF",
        "exchange": "NYSE",
        "category": "Small Cap Value",
        "sector": "Value Factor",
        "description": "Vanguard's small-cap value approach - low cost alternative",
        "expense_ratio": 0.07,
        "aum_billions": 25.0,
        "priority": 1,
        "rationale": "Vanguard's low-cost approach to small-cap value"
    },

    # Mid-Cap Value
    {
        "symbol": "IWS",
        "name": "iShares Russell Mid-Cap Value ETF",
        "exchange": "NYSE",
        "category": "Mid Cap Value",
        "sector": "Value Factor",
        "description": "Russell mid-cap value stocks - completes size spectrum",
        "expense_ratio": 0.24,
        "aum_billions": 8.0,
        "priority": 2,
        "rationale": "Mid-cap value completion for full market cap coverage"
    },

    # Deep Value Options
    {
        "symbol": "VOOV",
        "name": "Vanguard Russell 1000 Value ETF",
        "exchange": "NASDAQ",
        "category": "Large Cap Value",
        "sector": "Value Factor",
        "description": "Alternative Russell 1000 value tracking",
        "expense_ratio": 0.10,
        "aum_billions": 5.0,
        "priority": 2,
        "rationale": "Alternative value methodology for diversification"
    },

    # International Value
    {
        "symbol": "VTEB",
        "name": "Vanguard Tax-Exempt Bond ETF",
        "exchange": "NASDAQ",
        "category": "Fixed Income",
        "sector": "Municipal Bonds",
        "description": "Tax-exempt municipal bonds - defensive value play",
        "expense_ratio": 0.05,
        "aum_billions": 85.0,
        "priority": 3,
        "rationale": "Tax-efficient fixed income with value characteristics"
    },

    {
        "symbol": "EFV",
        "name": "iShares MSCI EAFE Value ETF",
        "exchange": "NYSE",
        "category": "International Value",
        "sector": "Value Factor",
        "description": "International developed market value exposure",
        "expense_ratio": 0.39,
        "aum_billions": 4.5,
        "priority": 2,
        "rationale": "International diversification for value strategies"
    },

    # Dividend Value Hybrids
    {
        "symbol": "SCHD",
        "name": "Schwab US Dividend Equity ETF",
        "exchange": "NYSE",
        "category": "Dividend Value",
        "sector": "Dividend Growth",
        "description": "High-quality dividend stocks with value tilt",
        "expense_ratio": 0.06,
        "aum_billions": 55.0,
        "priority": 1,
        "rationale": "Excellent dividend+value combination with quality screen"
    },

    {
        "symbol": "VYM",
        "name": "Vanguard High Dividend Yield ETF",
        "exchange": "NYSE",
        "category": "Dividend Value",
        "sector": "High Dividend",
        "description": "High dividend yield stocks with value characteristics",
        "expense_ratio": 0.06,
        "aum_billions": 50.0,
        "priority": 2,
        "rationale": "High dividend yield often correlates with value investing"
    }
]


class ValueETFResearcher:
    """Research value ETF coverage across vendors"""

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
        """Test Polygon coverage for value ETFs"""
        print("🔍 Testing Polygon Coverage for Value ETFs...")

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
                                'rationale': etf['rationale'],
                                'expense_ratio': etf['expense_ratio']
                            }
                            print(f"✅ {symbol}: {len(results)} records ({etf['category']}, Priority {etf['priority']}, ER: {etf['expense_ratio']}%)")
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
        """Test Tiingo coverage for value ETFs"""
        print("🔍 Testing Tiingo Coverage for Value ETFs...")

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
                                'rationale': etf['rationale'],
                                'expense_ratio': etf['expense_ratio']
                            }
                            print(f"✅ {symbol}: {len(data)} records ({etf['category']}, Priority {etf['priority']}, ER: {etf['expense_ratio']}%)")
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
        """Test EODHD coverage for value ETFs"""
        print("🔍 Testing EODHD Coverage for Value ETFs...")

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
                                'rationale': etf['rationale'],
                                'expense_ratio': etf['expense_ratio']
                            }
                            print(f"✅ {symbol}: {len(data)} records ({etf['category']}, Priority {etf['priority']}, ER: {etf['expense_ratio']}%)")
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

    async def run_value_etf_research(self) -> Dict:
        """Run comprehensive value ETF research"""

        print("🎯 Value ETF Research")
        print("=" * 50)

        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

            # Test all vendors
            print("\n📊 POLYGON API TESTING")
            print("-" * 30)
            self.results['polygon']['daily'] = await self.test_polygon_coverage(session, VALUE_ETFS)

            print("\n📊 TIINGO API TESTING")
            print("-" * 30)
            self.results['tiingo']['daily'] = await self.test_tiingo_coverage(session, VALUE_ETFS)

            print("\n📊 EODHD API TESTING")
            print("-" * 30)
            self.results['eodhd']['daily'] = await self.test_eodhd_coverage(session, VALUE_ETFS)

        return self.results

    def generate_value_report(self) -> str:
        """Generate value ETF research report"""

        report = []
        report.append("=" * 80)
        report.append("🏦 VALUE ETF RESEARCH REPORT")
        report.append("=" * 80)
        report.append("")

        # Vendor coverage summary
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            daily_data = self.results[vendor]['daily']
            available_count = sum(1 for etf in daily_data.values() if etf.get('available', False))
            total_count = len(daily_data)

            report.append(f"📊 {vendor.upper()} Coverage: {available_count}/{total_count} ETFs ({available_count/total_count*100 if total_count > 0 else 0:.1f}%)")

        report.append("")
        report.append("📋 DETAILED VALUE ETF ANALYSIS BY CATEGORY")
        report.append("-" * 50)

        # Group by category and priority
        categories = {}
        for etf in VALUE_ETFS:
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
                expense_ratio = etf['expense_ratio']

                report.append(f"\n   💎 {symbol} - {name} (Priority {priority})")
                report.append(f"      💡 {rationale}")
                report.append(f"      💰 Expense Ratio: {expense_ratio}%")

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
        report.append("📝 VALUE ETF RECOMMENDATIONS")
        report.append("=" * 60)

        # Categorize by priority
        priority_1 = [etf for etf in VALUE_ETFS if etf['priority'] == 1]
        priority_2 = [etf for etf in VALUE_ETFS if etf['priority'] == 2]

        if priority_1:
            report.append("\n🏆 MUST-HAVE VALUE ETFs (Priority 1):")
            for etf in priority_1:
                symbol = etf['symbol']
                # Check coverage across vendors
                total_coverage = 0
                for vendor in ['polygon', 'tiingo', 'eodhd']:
                    daily_data = self.results[vendor]['daily']
                    if symbol in daily_data and daily_data[symbol].get('available', False):
                        total_coverage += 1

                coverage_status = "✅ Excellent" if total_coverage == 3 else f"⚠️ {total_coverage}/3 vendors"
                expense_ratio = etf['expense_ratio']
                report.append(f"   • {symbol}: {etf['name']} - {coverage_status} (ER: {expense_ratio}%)")
                report.append(f"     {etf['rationale']}")

        if priority_2:
            report.append("\n📊 RECOMMENDED VALUE ETFs (Priority 2):")
            for etf in priority_2:
                symbol = etf['symbol']
                total_coverage = 0
                for vendor in ['polygon', 'tiingo', 'eodhd']:
                    daily_data = self.results[vendor]['daily']
                    if symbol in daily_data and daily_data[symbol].get('available', False):
                        total_coverage += 1

                coverage_status = "✅ Excellent" if total_coverage == 3 else f"⚠️ {total_coverage}/3 vendors"
                expense_ratio = etf['expense_ratio']
                report.append(f"   • {symbol}: {etf['name']} - {coverage_status} (ER: {expense_ratio}%)")

        # Final recommendations
        report.append("\n💡 IMPLEMENTATION RECOMMENDATIONS:")
        report.append("")
        report.append("1. **IWD (Priority 1)**: Core large-cap value exposure with excellent liquidity")
        report.append("2. **VTV (Priority 1)**: Ultra-low cost Vanguard large-cap value alternative")
        report.append("3. **IWN (Priority 1)**: Essential small-cap value to complete size spectrum")
        report.append("4. **VBR (Priority 1)**: Low-cost Vanguard small-cap value alternative")
        report.append("5. **SCHD (Priority 1)**: Excellent dividend+value hybrid with quality screen")
        report.append("")
        report.append("🎯 These additions provide:")
        report.append("   • Complete value factor coverage (large/mid/small-cap)")
        report.append("   • Multiple provider options (iShares vs Vanguard)")
        report.append("   • Cost-effective exposure (VTV at 0.04%, VBR at 0.07%)")
        report.append("   • Dividend+value hybrid strategies (SCHD, VYM)")
        report.append("   • International value diversification (EFV)")

        return "\n".join(report)


# Test class
class TestValueETFResearch:
    """Test suite for value ETF research"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_value_etf_coverage(self):
        """Test value ETF coverage across vendors"""

        researcher = ValueETFResearcher()
        results = await researcher.run_value_etf_research()

        # Generate report
        report = researcher.generate_value_report()
        print("\n" + report)

        # Basic validations
        assert 'polygon' in results
        assert 'tiingo' in results
        assert 'eodhd' in results

        # Check that we tested the key value ETFs
        key_etfs = ['IWD', 'VTV', 'IWN', 'SCHD']
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            daily_data = results[vendor]['daily']
            for etf_symbol in key_etfs:
                assert etf_symbol in daily_data, f"{etf_symbol} should be tested in {vendor}"

        print(f"\n🎉 Tested {len(VALUE_ETFS)} value ETFs across 3 vendors!")
        print("Research shows which value ETFs have best data coverage.")

        return results


if __name__ == "__main__":
    async def main():
        researcher = ValueETFResearcher()
        results = await researcher.run_value_etf_research()
        report = researcher.generate_value_report()
        print(report)
        return results

    results = asyncio.run(main())
    print("\n🚀 Value ETF Research Complete!")
    print("Run with: PYTHONPATH=src pytest tests/integration/test_value_etf_research.py -v -s")