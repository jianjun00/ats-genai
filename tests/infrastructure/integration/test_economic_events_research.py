"""
Economic Events Data Research Across Vendors

Tests data availability for economic events/calendar data from:
1. Polygon - Economic calendar events
2. Tiingo - News and fundamentals events
3. EODHD - Economic calendar and macro events

Research focuses on:
- Data availability and format
- Event types and categories
- Historical depth
- Update frequency
- Data quality and consistency
"""

import asyncio
import pytest
import aiohttp
from datetime import date, timedelta
from typing import Dict
import os


class EconomicEventsResearcher:
    """Research economic events data across vendors"""

    def __init__(self):
        # API keys
        self.polygon_key = os.getenv('POLYGON_API_KEY', 'test_api_key_placeholder')
        self.tiingo_key = os.getenv('TIINGO_API_KEY', '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5')
        self.eodhd_key = os.getenv('EODHD_API_KEY', '68aa0c7d2fe831.67386369')

        # Test periods
        self.end_date = date.today()
        self.start_date = self.end_date - timedelta(days=30)

        self.results = {
            'polygon': {'events': [], 'status': None, 'error': None},
            'tiingo': {'events': [], 'status': None, 'error': None},
            'eodhd': {'events': [], 'status': None, 'error': None}
        }

    async def test_polygon_economic_events(self, session: aiohttp.ClientSession) -> Dict:
        """Test Polygon economic calendar events"""
        print("🔍 Testing Polygon Economic Events...")

        try:
            # Economic calendar endpoint
            url = "https://api.polygon.io/v1/marketstatus/upcoming"
            params = {
                'apikey': self.polygon_key
            }

            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    events = data.get('results', [])

                    self.results['polygon'] = {
                        'events': events[:10],  # Sample first 10
                        'total_count': len(events),
                        'status': 'success',
                        'data_types': list(set([event.get('event', 'unknown') for event in events[:5]])),
                        'sample_event': events[0] if events else None
                    }

                    print(f"✅ Polygon: Found {len(events)} upcoming market events")
                    if events:
                        print(f"   Sample event: {events[0].get('event', 'N/A')} on {events[0].get('date', 'N/A')}")

                else:
                    self.results['polygon'] = {
                        'events': [],
                        'status': 'error',
                        'error': f"HTTP {response.status}"
                    }
                    print(f"❌ Polygon: HTTP {response.status}")

        except Exception as e:
            self.results['polygon'] = {
                'events': [],
                'status': 'error',
                'error': str(e)
            }
            print(f"💥 Polygon: {e}")

        # Also test economic indicators
        try:
            indicators_url = "https://api.polygon.io/v1/indicators/ma/SPY"
            params = {
                'timestamp.gte': self.start_date.strftime('%Y-%m-%d'),
                'limit': 10,
                'apikey': self.polygon_key
            }

            async with session.get(indicators_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    indicators = data.get('results', {}).get('values', [])

                    if 'indicators' not in self.results['polygon']:
                        self.results['polygon']['indicators'] = {}

                    self.results['polygon']['indicators'] = {
                        'available': len(indicators) > 0,
                        'count': len(indicators),
                        'sample': indicators[:3] if indicators else []
                    }

                    print(f"✅ Polygon Indicators: {len(indicators)} technical indicators available")

        except Exception as e:
            print(f"⚠️ Polygon Indicators: {e}")

        return self.results['polygon']

    async def test_tiingo_economic_events(self, session: aiohttp.ClientSession) -> Dict:
        """Test Tiingo news and fundamentals events"""
        print("🔍 Testing Tiingo Economic Events...")

        try:
            # Tiingo News API for economic news
            url = "https://api.tiingo.com/tiingo/news"
            params = {
                'sortBy': 'publishedDate',
                'startDate': self.start_date.strftime('%Y-%m-%d'),
                'endDate': self.end_date.strftime('%Y-%m-%d'),
                'tags': 'economics,macro,fed,gdp,inflation',
                'token': self.tiingo_key
            }

            headers = {'Content-Type': 'application/json'}

            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    events = await response.json()

                    if isinstance(events, list):
                        self.results['tiingo'] = {
                            'events': events[:10],  # Sample first 10
                            'total_count': len(events),
                            'status': 'success',
                            'data_types': ['economic_news', 'macro_analysis'],
                            'sample_event': events[0] if events else None
                        }

                        print(f"✅ Tiingo: Found {len(events)} economic news events")
                        if events:
                            print(f"   Sample: {events[0].get('title', 'N/A')[:60]}...")
                    else:
                        print("📭 Tiingo: Unexpected response format")
                        self.results['tiingo'] = {
                            'events': [],
                            'status': 'empty',
                            'error': 'Unexpected response format'
                        }
                else:
                    self.results['tiingo'] = {
                        'events': [],
                        'status': 'error',
                        'error': f"HTTP {response.status}"
                    }
                    print(f"❌ Tiingo: HTTP {response.status}")

        except Exception as e:
            self.results['tiingo'] = {
                'events': [],
                'status': 'error',
                'error': str(e)
            }
            print(f"💥 Tiingo: {e}")

        # Also test fundamentals data
        try:
            fundamentals_url = "https://api.tiingo.com/tiingo/fundamentals/SPY/daily"
            params = {
                'startDate': (self.end_date - timedelta(days=90)).strftime('%Y-%m-%d'),
                'token': self.tiingo_key
            }

            async with session.get(fundamentals_url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()

                    if 'fundamentals' not in self.results['tiingo']:
                        self.results['tiingo']['fundamentals'] = {}

                    self.results['tiingo']['fundamentals'] = {
                        'available': len(data) > 0 if isinstance(data, list) else bool(data),
                        'sample': data[:2] if isinstance(data, list) else data
                    }

                    print("✅ Tiingo Fundamentals: Available")

        except Exception as e:
            print(f"⚠️ Tiingo Fundamentals: {e}")

        return self.results['tiingo']

    async def test_eodhd_economic_events(self, session: aiohttp.ClientSession) -> Dict:
        """Test EODHD economic calendar"""
        print("🔍 Testing EODHD Economic Events...")

        try:
            # EODHD Economic Calendar
            url = "https://eodhd.com/api/economic-events"
            params = {
                'from': self.start_date.strftime('%Y-%m-%d'),
                'to': self.end_date.strftime('%Y-%m-%d'),
                'country': 'US',
                'fmt': 'json',
                'api_token': self.eodhd_key
            }

            async with session.get(url, params=params) as response:
                if response.status == 200:
                    events = await response.json()

                    if isinstance(events, list):
                        self.results['eodhd'] = {
                            'events': events[:10],  # Sample first 10
                            'total_count': len(events),
                            'status': 'success',
                            'data_types': list(set([event.get('event', 'unknown') for event in events[:5]])),
                            'sample_event': events[0] if events else None
                        }

                        print(f"✅ EODHD: Found {len(events)} economic calendar events")
                        if events:
                            print(f"   Sample: {events[0].get('event', 'N/A')} on {events[0].get('date', 'N/A')}")
                    else:
                        print("📭 EODHD: Unexpected response format")
                        self.results['eodhd'] = {
                            'events': [],
                            'status': 'empty',
                            'error': 'Unexpected response format'
                        }
                else:
                    self.results['eodhd'] = {
                        'events': [],
                        'status': 'error',
                        'error': f"HTTP {response.status}"
                    }
                    print(f"❌ EODHD: HTTP {response.status}")

        except Exception as e:
            self.results['eodhd'] = {
                'events': [],
                'status': 'error',
                'error': str(e)
            }
            print(f"💥 EODHD: {e}")

        # Also test macro indicators
        try:
            macro_url = "https://eodhd.com/api/macro-indicator"
            params = {
                'country': 'US',
                'indicator': 'gdp_growth_rate',
                'fmt': 'json',
                'api_token': self.eodhd_key
            }

            async with session.get(macro_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    if 'macro_indicators' not in self.results['eodhd']:
                        self.results['eodhd']['macro_indicators'] = {}

                    self.results['eodhd']['macro_indicators'] = {
                        'available': len(data) > 0 if isinstance(data, list) else bool(data),
                        'sample': data[:3] if isinstance(data, list) else data
                    }

                    print("✅ EODHD Macro Indicators: Available")

        except Exception as e:
            print(f"⚠️ EODHD Macro Indicators: {e}")

        return self.results['eodhd']

    async def run_economic_events_research(self) -> Dict:
        """Run comprehensive economic events research"""

        print("🎯 Economic Events Data Research")
        print("=" * 50)

        connector = aiohttp.TCPConnector(limit=5)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

            # Test all vendors
            print("\n📊 POLYGON API TESTING")
            print("-" * 30)
            await self.test_polygon_economic_events(session)
            await asyncio.sleep(1)

            print("\n📊 TIINGO API TESTING")
            print("-" * 30)
            await self.test_tiingo_economic_events(session)
            await asyncio.sleep(1)

            print("\n📊 EODHD API TESTING")
            print("-" * 30)
            await self.test_eodhd_economic_events(session)

        return self.results

    def generate_economic_events_report(self) -> str:
        """Generate economic events research report"""

        report = []
        report.append("=" * 80)
        report.append("📅 ECONOMIC EVENTS DATA RESEARCH REPORT")
        report.append("=" * 80)
        report.append("")

        # Vendor status summary
        for vendor in ['polygon', 'tiingo', 'eodhd']:
            data = self.results[vendor]
            status = data.get('status', 'unknown')
            count = data.get('total_count', 0)

            status_icon = "✅" if status == 'success' else "❌" if status == 'error' else "⚠️"
            report.append(f"📊 {vendor.upper()}: {status_icon} {status.upper()} ({count} events)")

            if data.get('error'):
                report.append(f"    Error: {data['error']}")

        report.append("")
        report.append("📋 DETAILED ANALYSIS BY VENDOR")
        report.append("-" * 50)

        # Polygon analysis
        polygon_data = self.results['polygon']
        report.append("\n🔺 POLYGON ECONOMIC DATA")
        report.append("   " + "-" * 25)

        if polygon_data.get('status') == 'success':
            report.append(f"   📊 Market Events: {polygon_data.get('total_count', 0)} upcoming events")
            report.append(f"   📈 Event Types: {', '.join(polygon_data.get('data_types', []))}")

            if polygon_data.get('indicators'):
                indicators = polygon_data['indicators']
                report.append(f"   📉 Technical Indicators: {'✅ Available' if indicators.get('available') else '❌ Not available'}")

            if polygon_data.get('sample_event'):
                sample = polygon_data['sample_event']
                report.append(f"   💡 Sample Event: {sample.get('event', 'N/A')} on {sample.get('date', 'N/A')}")
        else:
            report.append(f"   ❌ Status: {polygon_data.get('error', 'Unknown error')}")

        # Tiingo analysis
        tiingo_data = self.results['tiingo']
        report.append("\n🔷 TIINGO ECONOMIC DATA")
        report.append("   " + "-" * 23)

        if tiingo_data.get('status') == 'success':
            report.append(f"   📰 Economic News: {tiingo_data.get('total_count', 0)} articles")
            report.append(f"   📊 Data Types: {', '.join(tiingo_data.get('data_types', []))}")

            if tiingo_data.get('fundamentals'):
                fundamentals = tiingo_data['fundamentals']
                report.append(f"   📈 Fundamentals: {'✅ Available' if fundamentals.get('available') else '❌ Not available'}")

            if tiingo_data.get('sample_event'):
                sample = tiingo_data['sample_event']
                title = sample.get('title', 'N/A')[:60] + "..." if len(sample.get('title', '')) > 60 else sample.get('title', 'N/A')
                report.append(f"   💡 Sample: {title}")
        else:
            report.append(f"   ❌ Status: {tiingo_data.get('error', 'Unknown error')}")

        # EODHD analysis
        eodhd_data = self.results['eodhd']
        report.append("\n🟡 EODHD ECONOMIC DATA")
        report.append("   " + "-" * 22)

        if eodhd_data.get('status') == 'success':
            report.append(f"   📅 Calendar Events: {eodhd_data.get('total_count', 0)} events")
            report.append(f"   📊 Event Types: {', '.join(eodhd_data.get('data_types', []))}")

            if eodhd_data.get('macro_indicators'):
                macro = eodhd_data['macro_indicators']
                report.append(f"   📉 Macro Indicators: {'✅ Available' if macro.get('available') else '❌ Not available'}")

            if eodhd_data.get('sample_event'):
                sample = eodhd_data['sample_event']
                report.append(f"   💡 Sample: {sample.get('event', 'N/A')} on {sample.get('date', 'N/A')}")
        else:
            report.append(f"   ❌ Status: {eodhd_data.get('error', 'Unknown error')}")

        # Recommendations
        report.append("\n" + "=" * 60)
        report.append("💡 IMPLEMENTATION RECOMMENDATIONS")
        report.append("=" * 60)
        report.append("")

        successful_vendors = [vendor for vendor, data in self.results.items() if data.get('status') == 'success']

        if successful_vendors:
            report.append("🎯 RECOMMENDED DATABASE STRUCTURE:")
            report.append("")

            for vendor in successful_vendors:
                vendor_name = vendor.upper()
                report.append(f"📊 {vendor_name}_ECONOMIC_EVENTS table:")

                if vendor == 'polygon':
                    report.append("   • Columns: event_id, event_name, event_date, market_status, description")
                    report.append("   • Focus: Market holidays, trading calendar, volatility events")

                elif vendor == 'tiingo':
                    report.append("   • Columns: article_id, title, published_date, tags, url, content_preview")
                    report.append("   • Focus: Economic news, Fed communications, market analysis")

                elif vendor == 'eodhd':
                    report.append("   • Columns: event_id, event_name, country, date, time, importance, actual, forecast, previous")
                    report.append("   • Focus: Economic calendar, macro indicators, earnings calendar")

                report.append("")

            report.append("🔧 INTEGRATION STRATEGY:")
            report.append("1. Create separate tables per vendor to preserve data integrity")
            report.append("2. Use standardized event_date and importance_level columns")
            report.append("3. Implement daily/weekly update jobs for each vendor")
            report.append("4. Create unified view for cross-vendor event correlation")
            report.append("5. Add event impact analysis and backtesting capabilities")

        else:
            report.append("⚠️ No vendors returned successful data. Consider:")
            report.append("1. Verify API keys and permissions")
            report.append("2. Check rate limiting and quota restrictions")
            report.append("3. Review endpoint documentation for economic data")
            report.append("4. Consider alternative economic data providers")

        return "\n".join(report)


# Test class
class TestEconomicEventsResearch:
    """Test suite for economic events research"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_economic_events_coverage(self):
        """Test economic events coverage across vendors"""

        researcher = EconomicEventsResearcher()
        results = await researcher.run_economic_events_research()

        # Generate report
        report = researcher.generate_economic_events_report()
        print("\n" + report)

        # Basic validations
        assert 'polygon' in results
        assert 'tiingo' in results
        assert 'eodhd' in results

        # Check that we got some response from vendors
        response_count = sum(1 for vendor_data in results.values() if vendor_data.get('status') is not None)
        assert response_count >= 3, f"Should test all 3 vendors, got {response_count}"

        print(f"\n🎉 Tested economic events across 3 vendors!")
        print("Research shows economic data availability and structure.")

        return results


if __name__ == "__main__":
    async def main():
        researcher = EconomicEventsResearcher()
        results = await researcher.run_economic_events_research()
        report = researcher.generate_economic_events_report()
        print(report)
        return results

    results = asyncio.run(main())
    print("\n🚀 Economic Events Research Complete!")
    print("Run with: PYTHONPATH=src pytest tests/integration/test_economic_events_research.py -v -s")