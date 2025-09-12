#!/usr/bin/env python3
"""
Research Universe Entry/Exit Patterns with Real Market Data
Analyzes actual volume data to identify when stocks likely qualified/disqualified
for universe membership based on $100M volume threshold
"""

import sys
import os
sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json

def research_entry_exit_patterns():
    """Research actual entry/exit patterns for key stocks"""

    print("🔍 RESEARCHING UNIVERSE ENTRY/EXIT PATTERNS WITH REAL DATA")
    print("="*70)
    print(f"📅 Research Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    os.environ['ENVIRONMENT'] = 'intg'

    # Key stocks to analyze with expected patterns
    research_stocks = {
        'SMCI': {
            'name': 'Super Micro Computer',
            'expected_pattern': 'AI Boom Entry (~2023-03)',
            'background': 'Low volume until AI infrastructure demand surge'
        },
        'MSTR': {
            'name': 'MicroStrategy',
            'expected_pattern': 'Bitcoin Strategy Entry (~2020-08)',
            'background': 'Corporate Bitcoin adoption drove trading volume'
        },
        'PTON': {
            'name': 'Peloton',
            'expected_pattern': 'Pandemic Entry → Post-Pandemic Exit',
            'background': 'Home fitness surge then decline'
        },
        'BYND': {
            'name': 'Beyond Meat',
            'expected_pattern': 'Hype Entry → Reality Check Exit',
            'background': 'Plant-based meat hype cycle'
        },
        'ARKB': {
            'name': 'ARK Bitcoin ETF',
            'expected_pattern': 'Recent Launch Entry (2024-01)',
            'background': 'Bitcoin ETF approval drove volume'
        },
        'NVDA': {
            'name': 'NVIDIA',
            'expected_pattern': 'Multiple Entries (Gaming → AI booms)',
            'background': 'GPU demand cycles from gaming, crypto, AI'
        }
    }

    with get_raw_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            print("\n📊 1. Analyzing Volume Patterns by Time Period")

            # Define key market periods for analysis
            analysis_periods = [
                ('Pre-COVID', '2019-01-01', '2020-02-29'),
                ('COVID Peak', '2020-03-01', '2021-12-31'),
                ('Post-COVID', '2022-01-01', '2022-12-31'),
                ('AI Boom', '2023-01-01', '2024-09-03')
            ]

            results = {}

            for symbol, info in research_stocks.items():
                print(f"\n🔬 Analyzing {symbol} ({info['name']}):")
                print(f"   Expected: {info['expected_pattern']}")
                print(f"   Background: {info['background']}")

                stock_results = {}

                for period_name, start_date, end_date in analysis_periods:
                    cursor.execute("""
                        SELECT
                            %s as period,
                            symbol,
                            COUNT(*) as trading_days,
                            AVG(close * volume) as avg_dollar_volume,
                            MIN(close * volume) as min_dollar_volume,
                            MAX(close * volume) as max_dollar_volume,
                            MIN(date) as earliest_date,
                            MAX(date) as latest_date,
                            CASE
                                WHEN AVG(close * volume) >= 100000000 THEN 'QUALIFIES'
                                ELSE 'BELOW_THRESHOLD'
                            END as qualification_status
                        FROM intg_daily_prices_polygon
                        WHERE symbol = %s
                        AND date >= %s
                        AND date <= %s
                        AND volume > 0  -- Exclude non-trading days
                        GROUP BY symbol
                    """, (period_name, symbol, start_date, end_date))

                    period_data = cursor.fetchone()

                    if period_data:
                        avg_volume = period_data['avg_dollar_volume']
                        status = period_data['qualification_status']
                        days = period_data['trading_days']

                        print(f"      {period_name}: ${avg_volume:,.0f} avg volume ({status}) - {days} trading days")

                        stock_results[period_name] = {
                            'avg_volume': float(avg_volume),
                            'status': status,
                            'trading_days': days,
                            'period_range': f"{start_date} to {end_date}"
                        }
                    else:
                        print(f"      {period_name}: No trading data found")
                        stock_results[period_name] = None

                results[symbol] = {
                    'info': info,
                    'periods': stock_results
                }

            print(f"\n📈 2. Identifying Entry/Exit Event Dates")

            # Analyze daily volume data to find specific qualification change dates
            entry_exit_events = {}

            for symbol, info in research_stocks.items():
                print(f"\n🎯 {symbol} Entry/Exit Event Analysis:")

                # Get rolling 50-day volume averages to identify qualification changes
                cursor.execute("""
                    WITH daily_volume AS (
                        SELECT
                            date,
                            symbol,
                            close * volume as dollar_volume
                        FROM intg_daily_prices_polygon
                        WHERE symbol = %s
                        AND date >= '2019-01-01'
                        AND date <= '2024-09-03'
                        AND volume > 0
                        ORDER BY date
                    ),
                    rolling_averages AS (
                        SELECT
                            date,
                            symbol,
                            dollar_volume,
                            AVG(dollar_volume) OVER (
                                ORDER BY date
                                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                            ) as rolling_50d_avg
                        FROM daily_volume
                    ),
                    qualification_status AS (
                        SELECT
                            date,
                            symbol,
                            dollar_volume,
                            rolling_50d_avg,
                            CASE WHEN rolling_50d_avg >= 100000000 THEN 1 ELSE 0 END as qualifies,
                            LAG(CASE WHEN rolling_50d_avg >= 100000000 THEN 1 ELSE 0 END) OVER (ORDER BY date) as prev_qualifies
                        FROM rolling_averages
                        WHERE date >= '2019-03-01'  -- Allow 50 days for rolling average
                    )
                    SELECT
                        date,
                        rolling_50d_avg,
                        qualifies,
                        prev_qualifies,
                        CASE
                            WHEN qualifies = 1 AND (prev_qualifies IS NULL OR prev_qualifies = 0) THEN 'ENTRY'
                            WHEN qualifies = 0 AND prev_qualifies = 1 THEN 'EXIT'
                            ELSE 'CONTINUE'
                        END as event_type
                    FROM qualification_status
                    WHERE qualifies != COALESCE(prev_qualifies, qualifies)  -- Only qualification changes
                    ORDER BY date
                    LIMIT 10  -- First 10 events
                """, (symbol,))

                events = cursor.fetchall()

                stock_events = []
                for event in events:
                    event_type = event['event_type']
                    date = event['date']
                    volume = event['rolling_50d_avg']

                    print(f"      {event_type}: {date} (${volume:,.0f} rolling avg)")
                    stock_events.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'event_type': event_type,
                        'rolling_volume': float(volume)
                    })

                entry_exit_events[symbol] = stock_events

            print(f"\n🧪 3. Generating Test Cases for Universe Logic")

            # Generate specific test scenarios based on research
            test_scenarios = []

            for symbol, data in results.items():
                info = data['info']
                periods = data['periods']
                events = entry_exit_events.get(symbol, [])

                # Analyze qualification pattern
                qualified_periods = [p for p, d in periods.items() if d and d['status'] == 'QUALIFIES']

                if len(qualified_periods) == 0:
                    pattern = "Never Qualified"
                elif len(qualified_periods) == len([p for p in periods.values() if p]):
                    pattern = "Always Qualified"
                else:
                    pattern = "Intermittent Qualification"

                test_scenario = {
                    'symbol': symbol,
                    'name': info['name'],
                    'expected_pattern': info['expected_pattern'],
                    'actual_pattern': pattern,
                    'qualified_periods': qualified_periods,
                    'key_events': events[:3],  # First 3 events
                    'test_requirements': []
                }

                # Generate specific test requirements
                if 'SMCI' in symbol and any('AI Boom' in p for p in qualified_periods):
                    test_scenario['test_requirements'].append('Verify entry date around 2023-03-15 when AI boom started')

                if 'PTON' in symbol:
                    if 'COVID Peak' in qualified_periods and 'Post-COVID' not in qualified_periods:
                        test_scenario['test_requirements'].append('Verify entry during COVID, exit post-pandemic')

                if events:
                    first_entry = next((e for e in events if e['event_type'] == 'ENTRY'), None)
                    if first_entry:
                        test_scenario['test_requirements'].append(f"Verify first entry on {first_entry['date']}")

                test_scenarios.append(test_scenario)

            print(f"\n📋 RESEARCH SUMMARY & TEST REQUIREMENTS:")
            print("="*70)

            for scenario in test_scenarios:
                print(f"\n🎯 {scenario['symbol']} ({scenario['name']}):")
                print(f"   Expected: {scenario['expected_pattern']}")
                print(f"   Actual: {scenario['actual_pattern']}")
                print(f"   Qualified Periods: {', '.join(scenario['qualified_periods'])}")

                if scenario['key_events']:
                    print(f"   Key Events:")
                    for event in scenario['key_events']:
                        print(f"     • {event['event_type']}: {event['date']} (${event['rolling_volume']:,.0f})")

                if scenario['test_requirements']:
                    print(f"   Test Requirements:")
                    for req in scenario['test_requirements']:
                        print(f"     ✅ {req}")

            # Save results for test generation
            research_output = {
                'research_date': datetime.now().isoformat(),
                'analysis_periods': {p[0]: {'start': p[1], 'end': p[2]} for p in analysis_periods},
                'stock_results': results,
                'entry_exit_events': entry_exit_events,
                'test_scenarios': test_scenarios
            }

            with open('/home/jianjun/ats-genai-admin/universe_research_data.json', 'w') as f:
                json.dump(research_output, f, indent=2, default=str)

            print(f"\n💾 Research data saved to: universe_research_data.json")
            print(f"\n🚀 Next Steps:")
            print(f"   1. Create comprehensive test framework based on research findings")
            print(f"   2. Implement correct daily evaluation logic")
            print(f"   3. Validate start_at/end_at dates match actual qualification events")
            print(f"   4. Test multiple entry/exit patterns with real examples")

            return True

if __name__ == "__main__":
    try:
        success = research_entry_exit_patterns()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Error researching patterns: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)