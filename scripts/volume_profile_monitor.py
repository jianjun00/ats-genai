#!/usr/bin/env python3
"""
Volume Profile Monitoring Dashboard
Real-time monitoring and analysis of Volume Profile indicators with market data.
"""

import sys
import os
import pandas as pd
import numpy as np
import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from domains.trading.signals.indicator import VolumeProfile
from domains.trading.signals.advanced_volume_profile import SessionVolumeProfile, AdaptiveVolumeProfile, MultiTimeframeVolumeProfile

class VolumeProfileMonitor:
    """Advanced Volume Profile monitoring system."""

    def __init__(self):
        self.symbols = ['AAPL', 'TSLA', 'MSFT', 'NVDA']
        self.timeframes = ['5m', '15m', '1h', '1d']
        self.volume_profiles: Dict[str, Dict[str, VolumeProfile]] = {}
        self.advanced_profiles: Dict[str, Dict[str, object]] = {}
        self.last_update = None

        # Initialize Volume Profile indicators for each symbol/timeframe
        self._initialize_indicators()

    def _initialize_indicators(self):
        """Initialize Volume Profile indicators."""
        print("🔧 Initializing Volume Profile indicators...")

        for symbol in self.symbols:
            self.volume_profiles[symbol] = {}
            self.advanced_profiles[symbol] = {}

            for timeframe in self.timeframes:
                # Standard Volume Profile
                self.volume_profiles[symbol][timeframe] = VolumeProfile(
                    period=20 if timeframe in ['5m', '15m'] else 30,
                    bin_count=30 if timeframe in ['5m', '15m'] else 50,
                    value_area_pct=70.0
                )

                # Advanced Volume Profiles
                from datetime import time
                self.advanced_profiles[symbol][timeframe] = {
                    'session': SessionVolumeProfile(
                        period=20,
                        session_start=time(9, 30),
                        session_end=time(16, 0)
                    ),
                    'adaptive': AdaptiveVolumeProfile(base_period=20),
                    'multi_timeframe': MultiTimeframeVolumeProfile({'5m': 12, '15m': 20, '1h': 24})
                }

        print(f"✅ Initialized {len(self.symbols)} symbols × {len(self.timeframes)} timeframes = {len(self.symbols) * len(self.timeframes)} Volume Profile indicators")

    def generate_realistic_market_data(self, symbol: str, periods: int = 100) -> pd.DataFrame:
        """Generate realistic market data for testing."""
        np.random.seed(hash(symbol) % 1000)  # Consistent seed per symbol

        # Symbol-specific parameters
        symbol_params = {
            'AAPL': {'base_price': 225.0, 'volatility': 0.015, 'volume_base': 13.5},
            'TSLA': {'base_price': 250.0, 'volatility': 0.025, 'volume_base': 13.8},
            'MSFT': {'base_price': 420.0, 'volatility': 0.012, 'volume_base': 13.2},
            'NVDA': {'base_price': 140.0, 'volatility': 0.030, 'volume_base': 14.1}
        }

        params = symbol_params.get(symbol, symbol_params['AAPL'])

        # Generate price data
        returns = np.random.normal(0.0002, params['volatility'], periods)
        prices = params['base_price'] * np.exp(np.cumsum(returns))

        # Create OHLCV data
        data = pd.DataFrame({
            'timestamp': pd.date_range(datetime.now() - timedelta(hours=periods),
                                     datetime.now(), periods=periods),
            'open': prices * (1 + np.random.normal(0, 0.0005, periods)),
            'high': prices * (1 + np.random.uniform(0.001, 0.008, periods)),
            'low': prices * (1 - np.random.uniform(0.001, 0.008, periods)),
            'close': prices,
            'volume': np.random.lognormal(params['volume_base'], 0.4, periods)
        })

        return data

    def create_mock_intervals(self, data: pd.DataFrame) -> List:
        """Create mock InstrumentInterval objects from DataFrame."""
        intervals = []
        for _, row in data.iterrows():
            class MockInterval:
                def __init__(self, row):
                    self.open = float(row['open'])
                    self.high = float(row['high'])
                    self.low = float(row['low'])
                    self.close = float(row['close'])
                    self.volume = float(row['volume'])
                    self.traded_volume = float(row['volume'])
                    self.status = 'ok'
                    self.timestamp = row['timestamp']
            intervals.append(MockInterval(row))
        return intervals

    def update_volume_profiles(self):
        """Update all Volume Profile indicators with latest data."""
        print(f"\n🔄 Updating Volume Profile indicators at {datetime.now().strftime('%H:%M:%S')}")

        results = {}

        for symbol in self.symbols:
            print(f"\n📊 Processing {symbol}:")
            results[symbol] = {}

            # Generate market data for this symbol
            data = self.generate_realistic_market_data(symbol)
            intervals = self.create_mock_intervals(data)

            for timeframe in self.timeframes:
                # Update standard Volume Profile
                vp = self.volume_profiles[symbol][timeframe]
                vp.update(intervals)

                # Collect results
                vp_result = {
                    'poc': vp.latest_poc,
                    'vah': vp.latest_vah,
                    'val': vp.latest_val,
                    'profile_shape': vp.profile_shape,
                    'market_bias': vp.market_bias,
                    'total_volume': vp.total_volume,
                    'volume_concentration': vp.volume_concentration,
                    'status': vp.status
                }

                # Update advanced profiles
                advanced_results = {}
                for profile_type, profile in self.advanced_profiles[symbol][timeframe].items():
                    try:
                        profile.update(intervals)
                        advanced_results[profile_type] = {
                            'poc': getattr(profile, 'latest_poc', None),
                            'vah': getattr(profile, 'latest_vah', None),
                            'val': getattr(profile, 'latest_val', None),
                            'status': getattr(profile, 'status', 'unknown')
                        }
                    except Exception as e:
                        advanced_results[profile_type] = {'error': str(e)}

                results[symbol][timeframe] = {
                    'standard': vp_result,
                    'advanced': advanced_results,
                    'current_price': float(data['close'].iloc[-1]),
                    'price_range': [float(data['close'].min()), float(data['close'].max())],
                    'volume_range': [float(data['volume'].min()), float(data['volume'].max())]
                }

                # Display key metrics
                if vp_result['poc'] is not None:
                    current_price = results[symbol][timeframe]['current_price']
                    poc_distance = current_price - vp_result['poc']
                    va_range = vp_result['vah'] - vp_result['val'] if vp_result['vah'] and vp_result['val'] else 0

                    print(f"  {timeframe}: POC=${vp_result['poc']:.2f}, "
                          f"VA=[${vp_result['val']:.2f}-${vp_result['vah']:.2f}], "
                          f"Price=${current_price:.2f} ({poc_distance:+.2f} vs POC), "
                          f"Shape={vp_result['profile_shape']}, "
                          f"Bias={vp_result['market_bias']}")
                else:
                    print(f"  {timeframe}: No Volume Profile data available")

        self.last_update = datetime.now()
        return results

    def generate_market_analysis(self, results: Dict) -> Dict:
        """Generate comprehensive market analysis from Volume Profile data."""
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'summary': {},
            'alerts': [],
            'institutional_activity': {},
            'market_structure': {}
        }

        for symbol in results:
            symbol_analysis = {
                'overall_bias': None,
                'key_levels': [],
                'volume_concentration': 0.0,
                'profile_consensus': {},
                'risk_assessment': 'neutral'
            }

            # Analyze across timeframes
            timeframe_biases = []
            pocs = []

            for timeframe in self.timeframes:
                tf_data = results[symbol][timeframe]
                standard = tf_data['standard']
                current_price = tf_data['current_price']

                if standard['poc'] is not None:
                    pocs.append(standard['poc'])

                    # Price position relative to POC
                    poc_distance = current_price - standard['poc']
                    if poc_distance > 2.0:
                        timeframe_biases.append('bullish')
                    elif poc_distance < -2.0:
                        timeframe_biases.append('bearish')
                    else:
                        timeframe_biases.append('neutral')

                    # Key support/resistance levels
                    symbol_analysis['key_levels'].extend([
                        {'level': standard['poc'], 'type': 'POC', 'timeframe': timeframe},
                        {'level': standard['vah'], 'type': 'VAH', 'timeframe': timeframe},
                        {'level': standard['val'], 'type': 'VAL', 'timeframe': timeframe}
                    ])

                    if standard['volume_concentration']:
                        symbol_analysis['volume_concentration'] += standard['volume_concentration']

            # Determine overall bias
            if len(timeframe_biases) > 0:
                bias_counts = {bias: timeframe_biases.count(bias) for bias in ['bullish', 'bearish', 'neutral']}
                symbol_analysis['overall_bias'] = max(bias_counts, key=bias_counts.get)

            # POC clustering analysis
            if len(pocs) > 2:
                poc_std = np.std(pocs)
                if poc_std < 1.0:
                    analysis['alerts'].append(f"{symbol}: POC cluster detected at ${np.mean(pocs):.2f} ± ${poc_std:.2f}")

            # Risk assessment
            if symbol_analysis['volume_concentration'] > 0.8:
                symbol_analysis['risk_assessment'] = 'high_concentration'
            elif symbol_analysis['volume_concentration'] > 0.6:
                symbol_analysis['risk_assessment'] = 'moderate_concentration'

            analysis['summary'][symbol] = symbol_analysis

        return analysis

    def print_dashboard(self, results: Dict, analysis: Dict):
        """Print comprehensive dashboard."""
        print("\n" + "="*80)
        print("📈 VOLUME PROFILE MONITORING DASHBOARD")
        print("="*80)
        print(f"🕐 Last Update: {self.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Monitoring: {', '.join(self.symbols)}")
        print(f"⏱️  Timeframes: {', '.join(self.timeframes)}")

        # Market Summary
        print(f"\n📋 MARKET SUMMARY:")
        for symbol, summary in analysis['summary'].items():
            bias = summary['overall_bias'] or 'unknown'
            bias_emoji = {'bullish': '🟢', 'bearish': '🔴', 'neutral': '🟡'}.get(bias, '⚪')

            print(f"  {symbol}: {bias_emoji} {bias.upper()} | "
                  f"Volume Concentration: {summary['volume_concentration']:.2f} | "
                  f"Risk: {summary['risk_assessment']}")

        # Key Alerts
        if analysis['alerts']:
            print(f"\n🚨 VOLUME PROFILE ALERTS:")
            for alert in analysis['alerts']:
                print(f"  ⚠️  {alert}")
        else:
            print(f"\n✅ No critical Volume Profile alerts")

        # Detailed Volume Profile Levels
        print(f"\n🎯 KEY VOLUME PROFILE LEVELS:")
        for symbol in self.symbols:
            if symbol in results:
                print(f"\n  {symbol}:")
                for timeframe in self.timeframes:
                    tf_data = results[symbol][timeframe]
                    standard = tf_data['standard']
                    current_price = tf_data['current_price']

                    if standard['poc'] is not None:
                        poc_dist = current_price - standard['poc']
                        val_dist = current_price - standard['val'] if standard['val'] else 0
                        vah_dist = current_price - standard['vah'] if standard['vah'] else 0

                        print(f"    {timeframe}: Price=${current_price:.2f} | "
                              f"POC=${standard['poc']:.2f} ({poc_dist:+.2f}) | "
                              f"VA=[${standard['val']:.2f}({val_dist:+.2f}) - ${standard['vah']:.2f}({vah_dist:+.2f})]")

        # Performance Stats
        print(f"\n⚡ PERFORMANCE STATISTICS:")
        total_indicators = len(self.symbols) * len(self.timeframes)
        active_indicators = sum(1 for symbol in results.values()
                               for tf in symbol.values()
                               if tf['standard']['status'] == 'ok')

        print(f"  📊 Active Indicators: {active_indicators}/{total_indicators} ({active_indicators/total_indicators*100:.1f}%)")
        print(f"  🔧 Status: {'✅ All Systems Operational' if active_indicators == total_indicators else '⚠️ Some Indicators Offline'}")

    def run_monitoring_cycle(self, cycles: int = 1, interval: int = 10):
        """Run monitoring cycles."""
        print(f"🚀 Starting Volume Profile monitoring ({cycles} cycles, {interval}s interval)")

        try:
            for cycle in range(cycles):
                print(f"\n{'='*20} CYCLE {cycle + 1}/{cycles} {'='*20}")

                # Update Volume Profiles
                results = self.update_volume_profiles()

                # Generate analysis
                analysis = self.generate_market_analysis(results)

                # Display dashboard
                self.print_dashboard(results, analysis)

                # Save results to file
                output_file = f"/tmp/volume_profile_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(output_file, 'w') as f:
                    combined_results = {
                        'volume_profiles': results,
                        'analysis': analysis,
                        'metadata': {
                            'cycle': cycle + 1,
                            'total_cycles': cycles,
                            'symbols': self.symbols,
                            'timeframes': self.timeframes
                        }
                    }
                    json.dump(combined_results, f, indent=2, default=str)

                print(f"\n💾 Results saved to: {output_file}")

                if cycle < cycles - 1:
                    print(f"\n⏳ Waiting {interval} seconds until next cycle...")
                    import time
                    time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\n\n🛑 Monitoring stopped by user")
        except Exception as e:
            print(f"\n❌ Error during monitoring: {e}")
            import traceback
            traceback.print_exc()

    def run_validation_test(self):
        """Run validation test to ensure system works correctly."""
        print("🧪 Running Volume Profile validation test...")

        try:
            # Test data generation
            test_data = self.generate_realistic_market_data('AAPL', 50)
            assert len(test_data) == 50, "Data generation failed"
            print("✅ Market data generation: PASS")

            # Test Volume Profile calculation
            intervals = self.create_mock_intervals(test_data)
            test_vp = VolumeProfile(period=20, bin_count=30)
            test_vp.update(intervals)

            assert test_vp.status == 'ok', f"Volume Profile calculation failed: {test_vp.status}"
            assert test_vp.latest_poc is not None, "POC calculation failed"
            print("✅ Volume Profile calculation: PASS")

            # Test full monitoring cycle
            results = self.update_volume_profiles()
            assert len(results) == len(self.symbols), "Monitoring update failed"
            print("✅ Full monitoring cycle: PASS")

            # Test analysis generation
            analysis = self.generate_market_analysis(results)
            assert 'summary' in analysis, "Analysis generation failed"
            print("✅ Market analysis generation: PASS")

            print("🎉 All validation tests PASSED!")
            return True

        except Exception as e:
            print(f"❌ Validation test FAILED: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    parser = argparse.ArgumentParser(description="Volume Profile Monitoring Dashboard")
    parser.add_argument('--cycles', type=int, default=3, help='Number of monitoring cycles')
    parser.add_argument('--interval', type=int, default=10, help='Interval between cycles (seconds)')
    parser.add_argument('--symbols', nargs='+', default=['AAPL', 'TSLA', 'MSFT', 'NVDA'],
                       help='Symbols to monitor')
    parser.add_argument('--validate', action='store_true', help='Run validation test only')

    args = parser.parse_args()

    # Initialize monitor
    monitor = VolumeProfileMonitor()
    monitor.symbols = args.symbols

    if args.validate:
        success = monitor.run_validation_test()
        sys.exit(0 if success else 1)
    else:
        monitor.run_monitoring_cycle(args.cycles, args.interval)

if __name__ == "__main__":
    main()