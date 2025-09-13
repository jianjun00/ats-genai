#!/usr/bin/env python3
"""
🧪 UNIT TEST: Direct Timeframe Granularity Issue Detection

This test directly validates the timeframe generation logic without requiring
full database connectivity. It focuses on detecting the core issue where
all timeframes generate records at hourly frequency instead of native frequency.
"""

import pytest
import sys
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

# Add src to path for imports
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

from services.core.app.runner import Runner
from shared.utils.environment import Environment, EnvironmentType


class MockTimeframeGranularityTest:
    """Direct test of timeframe granularity logic."""
    
    def test_runner_interval_generation(self):
        """Test that Runner generates intervals at correct base_duration frequency."""
        print("\n🔍 Testing Runner interval generation frequency...")
        
        # Test different base_duration values
        test_cases = [
            ('5m', 5, 12),   # 5 minutes -> 12 intervals per hour
            ('15m', 15, 4),  # 15 minutes -> 4 intervals per hour  
            ('60m', 60, 1),  # 60 minutes -> 1 interval per hour
            ('1h', 60, 1),   # 1 hour -> 1 interval per hour
        ]
        
        results = {}
        
        for base_duration, expected_interval_minutes, expected_per_hour in test_cases:
            print(f"\n📊 Testing base_duration: {base_duration}")
            
            try:
                # Create mock environment to avoid database issues
                mock_env = Mock(spec=Environment)
                mock_env.env_type = EnvironmentType.TEST
                
                with patch('services.core.app.runner.SecurityMaster'), \
                     patch('services.core.app.runner.UniverseStateManager'), \
                     patch('services.core.app.runner.UniverseManager'), \
                     patch('services.core.app.runner.DailyPriceMarketDataManager'):
                    
                    runner = Runner(
                        start_date='2025-07-01',
                        end_date='2025-07-01', 
                        environment=mock_env,
                        universe_id=1,
                        callbacks=[],
                        base_duration=base_duration,
                        enable_run_isolation=False  # Disable to avoid database calls
                    )
                    
                    # Count interval events for one trading day
                    interval_count = 0
                    event_times = []
                    
                    for event_time, event_type in runner.iter_events():
                        if event_type == "interval":
                            interval_count += 1
                            event_times.append(event_time)
                            print(f"   Interval {interval_count}: {event_time}")
                    
                    print(f"   Total intervals: {interval_count}")
                    print(f"   Expected per hour: {expected_per_hour}")
                    
                    # Calculate actual intervals per hour
                    if len(event_times) > 1:
                        time_diff = event_times[1] - event_times[0]
                        actual_interval_minutes = time_diff.total_seconds() / 60
                        print(f"   Actual interval: {actual_interval_minutes} minutes")
                        
                        results[base_duration] = {
                            'expected_interval_minutes': expected_interval_minutes,
                            'actual_interval_minutes': actual_interval_minutes,
                            'expected_per_hour': expected_per_hour,
                            'actual_count': interval_count,
                            'correct_frequency': abs(actual_interval_minutes - expected_interval_minutes) < 1
                        }
                    else:
                        results[base_duration] = {
                            'expected_interval_minutes': expected_interval_minutes,
                            'actual_interval_minutes': None,
                            'expected_per_hour': expected_per_hour, 
                            'actual_count': interval_count,
                            'correct_frequency': False
                        }
                        
            except Exception as e:
                print(f"   ❌ Error testing {base_duration}: {e}")
                results[base_duration] = {
                    'error': str(e),
                    'correct_frequency': False
                }
                
        return results
    
    def analyze_granularity_results(self, results):
        """Analyze results and detect granularity issues."""
        print("\n🎯 GRANULARITY ANALYSIS:")
        
        issues_detected = []
        
        for base_duration, result in results.items():
            if 'error' in result:
                print(f"   {base_duration}: ❌ ERROR - {result['error']}")
                continue
                
            expected_minutes = result.get('expected_interval_minutes', 0)
            actual_minutes = result.get('actual_interval_minutes', 0)
            correct = result.get('correct_frequency', False)
            
            if correct:
                print(f"   {base_duration}: ✅ CORRECT - {actual_minutes}min intervals")
            else:
                print(f"   {base_duration}: ❌ WRONG - Expected {expected_minutes}min, got {actual_minutes}min")
                
                # Detect specific patterns
                if actual_minutes == 60 and expected_minutes != 60:
                    issues_detected.append({
                        'timeframe': base_duration,
                        'issue': 'FORCED_HOURLY_FREQUENCY',
                        'description': f'{base_duration} generates hourly intervals instead of {expected_minutes}-minute intervals',
                        'expected_interval': expected_minutes,
                        'actual_interval': actual_minutes
                    })
                    
        return issues_detected


def test_timeframe_granularity_direct():
    """🧪 DIRECT TEST: Timeframe granularity validation without full infrastructure."""
    print("\n" + "="*80)
    print("🧪 DIRECT TIMEFRAME GRANULARITY TEST")
    print("="*80)
    
    test_suite = MockTimeframeGranularityTest()
    
    # Test Runner interval generation
    results = test_suite.test_runner_interval_generation()
    
    # Analyze for granularity issues
    issues = test_suite.analyze_granularity_results(results)
    
    # Report results
    print("\n" + "="*80)
    print("🎯 TEST RESULTS")
    print("="*80)
    
    total_tested = len(results)
    correct_count = sum(1 for r in results.values() if r.get('correct_frequency', False))
    
    print(f"📊 Base durations tested: {total_tested}")
    print(f"✅ Correct frequency: {correct_count}")
    print(f"❌ Incorrect frequency: {total_tested - correct_count}")
    print(f"🚨 Issues detected: {len(issues)}")
    
    if issues:
        print("\n🚨 CRITICAL GRANULARITY ISSUES:")
        for issue in issues:
            print(f"   • {issue['issue']}: {issue['description']}")
            print(f"     Expected: {issue['expected_interval']} min intervals")
            print(f"     Actual: {issue['actual_interval']} min intervals")
            
        print("\n💡 ACTION REQUIRED:")
        print("   The Runner.iter_events() method appears to force all timeframes")
        print("   to generate at the same frequency regardless of base_duration.")
        print("   This confirms the granularity issue described in the PRD/DRD.")
        
        # This test should PASS when it detects the issue (working as intended)
        return True
    else:
        print("\n🎉 No granularity issues detected")
        return False


if __name__ == "__main__":
    """Direct execution for testing."""
    success = test_timeframe_granularity_direct()
    
    if success:
        print("\n✅ TEST SUCCESS: Granularity issues detected as expected")
        print("   This confirms the core problem that needs to be fixed")
    else:
        print("\n❌ TEST FAILURE: No granularity issues detected") 
        print("   Either the issue is fixed or the test needs adjustment")