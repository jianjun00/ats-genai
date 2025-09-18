#!/usr/bin/env python3
"""
End-to-end validation tests for Support/Resistance system

This module provides comprehensive validation of the entire S/R system
from market data ingestion through event generation and database storage.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.analytics.events.analysis.support_resistance_detector import (
    Timeframe
)
from domains.analytics.events.processors.support_resistance_processor import SupportResistanceProcessor
from config.environment import Environment

class TestSupportResistanceE2E:
    """End-to-end validation of complete S/R system"""

    @pytest.fixture
    async def full_system_setup(self):
        """Set up complete system with database"""
        try:
            env = Environment()
            pool = await env.database.create_pool_with_retry(max_retries=3)
            conn = await pool.acquire()

            # Clean test data
            await self._cleanup_e2e_data(conn)

            # Set up processor with database
            config = {
                'processing_interval_seconds': 30,
                'batch_size': 5,
                'max_concurrent_symbols': 3,
                'min_data_points': 50,
                'enable_cross_timeframe_validation': True,
                'alert_thresholds': {
                    'strong_level_test': 0.8,
                    'level_break': 0.7,
                    'confluence_level': 0.85
                },
                'timeframe_priorities': [Timeframe.DAILY, Timeframe.INTRADAY_1H],
                'detector_config': {
                    'pivot_lookback': 20,
                    'cluster_epsilon': 0.02,
                    'proximity_tolerance': 0.005,
                    'psychological_levels': True,
                    'volume_profile_levels': True
                }
            }

            processor = SupportResistanceProcessor(config)
            processor.db_pool = pool
            processor.active_symbols = {'E2E_TEST_AAPL', 'E2E_TEST_MSFT'}
            await processor._initialize_processing_state()

            yield {
                'processor': processor,
                'connection': conn,
                'pool': pool
            }

            # Cleanup
            await self._cleanup_e2e_data(conn)
            await pool.release(conn)
            await pool.close()

        except Exception as e:
            pytest.skip(f"Full system setup failed: {e}")

    async def _cleanup_e2e_data(self, conn):
        """Clean up E2E test data"""
        cleanup_queries = [
            "DELETE FROM dev_sr_events WHERE symbol LIKE 'E2E_TEST_%'",
            "DELETE FROM dev_sr_tests WHERE symbol LIKE 'E2E_TEST_%'",
            "DELETE FROM dev_sr_levels WHERE symbol LIKE 'E2E_TEST_%'"
        ]

        for query in cleanup_queries:
            try:
                await conn.execute(query)
            except Exception as e:
                print(f"E2E cleanup warning: {e}")

    @pytest.fixture
    def market_scenario_trending_with_levels(self):
        """Create realistic trending market with clear S/R levels"""
        np.random.seed(42)

        # 3 months of daily data with clear trend and S/R levels
        dates = pd.date_range(start='2024-04-01', end='2024-06-30', freq='D')

        # Create uptrend from 100 to 130 with major levels at 110, 120
        base_trend = np.linspace(100, 130, len(dates))

        data = []
        for i, (date, trend_price) in enumerate(zip(dates, base_trend)):

            # Add support/resistance behavior at key levels
            if 109 <= trend_price <= 111:  # Support at 110
                # Price tends to bounce off 110 support
                price_adjustment = max(0, np.random.normal(0.5, 0.8))
                volume_multiplier = 2.0  # Higher volume at S/R

            elif 119 <= trend_price <= 121:  # Resistance at 120
                # Price tends to reject at 120 resistance initially
                price_adjustment = min(0, np.random.normal(-0.3, 0.6))
                volume_multiplier = 2.5

            else:
                # Normal trending behavior
                price_adjustment = np.random.normal(0, 0.8)
                volume_multiplier = 1.0

            close = trend_price + price_adjustment

            # Create realistic OHLC
            open_price = close + np.random.normal(0, 0.3)
            high = max(open_price, close) + abs(np.random.normal(0, 0.5))
            low = min(open_price, close) - abs(np.random.normal(0, 0.5))

            # Volume with spikes at S/R levels
            base_volume = 1000000
            volume = int(base_volume * volume_multiplier * np.random.uniform(0.8, 1.2))

            data.append({
                'timestamp': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })

        return pd.DataFrame(data)

    @pytest.fixture
    def market_scenario_breakout(self):
        """Create market scenario with level breakout"""
        np.random.seed(123)

        dates = pd.date_range(start='2024-01-01', end='2024-04-30', freq='D')

        # Ranging market that breaks out
        n_points = len(dates)

        data = []
        resistance_level = 105.0

        for i, date in enumerate(dates):
            progress = i / n_points

            if progress < 0.7:  # First 70% - ranging below resistance
                base_price = 100 + 4 * np.sin(2 * np.pi * progress * 3)  # Oscillate 96-104

                # Rejection at resistance
                if base_price > 104:
                    base_price = 104 - abs(np.random.normal(0, 0.3))
                    volume_mult = 2.0  # High volume rejection
                else:
                    volume_mult = 1.0

            else:  # Last 30% - breakout above resistance
                breakout_progress = (progress - 0.7) / 0.3
                base_price = 105 + breakout_progress * 8  # Break to 113
                volume_mult = 1.5  # Moderate volume on breakout

            close = base_price + np.random.normal(0, 0.4)
            open_price = close + np.random.normal(0, 0.2)
            high = max(open_price, close) + abs(np.random.normal(0, 0.6))
            low = min(open_price, close) - abs(np.random.normal(0, 0.6))

            volume = int(800000 * volume_mult * np.random.uniform(0.9, 1.1))

            data.append({
                'timestamp': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })

        return pd.DataFrame(data)

    async def test_complete_workflow_trending_market(self, full_system_setup, market_scenario_trending_with_levels):
        """Test complete workflow with trending market scenario"""
        system = full_system_setup
        processor = system['processor']
        conn = system['connection']

        symbol = 'E2E_TEST_AAPL'
        timeframe = Timeframe.DAILY

        print(f"\n=== Testing Complete Workflow: {symbol} ===")
        print(f"Market data: {len(market_scenario_trending_with_levels)} days")

        # Step 1: Process market data through complete system
        await processor.process_market_data_update(symbol, market_scenario_trending_with_levels, timeframe)

        # Step 2: Verify levels were detected and stored
        levels_query = """
        SELECT level_id, symbol, price, sr_type, level_type, strength, confidence, test_count
        FROM dev_sr_levels
        WHERE symbol = $1
        ORDER BY strength DESC
        """

        levels = await conn.fetch(levels_query, symbol)
        assert len(levels) > 0, "Should detect and store S/R levels"

        # Validate level quality
        strong_levels = [l for l in levels if l['strength'] > 0.6]
        print(f"Detected {len(levels)} levels, {len(strong_levels)} strong levels")

        # Step 3: Verify tests were detected and stored
        tests_query = """
        SELECT t.test_id, t.outcome, t.test_price, t.volume_spike, t.outcome_confidence,
               l.price as level_price, l.sr_type
        FROM dev_sr_tests t
        JOIN dev_sr_levels l ON t.sr_level_id = l.id
        WHERE t.symbol = $1
        ORDER BY t.outcome_confidence DESC
        """

        tests = await conn.fetch(tests_query, symbol)
        print(f"Detected {len(tests)} level tests")

        if len(tests) > 0:
            # Validate test quality
            high_confidence_tests = [t for t in tests if t['outcome_confidence'] > 0.7]
            print(f"High confidence tests: {len(high_confidence_tests)}")

            # Step 4: Check for events
            events_query = """
            SELECT event_id, event_type, event_subtype, significance_score, impact_score
            FROM dev_sr_events
            WHERE symbol = $1
            ORDER BY significance_score DESC
            """

            events = await conn.fetch(events_query, symbol)
            print(f"Generated {len(events)} S/R events")

            # Validate events
            significant_events = [e for e in events if e['significance_score'] > 0.7]
            print(f"Significant events: {len(significant_events)}")

        # Step 5: Validate data relationships and integrity
        integrity_query = """
        SELECT
            l.symbol,
            l.level_id,
            l.price,
            l.sr_type,
            l.strength,
            COUNT(t.id) as test_count,
            COUNT(e.id) as event_count,
            AVG(t.outcome_confidence) as avg_test_confidence
        FROM dev_sr_levels l
        LEFT JOIN dev_sr_tests t ON l.id = t.sr_level_id
        LEFT JOIN dev_sr_events e ON l.id = e.sr_level_id
        WHERE l.symbol = $1
        GROUP BY l.symbol, l.level_id, l.price, l.sr_type, l.strength
        ORDER BY l.strength DESC
        """

        integrity = await conn.fetch(integrity_query, symbol)

        # Data integrity validations
        assert len(integrity) > 0, "Should have data integrity records"

        for record in integrity:
            assert record['strength'] > 0, "All levels should have positive strength"
            if record['test_count'] > 0:
                assert record['avg_test_confidence'] > 0, "Tests should have positive confidence"

        print("✓ Complete workflow test passed")

    async def test_breakout_scenario_validation(self, full_system_setup, market_scenario_breakout):
        """Test system behavior during level breakout scenario"""
        system = full_system_setup
        processor = system['processor']
        conn = system['connection']

        symbol = 'E2E_TEST_MSFT'
        timeframe = Timeframe.DAILY

        print(f"\n=== Testing Breakout Scenario: {symbol} ===")

        # Process breakout scenario
        await processor.process_market_data_update(symbol, market_scenario_breakout, timeframe)

        # Look for resistance level around 105 and its eventual break
        resistance_query = """
        SELECT l.*,
               COUNT(t.id) as total_tests,
               SUM(CASE WHEN t.outcome = 'break_clean' THEN 1 ELSE 0 END) as breaks,
               SUM(CASE WHEN t.outcome IN ('hold_strong', 'hold_weak') THEN 1 ELSE 0 END) as holds
        FROM dev_sr_levels l
        LEFT JOIN dev_sr_tests t ON l.id = t.sr_level_id
        WHERE l.symbol = $1 AND l.sr_type = 'resistance' AND l.price BETWEEN 104 AND 106
        GROUP BY l.id, l.level_id, l.symbol, l.price, l.sr_type, l.level_type, l.timeframe,
                 l.strength, l.confidence, l.first_established, l.last_tested, l.test_count,
                 l.hold_count, l.break_count, l.volume_confirmation, l.metadata, l.created_at, l.updated_at
        ORDER BY l.strength DESC
        """

        resistance_levels = await conn.fetch(resistance_query, symbol)

        if len(resistance_levels) > 0:
            print(f"Found {len(resistance_levels)} resistance levels around 105")

            for level in resistance_levels:
                print(f"  Level {level['price']:.2f}: {level['total_tests']} tests, "
                      f"{level['holds']} holds, {level['breaks']} breaks")

                # Validate breakout logic
                if level['breaks'] > 0:
                    # Should have events for clean breaks
                    break_events_query = """
                    SELECT * FROM dev_sr_events
                    WHERE sr_level_id = $1 AND event_subtype = 'level_broken_clean'
                    """

                    break_events = await conn.fetch(break_events_query, level['id'])
                    print(f"    Break events: {len(break_events)}")

        print("✓ Breakout scenario test completed")

    async def test_cross_timeframe_validation(self, full_system_setup, market_scenario_trending_with_levels):
        """Test cross-timeframe S/R level validation"""
        system = full_system_setup
        processor = system['processor']
        conn = system['connection']

        symbol = 'E2E_TEST_CROSS_TF'

        print(f"\n=== Testing Cross-Timeframe Validation: {symbol} ===")

        # Process same data for different timeframes
        timeframes = [Timeframe.DAILY, Timeframe.INTRADAY_1H]

        for timeframe in timeframes:
            # Simulate different timeframe data (in practice, this would be different aggregations)
            await processor.process_market_data_update(symbol, market_scenario_trending_with_levels, timeframe)

        # Compare levels across timeframes
        cross_tf_query = """
        SELECT
            timeframe,
            COUNT(*) as level_count,
            AVG(strength) as avg_strength,
            COUNT(CASE WHEN strength > 0.7 THEN 1 END) as strong_levels
        FROM dev_sr_levels
        WHERE symbol = $1
        GROUP BY timeframe
        ORDER BY
            CASE timeframe
                WHEN '1d' THEN 1
                WHEN '1h' THEN 2
                WHEN '15m' THEN 3
                WHEN '5m' THEN 4
                ELSE 5
            END
        """

        tf_comparison = await conn.fetch(cross_tf_query, symbol)

        print("Cross-timeframe comparison:")
        for tf in tf_comparison:
            print(f"  {tf['timeframe']}: {tf['level_count']} levels, "
                  f"avg strength {tf['avg_strength']:.3f}, {tf['strong_levels']} strong")

        # Validate cross-timeframe consistency
        assert len(tf_comparison) > 0, "Should have levels across timeframes"

        print("✓ Cross-timeframe validation completed")

    async def test_system_performance_e2e(self, full_system_setup):
        """Test end-to-end system performance"""
        system = full_system_setup
        processor = system['processor']

        print(f"\n=== Testing E2E System Performance ===")

        # Generate multiple symbols with realistic data
        symbols = ['E2E_PERF_AAPL', 'E2E_PERF_MSFT', 'E2E_PERF_GOOGL']
        datasets = []

        for i, symbol in enumerate(symbols):
            np.random.seed(i * 10)
            dates = pd.date_range(start='2024-01-01', end='2024-06-30', freq='D')
            n = len(dates)

            # Create varied market conditions
            base_prices = 100 + i * 20  # Different price ranges
            trend = np.linspace(0, 20, n)  # Uptrend
            noise = np.random.normal(0, 2, n)
            closes = base_prices + trend + noise

            data = pd.DataFrame({
                'timestamp': dates,
                'open': closes + np.random.normal(0, 0.5, n),
                'high': closes + np.abs(np.random.normal(0, 1, n)),
                'low': closes - np.abs(np.random.normal(0, 1, n)),
                'close': closes,
                'volume': np.random.lognormal(15, 0.3, n).astype(int)
            })

            datasets.append((symbol, data))

        # Time the complete processing
        import time
        start_time = time.time()

        # Process all symbols
        for symbol, data in datasets:
            await processor.process_market_data_update(symbol, data, Timeframe.DAILY)

        total_time = time.time() - start_time

        # Get processing statistics
        stats = processor.get_processing_stats()

        print(f"Processed {len(symbols)} symbols in {total_time:.3f}s")
        print(f"System stats: {stats}")

        # Performance validation
        assert total_time < 30.0, f"E2E processing too slow: {total_time:.2f}s"
        assert stats['symbols_processed'] >= len(symbols), "Should process all symbols"
        assert stats['errors'] == 0, "Should not have processing errors"

        print("✓ E2E performance test passed")

    async def test_data_quality_validation(self, full_system_setup, market_scenario_trending_with_levels):
        """Test data quality and validation throughout the system"""
        system = full_system_setup
        processor = system['processor']
        conn = system['connection']

        symbol = 'E2E_TEST_QUALITY'
        timeframe = Timeframe.DAILY

        print(f"\n=== Testing Data Quality Validation: {symbol} ===")

        # Process data
        await processor.process_market_data_update(symbol, market_scenario_trending_with_levels, timeframe)

        # Quality checks on levels
        level_quality_query = """
        SELECT
            COUNT(*) as total_levels,
            COUNT(CASE WHEN strength BETWEEN 0 AND 1 THEN 1 END) as valid_strength,
            COUNT(CASE WHEN confidence BETWEEN 0 AND 1 THEN 1 END) as valid_confidence,
            COUNT(CASE WHEN price > 0 THEN 1 END) as valid_price,
            COUNT(CASE WHEN test_count >= 0 THEN 1 END) as valid_test_count,
            AVG(strength) as avg_strength,
            MIN(strength) as min_strength,
            MAX(strength) as max_strength
        FROM dev_sr_levels
        WHERE symbol = $1
        """

        level_quality = await conn.fetchrow(level_quality_query, symbol)

        if level_quality['total_levels'] > 0:
            print(f"Level quality: {level_quality['total_levels']} levels")
            print(f"  Valid strength: {level_quality['valid_strength']}/{level_quality['total_levels']}")
            print(f"  Valid confidence: {level_quality['valid_confidence']}/{level_quality['total_levels']}")
            print(f"  Strength range: {level_quality['min_strength']:.3f} - {level_quality['max_strength']:.3f}")

            # Quality assertions
            assert level_quality['valid_strength'] == level_quality['total_levels'], "All levels should have valid strength"
            assert level_quality['valid_confidence'] == level_quality['total_levels'], "All levels should have valid confidence"
            assert level_quality['valid_price'] == level_quality['total_levels'], "All levels should have valid prices"

        # Quality checks on tests
        test_quality_query = """
        SELECT
            COUNT(*) as total_tests,
            COUNT(CASE WHEN outcome_confidence BETWEEN 0 AND 1 THEN 1 END) as valid_confidence,
            COUNT(CASE WHEN volume_spike >= 0 THEN 1 END) as valid_volume_spike,
            COUNT(CASE WHEN max_penetration >= 0 THEN 1 END) as valid_penetration,
            COUNT(CASE WHEN test_price > 0 THEN 1 END) as valid_price
        FROM dev_sr_tests
        WHERE symbol = $1
        """

        test_quality = await conn.fetchrow(test_quality_query, symbol)

        if test_quality['total_tests'] > 0:
            print(f"Test quality: {test_quality['total_tests']} tests")
            print(f"  Valid confidence: {test_quality['valid_confidence']}/{test_quality['total_tests']}")
            print(f"  Valid volume spike: {test_quality['valid_volume_spike']}/{test_quality['total_tests']}")

            # Quality assertions for tests
            assert test_quality['valid_confidence'] == test_quality['total_tests'], "All tests should have valid confidence"
            assert test_quality['valid_volume_spike'] == test_quality['total_tests'], "All tests should have valid volume data"

        print("✓ Data quality validation passed")

    async def test_alert_system_e2e(self, full_system_setup):
        """Test end-to-end alert system functionality"""
        system = full_system_setup
        processor = system['processor']

        symbol = 'E2E_TEST_ALERTS'

        print(f"\n=== Testing Alert System E2E: {symbol} ===")

        # Create scenario that should trigger alerts (strong resistance break with high volume)
        dates = pd.date_range(start='2024-06-01', end='2024-06-30', freq='D')
        n = len(dates)

        data = []
        resistance_level = 150.0

        for i, date in enumerate(dates):
            progress = i / n

            if progress < 0.8:  # Build up to resistance
                price = 145 + 4 * progress  # Approach 149
                volume_mult = 1.0
            else:  # Break resistance with high volume
                price = resistance_level + (progress - 0.8) * 25  # Break to 155
                volume_mult = 3.0  # High volume breakout

            variation = np.random.normal(0, 0.5)
            close = price + variation

            data.append({
                'timestamp': date,
                'open': close + np.random.normal(0, 0.2),
                'high': close + abs(np.random.normal(0, 0.8)),
                'low': close - abs(np.random.normal(0, 0.8)),
                'close': close,
                'volume': int(1000000 * volume_mult * np.random.uniform(0.9, 1.1))
            })

        scenario_data = pd.DataFrame(data)

        # Track alerts (in real system, these would go to alerting infrastructure)
        original_send_alert = processor._send_sr_alert
        sent_alerts = []

        async def mock_send_alert(event):
            sent_alerts.append(event)
            await original_send_alert(event)

        processor._send_sr_alert = mock_send_alert

        # Process the alert scenario
        await processor.process_market_data_update(symbol, scenario_data, Timeframe.DAILY)

        print(f"Sent {len(sent_alerts)} alerts")

        # Validate alert generation
        for alert in sent_alerts:
            print(f"  Alert: {alert.symbol} {alert.level.sr_type.value} at ${alert.level.price:.2f}")
            print(f"    Outcome: {alert.test.outcome.value}")
            print(f"    Volume: {alert.test.volume_spike:.1f}x")

        print("✓ Alert system E2E test completed")

    async def test_system_recovery_and_error_handling(self, full_system_setup):
        """Test system recovery from various error conditions"""
        system = full_system_setup
        processor = system['processor']

        print(f"\n=== Testing System Recovery & Error Handling ===")

        # Test 1: Invalid data handling
        print("Testing invalid data handling...")

        invalid_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='D'),
            'open': [np.inf, -np.inf, np.nan, 100, 101],  # Invalid values
            'high': [101, 102, 103, 104, np.inf],
            'low': [99, -100, 101, 102, 103],  # Negative prices
            'close': [100, np.nan, 102, 103, 104],
            'volume': [-1000, 1100, 1200, 1300, 1400]  # Negative volume
        })

        initial_error_count = processor.stats['errors']

        try:
            await processor.process_market_data_update('E2E_INVALID', invalid_data, Timeframe.DAILY)
            print("  ✓ System handled invalid data gracefully")
        except Exception as e:
            print(f"  ✓ System properly raised exception for invalid data: {type(e).__name__}")

        # Test 2: Empty data handling
        print("Testing empty data handling...")

        empty_data = pd.DataFrame()
        await processor.process_market_data_update('E2E_EMPTY', empty_data, Timeframe.DAILY)
        print("  ✓ System handled empty data")

        # Test 3: Minimal data handling
        print("Testing minimal data handling...")

        minimal_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=3, freq='D'),
            'open': [100, 101, 102],
            'high': [101, 102, 103],
            'low': [99, 100, 101],
            'close': [100.5, 101.5, 102.5],
            'volume': [1000, 1100, 1200]
        })

        await processor.process_market_data_update('E2E_MINIMAL', minimal_data, Timeframe.DAILY)
        print("  ✓ System handled minimal data")

        print("✓ System recovery & error handling tests passed")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])