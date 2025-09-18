#!/usr/bin/env python3
"""
Data Quality Validation Tests for Real-time Collection System

Comprehensive tests for data quality validation including:
- OHLCV price relationship validation
- Volume and trade data validation
- Cross-vendor consistency checks
- Time series continuity validation
- Statistical outlier detection
- Data completeness and accuracy metrics
"""

import pytest
import asyncio
import asyncpg
import logging
import statistics
from typing import Dict, Any
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from domains.market_data.services.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector

logger = logging.getLogger(__name__)

@pytest.fixture
async def quality_db_pool():
    """Database pool for quality testing"""
    dsn = "postgresql://postgres:intg_password@localhost:4432/intg_db"
    try:
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=5)
        yield pool
        await pool.close()
    except Exception as e:
        logger.warning(f"Cannot connect to quality database: {e}")
        pytest.skip("Quality database not available")


class DataQualityValidator:
    """Comprehensive data quality validation"""

    def __init__(self, db_pool):
        self.db_pool = db_pool

    async def validate_ohlc_relationships(self, table_name: str, time_window: int = 60) -> Dict[str, Any]:
        """Validate OHLC price relationships"""
        async with self.db_pool.acquire() as conn:
            violations = await conn.fetch(f"""
                SELECT
                    symbol,
                    timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    CASE
                        WHEN high_price < low_price THEN 'high_less_than_low'
                        WHEN high_price < open_price THEN 'high_less_than_open'
                        WHEN high_price < close_price THEN 'high_less_than_close'
                        WHEN low_price > open_price THEN 'low_greater_than_open'
                        WHEN low_price > close_price THEN 'low_greater_than_close'
                        WHEN open_price <= 0 THEN 'invalid_open'
                        WHEN close_price <= 0 THEN 'invalid_close'
                        WHEN high_price <= 0 THEN 'invalid_high'
                        WHEN low_price <= 0 THEN 'invalid_low'
                        ELSE NULL
                    END as violation_type
                FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '{time_window} minutes'
                    AND (
                        high_price < low_price OR
                        high_price < open_price OR
                        high_price < close_price OR
                        low_price > open_price OR
                        low_price > close_price OR
                        open_price <= 0 OR
                        close_price <= 0 OR
                        high_price <= 0 OR
                        low_price <= 0
                    )
            """)

            # Count violations by type
            violation_counts = {}
            for row in violations:
                vtype = row['violation_type']
                violation_counts[vtype] = violation_counts.get(vtype, 0) + 1

            # Get total records for comparison
            total_records = await conn.fetchval(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '{time_window} minutes'
            """)

            return {
                'total_records': total_records,
                'total_violations': len(violations),
                'violation_rate': len(violations) / total_records if total_records > 0 else 0,
                'violation_counts': violation_counts,
                'violations': [dict(row) for row in violations[:10]]  # First 10 for debugging
            }

    async def validate_price_continuity(self, table_name: str, symbol: str = None) -> Dict[str, Any]:
        """Validate price continuity and detect gaps"""
        symbol_filter = f"AND symbol = '{symbol}'" if symbol else ""

        async with self.db_pool.acquire() as conn:
            # Find time gaps in the data
            gaps = await conn.fetch(f"""
                WITH ordered_data AS (
                    SELECT
                        symbol,
                        timestamp,
                        close_price,
                        LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_timestamp,
                        LAG(close_price) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_close
                    FROM {table_name}
                    WHERE timestamp >= NOW() - INTERVAL '2 hours'
                    {symbol_filter}
                    ORDER BY symbol, timestamp
                ),
                gaps AS (
                    SELECT
                        symbol,
                        prev_timestamp,
                        timestamp,
                        EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as gap_seconds,
                        prev_close,
                        close_price,
                        ABS(close_price - prev_close) / prev_close as price_change_pct
                    FROM ordered_data
                    WHERE prev_timestamp IS NOT NULL
                        AND (
                            EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) > 120 OR  -- > 2 minute gap
                            ABS(close_price - prev_close) / prev_close > 0.1  -- > 10% price change
                        )
                )
                SELECT * FROM gaps ORDER BY gap_seconds DESC LIMIT 20
            """)

            # Calculate continuity metrics
            total_points = await conn.fetchval(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '2 hours' {symbol_filter}
            """)

            return {
                'total_data_points': total_points,
                'time_gaps_found': len(gaps),
                'gaps': [dict(row) for row in gaps],
                'continuity_score': max(0, 1 - (len(gaps) / total_points)) if total_points > 0 else 0
            }

    async def validate_volume_consistency(self, table_name: str) -> Dict[str, Any]:
        """Validate volume data consistency"""
        async with self.db_pool.acquire() as conn:
            volume_stats = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN volume = 0 THEN 1 END) as zero_volume_count,
                    COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume_count,
                    AVG(volume) as avg_volume,
                    STDDEV(volume) as volume_stddev,
                    MIN(volume) as min_volume,
                    MAX(volume) as max_volume,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume) as median_volume,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY volume) as p95_volume
                FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    AND volume IS NOT NULL
            """)

            # Detect volume outliers
            outliers = await conn.fetch(f"""
                WITH volume_stats AS (
                    SELECT
                        AVG(volume) as mean_vol,
                        STDDEV(volume) as std_vol
                    FROM {table_name}
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                        AND volume > 0
                ),
                outlier_detection AS (
                    SELECT
                        symbol,
                        timestamp,
                        volume,
                        (volume - vs.mean_vol) / vs.std_vol as z_score
                    FROM {table_name} t
                    CROSS JOIN volume_stats vs
                    WHERE t.timestamp >= NOW() - INTERVAL '1 hour'
                        AND t.volume > 0
                        AND ABS((t.volume - vs.mean_vol) / vs.std_vol) > 3  -- 3 sigma outliers
                )
                SELECT * FROM outlier_detection ORDER BY ABS(z_score) DESC LIMIT 10
            """)

            return {
                'volume_stats': dict(volume_stats),
                'volume_outliers': [dict(row) for row in outliers],
                'zero_volume_rate': volume_stats['zero_volume_count'] / volume_stats['total_records'] if volume_stats['total_records'] > 0 else 0,
                'negative_volume_rate': volume_stats['negative_volume_count'] / volume_stats['total_records'] if volume_stats['total_records'] > 0 else 0
            }

    async def validate_cross_vendor_consistency(self, time_window: int = 30) -> Dict[str, Any]:
        """Validate consistency between vendors for same symbols and timestamps"""
        async with self.db_pool.acquire() as conn:
            # Find matching records between vendors
            matches = await conn.fetch(f"""
                SELECT
                    t.symbol,
                    t.timestamp,
                    t.close_price as tiingo_close,
                    p.close_price as polygon_close,
                    t.volume as tiingo_volume,
                    p.volume as polygon_volume,
                    ABS(t.close_price - p.close_price) / t.close_price as price_diff_pct,
                    ABS(CAST(t.volume as NUMERIC) - p.volume) / CAST(t.volume as NUMERIC) as volume_diff_pct
                FROM intg_one_minute_live_tiingo t
                INNER JOIN intg_one_minute_live_polygon p
                    ON t.symbol = p.symbol AND t.timestamp = p.timestamp
                WHERE t.timestamp >= NOW() - INTERVAL '{time_window} minutes'
                    AND t.close_price > 0 AND p.close_price > 0
                    AND t.volume > 0
                ORDER BY t.symbol, t.timestamp DESC
            """)

            if matches:
                price_diffs = [float(row['price_diff_pct']) for row in matches if row['price_diff_pct'] is not None]
                volume_diffs = [float(row['volume_diff_pct']) for row in matches if row['volume_diff_pct'] is not None and row['volume_diff_pct'] < 10]  # Cap at 1000%

                # Identify significant discrepancies
                large_discrepancies = [row for row in matches if row['price_diff_pct'] and float(row['price_diff_pct']) > 0.05]  # > 5% difference

                consistency_metrics = {
                    'matched_records': len(matches),
                    'avg_price_diff_pct': statistics.mean(price_diffs) if price_diffs else 0,
                    'max_price_diff_pct': max(price_diffs) if price_diffs else 0,
                    'avg_volume_diff_pct': statistics.mean(volume_diffs) if volume_diffs else 0,
                    'large_discrepancies': len(large_discrepancies),
                    'price_consistency_score': 1 - (statistics.mean(price_diffs) if price_diffs else 0),
                    'discrepancy_examples': [dict(row) for row in large_discrepancies[:5]]
                }
            else:
                consistency_metrics = {
                    'matched_records': 0,
                    'avg_price_diff_pct': None,
                    'max_price_diff_pct': None,
                    'avg_volume_diff_pct': None,
                    'large_discrepancies': 0,
                    'price_consistency_score': 0,
                    'discrepancy_examples': []
                }

            return consistency_metrics

    async def validate_quality_scores(self, table_name: str) -> Dict[str, Any]:
        """Validate quality score distribution and accuracy"""
        async with self.db_pool.acquire() as conn:
            quality_stats = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(quality_score) as non_null_scores,
                    AVG(quality_score) as avg_quality,
                    MIN(quality_score) as min_quality,
                    MAX(quality_score) as max_quality,
                    STDDEV(quality_score) as quality_stddev,
                    COUNT(CASE WHEN quality_score < 0 THEN 1 END) as invalid_low_scores,
                    COUNT(CASE WHEN quality_score > 1 THEN 1 END) as invalid_high_scores,
                    COUNT(CASE WHEN quality_score < 0.5 THEN 1 END) as low_quality_count
                FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            """)

            # Distribution analysis
            distribution = await conn.fetch(f"""
                SELECT
                    CASE
                        WHEN quality_score >= 0.9 THEN 'Excellent (0.9-1.0)'
                        WHEN quality_score >= 0.8 THEN 'Good (0.8-0.9)'
                        WHEN quality_score >= 0.7 THEN 'Fair (0.7-0.8)'
                        WHEN quality_score >= 0.5 THEN 'Poor (0.5-0.7)'
                        ELSE 'Very Poor (<0.5)'
                    END as quality_bracket,
                    COUNT(*) as count
                FROM {table_name}
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    AND quality_score IS NOT NULL
                GROUP BY quality_bracket
                ORDER BY MIN(quality_score) DESC
            """)

            return {
                'quality_stats': dict(quality_stats),
                'quality_distribution': [dict(row) for row in distribution],
                'score_coverage': quality_stats['non_null_scores'] / quality_stats['total_records'] if quality_stats['total_records'] > 0 else 0,
                'invalid_score_rate': (quality_stats['invalid_low_scores'] + quality_stats['invalid_high_scores']) / quality_stats['total_records'] if quality_stats['total_records'] > 0 else 0
            }


class TestOHLCValidation:
    """Test OHLC data validation"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_ohlc_relationships(self, quality_db_pool):
        """Test OHLC relationships in Tiingo data"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate some data first
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(10):
            await collector.generate_and_store_data()

        # Validate OHLC relationships
        validation_result = await validator.validate_ohlc_relationships('intg_one_minute_live_tiingo')

        logger.info(f"Tiingo OHLC validation: {validation_result['total_records']} records, {validation_result['total_violations']} violations")

        # Assertions
        assert validation_result['total_records'] > 0
        assert validation_result['violation_rate'] < 0.01  # Less than 1% violations
        assert 'invalid_open' not in validation_result['violation_counts']
        assert 'invalid_close' not in validation_result['violation_counts']

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_polygon_ohlc_relationships(self, quality_db_pool):
        """Test OHLC relationships in Polygon data"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate data
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(10):
            await collector.generate_and_store_data()

        # Validate OHLC relationships
        validation_result = await validator.validate_ohlc_relationships('intg_one_minute_live_polygon')

        logger.info(f"Polygon OHLC validation: {validation_result['total_records']} records, {validation_result['total_violations']} violations")

        # Assertions
        assert validation_result['total_records'] > 0
        assert validation_result['violation_rate'] < 0.01  # Less than 1% violations

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_range_validation(self, quality_db_pool):
        """Test that prices are within reasonable ranges"""
        async with quality_db_pool.acquire() as conn:
            # Check price ranges for known symbols
            price_ranges = await conn.fetch("""
                SELECT
                    symbol,
                    MIN(close_price) as min_price,
                    MAX(close_price) as max_price,
                    AVG(close_price) as avg_price,
                    COUNT(*) as record_count
                FROM (
                    SELECT symbol, close_price FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    UNION ALL
                    SELECT symbol, close_price FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                ) combined
                GROUP BY symbol
            """)

            for row in price_ranges:
                symbol = row['symbol']
                min_price = float(row['min_price'])
                max_price = float(row['max_price'])
                avg_price = float(row['avg_price'])

                logger.info(f"{symbol}: ${min_price:.2f} - ${max_price:.2f} (avg: ${avg_price:.2f})")

                # Validate reasonable price ranges
                if symbol == 'AAPL':
                    assert 200 <= avg_price <= 250, f"AAPL average price {avg_price} outside expected range"
                    assert max_price - min_price < avg_price * 0.1, f"AAPL price range too large"
                elif symbol == 'TSLA':
                    assert 300 <= avg_price <= 360, f"TSLA average price {avg_price} outside expected range"
                    assert max_price - min_price < avg_price * 0.1, f"TSLA price range too large"


class TestVolumeValidation:
    """Test volume data validation"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_volume_consistency(self, quality_db_pool):
        """Test volume data consistency"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate data
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(15):
            await collector.generate_and_store_data()

        # Test Tiingo volume consistency
        tiingo_validation = await validator.validate_volume_consistency('intg_one_minute_live_tiingo')

        logger.info(f"Tiingo volume validation: {tiingo_validation['volume_stats']['total_records']} records")
        logger.info(f"Zero volume rate: {tiingo_validation['zero_volume_rate']:.2%}")
        logger.info(f"Negative volume rate: {tiingo_validation['negative_volume_rate']:.2%}")

        # Assertions
        assert tiingo_validation['zero_volume_rate'] < 0.1  # Less than 10% zero volume
        assert tiingo_validation['negative_volume_rate'] == 0  # No negative volume
        assert tiingo_validation['volume_stats']['avg_volume'] > 0

        # Test Polygon volume consistency
        polygon_validation = await validator.validate_volume_consistency('intg_one_minute_live_polygon')

        assert polygon_validation['negative_volume_rate'] == 0
        assert polygon_validation['volume_stats']['avg_volume'] > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_volume_distribution(self, quality_db_pool):
        """Test volume distribution patterns"""
        async with quality_db_pool.acquire() as conn:
            volume_distribution = await conn.fetch("""
                SELECT
                    symbol,
                    vendor,
                    COUNT(*) as record_count,
                    AVG(volume) as avg_volume,
                    STDDEV(volume) as volume_stddev,
                    MIN(volume) as min_volume,
                    MAX(volume) as max_volume
                FROM (
                    SELECT symbol, 'tiingo' as vendor, volume
                    FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                    UNION ALL
                    SELECT symbol, 'polygon' as vendor, volume
                    FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                ) combined
                GROUP BY symbol, vendor
                ORDER BY symbol, vendor
            """)

            for row in volume_distribution:
                symbol = row['symbol']
                vendor = row['vendor']
                avg_volume = float(row['avg_volume']) if row['avg_volume'] else 0
                volume_stddev = float(row['volume_stddev']) if row['volume_stddev'] else 0

                logger.info(f"{symbol} {vendor}: avg_vol={avg_volume:.0f}, stddev={volume_stddev:.0f}")

                # Volume should be reasonable
                assert avg_volume > 1000, f"Average volume too low for {symbol} {vendor}"
                assert volume_stddev > 0, f"Volume should have variability for {symbol} {vendor}"

                # Coefficient of variation should be reasonable
                cv = volume_stddev / avg_volume if avg_volume > 0 else 0
                assert cv < 2.0, f"Volume variability too high for {symbol} {vendor}"


class TestCrossVendorConsistency:
    """Test consistency between vendors"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_vendor_price_consistency(self, quality_db_pool):
        """Test price consistency between Tiingo and Polygon"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate synchronized data
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(10):
            await collector.generate_and_store_data()
            await asyncio.sleep(0.1)  # Small delay

        # Validate cross-vendor consistency
        consistency_result = await validator.validate_cross_vendor_consistency()

        logger.info(f"Cross-vendor consistency: {consistency_result['matched_records']} matched records")
        if consistency_result['avg_price_diff_pct'] is not None:
            logger.info(f"Average price difference: {consistency_result['avg_price_diff_pct']:.3%}")
            logger.info(f"Price consistency score: {consistency_result['price_consistency_score']:.3f}")

        # Assertions
        assert consistency_result['matched_records'] > 0
        if consistency_result['avg_price_diff_pct'] is not None:
            assert consistency_result['avg_price_diff_pct'] < 0.05  # Less than 5% average difference
            assert consistency_result['large_discrepancies'] < consistency_result['matched_records'] * 0.1  # Less than 10% large discrepancies

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_vendor_coverage_consistency(self, quality_db_pool):
        """Test that both vendors cover the same symbols and timeframes"""
        async with quality_db_pool.acquire() as conn:
            coverage_comparison = await conn.fetchrow("""
                WITH tiingo_coverage AS (
                    SELECT
                        COUNT(DISTINCT symbol) as symbols,
                        COUNT(DISTINCT DATE_TRUNC('minute', timestamp)) as time_points,
                        COUNT(*) as total_records
                    FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                ),
                polygon_coverage AS (
                    SELECT
                        COUNT(DISTINCT symbol) as symbols,
                        COUNT(DISTINCT DATE_TRUNC('minute', timestamp)) as time_points,
                        COUNT(*) as total_records
                    FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                )
                SELECT
                    t.symbols as tiingo_symbols,
                    p.symbols as polygon_symbols,
                    t.time_points as tiingo_timepoints,
                    p.time_points as polygon_timepoints,
                    t.total_records as tiingo_records,
                    p.total_records as polygon_records
                FROM tiingo_coverage t
                CROSS JOIN polygon_coverage p
            """)

            logger.info(f"Coverage comparison: Tiingo {coverage_comparison['tiingo_symbols']} symbols, Polygon {coverage_comparison['polygon_symbols']} symbols")

            # Both vendors should cover same symbols
            assert coverage_comparison['tiingo_symbols'] == coverage_comparison['polygon_symbols']

            # Time point coverage should be similar
            time_diff_ratio = abs(coverage_comparison['tiingo_timepoints'] - coverage_comparison['polygon_timepoints']) / max(coverage_comparison['tiingo_timepoints'], 1)
            assert time_diff_ratio < 0.2  # Less than 20% difference in time point coverage


class TestQualityScoreValidation:
    """Test quality score validation"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_quality_score_ranges(self, quality_db_pool):
        """Test quality score ranges and distributions"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate data
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(20):
            await collector.generate_and_store_data()

        # Test Tiingo quality scores
        tiingo_quality = await validator.validate_quality_scores('intg_one_minute_live_tiingo')

        logger.info(f"Tiingo quality: avg={tiingo_quality['quality_stats']['avg_quality']:.3f}")
        logger.info(f"Quality distribution: {tiingo_quality['quality_distribution']}")

        # Assertions
        assert tiingo_quality['score_coverage'] > 0.95  # 95% of records should have quality scores
        assert tiingo_quality['invalid_score_rate'] == 0  # No invalid scores
        assert tiingo_quality['quality_stats']['avg_quality'] > 0.7  # Average quality should be good

        # Test Polygon quality scores
        polygon_quality = await validator.validate_quality_scores('intg_one_minute_live_polygon')

        assert polygon_quality['score_coverage'] > 0.95
        assert polygon_quality['invalid_score_rate'] == 0
        assert polygon_quality['quality_stats']['avg_quality'] > 0.7

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_quality_score_correlation(self, quality_db_pool):
        """Test correlation between quality scores and other metrics"""
        async with quality_db_pool.acquire() as conn:
            correlations = await conn.fetch("""
                SELECT
                    symbol,
                    CORR(quality_score, volume) as quality_volume_corr,
                    CORR(quality_score, data_latency_ms) as quality_latency_corr,
                    AVG(quality_score) as avg_quality,
                    COUNT(*) as record_count
                FROM (
                    SELECT symbol, quality_score, volume, data_latency_ms
                    FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                        AND quality_score IS NOT NULL
                        AND volume > 0
                        AND data_latency_ms > 0
                    UNION ALL
                    SELECT symbol, quality_score, volume, data_latency_ms
                    FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                        AND quality_score IS NOT NULL
                        AND volume > 0
                        AND data_latency_ms > 0
                ) combined
                GROUP BY symbol
                HAVING COUNT(*) >= 10
            """)

            for row in correlations:
                symbol = row['symbol']
                vol_corr = float(row['quality_volume_corr']) if row['quality_volume_corr'] else 0
                latency_corr = float(row['quality_latency_corr']) if row['quality_latency_corr'] else 0

                logger.info(f"{symbol}: quality-volume corr={vol_corr:.3f}, quality-latency corr={latency_corr:.3f}")

                # Quality scores should have reasonable correlations
                # High volume might correlate with higher quality
                # High latency might correlate with lower quality
                assert abs(vol_corr) < 1.0  # Correlation should be bounded
                assert abs(latency_corr) < 1.0


class TestTimeSeriesContinuity:
    """Test time series continuity and gaps"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_time_series_gaps(self, quality_db_pool):
        """Test detection of time series gaps"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate continuous data
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(15):
            await collector.generate_and_store_data()
            await asyncio.sleep(0.05)  # Very short delay

        # Test continuity for AAPL
        aapl_continuity = await validator.validate_price_continuity('intg_one_minute_live_tiingo', 'AAPL')

        logger.info(f"AAPL continuity: {aapl_continuity['time_gaps_found']} gaps in {aapl_continuity['total_data_points']} points")
        logger.info(f"Continuity score: {aapl_continuity['continuity_score']:.3f}")

        # Assertions
        assert aapl_continuity['total_data_points'] > 0
        assert aapl_continuity['continuity_score'] > 0.8  # Good continuity

        # Test for TSLA as well
        tsla_continuity = await validator.validate_price_continuity('intg_one_minute_live_polygon', 'TSLA')
        assert tsla_continuity['continuity_score'] > 0.8

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_timestamp_precision(self, quality_db_pool):
        """Test timestamp precision and ordering"""
        async with quality_db_pool.acquire() as conn:
            timestamp_analysis = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT timestamp) as unique_timestamps,
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest,
                    EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) as time_span_seconds
                FROM (
                    SELECT timestamp FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                    UNION
                    SELECT timestamp FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '1 hour'
                ) combined
            """)

            # Check timestamp ordering
            ordering_check = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT
                        timestamp,
                        LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                    FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                    ORDER BY timestamp
                ) ordered
                WHERE timestamp < prev_timestamp  -- Out of order
            """)

            logger.info(f"Timestamp analysis: {timestamp_analysis['total_records']} records, {timestamp_analysis['unique_timestamps']} unique timestamps")
            logger.info(f"Out of order timestamps: {ordering_check}")

            # Assertions
            assert timestamp_analysis['total_records'] > 0
            assert ordering_check == 0  # All timestamps should be in order
            assert timestamp_analysis['time_span_seconds'] > 0


class TestComprehensiveDataQuality:
    """Comprehensive data quality assessment"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_overall_data_quality_score(self, quality_db_pool):
        """Calculate comprehensive data quality score"""
        validator = DataQualityValidator(quality_db_pool)

        # Generate substantial test data
        collector = AAPLTSLASyntheticCollector()
        collector.pool = quality_db_pool

        for _ in range(30):
            await collector.generate_and_store_data()

        # Run all validations
        tiingo_ohlc = await validator.validate_ohlc_relationships('intg_one_minute_live_tiingo')
        polygon_ohlc = await validator.validate_ohlc_relationships('intg_one_minute_live_polygon')
        tiingo_volume = await validator.validate_volume_consistency('intg_one_minute_live_tiingo')
        polygon_volume = await validator.validate_volume_consistency('intg_one_minute_live_polygon')
        cross_vendor = await validator.validate_cross_vendor_consistency()
        tiingo_quality = await validator.validate_quality_scores('intg_one_minute_live_tiingo')
        polygon_quality = await validator.validate_quality_scores('intg_one_minute_live_polygon')

        # Calculate composite quality score
        quality_components = {
            'tiingo_ohlc_quality': 1 - tiingo_ohlc['violation_rate'],
            'polygon_ohlc_quality': 1 - polygon_ohlc['violation_rate'],
            'tiingo_volume_quality': 1 - tiingo_volume['zero_volume_rate'],
            'polygon_volume_quality': 1 - polygon_volume['zero_volume_rate'],
            'cross_vendor_consistency': cross_vendor['price_consistency_score'] if cross_vendor['price_consistency_score'] else 0.5,
            'tiingo_quality_scores': tiingo_quality['quality_stats']['avg_quality'] or 0.5,
            'polygon_quality_scores': polygon_quality['quality_stats']['avg_quality'] or 0.5
        }

        # Weighted composite score
        composite_score = (
            quality_components['tiingo_ohlc_quality'] * 0.2 +
            quality_components['polygon_ohlc_quality'] * 0.2 +
            quality_components['tiingo_volume_quality'] * 0.15 +
            quality_components['polygon_volume_quality'] * 0.15 +
            quality_components['cross_vendor_consistency'] * 0.15 +
            quality_components['tiingo_quality_scores'] * 0.075 +
            quality_components['polygon_quality_scores'] * 0.075
        )

        logger.info("Data Quality Assessment:")
        for component, score in quality_components.items():
            logger.info(f"  {component}: {score:.3f}")
        logger.info(f"Composite Quality Score: {composite_score:.3f}")

        # Overall quality should be high
        assert composite_score > 0.8, f"Composite quality score {composite_score:.3f} below threshold"

        # Individual components should be reasonable
        assert quality_components['tiingo_ohlc_quality'] > 0.95
        assert quality_components['polygon_ohlc_quality'] > 0.95
        assert quality_components['tiingo_volume_quality'] > 0.8
        assert quality_components['polygon_volume_quality'] > 0.8


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])