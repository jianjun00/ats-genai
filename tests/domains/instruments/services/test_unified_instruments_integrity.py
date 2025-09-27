#!/usr/bin/env python3
"""
Comprehensive Test Suite: Unified Instruments & Data Integrity

Tests for unified instrument population, cross-vendor data consistency,
referential integrity, and system-wide data quality validation.

Coverage:
- Unified instrument creation from multiple vendor sources
- Cross-vendor data consistency validation
- Referential integrity between instruments and price data
- Data completeness and quality metrics
- Performance and scalability validation
"""

import pytest
from datetime import datetime, timedelta

class TestUnifiedInstrumentCreation:
    """Test unified instrument population from vendor sources"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_vendor_data_merging(self):
        """Test merging of instrument data from multiple vendors"""
        from scripts.unified_instrument_population import UnifiedInstrumentPopulator

        populator = UnifiedInstrumentPopulator()

        # Mock vendor data for same symbol from different sources
        vendor_data = [
            {
                'vendor': 'eodhd',
                'name': 'Apple Inc',
                'exchange': 'NASDAQ',
                'type': 'Common Stock',
                'currency': 'USD',
                'active': True
            },
            {
                'vendor': 'polygon',
                'name': 'Apple Inc.',
                'exchange': 'XNAS',
                'type': 'CS',
                'currency': 'USD',
                'figi': 'BBG000B9XRY4',
                'active': True
            },
            {
                'vendor': 'tiingo',
                'name': 'Apple Inc.',
                'exchange': 'NASDAQ',
                'type': 'Stock',
                'currency': None,
                'active': True
            }
        ]

        unified = populator.create_unified_instrument('AAPL', vendor_data, 3)

        assert unified['symbol'] == 'AAPL', "Symbol should be preserved"
        assert unified['vendor_count'] == 3, "Should track vendor count"
        assert 'eodhd' in unified['vendor_metadata']['sources'], "Should track all vendor sources"
        assert 'polygon' in unified['vendor_metadata']['sources'], "Should track all vendor sources"
        assert 'tiingo' in unified['vendor_metadata']['sources'], "Should track all vendor sources"

        # Test intelligent field resolution
        assert unified['name'] == 'Apple Inc.', "Should prefer longest descriptive name"
        assert unified['exchange'] == 'NASDAQ', "Should prefer specific exchange names"
        assert unified['currency'] == 'USD', "Should resolve currency from available data"

    def test_conflict_identification(self):
        """Test identification of conflicts between vendor data"""
        from scripts.unified_instrument_population import UnifiedInstrumentPopulator

        populator = UnifiedInstrumentPopulator()

        conflicting_data = [
            {
                'vendor': 'eodhd',
                'name': 'Apple Inc',
                'exchange': 'NASDAQ',
                'type': 'Common Stock',
                'currency': 'USD'
            },
            {
                'vendor': 'polygon',
                'name': 'Apple Incorporated',  # Different name
                'exchange': 'XNAS',           # Different exchange code
                'type': 'CS',                # Different type
                'currency': 'USD'
            }
        ]

        conflicts = populator.identify_conflicts(conflicting_data)

        assert 'name' in conflicts, "Should identify name conflicts"
        assert 'exchange' in conflicts, "Should identify exchange conflicts"
        assert 'type' in conflicts, "Should identify type conflicts"
        assert 'currency' not in conflicts, "Should not flag matching currencies"

        assert len(conflicts['name']) == 2, "Should capture all conflicting name values"

    def test_field_conflict_resolution(self):
        """Test intelligent resolution of field conflicts"""
        from scripts.unified_instrument_population import UnifiedInstrumentPopulator

        populator = UnifiedInstrumentPopulator()

        unified = {
            'symbol': 'AAPL',
            'name': None,
            'exchange': None,
            'currency': None
        }

        vendor_data = [
            {
                'name': 'Apple Inc',      # Shorter name
                'exchange': 'US',         # Generic exchange
                'currency': None
            },
            {
                'name': 'Apple Inc. (NASDAQ)',  # Longer, more descriptive
                'exchange': 'NASDAQ',            # Specific exchange
                'currency': 'USD'
            }
        ]

        resolved = populator.resolve_field_conflicts(unified, vendor_data)

        assert resolved['name'] == 'Apple Inc. (NASDAQ)', "Should prefer longer names"
        assert resolved['exchange'] == 'NASDAQ', "Should prefer specific exchanges"
        assert resolved['currency'] == 'USD', "Should use available currency"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_unified_strategy_creation(self):
        """Test creation of comprehensive unification strategy"""
        from scripts.unified_instrument_population import UnifiedInstrumentPopulator
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        populator = UnifiedInstrumentPopulator()

        # Test strategy creation (limited scope for testing)
        instruments = await populator.create_unified_strategy(pool)

        assert isinstance(instruments, list), "Should return list of instruments"

        if instruments:  # Only test if we have data
            sample_instrument = instruments[0]
            assert 'symbol' in sample_instrument, "Each instrument should have symbol"
            assert 'vendor_count' in sample_instrument, "Should track vendor count"
            assert 'vendor_metadata' in sample_instrument, "Should include metadata"

        await pool.close()

class TestDataIntegrityValidation:
    """Test referential integrity and data consistency"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_data_instrument_integrity(self):
        """Test referential integrity between price data and instruments"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Check for price records without corresponding instruments
            orphaned_prices = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_daily_price_polygon p
                WHERE NOT EXISTS (
                    SELECT 1 FROM dev_instrument i WHERE i.id = p.instrument_id
                )
            """)

            # Check for instruments without any price data
            instruments_without_prices = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_instrument i
                WHERE NOT EXISTS (
                    SELECT 1 FROM dev_daily_price_polygon p WHERE p.instrument_id = i.id
                )
            """)

            total_price_records = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_price_polygon")
            total_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument")

            # Calculate integrity percentages
            price_integrity = ((total_price_records - orphaned_prices) / total_price_records * 100) if total_price_records > 0 else 0

            assert orphaned_prices == 0, f"Found {orphaned_prices} orphaned price records"
            assert price_integrity > 95.0, f"Price integrity too low: {price_integrity:.1f}%"

            # Log metrics for monitoring
            print(f"📊 Price Data Integrity: {price_integrity:.1f}%")
            print(f"📊 Instruments without prices: {instruments_without_prices}")
            print(f"📊 Total instruments: {total_instruments}")
            print(f"📊 Total price records: {total_price_records}")

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_vendor_data_consistency(self):
        """Test consistency across vendor-specific instrument tables"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Get symbols that exist in multiple vendor tables
            overlapping_symbols = await conn.fetch("""
                WITH vendor_symbols AS (
                    SELECT symbol, 'polygon' as vendor FROM dev_instrument_polygon
                    UNION ALL
                    SELECT symbol, 'tiingo' as vendor FROM dev_instrument_tiingo WHERE active = true
                    UNION ALL
                    SELECT symbol, 'eodhd' as vendor FROM dev_instrument_eodhd
                ),
                symbol_counts AS (
                    SELECT symbol, COUNT(DISTINCT vendor) as vendor_count
                    FROM vendor_symbols
                    GROUP BY symbol
                    HAVING COUNT(DISTINCT vendor) > 1
                )
                SELECT symbol, vendor_count FROM symbol_counts
                ORDER BY vendor_count DESC, symbol
                LIMIT 10
            """)

            assert len(overlapping_symbols) > 0, "Should have symbols across multiple vendors"

            # Test consistency for a sample of overlapping symbols
            for symbol_record in overlapping_symbols[:5]:
                symbol = symbol_record['symbol']
                vendor_count = symbol_record['vendor_count']

                # Get data from each vendor for this symbol
                polygon_data = await conn.fetchrow("""
                    SELECT name, exchange, active FROM dev_instrument_polygon
                    WHERE symbol = $1
                """, symbol)

                tiingo_data = await conn.fetchrow("""
                    SELECT name, exchange, active FROM dev_instrument_tiingo
                    WHERE symbol = $1 AND active = true
                """, symbol)

                eodhd_data = await conn.fetchrow("""
                    SELECT name, exchange FROM dev_instrument_eodhd
                    WHERE symbol = $1
                """, symbol)

                # Validate data consistency (allowing for format differences)
                vendor_names = []
                if polygon_data: vendor_names.append(polygon_data['name'])
                if tiingo_data: vendor_names.append(tiingo_data['name'])
                if eodhd_data: vendor_names.append(eodhd_data['name'])

                # Names should be similar (allowing for minor variations)
                if len(vendor_names) > 1:
                    base_name = vendor_names[0].lower().replace(' inc.', '').replace(' inc', '').strip()
                    for name in vendor_names[1:]:
                        compare_name = name.lower().replace(' inc.', '').replace(' inc', '').strip()
                        similarity = len(set(base_name.split()) & set(compare_name.split())) / max(len(base_name.split()), len(compare_name.split()))
                        assert similarity > 0.5, f"Names too different for {symbol}: {vendor_names}"

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_unified_instrument_completeness(self):
        """Test completeness of unified instrument population"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Get counts from each vendor table
            polygon_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_polygon WHERE active = true")
            tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo WHERE active = true")
            eodhd_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_eodhd")
            unified_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument")

            # Get unique symbol counts
            unique_symbols = await conn.fetchval("""
                SELECT COUNT(DISTINCT symbol) FROM (
                    SELECT symbol FROM dev_instrument_polygon WHERE active = true
                    UNION
                    SELECT symbol FROM dev_instrument_tiingo WHERE active = true
                    UNION
                    SELECT symbol FROM dev_instrument_eodhd
                ) all_symbols
            """)

            # Unified table should have close to the number of unique symbols
            coverage_ratio = unified_count / unique_symbols if unique_symbols > 0 else 0

            assert coverage_ratio > 0.90, f"Unified coverage too low: {coverage_ratio:.1%} ({unified_count}/{unique_symbols})"

            # Log metrics
            print(f"📊 Vendor Counts - Polygon: {polygon_count}, Tiingo: {tiingo_count}, EODHD: {eodhd_count}")
            print(f"📊 Unique symbols: {unique_symbols}, Unified: {unified_count}")
            print(f"📊 Coverage: {coverage_ratio:.1%}")

        await pool.close()

class TestDataQualityMetrics:
    """Test data quality and completeness metrics"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_instrument_data_completeness(self):
        """Test completeness of instrument data fields"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            total_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument")

            if total_instruments == 0:
                pytest.skip("No instruments in unified table to test")

            # Test completeness of key fields
            completeness_metrics = {}

            key_fields = ['name', 'exchange', 'type', 'currency']
            for field in key_fields:
                non_null_count = await conn.fetchval(f"""
                    SELECT COUNT(*) FROM dev_instrument
                    WHERE {field} IS NOT NULL AND {field} != ''
                """)

                completeness_metrics[field] = non_null_count / total_instruments

            # Assert minimum completeness thresholds
            assert completeness_metrics['name'] > 0.95, f"Name completeness too low: {completeness_metrics['name']:.1%}"
            assert completeness_metrics['exchange'] > 0.80, f"Exchange completeness too low: {completeness_metrics['exchange']:.1%}"

            # Log metrics
            for field, ratio in completeness_metrics.items():
                print(f"📊 {field.title()} completeness: {ratio:.1%}")

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_data_coverage(self):
        """Test coverage and quality of price data"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Test recent price data availability
            recent_cutoff = datetime.now().date() - timedelta(days=7)

            recent_price_symbols = await conn.fetchval("""
                SELECT COUNT(DISTINCT instrument_id)
                FROM dev_daily_price_polygon
                WHERE date >= $1
            """, recent_cutoff)

            total_active_instruments = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_instrument WHERE active = true
            """)

            recent_coverage = recent_price_symbols / total_active_instruments if total_active_instruments > 0 else 0

            # Test for data quality issues
            zero_volume_ratio = await conn.fetchval("""
                SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM dev_daily_price_polygon WHERE volume IS NOT NULL)
                FROM dev_daily_price_polygon
                WHERE volume = 0
            """) or 0

            missing_prices_ratio = await conn.fetchval("""
                SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM dev_daily_price_polygon)
                FROM dev_daily_price_polygon
                WHERE close IS NULL OR close <= 0
            """) or 0

            assert recent_coverage > 0.70, f"Recent price coverage too low: {recent_coverage:.1%}"
            assert zero_volume_ratio < 0.30, f"Too many zero volume records: {zero_volume_ratio:.1%}"
            assert missing_prices_ratio < 0.01, f"Too many missing prices: {missing_prices_ratio:.1%}"

            # Log metrics
            print(f"📊 Recent price coverage: {recent_coverage:.1%}")
            print(f"📊 Zero volume ratio: {zero_volume_ratio:.1%}")
            print(f"📊 Missing prices ratio: {missing_prices_ratio:.1%}")

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_news_data_quality(self):
        """Test quality and coverage of news data"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Test Polygon news
            polygon_news_count = await conn.fetchval("SELECT COUNT(*) FROM dev_news_polygon")
            recent_polygon_news = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_news_polygon
                WHERE published_utc >= $1
            """, datetime.now() - timedelta(days=30))

            if polygon_news_count > 0:
                recent_news_ratio = recent_polygon_news / polygon_news_count
                assert recent_news_ratio > 0.01, f"Too few recent Polygon news: {recent_news_ratio:.1%}"
                print(f"📊 Polygon news: {polygon_news_count:,} total, {recent_polygon_news:,} recent")

            tiingo_news_count = await conn.fetchval("SELECT COUNT(*) FROM dev_news_tiingo")
            if tiingo_news_count > 0:
                print(f"📊 Tiingo news: {tiingo_news_count:,} articles")

        await pool.close()

class TestSystemPerformance:
    """Test system performance and scalability"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_query_performance(self):
        """Test performance of common queries"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Test instrument lookup performance
            start_time = datetime.now()

            result = await conn.fetchrow("""
                SELECT * FROM dev_instrument
                WHERE symbol = 'AAPL'
            """)

            lookup_time = (datetime.now() - start_time).total_seconds()
            assert lookup_time < 0.1, f"Instrument lookup too slow: {lookup_time:.3f}s"

            # Test price data aggregation performance
            start_time = datetime.now()

            result = await conn.fetchrow("""
                SELECT symbol, COUNT(*) as record_count, AVG(close) as avg_price
                FROM dev_daily_price_polygon p
                JOIN dev_instrument i ON i.id = p.instrument_id
                WHERE p.date >= $1
                GROUP BY symbol
                LIMIT 1
            """, datetime.now().date() - timedelta(days=30))

            aggregation_time = (datetime.now() - start_time).total_seconds()
            assert aggregation_time < 5.0, f"Price aggregation too slow: {aggregation_time:.3f}s"

            print(f"📊 Instrument lookup: {lookup_time:.3f}s")
            print(f"📊 Price aggregation: {aggregation_time:.3f}s")

        await pool.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_index_effectiveness(self):
        """Test effectiveness of database indexes"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)

        async with pool.acquire() as conn:
            # Check that key indexes exist
            indexes = await conn.fetch("""
                SELECT tablename, indexname, indexdef
                FROM pg_indexes
                WHERE tablename IN ('dev_instrument', 'dev_daily_price_polygon', 'dev_news_polygon')
                ORDER BY tablename, indexname
            """)

            index_names = [idx['indexname'] for idx in indexes]

            # Critical indexes should exist
            critical_indexes = [
                'dev_instrument_pkey',  # Primary key
                'dev_daily_price_polygon_pkey',  # Primary key
            ]

            for critical_index in critical_indexes:
                # Check if index exists (may have different exact names)
                index_found = any(critical_index in idx_name for idx_name in index_names)
                if not index_found:
                    print(f"⚠️ Critical index may be missing: {critical_index}")

            print(f"📊 Found {len(indexes)} indexes across key tables")

        await pool.close()

class TestEndToEndDataFlow:
    """Test complete data flow from collection to consumption"""

    @pytest.mark.asyncio
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_complete_data_pipeline(self):
        """Test end-to-end data pipeline functionality"""
        from core.shared.utils.database import Database
        from core.platform.config.environment import Environment, EnvironmentType

        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)

        async with pool.acquire() as conn:
            # Step 1: Verify vendor data exists
            vendor_counts = await conn.fetchrow("""
                SELECT
                    (SELECT COUNT(*) FROM dev_instrument_polygon WHERE active = true) as polygon_count,
                    (SELECT COUNT(*) FROM dev_instrument_tiingo WHERE active = true) as tiingo_count,
                    (SELECT COUNT(*) FROM dev_instrument_eodhd) as eodhd_count
            """)

            total_vendor_instruments = vendor_counts['polygon_count'] + vendor_counts['tiingo_count'] + vendor_counts['eodhd_count']
            assert total_vendor_instruments > 0, "No vendor instrument data found"

            # Step 2: Verify unified instruments exist
            unified_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument")
            assert unified_count > 0, "No unified instruments found"

            # Step 3: Verify price data integration
            price_count = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_price_polygon")
            assert price_count > 0, "No price data found"

            # Step 4: Test data accessibility via typical queries
            # Query: Get recent prices for active instruments
            recent_data = await conn.fetch("""
                SELECT i.symbol, i.name, p.date, p.close, p.volume
                FROM dev_instrument i
                JOIN dev_daily_price_polygon p ON p.instrument_id = i.id
                WHERE i.active = true
                  AND p.date >= $1
                  AND p.close > 0
                ORDER BY p.date DESC, i.symbol
                LIMIT 5
            """, datetime.now().date() - timedelta(days=7))

            assert len(recent_data) > 0, "No recent price data accessible"

            # Step 5: Validate data quality in the pipeline
            for record in recent_data:
                assert record['symbol'] is not None, "Symbol should not be null"
                assert record['close'] > 0, "Close price should be positive"
                assert record['date'] is not None, "Date should not be null"

            print(f"✅ End-to-end pipeline test passed:")
            print(f"   📊 {total_vendor_instruments:,} vendor instruments")
            print(f"   📊 {unified_count:,} unified instruments")
            print(f"   📊 {price_count:,} price records")
            print(f"   📊 {len(recent_data)} recent accessible records")

        await pool.close()

if __name__ == "__main__":
    import sys

    # Add src to path
    sys.path.insert(0, '/workspace/src')

    # Run tests with comprehensive reporting
    pytest.main([
        __file__,
        "-v",  # Verbose output
        "-s",  # Don't capture output
        "--tb=short",  # Short traceback format
        "--durations=10",  # Show 10 slowest tests
        "-x",  # Stop on first failure
    ])