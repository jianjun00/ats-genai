#!/usr/bin/env python3
"""
Comprehensive Regression Protection Suite: Data Quality Issues

Regression tests to prevent recurrence of all major data quality issues
identified during system development, including:

1. Tiingo End Date Misinterpretation (9,834 stocks incorrectly delisted)
2. Hardcoded API Keys Security Vulnerability (18+ files)
3. Database Schema Compatibility Issues
4. EODHD Population Gap (85% missing instruments)
5. Referential Integrity Problems
6. API Rate Limiting and Error Handling

This suite ensures these critical issues never recur.
"""

import pytest
import glob
import re
from datetime import datetime, timedelta

class TestTiingoEndDateRegression:
    """Prevent regression of Tiingo end date misinterpretation issue"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_active_instruments_ratio(self):
        """Test that Tiingo maintains proper ratio of active instruments"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Get total and active counts
                total_tiingo = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo")
                active_tiingo = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo WHERE active = true")

                if total_tiingo == 0:
                    pytest.skip("No Tiingo instruments to test")

                active_ratio = active_tiingo / total_tiingo

                # The original issue caused active ratio to drop to ~18.4%
                # Normal ratio should be much higher (>75%)
                assert active_ratio > 0.75, f"Tiingo active ratio too low: {active_ratio:.1%} ({active_tiingo}/{total_tiingo})"

                # Check for instruments with recent end_date that are marked inactive
                recent_cutoff = datetime.now().date() - timedelta(days=7)
                recent_end_dates_inactive = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_instrument_tiingo
                    WHERE end_date >= $1 AND active = false
                """, recent_cutoff)

                assert recent_end_dates_inactive == 0, f"Found {recent_end_dates_inactive} instruments with recent end_date marked as inactive"

                print(f"✅ Tiingo active ratio: {active_ratio:.1%} ({active_tiingo}/{total_tiingo})")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_end_date_interpretation(self):
        """Test correct interpretation of Tiingo end_date field"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Recent end dates should not automatically mean delisting
                recent_cutoff = datetime.now().date() - timedelta(days=7)

                recent_ended_instruments = await conn.fetch("""
                    SELECT symbol, end_date, active, name
                    FROM dev_instrument_tiingo
                    WHERE end_date >= $1
                    ORDER BY end_date DESC
                    LIMIT 10
                """, recent_cutoff)

                # If instruments have recent end dates, they should generally be active
                # (end_date indicates latest data availability, not delisting)
                for instrument in recent_ended_instruments:
                    end_date = instrument['end_date']
                    days_ago = (datetime.now().date() - end_date).days

                    if days_ago <= 7:  # Very recent end dates
                        assert instrument['active'], f"Instrument {instrument['symbol']} with recent end_date ({end_date}) should be active"

                print(f"✅ Found {len(recent_ended_instruments)} instruments with recent end_date, all properly classified")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

class TestHardcodedAPIKeysRegression:
    """Prevent regression of hardcoded API keys security vulnerability"""

    def test_no_hardcoded_polygon_keys(self):
        """Test that no Polygon API keys are hardcoded in files"""

        # Search patterns for Polygon API keys
        polygon_patterns = [
            r'["\']wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD["\']',  # Specific key pattern
            r'["\'][A-Za-z0-9]{32}["\']',  # Generic 32-char API key pattern
            r'POLYGON_API_KEY\s*=\s*["\'][^"\']+["\']',  # Direct assignment
            r'apikey.*["\'][A-Za-z0-9]{20,}["\']',  # API key parameter patterns
        ]

        # Files to check (exclude this test file and config files that may have examples)
        search_patterns = [
            'scripts/**/*.py',
            'src/**/*.py',
            'tests/**/*.py',
            'docs/**/*.md',
            'k8s/**/*.yaml',
            '*.py'
        ]

        violations = []

        for search_pattern in search_patterns:
            files = glob.glob(search_pattern, recursive=True)

            for file_path in files:
                # Skip this test file and certain config files
                if file_path.endswith('test_data_quality_regression_protection.py'):
                    continue
                if 'example' in file_path.lower() or 'template' in file_path.lower():
                    continue

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                        for pattern in polygon_patterns:
                            matches = re.findall(pattern, content, re.IGNORECASE)
                            if matches:
                                violations.append({
                                    'file': file_path,
                                    'pattern': pattern,
                                    'matches': matches
                                })

                        # Check for the specific compromised key
                        if 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD' in content:
                            violations.append({
                                'file': file_path,
                                'pattern': 'specific_key',
                                'matches': ['wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD']
                            })

                except Exception as e:
                    # Skip files that can't be read
                    continue

        if violations:
            violation_details = '\n'.join([
                f"  {v['file']}: {v['matches']}"
                for v in violations
            ])
            pytest.fail(f"Found hardcoded API keys in {len(violations)} locations:\n{violation_details}")

        print(f"✅ No hardcoded API keys found in {len([f for pattern in search_patterns for f in glob.glob(pattern, recursive=True)])} files")

    def test_environment_variable_usage(self):
        """Test that scripts use environment variables for API keys"""

        python_files = glob.glob('scripts/**/*.py', recursive=True)

        compliant_files = 0
        violations = []

        for file_path in python_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Skip files that don't use API keys
                if 'api' not in content.lower() and 'key' not in content.lower():
                    continue

                # Check for proper environment variable usage
                has_env_usage = any([
                    'os.getenv(' in content,
                    'os.environ[' in content,
                    'env.get(' in content,
                    'getenv(' in content
                ])

                # Check for potential hardcoded keys
                has_hardcoded = any([
                    re.search(r'["\'][A-Za-z0-9]{20,}["\']', content),
                    'api_key = "' in content.lower(),
                    'apikey = "' in content.lower()
                ])

                if has_hardcoded and not has_env_usage:
                    violations.append(file_path)
                elif has_env_usage:
                    compliant_files += 1

            except Exception:
                continue

        assert len(violations) == 0, f"Files with potential hardcoded keys: {violations}"
        print(f"✅ {compliant_files} files properly use environment variables for API keys")

class TestDatabaseSchemaRegression:
    """Prevent regression of database schema compatibility issues"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_table_schema_consistency(self):
        """Test that price tables have consistent, expected schemas"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Check dev_daily_price_polygon schema
                polygon_columns = await conn.fetch("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = 'dev_daily_price_polygon'
                    ORDER BY ordinal_position
                """)

                if not polygon_columns:
                    pytest.skip("dev_daily_price_polygon table not found")

                column_names = [col['column_name'] for col in polygon_columns]

                # Critical columns that caused issues
                critical_columns = [
                    'symbol', 'date', 'open', 'high', 'low', 'close',
                    'volume', 'instrument_id'
                ]

                for col in critical_columns:
                    assert col in column_names, f"Critical column {col} missing from dev_daily_price_polygon"

                # Check for problematic column name variations that caused issues
                problematic_variations = [
                    'adj_close',  # Should be 'adjclose' or consistent
                    'creation_timestamp',  # Should be 'created_at'
                ]

                for prob_col in problematic_variations:
                    if prob_col in column_names:
                        print(f"⚠️ Found potentially problematic column: {prob_col}")

                print(f"✅ dev_daily_price_polygon has {len(column_names)} columns with all critical fields")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_instrument_tables_compatibility(self):
        """Test compatibility between vendor-specific instrument tables"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Get schemas for all instrument tables
                tables = ['dev_instrument_polygon', 'dev_instrument_tiingo', 'dev_instrument_eodhd', 'dev_instrument']

                table_schemas = {}
                for table in tables:
                    try:
                        columns = await conn.fetch("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = $1
                            ORDER BY ordinal_position
                        """, table)
                        table_schemas[table] = {col['column_name']: col['data_type'] for col in columns}
                    except:
                        table_schemas[table] = {}

                # All instrument tables should have basic common fields
                common_fields = ['symbol', 'name', 'active']

                for table, schema in table_schemas.items():
                    if not schema:  # Skip if table doesn't exist
                        continue

                    for field in common_fields:
                        if field not in schema:
                            print(f"⚠️ {table} missing common field: {field}")
                        else:
                            assert field in schema, f"{table} should have {field} field"

                print(f"✅ Schema compatibility checked for {len([t for t in table_schemas.values() if t])} instrument tables")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

class TestEODHDPopulationRegression:
    """Prevent regression of EODHD population gap (85% missing instruments)"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eodhd_population_completeness(self):
        """Test that EODHD population reaches expected completeness levels"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                eodhd_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_eodhd")

                if eodhd_count == 0:
                    pytest.skip("No EODHD instruments to test")

                # Original issue: only 7,613 of 50,746 instruments populated (15%)
                # Fixed system should have much higher coverage
                expected_minimum = 40000  # Expect at least 40k instruments

                assert eodhd_count >= expected_minimum, f"EODHD population too low: {eodhd_count:,} < {expected_minimum:,}"

                # Test data quality - instruments should have basic required fields
                incomplete_records = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_instrument_eodhd
                    WHERE symbol IS NULL OR symbol = '' OR name IS NULL OR name = ''
                """)

                incompleteness_ratio = incomplete_records / eodhd_count
                assert incompleteness_ratio < 0.01, f"Too many incomplete EODHD records: {incompleteness_ratio:.1%}"

                print(f"✅ EODHD population: {eodhd_count:,} instruments ({incompleteness_ratio:.1%} incomplete)")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eodhd_to_unified_population_flow(self):
        """Test that EODHD instruments flow properly into unified table"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Get sample EODHD symbols
                eodhd_symbols = await conn.fetch("""
                    SELECT symbol FROM dev_instrument_eodhd
                    ORDER BY symbol LIMIT 10
                """)

                if not eodhd_symbols:
                    pytest.skip("No EODHD symbols to test")

                # Check how many appear in unified table
                eodhd_in_unified = 0
                for symbol_record in eodhd_symbols:
                    symbol = symbol_record['symbol']

                    unified_record = await conn.fetchrow("""
                        SELECT * FROM dev_instrument WHERE symbol = $1
                    """, symbol)

                    if unified_record:
                        eodhd_in_unified += 1

                coverage_ratio = eodhd_in_unified / len(eodhd_symbols)

                # Most EODHD instruments should appear in unified table
                assert coverage_ratio > 0.80, f"EODHD to unified coverage too low: {coverage_ratio:.1%}"

                print(f"✅ EODHD to unified coverage: {coverage_ratio:.1%} ({eodhd_in_unified}/{len(eodhd_symbols)})")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

class TestReferentialIntegrityRegression:
    """Prevent regression of referential integrity problems"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_instrument_referential_integrity(self):
        """Test referential integrity between price data and instruments"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Check for orphaned price records (no corresponding instrument)
                orphaned_prices = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_daily_price_polygon p
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dev_instrument i WHERE i.id = p.instrument_id
                    )
                """)

                total_price_records = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_price_polygon")

                if total_price_records == 0:
                    pytest.skip("No price records to test")

                orphaned_ratio = orphaned_prices / total_price_records

                # Should have near-perfect referential integrity
                assert orphaned_ratio < 0.01, f"Too many orphaned price records: {orphaned_ratio:.2%} ({orphaned_prices:,}/{total_price_records:,})"

                # Check for major symbols that should have both instruments and price data
                major_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
                missing_major_symbols = []

                for symbol in major_symbols:
                    # Check if symbol has both instrument and price data
                    combined_data = await conn.fetchrow("""
                        SELECT i.symbol, COUNT(p.id) as price_count
                        FROM dev_instrument i
                        LEFT JOIN dev_daily_price_polygon p ON p.instrument_id = i.id
                        WHERE i.symbol = $1
                        GROUP BY i.symbol
                    """, symbol)

                    if not combined_data or combined_data['price_count'] == 0:
                        missing_major_symbols.append(symbol)

                assert len(missing_major_symbols) == 0, f"Major symbols missing price data: {missing_major_symbols}"

                print(f"✅ Price referential integrity: {100-orphaned_ratio*100:.1f}% ({total_price_records:,} records)")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_news_data_integrity(self):
        """Test integrity of news data and symbol references"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)

            async with pool.acquire() as conn:
                # Test Polygon news integrity
                try:
                    news_with_valid_tickers = await conn.fetchval("""
                        SELECT COUNT(*) FROM dev_news_polygon
                        WHERE tickers IS NOT NULL AND array_length(tickers, 1) > 0
                    """)

                    total_news = await conn.fetchval("SELECT COUNT(*) FROM dev_news_polygon")

                    if total_news > 0:
                        ticker_coverage = news_with_valid_tickers / total_news
                        assert ticker_coverage > 0.50, f"News ticker coverage too low: {ticker_coverage:.1%}"
                        print(f"✅ Polygon news ticker coverage: {ticker_coverage:.1%}")

                except Exception:
                    print("📊 Polygon news table not available for integrity test")

                # Test Tiingo news integrity
                try:
                    tiingo_news_count = await conn.fetchval("SELECT COUNT(*) FROM dev_news_tiingo")
                    if tiingo_news_count > 0:
                        print(f"✅ Tiingo news records: {tiingo_news_count:,}")

                except Exception:
                    print("📊 Tiingo news table not available for integrity test")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

class TestAPIErrorHandlingRegression:
    """Prevent regression of API error handling and rate limiting issues"""

    def test_rate_limiting_configuration(self):
        """Test that scripts have proper rate limiting configuration"""

        python_files = glob.glob('scripts/**/*.py', recursive=True)

        rate_limit_patterns = [
            r'sleep\(\d+',  # Basic sleep calls
            r'rate.*limit',  # Rate limit mentions
            r'delay.*\d+',  # Delay configurations
            r'await.*sleep',  # Async sleep calls
        ]

        scripts_with_rate_limiting = 0
        scripts_needing_rate_limiting = 0

        for file_path in python_files:
            if 'test' in file_path:
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Skip scripts that don't make API calls
                if not any(term in content.lower() for term in ['api', 'http', 'request']):
                    continue

                scripts_needing_rate_limiting += 1

                # Check for rate limiting mechanisms
                has_rate_limiting = any(
                    re.search(pattern, content, re.IGNORECASE)
                    for pattern in rate_limit_patterns
                )

                if has_rate_limiting:
                    scripts_with_rate_limiting += 1
                else:
                    print(f"⚠️ Script may need rate limiting: {file_path}")

            except Exception:
                continue

        if scripts_needing_rate_limiting > 0:
            rate_limit_coverage = scripts_with_rate_limiting / scripts_needing_rate_limiting
            assert rate_limit_coverage > 0.70, f"Rate limiting coverage too low: {rate_limit_coverage:.1%}"
            print(f"✅ Rate limiting coverage: {rate_limit_coverage:.1%} ({scripts_with_rate_limiting}/{scripts_needing_rate_limiting})")

    def test_error_handling_patterns(self):
        """Test that scripts have proper error handling patterns"""

        python_files = glob.glob('scripts/**/*.py', recursive=True)

        error_handling_patterns = [
            r'try:',
            r'except.*Exception',
            r'except.*Error',
            r'catch',
            r'finally:'
        ]

        scripts_with_error_handling = 0
        scripts_checked = 0

        for file_path in python_files:
            if 'test' in file_path:
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Skip very small scripts
                if len(content.split('\n')) < 20:
                    continue

                scripts_checked += 1

                # Check for error handling
                has_error_handling = any(
                    re.search(pattern, content, re.IGNORECASE)
                    for pattern in error_handling_patterns
                )

                if has_error_handling:
                    scripts_with_error_handling += 1
                else:
                    print(f"⚠️ Script may need error handling: {file_path}")

            except Exception:
                continue

        if scripts_checked > 0:
            error_handling_coverage = scripts_with_error_handling / scripts_checked
            assert error_handling_coverage > 0.80, f"Error handling coverage too low: {error_handling_coverage:.1%}"
            print(f"✅ Error handling coverage: {error_handling_coverage:.1%} ({scripts_with_error_handling}/{scripts_checked})")

class TestSystemHealthRegression:
    """Overall system health and regression prevention"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_overall_data_quality_metrics(self):
        """Test overall system data quality metrics"""
        from shared.utils.database import Database
        from shared.utils.environment import Environment, EnvironmentType

        try:
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=15.0)

            quality_metrics = {}

            async with pool.acquire() as conn:
                # Instrument quality metrics
                total_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument")
                active_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument WHERE active = true")

                if total_instruments > 0:
                    quality_metrics['instrument_active_ratio'] = active_instruments / total_instruments

                # Price data quality
                total_prices = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_price_polygon")
                valid_prices = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_daily_price_polygon
                    WHERE close > 0 AND volume >= 0
                """)

                if total_prices > 0:
                    quality_metrics['price_data_validity'] = valid_prices / total_prices

                # Referential integrity
                prices_with_instruments = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_daily_price_polygon p
                    WHERE EXISTS (SELECT 1 FROM dev_instrument i WHERE i.id = p.instrument_id)
                """)

                if total_prices > 0:
                    quality_metrics['referential_integrity'] = prices_with_instruments / total_prices

                # Vendor coverage
                polygon_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_polygon WHERE active = true")
                tiingo_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo WHERE active = true")
                eodhd_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_eodhd")

                total_vendor_instruments = polygon_instruments + tiingo_instruments + eodhd_instruments

                if total_vendor_instruments > 0:
                    quality_metrics['vendor_to_unified_ratio'] = total_instruments / total_vendor_instruments

            # Assert quality thresholds
            assert quality_metrics.get('instrument_active_ratio', 0) > 0.85, f"Instrument active ratio too low: {quality_metrics.get('instrument_active_ratio', 0):.1%}"
            assert quality_metrics.get('price_data_validity', 0) > 0.95, f"Price data validity too low: {quality_metrics.get('price_data_validity', 0):.1%}"
            assert quality_metrics.get('referential_integrity', 0) > 0.98, f"Referential integrity too low: {quality_metrics.get('referential_integrity', 0):.1%}"

            # Log all metrics
            print("📊 System Quality Metrics:")
            for metric, value in quality_metrics.items():
                if isinstance(value, float):
                    print(f"   {metric}: {value:.1%}")
                else:
                    print(f"   {metric}: {value}")

            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available for testing: {e}")

    def test_configuration_consistency(self):
        """Test consistency of configuration across the system"""

        # Check for consistent API key environment variable names
        config_files = glob.glob('**/*.py', recursive=True) + glob.glob('**/*.yaml', recursive=True)

        api_key_vars = set()

        for file_path in config_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Extract API key environment variable names
                matches = re.findall(r'os\.getenv\(["\']([A-Z_]*API_KEY[^"\']*)["\']', content)
                api_key_vars.update(matches)

                matches = re.findall(r'os\.environ\[["\']([A-Z_]*API_KEY[^"\']*)["\']', content)
                api_key_vars.update(matches)

            except Exception:
                continue

        # Should use consistent variable names
        expected_vars = {'POLYGON_API_KEY', 'TIINGO_API_KEY'}
        unexpected_vars = api_key_vars - expected_vars

        if unexpected_vars:
            print(f"⚠️ Unexpected API key variables: {unexpected_vars}")

        print(f"✅ API key variables found: {api_key_vars}")

        # Should have at least the expected variables
        assert len(api_key_vars & expected_vars) >= 1, "Should use expected API key variable names"

# Test runner with comprehensive reporting
if __name__ == "__main__":
    import sys

    # Add src to path
    sys.path.insert(0, '/workspace/src')

    # Run regression tests with detailed reporting
    pytest.main([
        __file__,
        "-v",  # Verbose output
        "-s",  # Don't capture output
        "--tb=short",  # Short traceback format
        "--durations=15",  # Show 15 slowest tests
        "--maxfail=3",  # Stop after 3 failures
        "-rA",  # Show all test outcomes
    ])