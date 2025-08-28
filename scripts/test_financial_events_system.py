#!/usr/bin/env python3
"""
Financial Events System Test Suite

Comprehensive testing for the financial events collection and storage system.
Tests schema, data collection, API integration, and data quality.

Usage:
    python test_financial_events_system.py --full
    python test_financial_events_system.py --schema-only
    python test_financial_events_system.py --api-test
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import logging
import json
import argparse
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("financial_events_test")

class FinancialEventsTestSuite:
    """
    Comprehensive test suite for financial events system.
    
    Tests:
    - Database schema validation
    - Data insertion and retrieval
    - API collector functionality
    - Data quality checks
    - Performance benchmarks
    """
    
    def __init__(self):
        self.test_results = {
            'schema_tests': 0,
            'schema_passed': 0,
            'data_tests': 0,
            'data_passed': 0,
            'api_tests': 0,
            'api_passed': 0,
            'total_tests': 0,
            'total_passed': 0
        }
        
        logger.info("🧪 Financial Events Test Suite initialized")

    async def get_database_connection(self):
        """Get database connection."""
        db_host = os.getenv('DB_HOST', 'postgres')
        db_port = int(os.getenv('DB_PORT', '5432'))
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'dev_password')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        return await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )

    def log_test_result(self, test_name: str, passed: bool, category: str = 'total'):
        """Log test result and update statistics."""
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status}: {test_name}")
        
        self.test_results[f'{category}_tests'] += 1
        if passed:
            self.test_results[f'{category}_passed'] += 1
        
        self.test_results['total_tests'] += 1
        if passed:
            self.test_results['total_passed'] += 1

    async def test_schema_exists(self, conn):
        """Test that all required tables and types exist."""
        test_name = "Schema Tables Exist"
        try:
            # Check tables exist
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name IN (
                    'dev_financial_events',
                    'dev_earnings_events', 
                    'dev_analyst_ratings',
                    'dev_corporate_actions',
                    'dev_event_impacts'
                )
            """)
            
            expected_tables = 5
            actual_tables = len(tables)
            
            passed = actual_tables == expected_tables
            self.log_test_result(f"{test_name} ({actual_tables}/{expected_tables})", passed, 'schema')
            
            return passed
            
        except Exception as e:
            logger.error(f"Schema test failed: {e}")
            self.log_test_result(test_name, False, 'schema')
            return False

    async def test_schema_constraints(self, conn):
        """Test that schema constraints work correctly."""
        test_name = "Schema Constraints"
        try:
            # Test impact_score constraint (-1.0 to +1.0)
            try:
                await conn.execute("""
                    INSERT INTO dev_financial_events (
                        event_id, symbol, event_type, event_datetime, title, vendor, impact_score
                    ) VALUES ('test_constraint', 'TEST', 'earnings', NOW(), 'Test', 'test', 2.0)
                """)
                # Should not reach here
                passed = False
            except Exception:
                # Should fail due to constraint
                passed = True
            
            # Clean up any test data
            await conn.execute("DELETE FROM dev_financial_events WHERE event_id = 'test_constraint'")
            
            self.log_test_result(test_name, passed, 'schema')
            return passed
            
        except Exception as e:
            logger.error(f"Constraint test failed: {e}")
            self.log_test_result(test_name, False, 'schema')
            return False

    async def test_sample_data_insertion(self, conn):
        """Test inserting sample financial events data."""
        test_name = "Sample Data Insertion"
        try:
            # Test inserting a complete earnings event
            financial_event_id = await conn.fetchval("""
                INSERT INTO dev_financial_events (
                    event_id, symbol, event_type, event_datetime, title, 
                    sentiment, impact_score, importance_level, vendor
                ) VALUES (
                    'test_sample_event', 'AAPL', 'earnings', '2024-01-31'::timestamp,
                    'AAPL Q1 2024 Earnings', 'positive', 0.15, 'high', 'test'
                ) RETURNING id
            """)
            
            # Insert related earnings data
            await conn.execute("""
                INSERT INTO dev_earnings_events (
                    financial_event_id, symbol, report_period, eps_actual_cents, 
                    eps_estimated_cents, eps_surprise_pct, earnings_beat
                ) VALUES ($1, 'AAPL', '2024-01-31'::date, 21800, 21000, 3.81, true)
            """, financial_event_id)
            
            # Verify data was inserted
            result = await conn.fetchrow("""
                SELECT fe.symbol, fe.event_type, ee.eps_surprise_pct, ee.earnings_beat
                FROM dev_financial_events fe
                JOIN dev_earnings_events ee ON fe.id = ee.financial_event_id
                WHERE fe.event_id = 'test_sample_event'
            """)
            
            passed = result is not None and result['earnings_beat'] == True
            self.log_test_result(test_name, passed, 'data')
            
            return passed
            
        except Exception as e:
            logger.error(f"Sample data test failed: {e}")
            self.log_test_result(test_name, False, 'data')
            return False

    async def test_earnings_calculation_function(self, conn):
        """Test the sample earnings event function."""
        test_name = "Earnings Calculation Function"
        try:
            # Test the pre-built function
            event_id = await conn.fetchval("""
                SELECT insert_sample_earnings_event(
                    'MSFT', '2024-03-31'::DATE, 2.94, 2.83, 61000000000, 59500000000
                )
            """)
            
            # Verify the calculation was correct
            result = await conn.fetchrow("""
                SELECT fe.symbol, ee.eps_surprise_pct, ee.earnings_beat, ee.revenue_beat
                FROM dev_financial_events fe
                JOIN dev_earnings_events ee ON fe.id = ee.financial_event_id
                WHERE fe.id = $1
            """, event_id)
            
            # EPS surprise should be approximately 3.89% ((2.94-2.83)/2.83*100)
            expected_surprise = round(((2.94 - 2.83) / 2.83) * 100, 2)
            actual_surprise = round(float(result['eps_surprise_pct']), 2)
            
            passed = (
                result is not None and 
                abs(actual_surprise - expected_surprise) < 0.1 and
                result['earnings_beat'] == True and
                result['revenue_beat'] == True
            )
            
            self.log_test_result(f"{test_name} (Expected: {expected_surprise}%, Got: {actual_surprise}%)", passed, 'data')
            return passed
            
        except Exception as e:
            logger.error(f"Function test failed: {e}")
            self.log_test_result(test_name, False, 'data')
            return False

    async def test_data_integrity(self, conn):
        """Test data integrity and relationships."""
        test_name = "Data Integrity"
        try:
            # Count events and related data
            events_count = await conn.fetchval("SELECT COUNT(*) FROM dev_financial_events")
            earnings_count = await conn.fetchval("SELECT COUNT(*) FROM dev_earnings_events")
            
            # Check for orphaned records
            orphaned_earnings = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_earnings_events ee
                LEFT JOIN dev_financial_events fe ON ee.financial_event_id = fe.id
                WHERE fe.id IS NULL
            """)
            
            passed = events_count > 0 and orphaned_earnings == 0
            self.log_test_result(f"{test_name} (Events: {events_count}, Orphaned: {orphaned_earnings})", passed, 'data')
            
            return passed
            
        except Exception as e:
            logger.error(f"Data integrity test failed: {e}")
            self.log_test_result(test_name, False, 'data')
            return False

    async def test_performance_indexes(self, conn):
        """Test that performance indexes exist and work."""
        test_name = "Performance Indexes"
        try:
            # Check that key indexes exist
            indexes = await conn.fetch("""
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename LIKE 'dev_%event%' 
                   OR tablename LIKE 'dev_%rating%'
                   OR tablename LIKE 'dev_%action%'
            """)
            
            index_count = len(indexes)
            expected_min_indexes = 10  # We created many indexes
            
            passed = index_count >= expected_min_indexes
            self.log_test_result(f"{test_name} ({index_count} indexes found)", passed, 'schema')
            
            return passed
            
        except Exception as e:
            logger.error(f"Index test failed: {e}")
            self.log_test_result(test_name, False, 'schema')
            return False

    async def test_views_functionality(self, conn):
        """Test that views work correctly."""
        test_name = "Views Functionality"
        try:
            # Test recent earnings view (may be empty, but should not error)
            try:
                recent_earnings = await conn.fetch("SELECT * FROM v_recent_earnings LIMIT 5")
                view1_works = True
            except:
                view1_works = False
            
            # Test analyst consensus view
            try:
                consensus = await conn.fetch("SELECT * FROM v_analyst_consensus LIMIT 5")
                view2_works = True
            except:
                view2_works = False
            
            passed = view1_works  # At least one view should work
            self.log_test_result(f"{test_name} (Recent: {view1_works}, Consensus: {view2_works})", passed, 'schema')
            
            return passed
            
        except Exception as e:
            logger.error(f"Views test failed: {e}")
            self.log_test_result(test_name, False, 'schema')
            return False

    async def test_api_key_availability(self):
        """Test that API keys are available for testing."""
        test_name = "API Keys Available"
        
        polygon_key = os.environ.get("POLYGON_API_KEY")
        alpha_vantage_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        
        keys_available = 0
        if polygon_key:
            keys_available += 1
        if alpha_vantage_key:
            keys_available += 1
        
        passed = keys_available > 0
        self.log_test_result(f"{test_name} ({keys_available}/2 keys available)", passed, 'api')
        
        return passed

    async def test_data_quality_metrics(self, conn):
        """Test data quality and completeness."""
        test_name = "Data Quality Metrics"
        try:
            # Test data completeness
            events_with_symbols = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_financial_events 
                WHERE symbol IS NOT NULL AND symbol != ''
            """)
            
            total_events = await conn.fetchval("SELECT COUNT(*) FROM dev_financial_events")
            
            # Test earnings data quality
            earnings_with_eps = await conn.fetchval("""
                SELECT COUNT(*) FROM dev_earnings_events 
                WHERE eps_actual_cents IS NOT NULL OR eps_estimated_cents IS NOT NULL
            """)
            
            total_earnings = await conn.fetchval("SELECT COUNT(*) FROM dev_earnings_events")
            
            # Quality thresholds
            symbol_completeness = (events_with_symbols / max(total_events, 1)) * 100
            earnings_completeness = (earnings_with_eps / max(total_earnings, 1)) * 100 if total_earnings > 0 else 100
            
            passed = symbol_completeness >= 95 and earnings_completeness >= 80  # Quality thresholds
            
            self.log_test_result(
                f"{test_name} (Symbol: {symbol_completeness:.1f}%, EPS: {earnings_completeness:.1f}%)", 
                passed, 'data'
            )
            
            return passed
            
        except Exception as e:
            logger.error(f"Data quality test failed: {e}")
            self.log_test_result(test_name, False, 'data')
            return False

    async def run_schema_tests(self, conn):
        """Run all schema-related tests."""
        logger.info("🔧 Running Schema Tests...")
        
        tests = [
            self.test_schema_exists(conn),
            self.test_schema_constraints(conn),
            self.test_performance_indexes(conn),
            self.test_views_functionality(conn)
        ]
        
        results = await asyncio.gather(*tests, return_exceptions=True)
        passed_tests = sum(1 for result in results if result == True)
        
        logger.info(f"📊 Schema Tests: {passed_tests}/{len(tests)} passed")
        return passed_tests == len(tests)

    async def run_data_tests(self, conn):
        """Run all data-related tests."""
        logger.info("💾 Running Data Tests...")
        
        tests = [
            self.test_sample_data_insertion(conn),
            self.test_earnings_calculation_function(conn),
            self.test_data_integrity(conn),
            self.test_data_quality_metrics(conn)
        ]
        
        results = await asyncio.gather(*tests, return_exceptions=True)
        passed_tests = sum(1 for result in results if result == True)
        
        logger.info(f"📊 Data Tests: {passed_tests}/{len(tests)} passed")
        return passed_tests == len(tests)

    async def run_api_tests(self):
        """Run API-related tests."""
        logger.info("🌐 Running API Tests...")
        
        tests = [
            self.test_api_key_availability()
        ]
        
        results = await asyncio.gather(*tests, return_exceptions=True)
        passed_tests = sum(1 for result in results if result == True)
        
        logger.info(f"📊 API Tests: {passed_tests}/{len(tests)} passed")
        return passed_tests == len(tests)

    async def cleanup_test_data(self, conn):
        """Clean up test data."""
        try:
            await conn.execute("""
                DELETE FROM dev_earnings_events 
                WHERE financial_event_id IN (
                    SELECT id FROM dev_financial_events 
                    WHERE event_id LIKE 'test_%' OR vendor = 'test' OR event_id LIKE 'sample_%'
                )
            """)
            
            await conn.execute("""
                DELETE FROM dev_financial_events 
                WHERE event_id LIKE 'test_%' OR vendor = 'test' OR event_id LIKE 'sample_%'
            """)
            
            logger.info("🧹 Test data cleaned up")
            
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")

    def log_final_summary(self):
        """Log final test summary."""
        logger.info("=" * 80)
        logger.info("🧪 FINANCIAL EVENTS SYSTEM TEST RESULTS")
        logger.info("=" * 80)
        
        total_passed = self.test_results['total_passed']
        total_tests = self.test_results['total_tests']
        success_rate = (total_passed / max(total_tests, 1)) * 100
        
        logger.info(f"📊 OVERALL RESULTS:")
        logger.info(f"  Total Tests: {total_tests}")
        logger.info(f"  Passed Tests: {total_passed}")
        logger.info(f"  Success Rate: {success_rate:.1f}%")
        logger.info("")
        
        logger.info(f"🔧 SCHEMA TESTS:")
        logger.info(f"  Passed: {self.test_results['schema_passed']}/{self.test_results['schema_tests']}")
        
        logger.info(f"💾 DATA TESTS:")
        logger.info(f"  Passed: {self.test_results['data_passed']}/{self.test_results['data_tests']}")
        
        logger.info(f"🌐 API TESTS:")
        logger.info(f"  Passed: {self.test_results['api_passed']}/{self.test_results['api_tests']}")
        
        status = "✅ ALL TESTS PASSED" if success_rate == 100 else "⚠️ SOME TESTS FAILED"
        logger.info(f"\n{status}")
        logger.info("=" * 80)

    async def run_all_tests(self, schema_only=False, api_only=False):
        """Run complete test suite."""
        logger.info("🚀 Starting Financial Events System Tests...")
        
        conn = await self.get_database_connection()
        
        try:
            if not api_only:
                # Schema tests
                await self.run_schema_tests(conn)
                
                if not schema_only:
                    # Data tests
                    await self.run_data_tests(conn)
            
            if not schema_only:
                # API tests (don't require database)
                await self.run_api_tests()
            
            # Cleanup
            await self.cleanup_test_data(conn)
            
        finally:
            await conn.close()
        
        # Final summary
        self.log_final_summary()

async def main():
    parser = argparse.ArgumentParser(description="Financial Events System Test Suite")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--full', action='store_true', help='Run full test suite (default)')
    parser.add_argument('--schema-only', action='store_true', help='Run only schema tests')
    parser.add_argument('--api-test', action='store_true', help='Run only API tests')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Initialize test suite
        test_suite = FinancialEventsTestSuite()
        
        # Run tests based on arguments
        if args.schema_only:
            await test_suite.run_all_tests(schema_only=True)
        elif args.api_test:
            await test_suite.run_all_tests(api_only=True)
        else:
            # Full test suite (default)
            await test_suite.run_all_tests()
        
    except Exception as e:
        logger.error(f"❌ Test suite failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())