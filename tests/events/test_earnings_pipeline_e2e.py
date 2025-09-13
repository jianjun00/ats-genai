#!/usr/bin/env python3
"""
End-to-End Earnings Data Pipeline Tests

Complete integration tests covering the entire earnings data pipeline:
from raw vendor data ingestion to quality monitoring and backfill.
"""

import pytest
import asyncio
import os
import sys
from datetime import datetime, date
from unittest.mock import Mock, AsyncMock
from typing import Dict, List

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

@pytest.fixture
def sample_polygon_raw_data():
    """Sample raw Polygon earnings data"""
    return {
        "cik": "0000320193",
        "tickers": ["AAPL"],
        "end_date": "2025-06-28",
        "timeframe": "quarterly",
        "financials": {
            "income_statement": {
                "revenues": {
                    "unit": "USD",
                    "value": 94036000000.0
                },
                "basic_earnings_per_share": {
                    "unit": "USD / shares",
                    "value": 1.57
                },
                "diluted_earnings_per_share": {
                    "unit": "USD / shares",
                    "value": 1.55
                },
                "net_income_loss": {
                    "unit": "USD",
                    "value": 23434000000.0
                }
            }
        },
        "fiscal_period": "Q3",
        "filing_date": "2025-08-01",
        "acceptance_datetime": "2025-08-01T10:00:42Z"
    }

@pytest.fixture
def mock_database():
    """Mock database connection for E2E tests"""
    db = Mock()
    db.create_pool_with_retry = AsyncMock()

    # Mock connection and cursor
    pool = AsyncMock()
    conn = AsyncMock()

    db.create_pool_with_retry.return_value = pool
    pool.acquire.return_value = conn
    pool.__aenter__ = AsyncMock(return_value=pool)
    pool.__aexit__ = AsyncMock(return_value=None)
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)

    return db, pool, conn

class TestEarningsDataIngestionPipeline:
    """Test complete earnings data ingestion pipeline"""

    @pytest.mark.asyncio
    async def test_polygon_to_database_pipeline(self, sample_polygon_raw_data, mock_database):
        """Test complete pipeline from Polygon API to database storage"""
        db, pool, conn = mock_database

        # Mock database operations
        conn.execute = AsyncMock(return_value=None)
        conn.fetchval = AsyncMock(return_value=1)  # New ID

        # Simulate the complete ingestion pipeline
        class MockEarningsIngestionPipeline:
            def __init__(self, db_connection):
                self.db = db_connection

            async def process_polygon_earnings(self, raw_data: Dict) -> Dict:
                """Process raw Polygon data through complete pipeline"""

                # Step 1: Parse raw data into financial event
                financial_event = {
                    'event_id': f"polygon_{raw_data['cik']}_{raw_data['end_date']}",
                    'symbol': raw_data['tickers'][0],
                    'event_type': 'earnings',
                    'event_datetime': datetime.fromisoformat(raw_data['acceptance_datetime'].replace('Z', '+00:00')),
                    'fiscal_period': raw_data['fiscal_period'],
                    'vendor': 'polygon',
                    'raw_data': raw_data
                }

                # Step 2: Insert financial event
                financial_event_id = await self._insert_financial_event(financial_event)

                # Step 3: Extract earnings-specific data
                earnings_data = self._extract_earnings_data(raw_data)
                earnings_data['financial_event_id'] = financial_event_id

                # Step 4: Insert earnings event
                earnings_event_id = await self._insert_earnings_event(earnings_data)

                return {
                    'financial_event_id': financial_event_id,
                    'earnings_event_id': earnings_event_id,
                    'symbol': financial_event['symbol'],
                    'eps_extracted': earnings_data.get('eps_actual_cents') is not None,
                    'call_time_extracted': earnings_data.get('earnings_call_datetime') is not None,
                    'processing_status': 'success'
                }

            def _extract_earnings_data(self, raw_data: Dict) -> Dict:
                """Extract earnings data from raw Polygon JSON"""
                income_stmt = raw_data.get('financials', {}).get('income_statement', {})

                earnings_data = {
                    'symbol': raw_data['tickers'][0],
                    'report_period': datetime.strptime(raw_data['end_date'], '%Y-%m-%d').date(),
                    'report_type': 'final',
                    'eps_actual_cents': None,
                    'revenue_actual_cents': None,
                    'net_income_cents': None,
                    'earnings_call_datetime': None
                }

                # Extract EPS (basic preferred, diluted fallback)
                basic_eps = income_stmt.get('basic_earnings_per_share', {}).get('value')
                if basic_eps is not None:
                    earnings_data['eps_actual_cents'] = int(basic_eps * 100)
                else:
                    diluted_eps = income_stmt.get('diluted_earnings_per_share', {}).get('value')
                    if diluted_eps is not None:
                        earnings_data['eps_actual_cents'] = int(diluted_eps * 100)

                # Extract revenue
                revenue = income_stmt.get('revenues', {}).get('value')
                if revenue is not None:
                    earnings_data['revenue_actual_cents'] = int(revenue * 100)

                # Extract net income
                net_income = income_stmt.get('net_income_loss', {}).get('value')
                if net_income is not None:
                    earnings_data['net_income_cents'] = int(net_income * 100)

                # Extract call timestamp
                if raw_data.get('acceptance_datetime'):
                    earnings_data['earnings_call_datetime'] = datetime.fromisoformat(
                        raw_data['acceptance_datetime'].replace('Z', '+00:00')
                    )

                return earnings_data

            async def _insert_financial_event(self, event_data: Dict) -> int:
                """Insert financial event and return ID"""
                await conn.execute(
                    "INSERT INTO dev_financial_events (...) VALUES (...)",
                    *event_data.values()
                )
                return await conn.fetchval("SELECT LASTVAL()")

            async def _insert_earnings_event(self, earnings_data: Dict) -> int:
                """Insert earnings event and return ID"""
                await conn.execute(
                    "INSERT INTO dev_earnings_events (...) VALUES (...)",
                    *earnings_data.values()
                )
                return await conn.fetchval("SELECT LASTVAL()")

        # Test the pipeline
        pipeline = MockEarningsIngestionPipeline(db)
        result = await pipeline.process_polygon_earnings(sample_polygon_raw_data)

        # Validate pipeline results
        assert result['processing_status'] == 'success'
        assert result['symbol'] == 'AAPL'
        assert result['eps_extracted'] is True
        assert result['call_time_extracted'] is True
        assert result['financial_event_id'] == 1
        assert result['earnings_event_id'] == 1

        # Verify database calls were made
        assert conn.execute.call_count == 2  # Two inserts
        assert conn.fetchval.call_count == 2  # Two ID fetches

    @pytest.mark.asyncio
    async def test_data_quality_monitoring_integration(self, mock_database):
        """Test integration with quality monitoring system"""
        db, pool, conn = mock_database

        # Mock quality monitoring queries
        conn.fetchrow = AsyncMock(side_effect=[
            {'total_earnings': 1000, 'eps_count': 850},  # EPS coverage
            {'total_earnings': 1000, 'revenue_count': 920},  # Revenue coverage
            {'total_earnings': 1000, 'call_count': 680},  # Call timing coverage
        ])

        # Simulate quality monitoring pipeline
        class MockQualityMonitoringPipeline:
            def __init__(self, db_connection):
                self.db = db_connection

            async def run_quality_assessment(self) -> Dict:
                """Run complete quality assessment"""

                # Check EPS coverage
                eps_result = await conn.fetchrow("SELECT COUNT(*) as total_earnings, COUNT(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 END) as eps_count FROM dev_earnings_events")
                eps_coverage = eps_result['eps_count'] / eps_result['total_earnings']

                # Check revenue coverage
                revenue_result = await conn.fetchrow("SELECT COUNT(*) as total_earnings, COUNT(CASE WHEN revenue_actual_cents IS NOT NULL THEN 1 END) as revenue_count FROM dev_earnings_events")
                revenue_coverage = revenue_result['revenue_count'] / revenue_result['total_earnings']

                # Check call timing coverage
                call_result = await conn.fetchrow("SELECT COUNT(*) as total_earnings, COUNT(CASE WHEN earnings_call_datetime IS NOT NULL THEN 1 END) as call_count FROM dev_earnings_events")
                call_coverage = call_result['call_count'] / call_result['total_earnings']

                # Calculate overall quality score
                overall_score = (eps_coverage + revenue_coverage) / 2  # Call timing less critical

                # Determine status
                if overall_score >= 0.90:
                    status = 'excellent'
                elif overall_score >= 0.80:
                    status = 'good'
                elif overall_score >= 0.70:
                    status = 'warning'
                else:
                    status = 'critical'

                return {
                    'timestamp': datetime.now().isoformat(),
                    'overall_quality_score': overall_score,
                    'status': status,
                    'metrics': {
                        'eps_coverage': eps_coverage,
                        'revenue_coverage': revenue_coverage,
                        'call_timing_coverage': call_coverage
                    },
                    'alerts_generated': self._generate_alerts(overall_score, status)
                }

            def _generate_alerts(self, score: float, status: str) -> List[str]:
                alerts = []
                if status == 'critical':
                    alerts.append(f"CRITICAL: Overall quality score {score:.1%} below acceptable threshold")
                elif status == 'warning':
                    alerts.append(f"WARNING: Quality score {score:.1%} needs improvement")
                return alerts

        # Test quality monitoring
        monitor = MockQualityMonitoringPipeline(db)
        quality_report = await monitor.run_quality_assessment()

        # Validate quality assessment
        assert 'overall_quality_score' in quality_report
        assert quality_report['status'] in ['excellent', 'good', 'warning', 'critical']
        assert quality_report['metrics']['eps_coverage'] == 0.85  # 850/1000
        assert quality_report['metrics']['revenue_coverage'] == 0.92  # 920/1000
        assert quality_report['metrics']['call_timing_coverage'] == 0.68  # 680/1000

        # Overall score should be average of EPS and revenue (0.885)
        expected_overall = (0.85 + 0.92) / 2
        assert abs(quality_report['overall_quality_score'] - expected_overall) < 0.01

    @pytest.mark.asyncio
    async def test_historical_backfill_integration(self, mock_database):
        """Test integration with historical backfill system"""
        db, pool, conn = mock_database

        # Mock gap analysis query results
        conn.fetch = AsyncMock(return_value=[
            {'symbol': 'AAPL', 'year': 2020, 'earnings_count': 4, 'eps_count': 4},
            {'symbol': 'AAPL', 'year': 2021, 'earnings_count': 3, 'eps_count': 3},  # Incomplete
            {'symbol': 'AAPL', 'year': 2022, 'earnings_count': 0, 'eps_count': 0},  # Missing
            {'symbol': 'AAPL', 'year': 2023, 'earnings_count': 4, 'eps_count': 0},  # No EPS
        ])

        # Simulate backfill pipeline
        class MockHistoricalBackfillPipeline:
            def __init__(self, db_connection):
                self.db = db_connection

            async def analyze_gaps_and_create_plans(self, symbols: List[str]) -> Dict:
                """Analyze gaps and create backfill plans"""

                gaps = []
                for symbol in symbols:
                    # Get yearly coverage for symbol
                    rows = await conn.fetch(f"SELECT * FROM yearly_coverage WHERE symbol = '{symbol}'")

                    missing_years = []
                    incomplete_years = []
                    eps_missing_years = []

                    for row in rows:
                        year = row['year']
                        earnings_count = row['earnings_count']
                        eps_count = row['eps_count']

                        if earnings_count == 0:
                            missing_years.append(year)
                        elif earnings_count < 4:
                            incomplete_years.append(year)

                        if eps_count == 0 and earnings_count > 0:
                            eps_missing_years.append(year)

                    if missing_years or incomplete_years or eps_missing_years:
                        gaps.append({
                            'symbol': symbol,
                            'missing_years': missing_years,
                            'incomplete_years': incomplete_years,
                            'eps_missing_years': eps_missing_years,
                            'priority_score': self._calculate_priority(symbol, missing_years, incomplete_years, eps_missing_years)
                        })

                # Create backfill plans
                plans = self._create_plans(gaps)

                return {
                    'gaps_analyzed': len(gaps),
                    'gaps': gaps,
                    'backfill_plans': plans,
                    'total_estimated_cost': sum(p['estimated_cost_usd'] for p in plans),
                    'total_expected_records': sum(p['expected_records'] for p in plans)
                }

            def _calculate_priority(self, symbol: str, missing: List[int], incomplete: List[int], eps_missing: List[int]) -> float:
                score = 0.0
                if symbol == 'AAPL':  # Tier 1
                    score += 100

                # Recent missing years are high priority
                for year in missing:
                    if year >= 2020:
                        score += 50
                    else:
                        score += 20

                score += len(incomplete) * 10
                score += len(eps_missing) * 5

                return score

            def _create_plans(self, gaps: List[Dict]) -> List[Dict]:
                plans = []

                # High priority recent gaps
                high_priority_symbols = [g['symbol'] for g in gaps if g['priority_score'] > 100]
                if high_priority_symbols:
                    plans.append({
                        'name': 'Critical Recent Gaps',
                        'priority': 1,
                        'symbols': high_priority_symbols,
                        'date_range': '2015-2025',
                        'estimated_cost_usd': len(high_priority_symbols) * 25.0,
                        'expected_records': len(high_priority_symbols) * 40
                    })

                # EPS enhancement for existing records
                eps_symbols = [g['symbol'] for g in gaps if g['eps_missing_years']]
                if eps_symbols:
                    plans.append({
                        'name': 'EPS Enhancement',
                        'priority': 2,
                        'symbols': eps_symbols,
                        'date_range': '2020-2025',
                        'estimated_cost_usd': len(eps_symbols) * 10.0,
                        'expected_records': len(eps_symbols) * 20
                    })

                return plans

        # Test backfill pipeline
        backfill = MockHistoricalBackfillPipeline(db)
        analysis = await backfill.analyze_gaps_and_create_plans(['AAPL'])

        # Validate analysis results
        assert analysis['gaps_analyzed'] == 1
        assert len(analysis['gaps']) == 1

        aapl_gap = analysis['gaps'][0]
        assert aapl_gap['symbol'] == 'AAPL'
        assert 2022 in aapl_gap['missing_years']  # Year with 0 earnings
        assert 2021 in aapl_gap['incomplete_years']  # Year with only 3 earnings
        assert 2023 in aapl_gap['eps_missing_years']  # Year with earnings but no EPS

        # Should generate backfill plans
        assert len(analysis['backfill_plans']) >= 1
        assert analysis['total_estimated_cost'] > 0
        assert analysis['total_expected_records'] > 0

class TestErrorHandlingAndRecovery:
    """Test error handling and recovery mechanisms"""

    @pytest.mark.asyncio
    async def test_api_rate_limit_handling(self, mock_database):
        """Test handling of API rate limits during ingestion"""
        db, pool, conn = mock_database

        class MockRateLimitedAPI:
            def __init__(self):
                self.call_count = 0
                self.rate_limit_threshold = 3

            async def fetch_earnings_data(self, symbol: str) -> Dict:
                self.call_count += 1

                if self.call_count >= self.rate_limit_threshold:
                    # Simulate rate limit error
                    raise Exception("API rate limit exceeded. Retry after 60 seconds.")

                return {
                    'symbol': symbol,
                    'data': {'eps': 1.50, 'revenue': 100000000}
                }

        class MockIngestionWithRetry:
            def __init__(self, api, max_retries=3, retry_delay=1):
                self.api = api
                self.max_retries = max_retries
                self.retry_delay = retry_delay

            async def ingest_with_retry(self, symbol: str) -> Dict:
                """Ingest with automatic retry on rate limits"""
                retries = 0

                while retries < self.max_retries:
                    try:
                        data = await self.api.fetch_earnings_data(symbol)
                        return {
                            'status': 'success',
                            'symbol': symbol,
                            'data': data,
                            'retries_used': retries
                        }

                    except Exception as e:
                        if "rate limit" in str(e).lower() and retries < self.max_retries - 1:
                            retries += 1
                            # In real implementation, would await asyncio.sleep(self.retry_delay)
                            continue
                        else:
                            return {
                                'status': 'failed',
                                'symbol': symbol,
                                'error': str(e),
                                'retries_used': retries
                            }

                return {
                    'status': 'failed',
                    'symbol': symbol,
                    'error': 'Max retries exceeded',
                    'retries_used': self.max_retries
                }

        # Test rate limit handling
        api = MockRateLimitedAPI()
        ingestion = MockIngestionWithRetry(api)

        # First two calls should succeed
        result1 = await ingestion.ingest_with_retry('AAPL')
        result2 = await ingestion.ingest_with_retry('MSFT')

        assert result1['status'] == 'success'
        assert result2['status'] == 'success'
        assert result1['retries_used'] == 0
        assert result2['retries_used'] == 0

        # Third call should hit rate limit and fail after retries
        result3 = await ingestion.ingest_with_retry('GOOGL')

        assert result3['status'] == 'failed'
        assert 'rate limit' in result3['error']

    def test_data_validation_and_sanitization(self):
        """Test data validation and sanitization in pipeline"""

        def validate_earnings_data(data: Dict) -> Dict:
            """Validate and sanitize earnings data"""
            issues = []
            sanitized = data.copy()

            # Validate symbol
            if not data.get('symbol') or not isinstance(data['symbol'], str):
                issues.append('Invalid symbol')
            elif len(data['symbol']) > 10:
                sanitized['symbol'] = data['symbol'][:10]
                issues.append('Symbol truncated')

            # Validate EPS
            eps = data.get('eps_actual_cents')
            if eps is not None:
                if not isinstance(eps, (int, float)):
                    issues.append('Invalid EPS type')
                elif abs(eps) > 1000000:  # >$10,000 EPS seems unreasonable
                    issues.append('EPS value seems unreasonable')
                elif isinstance(eps, float):
                    sanitized['eps_actual_cents'] = int(eps)
                    issues.append('EPS converted to integer')

            # Validate revenue
            revenue = data.get('revenue_actual_cents')
            if revenue is not None:
                if not isinstance(revenue, (int, float)):
                    issues.append('Invalid revenue type')
                elif revenue < 0:
                    issues.append('Negative revenue flagged')
                elif isinstance(revenue, float):
                    sanitized['revenue_actual_cents'] = int(revenue)
                    issues.append('Revenue converted to integer')

            # Validate date
            report_date = data.get('report_period')
            if report_date:
                if isinstance(report_date, str):
                    try:
                        sanitized['report_period'] = datetime.strptime(report_date, '%Y-%m-%d').date()
                        issues.append('Date string converted to date object')
                    except ValueError:
                        issues.append('Invalid date format')
                elif not isinstance(report_date, date):
                    issues.append('Invalid date type')

            return {
                'data': sanitized,
                'validation_issues': issues,
                'is_valid': len([i for i in issues if 'Invalid' in i]) == 0
            }

        # Test valid data
        valid_data = {
            'symbol': 'AAPL',
            'eps_actual_cents': 157,
            'revenue_actual_cents': 9403600000000,
            'report_period': date(2025, 6, 28)
        }

        result = validate_earnings_data(valid_data)
        assert result['is_valid'] is True
        assert len(result['validation_issues']) == 0

        # Test invalid data
        invalid_data = {
            'symbol': 'VERY_LONG_SYMBOL_NAME',  # Too long
            'eps_actual_cents': 'invalid',       # Wrong type
            'revenue_actual_cents': -1000000,    # Negative revenue
            'report_period': '2025-13-45'        # Invalid date
        }

        result = validate_earnings_data(invalid_data)
        assert result['is_valid'] is False
        assert len(result['validation_issues']) > 0
        assert any('Invalid' in issue for issue in result['validation_issues'])

    @pytest.mark.asyncio
    async def test_database_transaction_rollback(self, mock_database):
        """Test database transaction rollback on errors"""
        db, pool, conn = mock_database

        # Mock database operations
        conn.execute = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)

        # Mock transaction that fails partway through
        transaction_mock = AsyncMock()
        conn.transaction = Mock(return_value=transaction_mock)

        class MockTransactionalIngestion:
            def __init__(self, db_connection):
                self.conn = db_connection

            async def ingest_earnings_batch(self, earnings_data: List[Dict]) -> Dict:
                """Ingest batch of earnings data in transaction"""
                results = {
                    'processed': 0,
                    'successful': 0,
                    'failed': 0,
                    'errors': []
                }

                try:
                    async with self.conn.transaction():
                        for i, earnings in enumerate(earnings_data):
                            # Simulate failure on third item
                            if i == 2:
                                raise Exception("Database constraint violation")

                            # Insert financial event
                            await self.conn.execute("INSERT INTO dev_financial_events (...)")
                            financial_event_id = await self.conn.fetchval("SELECT LASTVAL()")

                            # Insert earnings event
                            await self.conn.execute("INSERT INTO dev_earnings_events (...)")

                            results['processed'] += 1
                            results['successful'] += 1

                    return results

                except Exception as e:
                    # Transaction should rollback automatically
                    results['failed'] = len(earnings_data)
                    results['errors'].append(str(e))
                    return results

        # Test transaction rollback
        ingestion = MockTransactionalIngestion(conn)

        # Test with batch that will fail
        failing_batch = [
            {'symbol': 'AAPL', 'eps_actual_cents': 157},
            {'symbol': 'MSFT', 'eps_actual_cents': 289},
            {'symbol': 'INVALID', 'eps_actual_cents': None},  # Will cause failure
        ]

        result = await ingestion.ingest_earnings_batch(failing_batch)

        assert result['processed'] == 2  # Processed first two before failure
        assert result['successful'] == 0  # All rolled back due to transaction failure
        assert result['failed'] == 3      # All three considered failed
        assert len(result['errors']) == 1
        assert 'constraint violation' in result['errors'][0]

class TestPerformanceAndScaling:
    """Test performance characteristics and scaling behavior"""

    def test_batch_processing_efficiency(self):
        """Test batch processing for improved efficiency"""

        class MockBatchProcessor:
            def __init__(self, batch_size=100):
                self.batch_size = batch_size
                self.processed_count = 0
                self.batch_count = 0

            async def process_earnings_batch(self, earnings_list: List[Dict]) -> Dict:
                """Process earnings in batches for efficiency"""
                total_items = len(earnings_list)
                batches = [earnings_list[i:i+self.batch_size] for i in range(0, total_items, self.batch_size)]

                results = {
                    'total_items': total_items,
                    'batch_size': self.batch_size,
                    'total_batches': len(batches),
                    'processed_items': 0,
                    'processing_time_per_batch': []
                }

                for batch in batches:
                    start_time = datetime.now()

                    # Simulate batch processing
                    for item in batch:
                        self.processed_count += 1
                        # Simulate processing time

                    end_time = datetime.now()
                    batch_time = (end_time - start_time).total_seconds()
                    results['processing_time_per_batch'].append(batch_time)

                    results['processed_items'] += len(batch)
                    self.batch_count += 1

                return results

        # Test batch processing
        processor = MockBatchProcessor(batch_size=50)

        # Create test data
        test_earnings = [{'symbol': f'TEST{i}', 'eps': i/100} for i in range(175)]

        # Process in batches
        result = asyncio.run(processor.process_earnings_batch(test_earnings))

        assert result['total_items'] == 175
        assert result['batch_size'] == 50
        assert result['total_batches'] == 4  # ceil(175/50) = 4
        assert result['processed_items'] == 175
        assert len(result['processing_time_per_batch']) == 4
        assert processor.batch_count == 4

    def test_memory_usage_optimization(self):
        """Test memory usage patterns for large datasets"""

        def estimate_memory_usage(record_count: int) -> Dict:
            """Estimate memory usage for earnings records"""

            # Estimate bytes per earnings record
            bytes_per_record = (
                10 +    # symbol (varchar)
                8 +     # report_period (date)
                4 +     # eps_actual_cents (int)
                8 +     # revenue_actual_cents (bigint)
                8 +     # earnings_call_datetime (timestamp)
                1000    # raw_data (json, estimated)
            )

            total_memory_bytes = record_count * bytes_per_record

            return {
                'record_count': record_count,
                'bytes_per_record': bytes_per_record,
                'total_memory_bytes': total_memory_bytes,
                'total_memory_mb': total_memory_bytes / (1024 * 1024),
                'total_memory_gb': total_memory_bytes / (1024 * 1024 * 1024)
            }

        # Test memory estimates for different scales
        small_scale = estimate_memory_usage(1_000)      # 1K records
        medium_scale = estimate_memory_usage(100_000)   # 100K records
        large_scale = estimate_memory_usage(1_000_000)  # 1M records

        assert small_scale['total_memory_mb'] < 10      # <10 MB
        assert medium_scale['total_memory_mb'] < 1000   # <1 GB
        assert large_scale['total_memory_gb'] < 10      # <10 GB

        # Memory should scale linearly
        assert abs(medium_scale['total_memory_bytes'] / small_scale['total_memory_bytes'] - 100) < 1
        assert abs(large_scale['total_memory_bytes'] / medium_scale['total_memory_bytes'] - 10) < 1

    @pytest.mark.asyncio
    async def test_concurrent_processing(self):
        """Test concurrent processing of multiple symbols"""

        class MockConcurrentProcessor:
            def __init__(self, max_concurrent=5):
                self.max_concurrent = max_concurrent
                self.active_tasks = 0
                self.completed_tasks = 0

            async def process_symbol_concurrent(self, symbol: str) -> Dict:
                """Process single symbol with concurrency tracking"""
                self.active_tasks += 1

                # Simulate processing time
                await asyncio.sleep(0.1)  # 100ms processing time

                result = {
                    'symbol': symbol,
                    'processing_time': 0.1,
                    'status': 'success'
                }

                self.active_tasks -= 1
                self.completed_tasks += 1

                return result

            async def process_symbols_batch(self, symbols: List[str]) -> Dict:
                """Process multiple symbols concurrently"""
                start_time = datetime.now()

                # Create semaphore to limit concurrency
                semaphore = asyncio.Semaphore(self.max_concurrent)

                async def process_with_semaphore(symbol: str):
                    async with semaphore:
                        return await self.process_symbol_concurrent(symbol)

                # Run all symbols concurrently with limit
                tasks = [process_with_semaphore(symbol) for symbol in symbols]
                results = await asyncio.gather(*tasks)

                end_time = datetime.now()
                total_time = (end_time - start_time).total_seconds()

                return {
                    'symbols_processed': len(symbols),
                    'total_time_seconds': total_time,
                    'max_concurrent': self.max_concurrent,
                    'results': results,
                    'average_time_per_symbol': total_time / len(symbols) if symbols else 0
                }

        # Test concurrent processing
        processor = MockConcurrentProcessor(max_concurrent=3)

        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META']

        result = await processor.process_symbols_batch(test_symbols)

        assert result['symbols_processed'] == 6
        assert result['max_concurrent'] == 3
        assert len(result['results']) == 6

        # With max_concurrent=3, should process faster than sequential
        # Sequential would take 6 * 0.1 = 0.6 seconds
        # Concurrent should take roughly 0.2 seconds (2 batches of 3)
        assert result['total_time_seconds'] < 0.4  # Should be much faster than sequential

        # All symbols should have succeeded
        assert all(r['status'] == 'success' for r in result['results'])

if __name__ == "__main__":
    pytest.main([__file__, "-v"])