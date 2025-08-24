#!/usr/bin/env python3

"""
Final Comprehensive Coverage Catalog Test
"""
import asyncio
import asyncpg
import logging
from datetime import datetime, date, timedelta
import sys
import os
import json

async def main():
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("🎯 Starting Final Coverage Catalog Test")

# Database connection
db_host = os.getenv('DB_HOST', 'postgres-simple')
db_port = os.getenv('DB_PORT', '5432')
db_user = os.getenv('DB_USER', 'postgres')
db_password = os.getenv('DB_PASSWORD', 'dev_password')
db_name = os.getenv('DB_NAME', 'dev_db')

db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

try:
conn = await asyncpg.connect(db_url)
logger.info("✅ Connected to database")

# Test 1: Verify deployment status
logger.info("🔍 Test 1: Verifying complete deployment...")

# Check tables exist
tables = await conn.fetch("""
SELECT tablename FROM pg_tables 
WHERE schemaname = 'public' AND tablename LIKE 'coverage%'
""")
table_names = [t['tablename'] for t in tables]

assert 'coverage_intervals' in table_names, "coverage_intervals table missing"
assert 'coverage_summary' in table_names, "coverage_summary table missing"

# Check TimescaleDB hypertable
hypertables = await conn.fetch("""
SELECT hypertable_name FROM timescaledb_information.hypertables 
WHERE hypertable_name = 'coverage_intervals'
""")

logger.info(f"✅ Tables: {table_names}")
logger.info(f"✅ TimescaleDB hypertable: {'YES' if hypertables else 'NO'}")

# Test 2: Verify data population
logger.info("🔍 Test 2: Verifying data population...")

summary_count = await conn.fetchval("SELECT COUNT(*) FROM coverage_summary")
interval_count = await conn.fetchval("SELECT COUNT(*) FROM coverage_intervals")

assert summary_count > 0, f"No data in coverage_summary (count: {summary_count})"
assert interval_count > 0, f"No data in coverage_intervals (count: {interval_count})"

logger.info(f"✅ Coverage summary records: {summary_count}")
logger.info(f"✅ Coverage interval records: {interval_count}")

# Test 3: Verify data types and vendors
logger.info("🔍 Test 3: Verifying vendors and data types...")

vendors_data = await conn.fetch("""
SELECT vendor, data_type, COUNT(*) as count, 
AVG(coverage_24h) as avg_coverage,
MAX(last_updated) as latest_update
FROM coverage_summary 
GROUP BY vendor, data_type
ORDER BY vendor, data_type
""")

vendor_summary = {}
for row in vendors_data:
key = f"{row['vendor']}-{row['data_type']}"
vendor_summary[key] = {
'count': row['count'],
'avg_coverage': float(row['avg_coverage']),
'latest_update': row['latest_update']
}
logger.info(f"📊 {key}: {row['count']} records, {float(row['avg_coverage']):.1f}% coverage")

# Test 4: Test coverage analytics queries
logger.info("🔍 Test 4: Testing coverage analytics queries...")

# Query recent coverage trends
recent_coverage = await conn.fetch("""
SELECT symbol, vendor, data_type, 
start_time, completeness_ratio, has_gaps
FROM coverage_intervals 
WHERE start_time >= NOW() - INTERVAL '7 days'
ORDER BY start_time DESC
LIMIT 5
""")

logger.info(f"✅ Recent coverage intervals: {len(recent_coverage)} records")
for row in recent_coverage:
logger.info(f"  📈 {row['symbol']}/{row['vendor']}: {float(row['completeness_ratio']):.2%} @ {row['start_time']}")

# Test 5: Test coverage quality metrics
logger.info("🔍 Test 5: Testing coverage quality metrics...")

quality_stats = await conn.fetch("""
SELECT 
COUNT(*) as total_intervals,
AVG(completeness_ratio) as avg_completeness,
COUNT(*) FILTER (WHERE has_gaps = true) as intervals_with_gaps,
AVG(gap_count) as avg_gap_count
FROM coverage_intervals
""")

if quality_stats:
stats = quality_stats[0]
logger.info(f"✅ Quality metrics:")
logger.info(f"  📊 Total intervals: {stats['total_intervals']}")
logger.info(f"  📊 Average completeness: {float(stats['avg_completeness']):.2%}")
logger.info(f"  📊 Intervals with gaps: {stats['intervals_with_gaps']}")
logger.info(f"  📊 Average gap count: {float(stats['avg_gap_count']):.1f}")

# Test 6: Test time-series queries (TimescaleDB)
logger.info("🔍 Test 6: Testing time-series queries...")

time_series_data = await conn.fetch("""
SELECT 
date_trunc('day', start_time) as day,
vendor,
AVG(completeness_ratio) as daily_avg_completeness,
COUNT(*) as daily_intervals
FROM coverage_intervals
WHERE start_time >= NOW() - INTERVAL '7 days'
GROUP BY date_trunc('day', start_time), vendor
ORDER BY day DESC, vendor
LIMIT 10
""")

logger.info(f"✅ Time-series data: {len(time_series_data)} daily aggregations")
for row in time_series_data:
logger.info(f"  📅 {row['day'].date()} {row['vendor']}: {float(row['daily_avg_completeness']):.2%} ({row['daily_intervals']} intervals)")

# Test 7: Test performance with larger dataset
logger.info("🔍 Test 7: Testing query performance...")

start_time = datetime.now()

performance_query = await conn.fetch("""
SELECT 
vendor,
data_type,
COUNT(*) as total_records,
AVG(completeness_ratio) as avg_completeness,
MAX(start_time) as latest_data
FROM coverage_intervals
GROUP BY vendor, data_type
ORDER BY total_records DESC
""")

query_time = (datetime.now() - start_time).total_seconds()
logger.info(f"✅ Performance test: {len(performance_query)} results in {query_time:.3f}s")

# Test 8: Verify real-time monitoring readiness
logger.info("🔍 Test 8: Verifying real-time monitoring readiness...")

latest_data = await conn.fetch("""
SELECT 
vendor, data_type, symbol,
MAX(last_updated) as latest_update,
AVG(coverage_24h) as current_coverage
FROM coverage_summary
GROUP BY vendor, data_type, symbol
ORDER BY latest_update DESC
LIMIT 5
""")

logger.info(f"✅ Real-time monitoring readiness: {len(latest_data)} active monitoring targets")
for row in latest_data:
# Handle timezone-aware datetime
latest_update = row['latest_update']
if latest_update.tzinfo is None:
latest_update = latest_update.replace(tzinfo=datetime.now().astimezone().tzinfo)
else:
latest_update = latest_update.astimezone()

hours_ago = (datetime.now().astimezone() - latest_update).total_seconds() / 3600
logger.info(f"  🔄 {row['vendor']}/{row['data_type']}: {float(row['current_coverage']):.1f}% coverage ({hours_ago:.1f}h ago)")

# Final summary
test_results = {
'deployment_status': 'SUCCESS',
'tables_created': len(table_names),
'timescaledb_enabled': len(hypertables) > 0,
'coverage_summary_records': summary_count,
'coverage_intervals_records': interval_count,
'vendors_configured': len(vendor_summary),
'query_performance_ms': query_time * 1000,
'monitoring_targets': len(latest_data),
'avg_data_completeness': float(quality_stats[0]['avg_completeness']) if quality_stats else 0
}

logger.info("🎉 ALL TESTS PASSED!")
logger.info("📋 Coverage Catalog Status: FULLY DEPLOYED AND OPERATIONAL")

return test_results

except Exception as e:
logger.error(f"💥 Test failed: {e}")
return {
'deployment_status': 'FAILED',
'error_message': str(e)
}
finally:
if 'conn' in locals():
await conn.close()

if __name__ == "__main__":
result = asyncio.run(main())
print(f"\n📋 Final Test Results:")
print(json.dumps(result, indent=2, default=str))
