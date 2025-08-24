#!/usr/bin/env python3

"""Environment configuration"""
import os

class Environment:
def __init__(self):
self.db_host = os.getenv('DB_HOST', 'postgres-simple')
self.db_port = os.getenv('DB_PORT', '5432')
self.db_user = os.getenv('DB_USER', 'postgres')
self.db_password = os.getenv('DB_PASSWORD', 'dev_password')
self.db_name = os.getenv('DB_NAME', 'dev_db')

def get_database_url(self):
return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

setup_monitoring_views.py: |
"""
Setup Monitoring Views and Dashboard Queries

Creates comprehensive monitoring views for real-time quality tracking
of the price unification system.
"""
import asyncio
import asyncpg
import logging
import json
import os
from datetime import date, datetime, timedelta
from environment import Environment

class MonitoringDashboardSetup:
def __init__(self):
self.env = Environment()
self.conn = None
self.logger = logging.getLogger(__name__)

async def connect(self):
"""Connect to database"""
self.conn = await asyncpg.connect(self.env.get_database_url())
self.logger.info("✅ Connected to database")

async def disconnect(self):
"""Disconnect from database"""
if self.conn:
await self.conn.close()

async def create_quality_monitoring_views(self):
"""Create comprehensive monitoring views for quality tracking"""

# 1. Real-time Quality Dashboard View
quality_dashboard_sql = """
CREATE OR REPLACE VIEW dev_price_quality_dashboard AS
WITH daily_metrics AS (
SELECT 
DATE(dp.created_at) as processing_date,
COUNT(*) as total_records,
COUNT(CASE WHEN pvs.code = 'valid' THEN 1 END) as valid_records,
COUNT(CASE WHEN pvs.code = 'outlier_statistical' THEN 1 END) as outlier_records,
COUNT(CASE WHEN pvs.code = 'manual_review' THEN 1 END) as manual_review_records,
AVG(dp.confidence_score) as avg_confidence,
MIN(dp.confidence_score) as min_confidence,
MAX(dp.confidence_score) as max_confidence,
AVG(dp.vendor_count) as avg_vendor_count,
COUNT(CASE WHEN dp.vendor_count = 1 THEN 1 END) as single_vendor_records,
COUNT(CASE WHEN dp.vendor_count > 1 THEN 1 END) as multi_vendor_records,
COUNT(CASE WHEN dp.polygon_price IS NOT NULL THEN 1 END) as polygon_coverage,
COUNT(CASE WHEN dp.tiingo_price IS NOT NULL THEN 1 END) as tiingo_coverage,
COUNT(CASE WHEN dp.fmp_price IS NOT NULL THEN 1 END) as fmp_coverage,
AVG(dp.price_variance) as avg_price_variance,
MAX(dp.price_variance) as max_price_variance,
COUNT(DISTINCT i.symbol) as unique_symbols
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
JOIN dev_instruments i ON dp.instrument_id = i.id
WHERE dp.created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(dp.created_at)
),
current_status AS (
SELECT 
COUNT(*) as total_records_today,
COUNT(CASE WHEN pvs.code = 'valid' THEN 1 END) as valid_records_today,
AVG(dp.confidence_score) as avg_confidence_today,
COUNT(DISTINCT i.symbol) as symbols_processed_today
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
JOIN dev_instruments i ON dp.instrument_id = i.id
WHERE DATE(dp.created_at) = CURRENT_DATE
)
SELECT 
dm.*,
ROUND((dm.valid_records::NUMERIC / NULLIF(dm.total_records, 0)) * 100, 2) as success_rate_pct,
ROUND((dm.polygon_coverage::NUMERIC / NULLIF(dm.total_records, 0)) * 100, 2) as polygon_coverage_pct,
ROUND((dm.tiingo_coverage::NUMERIC / NULLIF(dm.total_records, 0)) * 100, 2) as tiingo_coverage_pct,
ROUND((dm.fmp_coverage::NUMERIC / NULLIF(dm.total_records, 0)) * 100, 2) as fmp_coverage_pct,
cs.total_records_today,
cs.valid_records_today,
cs.avg_confidence_today,
cs.symbols_processed_today
FROM daily_metrics dm
CROSS JOIN current_status cs
ORDER BY dm.processing_date DESC;
"""

# 2. Real-time Alerts View
alerts_view_sql = """
CREATE OR REPLACE VIEW dev_price_quality_alerts AS
WITH recent_issues AS (
SELECT 
i.symbol,
dp.date,
dp.close,
dp.confidence_score,
dp.statistical_score,
dp.price_variance,
pvs.code as validation_status,
pvs.description as status_description,
dp.validation_notes,
dp.vendor_count,
dp.polygon_price,
dp.tiingo_price,
dp.fmp_price,
dp.created_at,
CASE 
WHEN pvs.code = 'outlier_statistical' THEN 'HIGH'
WHEN pvs.code = 'manual_review' THEN 'MEDIUM'
WHEN dp.confidence_score < 0.5 THEN 'MEDIUM'
WHEN dp.vendor_count = 1 AND dp.confidence_score < 0.7 THEN 'LOW'
ELSE 'INFO'
END as alert_severity,
CASE 
WHEN pvs.code = 'outlier_statistical' THEN 'Statistical Outlier Detected'
WHEN pvs.code = 'manual_review' THEN 'Manual Review Required'
WHEN dp.confidence_score < 0.5 THEN 'Low Confidence Score'
WHEN dp.vendor_count = 1 THEN 'Single Vendor Data'
ELSE 'Normal Processing'
END as alert_type
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
JOIN dev_instruments i ON dp.instrument_id = i.id
WHERE dp.created_at >= CURRENT_DATE - INTERVAL '7 days'
AND (pvs.code != 'valid' OR dp.confidence_score < 0.7 OR dp.vendor_count = 1)
)
SELECT *
FROM recent_issues
ORDER BY 
CASE alert_severity 
WHEN 'HIGH' THEN 1 
WHEN 'MEDIUM' THEN 2 
WHEN 'LOW' THEN 3 
ELSE 4 
END,
created_at DESC;
"""

# 3. Performance Monitoring View
performance_view_sql = """
CREATE OR REPLACE VIEW dev_price_unification_performance AS
WITH run_performance AS (
SELECT 
r.id as run_id,
r.run_type,
r.status,
r.start_time,
r.end_time,
EXTRACT(EPOCH FROM (r.end_time - r.start_time))::INTEGER as duration_seconds,
r.total_symbols,
r.total_price_points,
r.successful_unifications,
r.processing_rate_per_second,
r.peak_memory_usage_mb,
r.database_queries_executed,
r.avg_confidence_score,
r.polygon_coverage_pct,
r.tiingo_coverage_pct,
r.fmp_coverage_pct,
r.statistical_outliers,
r.vendor_disagreements,
ROUND((r.successful_unifications::NUMERIC / NULLIF(r.total_price_points, 0)) * 100, 2) as success_rate_pct,
ROW_NUMBER() OVER (PARTITION BY r.run_type ORDER BY r.start_time DESC) as run_rank
FROM dev_runs r
WHERE r.start_time >= CURRENT_DATE - INTERVAL '30 days'
AND r.run_type LIKE '%price_unification%'
),
performance_trends AS (
SELECT 
run_type,
AVG(duration_seconds) as avg_duration_seconds,
AVG(processing_rate_per_second) as avg_processing_rate,
AVG(success_rate_pct) as avg_success_rate,
AVG(avg_confidence_score) as avg_confidence,
COUNT(*) as total_runs,
COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_runs,
COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_runs
FROM run_performance
GROUP BY run_type
)
SELECT 
rp.*,
pt.avg_duration_seconds as type_avg_duration,
pt.avg_processing_rate as type_avg_rate,
pt.avg_success_rate as type_avg_success_rate,
CASE 
WHEN rp.duration_seconds > pt.avg_duration_seconds * 1.5 THEN 'SLOW'
WHEN rp.success_rate_pct < pt.avg_success_rate * 0.9 THEN 'LOW_SUCCESS'
WHEN rp.avg_confidence_score < pt.avg_confidence * 0.9 THEN 'LOW_QUALITY'
ELSE 'NORMAL'
END as performance_status
FROM run_performance rp
JOIN performance_trends pt ON rp.run_type = pt.run_type
ORDER BY rp.start_time DESC;
"""

# 4. Data Coverage Monitoring View
coverage_view_sql = """
CREATE OR REPLACE VIEW dev_price_data_coverage AS
WITH date_coverage AS (
SELECT 
dp.date,
COUNT(DISTINCT dp.instrument_id) as symbols_with_data,
COUNT(*) as total_price_records,
COUNT(CASE WHEN dp.polygon_price IS NOT NULL THEN 1 END) as polygon_records,
COUNT(CASE WHEN dp.tiingo_price IS NOT NULL THEN 1 END) as tiingo_records,
COUNT(CASE WHEN dp.fmp_price IS NOT NULL THEN 1 END) as fmp_records,
AVG(dp.vendor_count) as avg_vendor_count,
AVG(dp.confidence_score) as avg_confidence,
COUNT(CASE WHEN pvs.code = 'valid' THEN 1 END) as valid_records
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
WHERE dp.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY dp.date
),
symbol_coverage AS (
SELECT 
i.symbol,
COUNT(DISTINCT dp.date) as dates_with_data,
MAX(dp.date) as latest_data_date,
AVG(dp.confidence_score) as symbol_avg_confidence,
AVG(dp.vendor_count) as symbol_avg_vendor_count,
COUNT(CASE WHEN pvs.code = 'valid' THEN 1 END) as symbol_valid_records,
COUNT(*) as symbol_total_records
FROM dev_instruments i
LEFT JOIN dev_daily_prices dp ON i.id = dp.instrument_id 
AND dp.date >= CURRENT_DATE - INTERVAL '30 days'
LEFT JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
GROUP BY i.symbol
HAVING COUNT(dp.date) > 0
)
SELECT 
dc.*,
ROUND((dc.polygon_records::NUMERIC / NULLIF(dc.total_price_records, 0)) * 100, 2) as polygon_coverage_pct,
ROUND((dc.tiingo_records::NUMERIC / NULLIF(dc.total_price_records, 0)) * 100, 2) as tiingo_coverage_pct,
ROUND((dc.fmp_records::NUMERIC / NULLIF(dc.total_price_records, 0)) * 100, 2) as fmp_coverage_pct,
ROUND((dc.valid_records::NUMERIC / NULLIF(dc.total_price_records, 0)) * 100, 2) as success_rate_pct,
CASE 
WHEN dc.date = CURRENT_DATE - INTERVAL '1 day' THEN 'LATEST'
WHEN dc.date >= CURRENT_DATE - INTERVAL '7 days' THEN 'RECENT'
ELSE 'HISTORICAL'
END as data_recency
FROM date_coverage dc
ORDER BY dc.date DESC;
"""

# 5. Vendor Performance Comparison View
vendor_comparison_sql = """
CREATE OR REPLACE VIEW dev_vendor_performance_comparison AS
WITH vendor_stats AS (
SELECT 
'polygon' as vendor_name,
COUNT(CASE WHEN dp.polygon_price IS NOT NULL THEN 1 END) as records_provided,
COUNT(CASE WHEN dp.polygon_price IS NOT NULL AND dp.primary_vendor = 'polygon' THEN 1 END) as records_selected_primary,
AVG(CASE WHEN dp.polygon_price IS NOT NULL THEN dp.confidence_score END) as avg_confidence_when_present,
COUNT(CASE WHEN dp.polygon_price IS NOT NULL AND pvs.code = 'valid' THEN 1 END) as valid_records_contributed
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
WHERE dp.date >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
'tiingo' as vendor_name,
COUNT(CASE WHEN dp.tiingo_price IS NOT NULL THEN 1 END) as records_provided,
COUNT(CASE WHEN dp.tiingo_price IS NOT NULL AND dp.primary_vendor = 'tiingo' THEN 1 END) as records_selected_primary,
AVG(CASE WHEN dp.tiingo_price IS NOT NULL THEN dp.confidence_score END) as avg_confidence_when_present,
COUNT(CASE WHEN dp.tiingo_price IS NOT NULL AND pvs.code = 'valid' THEN 1 END) as valid_records_contributed
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
WHERE dp.date >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
'fmp' as vendor_name,
COUNT(CASE WHEN dp.fmp_price IS NOT NULL THEN 1 END) as records_provided,
COUNT(CASE WHEN dp.fmp_price IS NOT NULL AND dp.primary_vendor = 'fmp' THEN 1 END) as records_selected_primary,
AVG(CASE WHEN dp.fmp_price IS NOT NULL THEN dp.confidence_score END) as avg_confidence_when_present,
COUNT(CASE WHEN dp.fmp_price IS NOT NULL AND pvs.code = 'valid' THEN 1 END) as valid_records_contributed
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
WHERE dp.date >= CURRENT_DATE - INTERVAL '30 days'
),
total_records AS (
SELECT COUNT(*) as total_unified_records
FROM dev_daily_prices
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
vs.*,
tr.total_unified_records,
ROUND((vs.records_provided::NUMERIC / tr.total_unified_records) * 100, 2) as coverage_percentage,
ROUND((vs.records_selected_primary::NUMERIC / NULLIF(vs.records_provided, 0)) * 100, 2) as primary_selection_rate,
ROUND((vs.valid_records_contributed::NUMERIC / NULLIF(vs.records_provided, 0)) * 100, 2) as quality_contribution_rate
FROM vendor_stats vs
CROSS JOIN total_records tr
ORDER BY vs.records_provided DESC;
"""

# Execute all view creation statements
views = [
("Quality Dashboard", quality_dashboard_sql),
("Quality Alerts", alerts_view_sql),
("Performance Monitoring", performance_view_sql),
("Data Coverage", coverage_view_sql),
("Vendor Comparison", vendor_comparison_sql)
]

for view_name, sql in views:
try:
await self.conn.execute(sql)
self.logger.info(f"✅ Created {view_name} view")
except Exception as e:
self.logger.error(f"❌ Failed to create {view_name} view: {e}")
raise

async def create_monitoring_functions(self):
"""Create helper functions for monitoring dashboard"""

# Function to get current system health
health_function_sql = """
CREATE OR REPLACE FUNCTION get_price_unification_health()
RETURNS TABLE(
metric_name TEXT,
metric_value NUMERIC,
status TEXT,
description TEXT
) AS $$
BEGIN
-- Latest run success rate
RETURN QUERY
WITH latest_run AS (
SELECT 
successful_unifications::NUMERIC / NULLIF(total_price_points, 0) * 100 as success_rate,
avg_confidence_score,
processing_rate_per_second,
end_time
FROM dev_runs 
WHERE run_type LIKE '%price_unification%' 
AND status = 'completed'
ORDER BY end_time DESC 
LIMIT 1
),
today_stats AS (
SELECT 
COUNT(*) as records_today,
AVG(confidence_score) as avg_confidence_today,
COUNT(CASE WHEN pvs.code = 'valid' THEN 1 END)::NUMERIC / COUNT(*) * 100 as success_rate_today
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
WHERE DATE(dp.created_at) = CURRENT_DATE
)
SELECT 'success_rate'::TEXT, lr.success_rate, 
CASE WHEN lr.success_rate >= 95 THEN 'HEALTHY'
WHEN lr.success_rate >= 90 THEN 'WARNING' 
ELSE 'CRITICAL' END,
'Latest run success rate'::TEXT
FROM latest_run lr

UNION ALL

SELECT 'avg_confidence'::TEXT, lr.avg_confidence_score,
CASE WHEN lr.avg_confidence_score >= 0.8 THEN 'HEALTHY'
WHEN lr.avg_confidence_score >= 0.6 THEN 'WARNING'
ELSE 'CRITICAL' END,
'Average confidence score'::TEXT
FROM latest_run lr

UNION ALL

SELECT 'processing_rate'::TEXT, lr.processing_rate_per_second,
CASE WHEN lr.processing_rate_per_second >= 100 THEN 'HEALTHY'
WHEN lr.processing_rate_per_second >= 50 THEN 'WARNING'
ELSE 'CRITICAL' END,
'Processing rate (records/sec)'::TEXT
FROM latest_run lr

UNION ALL

SELECT 'records_today'::TEXT, ts.records_today,
CASE WHEN ts.records_today >= 1000 THEN 'HEALTHY'
WHEN ts.records_today >= 100 THEN 'WARNING'
ELSE 'CRITICAL' END,
'Records processed today'::TEXT
FROM today_stats ts;
END;
$$ LANGUAGE plpgsql;
"""

# Function to get trending metrics
trending_function_sql = """
CREATE OR REPLACE FUNCTION get_price_unification_trends()
RETURNS TABLE(
trend_period TEXT,
success_rate_trend NUMERIC,
confidence_trend NUMERIC,
volume_trend NUMERIC,
trend_direction TEXT
) AS $$
BEGIN
RETURN QUERY
WITH weekly_stats AS (
SELECT 
DATE_TRUNC('week', dp.created_at) as week_start,
COUNT(CASE WHEN pvs.code = 'valid' THEN 1 END)::NUMERIC / COUNT(*) * 100 as week_success_rate,
AVG(dp.confidence_score) as week_avg_confidence,
COUNT(*) as week_volume
FROM dev_daily_prices dp
JOIN dev_price_validation_status pvs ON dp.validation_status_id = pvs.id
WHERE dp.created_at >= CURRENT_DATE - INTERVAL '4 weeks'
GROUP BY DATE_TRUNC('week', dp.created_at)
ORDER BY week_start
),
trends AS (
SELECT 
'last_4_weeks'::TEXT as period,
(LAST_VALUE(week_success_rate) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 
FIRST_VALUE(week_success_rate) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as success_trend,
(LAST_VALUE(week_avg_confidence) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 
FIRST_VALUE(week_avg_confidence) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as confidence_trend,
(LAST_VALUE(week_volume) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 
FIRST_VALUE(week_volume) OVER (ORDER BY week_start ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as volume_trend
FROM weekly_stats
LIMIT 1
)
SELECT 
period,
success_trend,
confidence_trend,
volume_trend::NUMERIC,
CASE 
WHEN success_trend > 2 AND confidence_trend > 0.05 THEN 'IMPROVING'
WHEN success_trend < -2 OR confidence_trend < -0.05 THEN 'DECLINING'
ELSE 'STABLE'
END as direction
FROM trends;
END;
$$ LANGUAGE plpgsql;
"""

# Execute function creation
functions = [
("System Health Function", health_function_sql),
("Trending Metrics Function", trending_function_sql)
]

for func_name, sql in functions:
try:
await self.conn.execute(sql)
self.logger.info(f"✅ Created {func_name}")
except Exception as e:
self.logger.error(f"❌ Failed to create {func_name}: {e}")
raise

async def run_monitoring_tests(self):
"""Test the monitoring views and functions"""

self.logger.info("🧪 Testing monitoring views and functions...")

# Test each view
tests = [
("Quality Dashboard", "SELECT COUNT(*) FROM dev_price_quality_dashboard"),
("Quality Alerts", "SELECT COUNT(*) FROM dev_price_quality_alerts"),
("Performance Monitoring", "SELECT COUNT(*) FROM dev_price_unification_performance"),
("Data Coverage", "SELECT COUNT(*) FROM dev_price_data_coverage"),
("Vendor Comparison", "SELECT COUNT(*) FROM dev_vendor_performance_comparison"),
("System Health Function", "SELECT COUNT(*) FROM get_price_unification_health()"),
("Trending Function", "SELECT COUNT(*) FROM get_price_unification_trends()")
]

for test_name, query in tests:
try:
result = await self.conn.fetchval(query)
self.logger.info(f"✅ {test_name}: {result} records")
except Exception as e:
self.logger.error(f"❌ {test_name} test failed: {e}")

async def generate_sample_dashboard_data(self):
"""Generate sample dashboard queries for demonstration"""

self.logger.info("📊 Generating sample dashboard data...")

# Sample queries that would be used in a real dashboard
sample_queries = {
"current_health": """
SELECT metric_name, metric_value, status, description 
FROM get_price_unification_health()
ORDER BY 
CASE status 
WHEN 'CRITICAL' THEN 1 
WHEN 'WARNING' THEN 2 
ELSE 3 
END;
""",

"recent_alerts": """
SELECT symbol, alert_type, alert_severity, validation_status, 
confidence_score, date, created_at
FROM dev_price_quality_alerts 
WHERE alert_severity IN ('HIGH', 'MEDIUM')
ORDER BY 
CASE alert_severity 
WHEN 'HIGH' THEN 1 
WHEN 'MEDIUM' THEN 2 
ELSE 3 
END,
created_at DESC 
LIMIT 10;
""",

"daily_processing_summary": """
SELECT processing_date, total_records, success_rate_pct, 
avg_confidence, unique_symbols,
polygon_coverage_pct, tiingo_coverage_pct, fmp_coverage_pct
FROM dev_price_quality_dashboard 
WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY processing_date DESC;
""",

"vendor_performance": """
SELECT vendor_name, coverage_percentage, 
primary_selection_rate, quality_contribution_rate,
avg_confidence_when_present
FROM dev_vendor_performance_comparison
ORDER BY coverage_percentage DESC;
""",

"latest_run_performance": """
SELECT run_type, duration_seconds, processing_rate_per_second,
success_rate_pct, avg_confidence_score, performance_status,
total_symbols, successful_unifications
FROM dev_price_unification_performance
WHERE run_rank = 1
ORDER BY start_time DESC;
"""
}

# Execute sample queries and log results
for query_name, sql in sample_queries.items():
try:
rows = await self.conn.fetch(sql)
self.logger.info(f"📈 {query_name}: {len(rows)} records available")

# Log first few results for demonstration
if rows and len(rows) > 0:
self.logger.info(f"   Sample: {dict(rows[0])}")

except Exception as e:
self.logger.error(f"❌ {query_name} query failed: {e}")

async def run(self):
"""Main execution method for monitoring dashboard setup"""
self.logger.info("🚀 Setting up monitoring dashboards and views")

try:
await self.connect()

# Create all monitoring views
self.logger.info("📊 Creating monitoring views...")
await self.create_quality_monitoring_views()

# Create monitoring functions
self.logger.info("🔧 Creating monitoring functions...")
await self.create_monitoring_functions()

# Test everything
self.logger.info("🧪 Testing monitoring setup...")
await self.run_monitoring_tests()

# Generate sample data
self.logger.info("📈 Generating sample dashboard data...")
await self.generate_sample_dashboard_data()

self.logger.info("🎉 Monitoring dashboard setup completed successfully!")
self.logger.info("📊 Available monitoring views:")
self.logger.info("   - dev_price_quality_dashboard")
self.logger.info("   - dev_price_quality_alerts") 
self.logger.info("   - dev_price_unification_performance")
self.logger.info("   - dev_price_data_coverage")
self.logger.info("   - dev_vendor_performance_comparison")
self.logger.info("🔧 Available monitoring functions:")
self.logger.info("   - get_price_unification_health()")
self.logger.info("   - get_price_unification_trends()")

except Exception as e:
self.logger.error(f"💥 Monitoring setup failed: {e}")
raise
finally:
await self.disconnect()

async def main():
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(levelname)s - %(message)s'
)

setup = MonitoringDashboardSetup()
await setup.run()

if __name__ == "__main__":
asyncio.run(main())

generate_monitoring_report.py: |
"""
Generate Real-time Monitoring Report

Creates a comprehensive monitoring report showing current system status,
trends, and quality metrics.
"""
import asyncio
import asyncpg
import logging
import json
import os
from datetime import date, datetime, timedelta
from environment import Environment

class MonitoringReportGenerator:
def __init__(self):
self.env = Environment()
self.conn = None
self.logger = logging.getLogger(__name__)

async def connect(self):
"""Connect to database"""
self.conn = await asyncpg.connect(self.env.get_database_url())
self.logger.info("✅ Connected to database")

async def disconnect(self):
"""Disconnect from database"""
if self.conn:
await self.conn.close()

async def generate_comprehensive_report(self):
"""Generate comprehensive monitoring report"""

report = {
'generated_at': datetime.now().isoformat(),
'report_type': 'price_unification_monitoring',
'sections': {}
}

# 1. System Health Overview
health_data = await self.conn.fetch("SELECT * FROM get_price_unification_health()")
report['sections']['system_health'] = {
'status': 'HEALTHY' if all(row['status'] == 'HEALTHY' for row in health_data) else 'WARNING',
'metrics': [dict(row) for row in health_data]
}

# 2. Current Alerts
alerts_data = await self.conn.fetch("""
SELECT * FROM dev_price_quality_alerts 
WHERE alert_severity IN ('HIGH', 'MEDIUM') 
ORDER BY 
CASE alert_severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
created_at DESC 
LIMIT 20
""")
report['sections']['active_alerts'] = {
'total_alerts': len(alerts_data),
'high_severity': sum(1 for row in alerts_data if row['alert_severity'] == 'HIGH'),
'medium_severity': sum(1 for row in alerts_data if row['alert_severity'] == 'MEDIUM'),
'alerts': [dict(row) for row in alerts_data]
}

# 3. Processing Performance
performance_data = await self.conn.fetch("""
SELECT * FROM dev_price_unification_performance 
WHERE run_rank <= 5
ORDER BY start_time DESC
""")
report['sections']['recent_performance'] = {
'recent_runs': [dict(row) for row in performance_data]
}

# 4. Data Quality Trends
trends_data = await self.conn.fetch("SELECT * FROM get_price_unification_trends()")
report['sections']['quality_trends'] = {
'trends': [dict(row) for row in trends_data]
}

# 5. Vendor Performance
vendor_data = await self.conn.fetch("""
SELECT * FROM dev_vendor_performance_comparison 
ORDER BY coverage_percentage DESC
""")
report['sections']['vendor_performance'] = {
'vendors': [dict(row) for row in vendor_data]
}

# 6. Daily Processing Summary
daily_data = await self.conn.fetch("""
SELECT * FROM dev_price_quality_dashboard 
WHERE processing_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY processing_date DESC
""")
report['sections']['daily_processing'] = {
'last_7_days': [dict(row) for row in daily_data]
}

return report

async def print_summary_report(self, report):
"""Print a human-readable summary of the monitoring report"""

print("\n" + "="*80)
print("📊 PRICE UNIFICATION SYSTEM MONITORING REPORT")
print("="*80)
print(f"🕐 Generated: {report['generated_at']}")

# System Health
print(f"\n🏥 SYSTEM HEALTH: {report['sections']['system_health']['status']}")
for metric in report['sections']['system_health']['metrics']:
status_emoji = "✅" if metric['status'] == 'HEALTHY' else "⚠️" if metric['status'] == 'WARNING' else "🚨"
print(f"   {status_emoji} {metric['description']}: {metric['metric_value']:.2f} ({metric['status']})")

# Alerts
alerts = report['sections']['active_alerts']
print(f"\n🚨 ACTIVE ALERTS: {alerts['total_alerts']} total")
print(f"   🔴 High severity: {alerts['high_severity']}")
print(f"   🟡 Medium severity: {alerts['medium_severity']}")

if alerts['alerts']:
print("   Recent alerts:")
for alert in alerts['alerts'][:5]:  # Show top 5
severity_emoji = "🔴" if alert['alert_severity'] == 'HIGH' else "🟡"
print(f"     {severity_emoji} {alert['symbol']}: {alert['alert_type']} (confidence: {alert['confidence_score']:.2f})")

# Performance
performance = report['sections']['recent_performance']['recent_runs']
if performance:
latest = performance[0]
print(f"\n⚡ LATEST RUN PERFORMANCE:")
print(f"   📈 Success Rate: {latest['success_rate_pct']:.1f}%")
print(f"   🎯 Avg Confidence: {latest['avg_confidence_score']:.3f}")
print(f"   ⏱️  Processing Rate: {latest['processing_rate_per_second']:.1f} records/sec")
print(f"   📊 Records Processed: {latest['successful_unifications']:,}")
print(f"   🕐 Duration: {latest['duration_seconds']:,} seconds")

# Vendor Performance
vendors = report['sections']['vendor_performance']['vendors']
print(f"\n🏪 VENDOR PERFORMANCE:")
for vendor in vendors:
print(f"   📡 {vendor['vendor_name'].upper()}: {vendor['coverage_percentage']:.1f}% coverage, " +
f"{vendor['quality_contribution_rate']:.1f}% quality rate")

# Trends
trends = report['sections']['quality_trends']['trends']
if trends:
trend = trends[0]
trend_emoji = "📈" if trend['trend_direction'] == 'IMPROVING' else "📉" if trend['trend_direction'] == 'DECLINING' else "➡️"
print(f"\n{trend_emoji} TRENDS (4-week): {trend['trend_direction']}")
print(f"   📊 Success Rate Change: {trend['success_rate_trend']:.2f}%")
print(f"   🎯 Confidence Change: {trend['confidence_trend']:.3f}")
print(f"   📈 Volume Change: {trend['volume_trend']:.0f} records")

# Daily Summary
daily = report['sections']['daily_processing']['last_7_days']
if daily:
print(f"\n📅 DAILY PROCESSING SUMMARY (Last 7 days):")
for day in daily[:3]:  # Show last 3 days
print(f"   📆 {day['processing_date']}: {day['total_records']:,} records, " +
f"{day['success_rate_pct']:.1f}% success, {day['unique_symbols']} symbols")

print("\n" + "="*80)

async def run(self):
"""Generate and display monitoring report"""
self.logger.info("📊 Generating comprehensive monitoring report...")

try:
await self.connect()

# Generate comprehensive report
report = await self.generate_comprehensive_report()

# Print summary
await self.print_summary_report(report)

# Save detailed report
report_file = f"/tmp/monitoring_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(report_file, 'w') as f:
json.dump(report, f, indent=2, default=str)

self.logger.info(f"📄 Detailed report saved to: {report_file}")
self.logger.info("🎉 Monitoring report generation completed!")

except Exception as e:
self.logger.error(f"💥 Report generation failed: {e}")
raise
finally:
await self.disconnect()

async def main():
logging.basicConfig(
level=logging.INFO,
format='%(asctime)s - %(levelname)s - %(message)s'
)

generator = MonitoringReportGenerator()
await generator.run()

if __name__ == "__main__":
asyncio.run(main())
