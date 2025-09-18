#!/usr/bin/env python3
"""
Data Quality Dashboard API - Working implementation with real issue detection

Shows actual data quality issues detected in the ATS system with a functional web dashboard.
"""

import sys
from datetime import datetime, date
from typing import List, Optional
from dataclasses import dataclass, asdict

sys.path.insert(0, '/home/jianjun/ats-genai-model/src')

from flask import Flask, jsonify
from infrastructure.database.connection_manager import get_database_connection

app = Flask(__name__)


@dataclass
class DataQualityIssue:
    """Actual data quality issue found in the system."""
    id: str
    symbol: str
    issue_type: str
    severity: str  # 'critical', 'high', 'medium', 'low'
    description: str
    detected_at: datetime
    affected_date: date
    field: str
    expected_value: Optional[float]
    actual_value: Optional[float]
    vendor_source: str
    status: str  # 'open', 'reviewing', 'fixed', 'false_positive'
    reviewer: Optional[str] = None


class RealDataQualityDetector:
    """Real data quality detector that finds actual issues in the database."""

    def __init__(self):
        self.connection_config = {
            'host': 'localhost',
            'port': 4432,  # INTG database
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db'
        }

    async def detect_price_anomalies(self, days_back: int = 7) -> List[DataQualityIssue]:
        """Detect real price anomalies in the database."""
        issues = []

        try:
            conn = await get_database_connection('intg')

            # Find price gaps (missing days)
            gap_query = """
            WITH date_series AS (
                SELECT generate_series(
                    CURRENT_DATE - INTERVAL '%s days',
                    CURRENT_DATE,
                    '1 day'::interval
                )::date as expected_date
            ),
            actual_dates AS (
                SELECT DISTINCT date_trunc('day', timestamp)::date as actual_date
                FROM intg_daily_price_polygon
                WHERE timestamp >= CURRENT_DATE - INTERVAL '%s days'
            )
            SELECT ds.expected_date
            FROM date_series ds
            LEFT JOIN actual_dates ad ON ds.expected_date = ad.actual_date
            WHERE ad.actual_date IS NULL
            AND EXTRACT(dow FROM ds.expected_date) NOT IN (0, 6)  -- Exclude weekends
            ORDER BY ds.expected_date;
            """ % (days_back, days_back)

            async with conn.cursor() as cursor:
                await cursor.execute(gap_query)
                missing_dates = await cursor.fetchall()

                for (missing_date,) in missing_dates:
                    issues.append(DataQualityIssue(
                        id=f"gap_{missing_date}",
                        symbol="ALL",
                        issue_type="missing_data",
                        severity="high",
                        description=f"No daily prices found for {missing_date}",
                        detected_at=datetime.now(),
                        affected_date=missing_date,
                        field="all_fields",
                        expected_value=None,
                        actual_value=None,
                        vendor_source="multiple",
                        status="open"
                    ))

            # Find extreme price movements
            extreme_query = """
            SELECT symbol, date_trunc('day', timestamp)::date as price_date,
                   open_price, high_price, low_price, close_price, volume,
                   LAG(close_price) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_close
            FROM intg_daily_price_polygon
            WHERE timestamp >= CURRENT_DATE - INTERVAL '%s days'
            AND (high_price / low_price > 1.5  -- 50%% intraday range
                 OR volume > 100000000)        -- 100M+ volume
            ORDER BY timestamp DESC;
            """ % days_back

            await cursor.execute(extreme_query)
            extreme_prices = await cursor.fetchall()

            for row in extreme_prices:
                symbol, price_date, open_p, high_p, low_p, close_p, volume, prev_close = row

                # Check for extreme intraday movement
                if high_p and low_p and high_p / low_p > 1.5:
                    intraday_range = (high_p - low_p) / close_p
                    issues.append(DataQualityIssue(
                        id=f"extreme_range_{symbol}_{price_date}",
                        symbol=symbol,
                        issue_type="extreme_price_range",
                        severity="medium",
                        description=f"Extreme intraday range: {intraday_range:.1%} (High: ${high_p}, Low: ${low_p})",
                        detected_at=datetime.now(),
                        affected_date=price_date,
                        field="high_low_range",
                        expected_value=close_p * 0.1,  # 10% range expected
                        actual_value=high_p - low_p,
                        vendor_source="polygon",
                        status="open"
                    ))

                # Check for extreme volume
                if volume and volume > 100000000:
                    issues.append(DataQualityIssue(
                        id=f"high_volume_{symbol}_{price_date}",
                        symbol=symbol,
                        issue_type="extreme_volume",
                        severity="medium",
                        description=f"Extremely high volume: {volume:,} shares",
                        detected_at=datetime.now(),
                        affected_date=price_date,
                        field="volume",
                        expected_value=10000000,  # 10M expected
                        actual_value=volume,
                        vendor_source="polygon",
                        status="open"
                    ))

                # Check for price gaps vs previous close
                if prev_close and close_p and abs(close_p - prev_close) / prev_close > 0.2:
                    gap_pct = (close_p - prev_close) / prev_close
                    issues.append(DataQualityIssue(
                        id=f"price_gap_{symbol}_{price_date}",
                        symbol=symbol,
                        issue_type="price_gap",
                        severity="high",
                        description=f"Large price gap: {gap_pct:.1%} from previous close",
                        detected_at=datetime.now(),
                        affected_date=price_date,
                        field="close_price",
                        expected_value=prev_close,
                        actual_value=close_p,
                        vendor_source="polygon",
                        status="open"
                    ))

            # Find duplicate records
            duplicate_query = """
            SELECT symbol, date_trunc('day', timestamp)::date as price_date, COUNT(*)
            FROM intg_daily_price_polygon
            WHERE timestamp >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY symbol, date_trunc('day', timestamp)::date
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC;
            """ % days_back

            await cursor.execute(duplicate_query)
            duplicates = await cursor.fetchall()

            for symbol, price_date, count in duplicates:
                issues.append(DataQualityIssue(
                    id=f"duplicate_{symbol}_{price_date}",
                    symbol=symbol,
                    issue_type="duplicate_records",
                    severity="critical",
                    description=f"Found {count} duplicate records for the same date",
                    detected_at=datetime.now(),
                    affected_date=price_date,
                    field="all_fields",
                    expected_value=1,
                    actual_value=count,
                    vendor_source="multiple",
                    status="open"
                ))

            await conn.close()

        except Exception as e:
            print(f"Error detecting anomalies: {e}")
            # Add error as an issue
            issues.append(DataQualityIssue(
                id=f"detection_error_{datetime.now().timestamp()}",
                symbol="SYSTEM",
                issue_type="detection_error",
                severity="critical",
                description=f"Data quality detection failed: {str(e)}",
                detected_at=datetime.now(),
                affected_date=date.today(),
                field="system",
                expected_value=None,
                actual_value=None,
                vendor_source="system",
                status="open"
            ))

        return issues


detector = RealDataQualityDetector()


@app.route('/data-quality/dashboard')
def dashboard():
    """Main data quality dashboard showing real issues."""

    dashboard_html = """
<!DOCTYPE html>
<html>
<head>
    <title>ATS Data Quality Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; margin: -20px -20px 20px -20px; }
        .header h1 { margin: 0; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stat-number { font-size: 2em; font-weight: bold; color: #e74c3c; }
        .stat-label { color: #7f8c8d; margin-top: 5px; }
        .issues-section { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .issue-item { border-left: 4px solid #e74c3c; margin: 10px 0; padding: 15px; background: #fff; border-radius: 4px; }
        .issue-critical { border-left-color: #e74c3c; }
        .issue-high { border-left-color: #f39c12; }
        .issue-medium { border-left-color: #f1c40f; }
        .issue-low { border-left-color: #27ae60; }
        .issue-header { display: flex; justify-content: between; align-items: center; margin-bottom: 10px; }
        .issue-title { font-weight: bold; color: #2c3e50; }
        .issue-severity { padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }
        .severity-critical { background: #e74c3c; color: white; }
        .severity-high { background: #f39c12; color: white; }
        .severity-medium { background: #f1c40f; color: black; }
        .severity-low { background: #27ae60; color: white; }
        .issue-meta { color: #7f8c8d; font-size: 0.9em; margin-top: 5px; }
        .refresh-btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        .refresh-btn:hover { background: #2980b9; }
        .loading { text-align: center; padding: 40px; color: #7f8c8d; }
        .charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
        .chart-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 ATS Data Quality Dashboard</h1>
        <p>Real-time monitoring of data quality issues in the ATS system</p>
        <button class="refresh-btn" onclick="refreshDashboard()">🔄 Refresh Data</button>
    </div>

    <div id="stats" class="stats-grid">
        <div class="stat-card">
            <div class="stat-number" id="total-issues">-</div>
            <div class="stat-label">Total Issues</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="critical-issues">-</div>
            <div class="stat-label">Critical Issues</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="symbols-affected">-</div>
            <div class="stat-label">Symbols Affected</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="last-updated">-</div>
            <div class="stat-label">Last Updated</div>
        </div>
    </div>

    <div class="charts-grid">
        <div class="chart-container">
            <h3>Issues by Severity</h3>
            <canvas id="severityChart" width="400" height="200"></canvas>
        </div>
        <div class="chart-container">
            <h3>Issues by Type</h3>
            <canvas id="typeChart" width="400" height="200"></canvas>
        </div>
    </div>

    <div class="issues-section">
        <h2>🔍 Detected Issues</h2>
        <div id="issues-list" class="loading">Loading data quality issues...</div>
    </div>

    <script>
        let severityChart, typeChart;

        async function loadDashboardData() {
            try {
                const response = await fetch('/data-quality/api/issues');
                const data = await response.json();

                updateStats(data.issues);
                updateIssuesList(data.issues);
                updateCharts(data.issues);

            } catch (error) {
                document.getElementById('issues-list').innerHTML =
                    `<div style="color: #e74c3c;">❌ Error loading data: ${error.message}</div>`;
            }
        }

        function updateStats(issues) {
            const totalIssues = issues.length;
            const criticalIssues = issues.filter(i => i.severity === 'critical').length;
            const uniqueSymbols = [...new Set(issues.map(i => i.symbol))].length;

            document.getElementById('total-issues').textContent = totalIssues;
            document.getElementById('critical-issues').textContent = criticalIssues;
            document.getElementById('symbols-affected').textContent = uniqueSymbols;
            document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
        }

        function updateIssuesList(issues) {
            const container = document.getElementById('issues-list');

            if (issues.length === 0) {
                container.innerHTML = '<div style="color: #27ae60; text-align: center; padding: 40px;">✅ No data quality issues detected!</div>';
                return;
            }

            const issuesHtml = issues.map(issue => `
                <div class="issue-item issue-${issue.severity}">
                    <div class="issue-header">
                        <div class="issue-title">${issue.symbol}: ${issue.description}</div>
                        <span class="issue-severity severity-${issue.severity}">${issue.severity.toUpperCase()}</span>
                    </div>
                    <div class="issue-meta">
                        📅 ${issue.affected_date} | 🏷️ ${issue.issue_type} | 📊 Field: ${issue.field} | 📡 ${issue.vendor_source}
                        ${issue.expected_value !== null ? `| Expected: ${issue.expected_value} | Actual: ${issue.actual_value}` : ''}
                    </div>
                </div>
            `).join('');

            container.innerHTML = issuesHtml;
        }

        function updateCharts(issues) {
            // Severity distribution
            const severityCounts = {
                critical: issues.filter(i => i.severity === 'critical').length,
                high: issues.filter(i => i.severity === 'high').length,
                medium: issues.filter(i => i.severity === 'medium').length,
                low: issues.filter(i => i.severity === 'low').length
            };

            // Issue type distribution
            const typeCounts = {};
            issues.forEach(issue => {
                typeCounts[issue.issue_type] = (typeCounts[issue.issue_type] || 0) + 1;
            });

            // Update severity chart
            const severityCtx = document.getElementById('severityChart').getContext('2d');
            if (severityChart) severityChart.destroy();
            severityChart = new Chart(severityCtx, {
                type: 'doughnut',
                data: {
                    labels: ['Critical', 'High', 'Medium', 'Low'],
                    datasets: [{
                        data: [severityCounts.critical, severityCounts.high, severityCounts.medium, severityCounts.low],
                        backgroundColor: ['#e74c3c', '#f39c12', '#f1c40f', '#27ae60']
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'bottom' }
                    }
                }
            });

            // Update type chart
            const typeCtx = document.getElementById('typeChart').getContext('2d');
            if (typeChart) typeChart.destroy();
            typeChart = new Chart(typeCtx, {
                type: 'bar',
                data: {
                    labels: Object.keys(typeCounts),
                    datasets: [{
                        label: 'Issues',
                        data: Object.values(typeCounts),
                        backgroundColor: '#3498db'
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { display: false }
                    },
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });
        }

        function refreshDashboard() {
            document.getElementById('issues-list').innerHTML = '<div class="loading">Refreshing data...</div>';
            loadDashboardData();
        }

        // Load data on page load
        loadDashboardData();

        // Auto-refresh every 30 seconds
        setInterval(loadDashboardData, 30000);
    </script>
</body>
</html>
    """

    return dashboard_html


@app.route('/data-quality/api/issues')
async def get_issues():
    """API endpoint returning actual detected data quality issues."""
    try:
        issues = await detector.detect_price_anomalies(days_back=7)

        # Convert to JSON serializable format
        issues_dict = []
        for issue in issues:
            issue_dict = asdict(issue)
            # Convert datetime objects to strings
            issue_dict['detected_at'] = issue.detected_at.isoformat()
            issue_dict['affected_date'] = issue.affected_date.isoformat()
            issues_dict.append(issue_dict)

        return jsonify({
            'issues': issues_dict,
            'total_count': len(issues_dict),
            'last_updated': datetime.now().isoformat(),
            'detection_period_days': 7
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'issues': [],
            'total_count': 0,
            'last_updated': datetime.now().isoformat()
        }), 500


@app.route('/data-quality/api/issue/<issue_id>')
async def get_issue_detail(issue_id):
    """Get detailed information about a specific issue."""
    try:
        issues = await detector.detect_price_anomalies(days_back=30)  # Longer period for specific issue

        issue = next((i for i in issues if i.id == issue_id), None)
        if not issue:
            return jsonify({'error': 'Issue not found'}), 404

        issue_dict = asdict(issue)
        issue_dict['detected_at'] = issue.detected_at.isoformat()
        issue_dict['affected_date'] = issue.affected_date.isoformat()

        return jsonify(issue_dict)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/data-quality/api/stats')
async def get_stats():
    """Get data quality statistics and metrics."""
    try:
        issues = await detector.detect_price_anomalies(days_back=7)

        # Calculate statistics
        total_issues = len(issues)
        severity_counts = {}
        type_counts = {}
        symbol_counts = {}

        for issue in issues:
            severity_counts[issue.severity] = severity_counts.get(issue.severity, 0) + 1
            type_counts[issue.issue_type] = type_counts.get(issue.issue_type, 0) + 1
            symbol_counts[issue.symbol] = symbol_counts.get(issue.symbol, 0) + 1

        return jsonify({
            'total_issues': total_issues,
            'severity_distribution': severity_counts,
            'issue_type_distribution': type_counts,
            'most_affected_symbols': dict(sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'last_updated': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    print("🎯 Starting ATS Data Quality Dashboard...")
    print("📊 Dashboard URL: http://localhost:5001/data-quality/dashboard")
    print("🔍 API endpoint: http://localhost:5001/data-quality/api/issues")
    print()

    app.run(host='0.0.0.0', port=5001, debug=True)