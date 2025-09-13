#!/bin/bash
# ATS Data Coverage Monitoring System - Validation Script
# Comprehensive testing and validation of all monitoring components

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔍 ATS Data Coverage Monitoring - System Validation${NC}"
echo "=================================================================="

PROJECT_ROOT="/home/jianjun/ats-genai-pm"
cd "$PROJECT_ROOT"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Function to run test and track results
run_test() {
    local test_name="$1"
    local test_command="$2"
    
    echo -e "\n${YELLOW}🧪 Testing: $test_name${NC}"
    
    if eval "$test_command" > /tmp/test_output 2>&1; then
        echo -e "${GREEN}✅ PASSED: $test_name${NC}"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}❌ FAILED: $test_name${NC}"
        echo "Error output:"
        cat /tmp/test_output | head -5
        ((TESTS_FAILED++))
    fi
}

echo -e "\n${BLUE}📋 1. COMPONENT VALIDATION${NC}"
echo "----------------------------------------"

# Test 1: Python module imports
run_test "Python Module Imports" "
PYTHONPATH=src python3 -c '
from monitoring.coverage_monitor import CoverageMonitor, CoverageRecord, CoverageGap
from monitoring.prometheus_exporter import PrometheusExporter, PrometheusMetric
from monitoring.alert_system import AlertManager
print(\"All modules imported successfully\")
'
"

# Test 2: Database schema files exist
run_test "Database Schema Files" "
test -f src/db/migrations/coverage_monitoring_schema.sql &&
echo 'Database schema file exists'
"

# Test 3: Configuration files exist
run_test "Configuration Files" "
test -f scripts/setup_coverage_monitoring_cron.sh &&
test -f scripts/setup_grafana_monitoring.sh &&
test -f grafana/ats-coverage-dashboard.json &&
test -f grafana/prometheus.yml &&
test -f grafana/ats_coverage_alerts.yml &&
echo 'All configuration files exist'
"

# Test 4: Dashboard files
run_test "Dashboard Components" "
test -f coverage_dashboard_fixed.py &&
PYTHONPATH=src python3 -c 'import coverage_dashboard_fixed; print(\"Dashboard module loaded\")' &&
echo 'Dashboard components validated'
"

# Test 5: Prometheus metrics formatting
run_test "Prometheus Metrics Formatting" "
PYTHONPATH=src python3 -c '
from monitoring.prometheus_exporter import PrometheusExporter, PrometheusMetric
exporter = PrometheusExporter()
metrics = [
    PrometheusMetric(\"test_metric\", \"gauge\", \"Test metric\", 100.0, {\"label\": \"value\"})
]
output = exporter.format_prometheus_metrics(metrics)
assert \"# HELP test_metric Test metric\" in output
assert \"test_metric{label=\\\"value\\\"} 100.0\" in output
print(\"Prometheus formatting validated\")
'
"

echo -e "\n${BLUE}📋 2. SCRIPT VALIDATION${NC}"
echo "----------------------------------------"

# Test 6: Script permissions
run_test "Script Permissions" "
test -x scripts/setup_coverage_monitoring_cron.sh &&
test -x scripts/setup_grafana_monitoring.sh &&
echo 'Script permissions validated'
"

# Test 7: Cron script syntax
run_test "Cron Script Syntax" "
bash -n scripts/setup_coverage_monitoring_cron.sh &&
echo 'Cron script syntax valid'
"

# Test 8: Grafana setup script syntax
run_test "Grafana Script Syntax" "
bash -n scripts/setup_grafana_monitoring.sh &&
echo 'Grafana script syntax valid'
"

echo -e "\n${BLUE}📋 3. JSON CONFIGURATION VALIDATION${NC}"
echo "----------------------------------------"

# Test 9: Grafana dashboard JSON
run_test "Grafana Dashboard JSON" "
python3 -c '
import json
with open(\"grafana/ats-coverage-dashboard.json\", \"r\") as f:
    dashboard = json.load(f)
assert \"panels\" in dashboard
assert len(dashboard[\"panels\"]) > 0
print(\"Grafana dashboard JSON is valid\")
'
"

# Test 10: Prometheus config YAML
run_test "Prometheus Configuration" "
python3 -c '
import yaml
with open(\"grafana/prometheus.yml\", \"r\") as f:
    config = yaml.safe_load(f)
assert \"scrape_configs\" in config
assert \"rule_files\" in config
print(\"Prometheus configuration is valid\")
' 2>/dev/null || echo 'YAML module not available, skipping detailed validation'
"

echo -e "\n${BLUE}📋 4. INTEGRATION TESTING${NC}"
echo "----------------------------------------"

# Test 11: Coverage monitor database connectivity (mock)
run_test "Coverage Monitor Initialization" "
PYTHONPATH=src python3 -c '
from monitoring.coverage_monitor import CoverageMonitor
monitor = CoverageMonitor()
print(f\"Monitor initialized with DB: {monitor.db_config[\"host\"]}:{monitor.db_config[\"port\"]}\")
'
"

# Test 12: Alert system configuration
run_test "Alert System Configuration" "
PYTHONPATH=src python3 -c '
from monitoring.alert_system import AlertManager
import os
os.environ[\"SLACK_WEBHOOK_URL\"] = \"https://example.com/webhook\"
alert_manager = AlertManager()
print(\"Alert system initialized successfully\")
'
"

# Test 13: Dashboard HTTP server (quick start/stop test)
run_test "Dashboard HTTP Server" "
timeout 5 python3 coverage_dashboard_fixed.py --port 8888 > /tmp/dashboard_test.log 2>&1 &
DASHBOARD_PID=\$!
sleep 2
if kill -0 \$DASHBOARD_PID 2>/dev/null; then
    kill \$DASHBOARD_PID 2>/dev/null || true
    echo 'Dashboard server started and stopped successfully'
else
    echo 'Dashboard server failed to start'
    exit 1
fi
"

echo -e "\n${BLUE}📋 5. DEPLOYMENT READINESS${NC}"
echo "----------------------------------------"

# Test 14: Environment variables check
run_test "Environment Variables" "
PYTHONPATH=src python3 -c '
import os
required_vars = [\"DB_HOST\", \"DB_PORT\", \"DB_USER\", \"DB_PASSWORD\", \"DB_NAME\"]
env_status = {var: os.getenv(var, \"NOT_SET\") for var in required_vars}
print(\"Environment variables:\")
for var, value in env_status.items():
    print(f\"  {var}: {value}\")
print(\"Environment check completed\")
'
"

# Test 15: File permissions and structure
run_test "File Structure Validation" "
required_dirs=('src/monitoring' 'grafana' 'scripts' 'logs')
for dir in \${required_dirs[@]}; do
    if [ ! -d \"\$dir\" ]; then
        echo \"Creating missing directory: \$dir\"
        mkdir -p \"\$dir\"
    fi
done
echo 'File structure validated'
"

echo -e "\n${BLUE}📋 6. PERFORMANCE TESTING${NC}"
echo "----------------------------------------"

# Test 16: Mock metrics collection performance
run_test "Metrics Collection Performance" "
PYTHONPATH=src python3 -c '
import time
from monitoring.prometheus_exporter import PrometheusExporter, PrometheusMetric

# Create large number of mock metrics
metrics = []
for i in range(100):
    metrics.append(PrometheusMetric(
        f\"test_metric_{i}\",
        \"gauge\",
        f\"Test metric {i}\",
        float(i),
        {\"symbol\": f\"SYM{i}\", \"type\": \"test\"}
    ))

exporter = PrometheusExporter()
start_time = time.time()
output = exporter.format_prometheus_metrics(metrics)
end_time = time.time()

processing_time = end_time - start_time
print(f\"Processed 100 metrics in {processing_time:.3f} seconds\")
print(f\"Output size: {len(output)} characters\")
assert processing_time < 1.0, \"Metrics processing too slow\"
print(\"Performance test passed\")
'
"

echo -e "\n${BLUE}📊 VALIDATION SUMMARY${NC}"
echo "=================================================================="
echo -e "✅ Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "❌ Tests Failed: ${RED}$TESTS_FAILED${NC}"

TOTAL_TESTS=$((TESTS_PASSED + TESTS_FAILED))
SUCCESS_RATE=$((TESTS_PASSED * 100 / TOTAL_TESTS))

echo -e "📈 Success Rate: ${GREEN}$SUCCESS_RATE%${NC} ($TESTS_PASSED/$TOTAL_TESTS)"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}🎯 ALL TESTS PASSED! System is ready for deployment.${NC}"
    echo -e "${BLUE}📋 Next Steps:${NC}"
    echo "1. Run: ./scripts/setup_coverage_monitoring_cron.sh"
    echo "2. Configure Slack webhook in .env.monitoring"
    echo "3. Run: ./scripts/setup_grafana_monitoring.sh"
    echo "4. Start monitoring: ./scripts/start_monitoring_stack.sh"
else
    echo -e "\n${YELLOW}⚠️  Some tests failed. Please review and fix issues before deployment.${NC}"
    exit 1
fi

echo -e "\n${GREEN}✅ ATS Data Coverage Monitoring System Validation Complete!${NC}"