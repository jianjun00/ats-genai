#!/bin/bash
"""
Setup Grafana Integration for ATS Data Coverage Monitoring
Configures Prometheus, Grafana, and monitoring stack
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔧 ATS Data Coverage Monitoring - Grafana Setup${NC}"
echo "=============================================================="

# Project root directory
PROJECT_ROOT="/home/jianjun/ats-genai-pm"
GRAFANA_DIR="$PROJECT_ROOT/grafana"
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/docker-compose.monitoring.yml"

# Create Grafana directory
echo -e "${YELLOW}📁 Creating Grafana configuration directory...${NC}"
mkdir -p "$GRAFANA_DIR/provisioning/dashboards"
mkdir -p "$GRAFANA_DIR/provisioning/datasources"

# Create Grafana datasource configuration
echo -e "${YELLOW}⚙️ Creating Grafana datasource configuration...${NC}"
cat > "$GRAFANA_DIR/provisioning/datasources/prometheus.yml" << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
EOF

# Create Grafana dashboard provisioning
echo -e "${YELLOW}📊 Creating Grafana dashboard provisioning...${NC}"
cat > "$GRAFANA_DIR/provisioning/dashboards/dashboard.yml" << 'EOF'
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
EOF

# Create Docker Compose file for monitoring stack
echo -e "${YELLOW}🐳 Creating Docker Compose monitoring stack...${NC}"
cat > "$DOCKER_COMPOSE_FILE" << 'EOF'
version: '3.8'

networks:
  ats-monitoring:
    driver: bridge

volumes:
  prometheus_data:
  grafana_data:

services:
  prometheus:
    image: prom/prometheus:latest
    container_name: ats-prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--storage.tsdb.retention.time=200h'
      - '--web.enable-lifecycle'
    restart: unless-stopped
    expose:
      - 9090
    ports:
      - "9090:9090"
    volumes:
      - ./grafana/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./grafana/ats_coverage_alerts.yml:/etc/prometheus/ats_coverage_alerts.yml
      - prometheus_data:/prometheus
    networks:
      - ats-monitoring

  grafana:
    image: grafana/grafana-oss:latest
    container_name: ats-grafana
    restart: unless-stopped
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=ats_admin_2024
      - GF_USERS_ALLOW_SIGN_UP=false
    expose:
      - 3000
    ports:
      - "3000:3000"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/ats-coverage-dashboard.json:/etc/grafana/provisioning/dashboards/ats-coverage-dashboard.json
    networks:
      - ats-monitoring
    depends_on:
      - prometheus

  node-exporter:
    image: prom/node-exporter:latest
    container_name: ats-node-exporter
    restart: unless-stopped
    command:
      - '--path.procfs=/host/proc'
      - '--path.rootfs=/rootfs'
      - '--path.sysfs=/host/sys'
      - '--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)'
    expose:
      - 9100
    ports:
      - "9100:9100"
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    networks:
      - ats-monitoring

  alertmanager:
    image: prom/alertmanager:latest
    container_name: ats-alertmanager
    restart: unless-stopped
    expose:
      - 9093
    ports:
      - "9093:9093"
    volumes:
      - ./grafana/alertmanager.yml:/etc/alertmanager/alertmanager.yml
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
      - '--storage.path=/alertmanager'
      - '--web.external-url=http://localhost:9093'
    networks:
      - ats-monitoring
EOF

# Create Alertmanager configuration
echo -e "${YELLOW}🚨 Creating Alertmanager configuration...${NC}"
cat > "$GRAFANA_DIR/alertmanager.yml" << 'EOF'
global:
  smtp_smarthost: 'localhost:587'
  smtp_from: 'ats-monitoring@localhost'

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'web.hook'

receivers:
  - name: 'web.hook'
    slack_configs:
      - api_url: '${SLACK_WEBHOOK_URL}'
        channel: '#ats-data-alerts'
        username: 'ATS Coverage Monitor'
        icon_emoji: ':chart_with_upwards_trend:'
        title: 'ATS Coverage Alert - {{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.summary }}\n{{ .Annotations.description }}{{ end }}'

inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname', 'dev', 'instance']
EOF

# Create monitoring startup script
echo -e "${YELLOW}🚀 Creating monitoring stack startup script...${NC}"
cat > "$PROJECT_ROOT/scripts/start_monitoring_stack.sh" << 'EOF'
#!/bin/bash
# Start ATS Coverage Monitoring Stack
# Launches Prometheus, Grafana, and Alertmanager

set -e

cd /home/jianjun/ats-genai-pm

echo "🚀 Starting ATS Data Coverage Monitoring Stack..."

# Start the monitoring stack
docker-compose -f docker-compose.monitoring.yml up -d

echo "⏳ Waiting for services to start..."
sleep 15

# Check service health
echo "🔍 Checking service health..."

# Check Prometheus
if curl -f -s http://localhost:9090/-/healthy > /dev/null 2>&1; then
    echo "✅ Prometheus is healthy (http://localhost:9090)"
else
    echo "❌ Prometheus health check failed"
fi

# Check Grafana
if curl -f -s http://localhost:3000/api/health > /dev/null 2>&1; then
    echo "✅ Grafana is healthy (http://localhost:3000)"
    echo "   📋 Default login: admin / ats_admin_2024"
else
    echo "❌ Grafana health check failed"
fi

# Check Node Exporter
if curl -f -s http://localhost:9100/metrics > /dev/null 2>&1; then
    echo "✅ Node Exporter is healthy (http://localhost:9100)"
else
    echo "❌ Node Exporter health check failed"
fi

# Check Alertmanager
if curl -f -s http://localhost:9093/-/healthy > /dev/null 2>&1; then
    echo "✅ Alertmanager is healthy (http://localhost:9093)"
else
    echo "❌ Alertmanager health check failed"
fi

echo ""
echo "🎯 Access Points:"
echo "  📊 Grafana Dashboard: http://localhost:3000"
echo "  📈 Prometheus: http://localhost:9090"
echo "  🚨 Alertmanager: http://localhost:9093"
echo "  💻 Node Exporter: http://localhost:9100"
echo ""
echo "✅ ATS Coverage Monitoring Stack is running!"
EOF

# Create monitoring stop script
cat > "$PROJECT_ROOT/scripts/stop_monitoring_stack.sh" << 'EOF'
#!/bin/bash
# Stop ATS Coverage Monitoring Stack

set -e

cd /home/jianjun/ats-genai-pm

echo "🛑 Stopping ATS Data Coverage Monitoring Stack..."

# Stop the monitoring stack
docker-compose -f docker-compose.monitoring.yml down

echo "✅ ATS Coverage Monitoring Stack stopped"
EOF

# Make scripts executable
chmod +x "$PROJECT_ROOT/scripts/start_monitoring_stack.sh"
chmod +x "$PROJECT_ROOT/scripts/stop_monitoring_stack.sh"

echo -e "${GREEN}✅ Grafana monitoring stack setup complete!${NC}"

echo ""
echo -e "${YELLOW}📋 Next Steps:${NC}"
echo "1. Configure Slack webhook in .env.monitoring:"
echo "   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
echo ""
echo "2. Start the monitoring stack:"
echo "   $PROJECT_ROOT/scripts/start_monitoring_stack.sh"
echo ""
echo "3. Access Grafana:"
echo "   URL: http://localhost:3000"
echo "   Login: admin / ats_admin_2024"
echo ""
echo "4. Import ATS Coverage Dashboard:"
echo "   Dashboard should be auto-provisioned from:"
echo "   $GRAFANA_DIR/ats-coverage-dashboard.json"
echo ""
echo "5. Set up Prometheus metrics export:"
echo "   Run: $PROJECT_ROOT/scripts/setup_coverage_monitoring_cron.sh"
echo ""
echo "6. View monitoring logs:"
echo "   docker-compose -f docker-compose.monitoring.yml logs -f"

echo ""
echo -e "${GREEN}🎯 Complete ATS Data Coverage Monitoring System Ready!${NC}"
echo -e "${BLUE}📊 Features: Real-time dashboard, Slack alerts, Prometheus metrics, Grafana visualization${NC}"