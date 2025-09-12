#!/bin/bash
# ATS Autostart Script - Automatically start ATS dev and intg environments on WSL startup
#
# This script automatically starts 8 services across DEV + INTG environments:
#
# 🔵 ATS-DEV Environment:
# 1. ats-dev-postgres      (localhost:3432) - PostgreSQL database
# 2. ats-dev-analytics     (localhost:3000) - Analytics service & EDA dashboard
# 3. ats-grafana           (localhost:3001) - Grafana monitoring dashboard
#
# 🟠 ATS-INTG Environment:
# 4. ats-intg-postgres     (localhost:4432) - PostgreSQL database
# 5. ats-intg-analytics    (localhost:4000) - Analytics service & EDA dashboard
# 6. ats-grafana-intg      (localhost:4002) - Grafana monitoring dashboard
# 7. ats-intg-prometheus-metrics (localhost:4080) - Prometheus metrics server
#
# 📊 Shared Monitoring:
# 8. ats-prometheus        (localhost:9090) - Prometheus time-series database
#
# All services use --restart unless-stopped for automatic recovery
# For complete documentation see: docs/ATS_AUTOSTART_SERVICES.md

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="/mnt/d/ats-logs/autostart.log"
PID_FILE="/tmp/ats_autostart.pid"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}


# Function to check if a service is already running
is_service_running() {
    docker ps --format "{{.Names}}" | grep -q "^$1$"
}

# Function to start ATS services using existing infrastructure
start_ats_services() {
    log "🚀 Starting ATS services using existing infrastructure..."

    cd "$PROJECT_ROOT" || {
        log "❌ Failed to change to project root: $PROJECT_ROOT"
        return 1
    }

    # Ensure Docker networks exist
    log "🔧 Ensuring Docker networks exist..."
    docker network create ats-network 2>/dev/null || log "ℹ️  ats-network already exists"
    docker network create ats-intg-network 2>/dev/null || log "ℹ️  ats-intg-network already exists"

    # Check if ATS-DEV PostgreSQL is running
    if is_service_running "ats-dev-postgres"; then
        log "✅ ATS-DEV PostgreSQL already running"
    else
        log "🔧 Starting ATS-DEV PostgreSQL..."
        if python3 scripts/run_dev.py start --service postgres >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-DEV PostgreSQL started successfully"
        else
            log "⚠️  ATS-DEV PostgreSQL failed to start (may already be running)"
        fi
    fi

    # Check if ATS-DEV Analytics is running
    if is_service_running "ats-dev-analytics"; then
        log "✅ ATS-DEV Analytics already running"
    else
        log "🔧 Starting ATS-DEV Analytics with restart policy..."
        if docker run -d \
            --name ats-dev-analytics \
            --network ats-network \
            -p 3000:3000 \
            -v "$PROJECT_ROOT":/workspace \
            -v /mnt/d/ats-data:/data \
            -v /mnt/d/ats-backup:/backup \
            -v /mnt/d/ats-logs:/logs \
            -e ENVIRONMENT=dev \
            -e DB_HOST=ats-dev-postgres \
            -e DB_PORT=5432 \
            -e DB_USER=postgres \
            -e DB_PASSWORD=dev_password \
            -e DB_NAME=dev_db \
            -e PYTHONPATH=/workspace/src \
            --restart unless-stopped \
            --workdir /workspace \
            dragonflyer762/ats-genai:latest \
            python3 src/services/analytics_service.py >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-DEV Analytics started successfully with restart policy"
        else
            log "⚠️  ATS-DEV Analytics failed to start (may already be running)"
        fi
    fi

    # Check if ATS-INTG PostgreSQL is running
    if is_service_running "ats-intg-postgres"; then
        log "✅ ATS-INTG PostgreSQL already running"
    else
        log "🔧 Starting ATS-INTG PostgreSQL..."
        if docker run -d \
            --name ats-intg-postgres \
            --network ats-intg-network \
            -p 4432:5432 \
            -e POSTGRES_DB=intg_db \
            -e POSTGRES_USER=postgres \
            -e POSTGRES_PASSWORD=intg_password \
            -v postgres-intg-data:/var/lib/postgresql/data \
            -v /mnt/d/ats-backup/intg:/backup \
            --restart unless-stopped \
            postgres:13 >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-INTG PostgreSQL started successfully"
        else
            log "⚠️  ATS-INTG PostgreSQL failed to start (may already be running)"
        fi
    fi

    # Check if ATS-INTG Analytics is running
    if is_service_running "ats-intg-analytics"; then
        log "✅ ATS-INTG Analytics already running"
    else
        log "🔧 Starting ATS-INTG Analytics..."
        if docker run -d \
            --name ats-intg-analytics \
            --network ats-intg-network \
            -p 4000:3000 \
            -v "$PROJECT_ROOT":/workspace \
            -v /mnt/d/ats-data:/data \
            -v /mnt/d/ats-backup:/backup \
            -v /mnt/d/ats-logs:/logs \
            -e ENVIRONMENT=intg \
            -e DB_HOST=ats-intg-postgres \
            -e DB_PORT=5432 \
            -e DB_USER=postgres \
            -e DB_PASSWORD=intg_password \
            -e DB_NAME=intg_db \
            -e PYTHONPATH=/workspace/src \
            --restart unless-stopped \
            --workdir /workspace \
            dragonflyer762/ats-genai:latest \
            python3 src/services/analytics_service.py >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-INTG Analytics started successfully"
        else
            log "⚠️  ATS-INTG Analytics failed to start (may already be running)"
        fi
    fi

    # Start Monitoring Services
    log "🔧 Starting monitoring services..."

    # Check if Prometheus is running
    if is_service_running "ats-prometheus"; then
        log "✅ Prometheus already running"
    else
        log "🔧 Starting Prometheus..."
        if docker run -d \
            --name ats-prometheus \
            --network ats-network \
            -p 9090:9090 \
            -v "$PROJECT_ROOT"/config/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
            -v /mnt/d/ats-data/prometheus:/prometheus \
            --restart unless-stopped \
            prom/prometheus:latest \
            --config.file=/etc/prometheus/prometheus.yml \
            --storage.tsdb.path=/prometheus \
            --web.console.libraries=/etc/prometheus/console_libraries \
            --web.console.templates=/etc/prometheus/consoles \
            --web.enable-lifecycle >> "$LOG_FILE" 2>&1; then
            log "✅ Prometheus started successfully"
        else
            log "⚠️  Prometheus failed to start (may already be running)"
        fi
    fi

    # Check if Grafana is running
    if is_service_running "ats-grafana"; then
        log "✅ Grafana already running"
    else
        log "🔧 Starting Grafana..."
        if docker run -d \
            --name ats-grafana \
            --network ats-network \
            -p 3001:3000 \
            -v /mnt/d/ats-data/grafana:/var/lib/grafana \
            -e GF_SECURITY_ADMIN_PASSWORD=admin123 \
            -e GF_USERS_ALLOW_SIGN_UP=false \
            --restart unless-stopped \
            grafana/grafana:latest >> "$LOG_FILE" 2>&1; then
            log "✅ Grafana started successfully"
        else
            log "⚠️  Grafana failed to start (may already be running)"
        fi
    fi

    # Check if INTG-specific Grafana is running
    if is_service_running "ats-grafana-intg"; then
        log "✅ ATS-INTG Grafana already running"
    else
        log "🔧 Starting ATS-INTG Grafana..."
        if docker run -d \
            --name ats-grafana-intg \
            --network ats-intg-network \
            -p 4002:3000 \
            -v /mnt/d/ats-data/grafana-intg:/var/lib/grafana \
            -e GF_SECURITY_ADMIN_PASSWORD=admin123 \
            -e GF_USERS_ALLOW_SIGN_UP=false \
            --restart unless-stopped \
            grafana/grafana:10.0.0 >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-INTG Grafana started successfully"
        else
            log "⚠️  ATS-INTG Grafana failed to start (may already be running)"
        fi
    fi

    # Check if Prometheus metrics service is running for INTG
    if is_service_running "ats-intg-prometheus-metrics"; then
        log "✅ ATS-INTG Prometheus Metrics already running"
    else
        log "🔧 Starting ATS-INTG Prometheus Metrics..."
        if docker run -d \
            --name ats-intg-prometheus-metrics \
            --network ats-intg-network \
            -p 4080:8080 \
            -v "$PROJECT_ROOT":/workspace \
            -v /mnt/d/ats-data:/data \
            -e ENVIRONMENT=intg \
            -e DB_HOST=ats-intg-postgres \
            -e DB_PORT=5432 \
            -e DB_USER=postgres \
            -e DB_PASSWORD=intg_password \
            -e DB_NAME=intg_db \
            -e PYTHONPATH=/workspace/src \
            --restart unless-stopped \
            --workdir /workspace \
            dragonflyer762/ats-genai:latest \
            python3 /workspace/scripts/prometheus_metrics_server.py >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-INTG Prometheus Metrics started successfully"
        else
            log "⚠️  ATS-INTG Prometheus Metrics failed to start (may already be running)"
        fi
    fi

    # Wait for services to be healthy
    log "⏳ Waiting for services to be healthy..."
    sleep 10

    # Show final status
    log "📊 Final ATS services status:"
    docker ps --filter "name=ats-dev" --filter "name=ats-intg" --filter "name=ats-prometheus" --filter "name=ats-grafana" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | tee -a "$LOG_FILE"

    # Show service URLs
    log "🌐 Service URLs:"
    log "  - ATS-DEV PostgreSQL: localhost:3432"
    log "  - ATS-DEV Analytics: http://localhost:3000"
    log "  - ATS-INTG PostgreSQL: localhost:4432"
    log "  - ATS-INTG Analytics: http://localhost:4000"
    log "  - Prometheus: http://localhost:9090"
    log "  - Grafana (DEV): http://localhost:3001 (admin/admin123)"
    log "  - Grafana (INTG): http://localhost:4002 (admin/admin123)"
    log "  - INTG Metrics: http://localhost:4080/health"

    # Test database connectivity
    log "🔍 Testing database connectivity..."
    sleep 5

    # Test ATS-DEV database
    if python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments" >> "$LOG_FILE" 2>&1; then
        dev_count=$(python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo "0")
        log "✅ ATS-DEV database accessible: $dev_count instruments"
    else
        log "⚠️  ATS-DEV database connectivity issues"
    fi

    # Test ATS-INTG database
    if PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT COUNT(*) FROM intg_instruments" >> "$LOG_FILE" 2>&1; then
        intg_count=$(PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -t -c "SELECT COUNT(*) FROM intg_instruments" 2>/dev/null | xargs || echo "0")
        log "✅ ATS-INTG database accessible: $intg_count instruments"
    else
        log "⚠️  ATS-INTG database connectivity issues"
    fi

    log "🎉 ATS autostart sequence completed"
}

# Main execution
main() {
    # Check if already running
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        log "⚠️  ATS autostart already running (PID: $(cat "$PID_FILE"))"
        exit 0
    fi

    # Store our PID
    echo $$ > "$PID_FILE"

    # Wait a bit for WSL to fully initialize
    sleep 5

    # Start services
    start_ats_services

    # Clean up PID file
    rm -f "$PID_FILE"
}

# Run in background if called directly
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main &
fi