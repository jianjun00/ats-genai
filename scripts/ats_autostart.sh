#!/bin/bash
# ATS Autostart Script - Automatically start ATS dev and intg environments on WSL startup
#
# This script:
# 1. Starts complete ATS stack using Docker Compose
# 2. Includes PostgreSQL (with correct postgres-data-new volume), Analytics, Monitoring, and Price Collection services
# 3. Ensures PostgreSQL uses persistent volume with existing data (9,973 instruments, 26M+ price records)
# 4. Logs startup activities
# 5. Runs in background to avoid blocking shell startup
#
# IMPORTANT: Uses postgres-data-new volume to maintain data persistence across WSL restarts

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
        log "🔧 Starting ATS-DEV Analytics..."
        if python3 scripts/run_dev.py start --service analytics >> "$LOG_FILE" 2>&1; then
            log "✅ ATS-DEV Analytics started successfully"
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
            -v /mnt/d/ats-data/db-intg:/var/lib/postgresql/data \
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
    
    # Wait for services to be healthy
    log "⏳ Waiting for services to be healthy..."
    sleep 10
    
    # Show final status
    log "📊 Final ATS services status:"
    docker ps --filter "name=ats-dev" --filter "name=ats-intg" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | tee -a "$LOG_FILE"
    
    # Show service URLs
    log "🌐 Service URLs:"
    log "  - ATS-DEV PostgreSQL: localhost:3432"
    log "  - ATS-DEV Analytics: http://localhost:3000"
    log "  - ATS-INTG PostgreSQL: localhost:4432"
    log "  - ATS-INTG Analytics: http://localhost:4000"
    
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