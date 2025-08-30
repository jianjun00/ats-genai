#!/bin/bash
"""
ATS Autostart Script - Automatically start ATS dev and intg environments on WSL startup

This script:
1. Starts complete ATS stack using Docker Compose
2. Includes PostgreSQL (with correct postgres-data-new volume), Analytics, Monitoring, and Price Collection services
3. Ensures PostgreSQL uses persistent volume with existing data (9,973 instruments, 26M+ price records)
4. Logs startup activities
5. Runs in background to avoid blocking shell startup

IMPORTANT: Uses postgres-data-new volume to maintain data persistence across WSL restarts
"""

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_FILE="/mnt/d/ats-logs/autostart.log"
PID_FILE="/tmp/ats_autostart.pid"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.ats.yml"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if Docker Compose services are running
is_compose_running() {
    cd "$PROJECT_ROOT" || return 1
    docker-compose -f docker-compose.ats.yml ps -q | wc -l
}

# Function to start ATS services using Docker Compose
start_ats_services() {
    log "🚀 Starting ATS autostart sequence with Docker Compose..."
    
    cd "$PROJECT_ROOT" || {
        log "❌ Failed to change to project root: $PROJECT_ROOT"
        return 1
    }
    
    # Check if Docker Compose file exists
    if [ ! -f "$COMPOSE_FILE" ]; then
        log "❌ Docker Compose file not found: $COMPOSE_FILE"
        return 1
    fi
    
    # Set up environment file if it doesn't exist
    if [ ! -f ".env" ] && [ -f ".env.ats" ]; then
        log "📝 Creating .env from .env.ats template"
        cp .env.ats .env
    fi
    
    # Check current running services
    running_services=$(is_compose_running)
    log "📊 Currently running ATS services: $running_services"
    
    if [ "$running_services" -gt 0 ]; then
        log "✅ Some ATS services already running, checking health..."
        docker-compose -f docker-compose.ats.yml ps >> "$LOG_FILE" 2>&1
    else
        log "🔧 Starting complete ATS stack..."
        
        # Start all services
        docker-compose -f docker-compose.ats.yml up -d >> "$LOG_FILE" 2>&1
        if [ $? -eq 0 ]; then
            log "✅ ATS Docker Compose stack started successfully"
        else
            log "❌ Failed to start ATS Docker Compose stack"
            return 1
        fi
    fi
    
    # Wait for services to be healthy
    log "⏳ Waiting for services to be healthy..."
    sleep 10
    
    # Show final status
    log "📊 Final ATS services status:"
    docker-compose -f docker-compose.ats.yml ps >> "$LOG_FILE" 2>&1
    
    # Show service URLs
    log "🌐 Service URLs:"
    log "  - Dev Analytics: http://localhost:3000"
    log "  - Intg Analytics: http://localhost:4000" 
    log "  - Grafana: http://localhost:3001"
    log "  - Prometheus: http://localhost:9090"
    log "  - Dev PostgreSQL: localhost:5432"
    log "  - Intg PostgreSQL: localhost:5433"
    
    # Verify database data is accessible
    log "🔍 Verifying database data accessibility..."
    sleep 5  # Give PostgreSQL time to fully start
    
    if docker exec ats-dev-postgres psql -U postgres -d dev_db -c "SELECT COUNT(*) FROM dev_instrument_tiingo;" >/dev/null 2>&1; then
        instrument_count=$(docker exec ats-dev-postgres psql -U postgres -d dev_db -t -c "SELECT COUNT(*) FROM dev_instrument_tiingo;" 2>/dev/null | xargs)
        log "✅ Database data verified: $instrument_count Tiingo instruments accessible"
    else
        log "⚠️  Could not verify database data - may still be starting up"
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