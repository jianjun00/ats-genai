#!/bin/bash
#
# INTG Environment Deployment Script
# Prevents network misconfigurations through comprehensive validation
#
set -e

# Configuration
ENVIRONMENT="intg"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.intg.yml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] ✅${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠️${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ❌${NC} $1"
}

# Pre-deployment validation
pre_deployment_validation() {
    log "🔍 Running pre-deployment validation for $ENVIRONMENT environment"

    # 1. Verify environment variable
    if [ -z "$ENVIRONMENT" ]; then
        log_error "ENVIRONMENT variable not set"
        exit 1
    fi

    # 2. Check Docker is running
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker is not running"
        exit 1
    fi

    # 3. Validate compose file exists
    if [ ! -f "$COMPOSE_FILE" ]; then
        log_error "Docker compose file not found: $COMPOSE_FILE"
        exit 1
    fi

    # 4. Check for container conflicts
    RUNNING_CONTAINERS=$(docker ps --format "{{.Names}}" | grep "ats-${ENVIRONMENT}-" || true)
    if [ -n "$RUNNING_CONTAINERS" ]; then
        log_warning "Existing containers found: $RUNNING_CONTAINERS"
        echo "Continue? (y/N)"
        read -r response
        if [ "$response" != "y" ]; then
            log "Deployment cancelled by user"
            exit 1
        fi
    fi

    log_success "Pre-deployment validation passed"
}

# Network setup
setup_network() {
    log "📡 Setting up network infrastructure"

    NETWORK_NAME="ats-${ENVIRONMENT}-network"

    # Create network if it doesn't exist
    if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
        log "Creating network: $NETWORK_NAME"
        docker network create "$NETWORK_NAME"
        log_success "Network created: $NETWORK_NAME"
    else
        log "Network already exists: $NETWORK_NAME"
    fi
}

# Service deployment
deploy_services() {
    log "🚀 Deploying services to $ENVIRONMENT environment"

    # Stop existing containers gracefully
    log "🛑 Stopping existing containers..."
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans || true

    # Start services with dependency order
    log "▶️ Starting services..."
    export ENVIRONMENT="$ENVIRONMENT"
    docker-compose -f "$COMPOSE_FILE" up -d

    log_success "Services started"
}

# Health checks
run_health_checks() {
    log "🏥 Running comprehensive health checks"

    # Wait for services to be ready
    log "⏳ Waiting for services to initialize..."
    sleep 15

    # 1. Postgres health check
    log "Checking postgres connectivity..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec "ats-${ENVIRONMENT}-postgres" pg_isready -U postgres >/dev/null 2>&1; then
            log_success "Postgres health check passed"
            break
        fi

        if [ $attempt -eq $max_attempts ]; then
            log_error "Postgres health check failed after $max_attempts attempts"
            return 1
        fi

        log "Postgres not ready, attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done

    # 2. Analytics service health check
    log "Checking analytics service..."
    local analytics_port
    if [ "$ENVIRONMENT" = "dev" ]; then
        analytics_port="3000"
    else
        analytics_port="4000"
    fi

    attempt=1
    while [ $attempt -le $max_attempts ]; do
        if curl -f "http://localhost:${analytics_port}/health" >/dev/null 2>&1; then
            log_success "Analytics service health check passed"
            break
        fi

        if [ $attempt -eq $max_attempts ]; then
            log_error "Analytics service health check failed after $max_attempts attempts"
            return 1
        fi

        log "Analytics service not ready, attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done

    # 3. Database table validation
    log "Validating database tables..."
    TABLE_COUNT=$(curl -s "http://localhost:${analytics_port}/api/tables" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    print(len(data.get('tables', [])))
except:
    print('0')
" 2>/dev/null || echo "0")

    local expected_tables
    if [ "$ENVIRONMENT" = "dev" ]; then
        expected_tables=30
    else
        expected_tables=50
    fi

    if [ "$TABLE_COUNT" -lt "$expected_tables" ]; then
        log_error "Database validation failed - only $TABLE_COUNT tables found, expected $expected_tables+"
        return 1
    fi

    log_success "Database validation passed - $TABLE_COUNT tables accessible"

    # 4. Network connectivity validation
    log "Validating container network connectivity..."
    if ! docker exec "ats-${ENVIRONMENT}-analytics" python3 -c "
import socket
try:
    socket.gethostbyname('ats-${ENVIRONMENT}-postgres')
    print('DNS_OK')
except:
    print('DNS_FAIL')
" 2>/dev/null | grep -q "DNS_OK"; then
        log_error "Network connectivity validation failed"
        return 1
    fi

    log_success "Network connectivity validation passed"

    return 0
}

# Post-deployment validation
post_deployment_validation() {
    log "✅ Running post-deployment validation"

    # Use environment validator
    if [ -f "$PROJECT_ROOT/src/core/platform/config/environment_validator.py" ]; then
        log "Running comprehensive environment validation..."
        cd "$PROJECT_ROOT"

        if PYTHONPATH=src python3 -c "
from core.platform.config.environment_validator import validate_environment_config
try:
    result = validate_environment_config('$ENVIRONMENT')
    print('Environment validation: PASSED')
    exit(0)
except Exception as e:
    print(f'Environment validation: FAILED - {e}')
    exit(1)
"; then
            log_success "Environment validation passed"
        else
            log_error "Environment validation failed"
            return 1
        fi
    fi

    return 0
}

# Deployment summary
deployment_summary() {
    local analytics_port
    if [ "$ENVIRONMENT" = "dev" ]; then
        analytics_port="3000"
    else
        analytics_port="4000"
    fi

    log_success "🎉 $ENVIRONMENT deployment completed successfully!"
    echo
    echo "📊 Access Points:"
    echo "  Dashboard: http://localhost:${analytics_port}"
    echo "  Health:    http://localhost:${analytics_port}/health"
    echo "  Tables:    http://localhost:${analytics_port}/api/tables"
    echo
    echo "🐳 Container Status:"
    docker ps --filter "name=ats-${ENVIRONMENT}-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo
    echo "📡 Network Information:"
    docker network inspect "ats-${ENVIRONMENT}-network" --format "{{.Name}}: {{len .Containers}} containers"
    echo
}

# Error handling
cleanup_on_error() {
    log_error "Deployment failed. Cleaning up..."
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans || true
    exit 1
}

# Main execution
main() {
    log "🚀 Starting ATS $ENVIRONMENT environment deployment"
    log "Project root: $PROJECT_ROOT"
    log "Compose file: $COMPOSE_FILE"
    echo

    # Set up error handling
    trap cleanup_on_error ERR

    # Run deployment steps
    pre_deployment_validation
    setup_network
    deploy_services

    if run_health_checks; then
        post_deployment_validation
        deployment_summary

        log_success "✅ Deployment completed successfully!"
        exit 0
    else
        log_error "❌ Health checks failed"
        cleanup_on_error
    fi
}

# Execute main function
main "$@"