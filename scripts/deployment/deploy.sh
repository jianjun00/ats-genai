#!/bin/bash
# ATS Platform Deployment Script
# Automated deployment with environment validation and health checks

set -euo pipefail

# Configuration
ENVIRONMENT=${ENVIRONMENT:-dev}
COMPOSE_FILE="docker-compose.optimized.yml"
ENV_FILE=".env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    if [[ ! -f "$ENV_FILE" ]]; then
        log_error "Environment file $ENV_FILE not found"
        log_info "Please create $ENV_FILE from .env.template"
        exit 1
    fi
    
    log_info "Prerequisites check passed"
}

# Validate environment configuration
validate_environment() {
    log_info "Validating environment configuration..."
    
    source "$ENV_FILE"
    
    required_vars=(
        "DB_PASSWORD"
        "POLYGON_API_KEY"
        "TIINGO_API_KEY"
    )
    
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log_error "Required environment variable $var is not set"
            exit 1
        fi
    done
    
    log_info "Environment validation passed"
}

# Deploy services
deploy_services() {
    log_info "Deploying ATS Platform services..."
    
    # Pull latest images
    docker-compose -f "$COMPOSE_FILE" pull
    
    # Stop existing services
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans
    
    # Start services
    docker-compose -f "$COMPOSE_FILE" up -d
    
    log_info "Services deployment initiated"
}

# Health check
health_check() {
    log_info "Performing health checks..."
    
    max_attempts=30
    attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        if docker-compose -f "$COMPOSE_FILE" ps --filter status=running | grep -q "ats-postgres"; then
            log_info "Database is running"
            break
        fi
        
        attempt=$((attempt + 1))
        log_warn "Waiting for database to start... ($attempt/$max_attempts)"
        sleep 5
    done
    
    if [[ $attempt -eq $max_attempts ]]; then
        log_error "Health check failed - database did not start"
        exit 1
    fi
    
    log_info "Health checks passed"
}

# Main deployment process
main() {
    log_info "Starting ATS Platform deployment for environment: $ENVIRONMENT"
    
    check_prerequisites
    validate_environment
    deploy_services
    health_check
    
    log_info "Deployment completed successfully!"
    log_info "Access the analytics dashboard at: http://localhost:3000"
}

# Handle script arguments
case "${1:-}" in
    "status")
        docker-compose -f "$COMPOSE_FILE" ps
        ;;
    "logs")
        docker-compose -f "$COMPOSE_FILE" logs -f "${2:-}"
        ;;
    "down")
        log_info "Stopping ATS Platform services..."
        docker-compose -f "$COMPOSE_FILE" down --remove-orphans
        ;;
    "restart")
        log_info "Restarting ATS Platform services..."
        docker-compose -f "$COMPOSE_FILE" restart "${2:-}"
        ;;
    *)
        main
        ;;
esac
