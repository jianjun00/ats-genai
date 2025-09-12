#!/bin/bash

# ATS Services Deployment Script
# Comprehensive deployment automation for service-based architecture

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEPLOYMENT_DIR="${PROJECT_ROOT}/deployment"
COMPOSE_FILE="${DEPLOYMENT_DIR}/docker-compose.services.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
ATS Services Deployment Script

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    start           Start all services
    stop            Stop all services
    restart         Restart all services
    status          Show service status
    logs            Show service logs
    scale           Scale services
    health          Check service health
    clean           Clean up resources
    build           Build service images
    deploy          Full deployment (build + start)

Options:
    -e, --env       Environment (dev|staging|production) [default: dev]
    -s, --service   Specific service to operate on
    -f, --follow    Follow logs output
    -r, --replicas  Number of replicas for scaling
    -h, --help      Show this help message

Examples:
    $0 start                                    # Start all services
    $0 start -e production                      # Start in production mode
    $0 logs -s instrument-service -f            # Follow logs for instrument service
    $0 scale -s analytics-service -r 3          # Scale analytics service to 3 replicas
    $0 health                                   # Check health of all services

EOF
}

# Parse command line arguments
COMMAND=""
ENVIRONMENT="dev"
SERVICE=""
FOLLOW_LOGS=false
REPLICAS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        start|stop|restart|status|logs|scale|health|clean|build|deploy)
            COMMAND="$1"
            shift
            ;;
        -e|--env)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -s|--service)
            SERVICE="$2"
            shift 2
            ;;
        -f|--follow)
            FOLLOW_LOGS=true
            shift
            ;;
        -r|--replicas)
            REPLICAS="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate command
if [[ -z "$COMMAND" ]]; then
    log_error "No command specified"
    show_help
    exit 1
fi

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|production)$ ]]; then
    log_error "Invalid environment: $ENVIRONMENT. Must be dev, staging, or production"
    exit 1
fi

# Set environment-specific configuration
set_environment_config() {
    case "$ENVIRONMENT" in
        dev)
            export COMPOSE_PROJECT_NAME="ats-services-dev"
            ;;
        staging)
            export COMPOSE_PROJECT_NAME="ats-services-staging"
            ;;
        production)
            export COMPOSE_PROJECT_NAME="ats-services-prod"
            ;;
    esac
    
    log_info "Environment set to: $ENVIRONMENT"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check Docker Compose
    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose V2 is not available"
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    # Check if compose file exists
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_error "Docker compose file not found: $COMPOSE_FILE"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Build service images
build_images() {
    log_info "Building service images..."
    
    cd "$PROJECT_ROOT"
    
    # Build all service images
    local services=("service-registry" "instrument-service" "analytics-service" "trading-service" "news-service" "api-gateway")
    
    for service in "${services[@]}"; do
        log_info "Building $service image..."
        docker build -f "deployment/dockerfiles/Dockerfile.$service" -t "ats-platform/$service:latest" .
        
        if [[ $? -eq 0 ]]; then
            log_success "Built $service image successfully"
        else
            log_error "Failed to build $service image"
            exit 1
        fi
    done
    
    log_success "All images built successfully"
}

# Start services
start_services() {
    log_info "Starting ATS services..."
    
    # Start infrastructure services first
    log_info "Starting infrastructure services..."
    docker compose -f "$COMPOSE_FILE" up -d postgres-services redis-services
    
    # Wait for infrastructure to be ready
    log_info "Waiting for infrastructure to be ready..."
    sleep 10
    
    # Check database health
    local max_attempts=30
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        if docker compose -f "$COMPOSE_FILE" exec -T postgres-services pg_isready -U services_user -d ats_services; then
            log_success "Database is ready"
            break
        fi
        
        log_info "Waiting for database... (attempt $attempt/$max_attempts)"
        sleep 5
        ((attempt++))
    done
    
    if [[ $attempt -gt $max_attempts ]]; then
        log_error "Database failed to start within timeout"
        exit 1
    fi
    
    # Start service registry
    log_info "Starting service registry..."
    docker compose -f "$COMPOSE_FILE" up -d service-registry
    
    # Wait for service registry
    sleep 10
    
    # Start business services
    log_info "Starting business services..."
    if [[ -n "$SERVICE" ]]; then
        docker compose -f "$COMPOSE_FILE" up -d "$SERVICE"
    else
        docker compose -f "$COMPOSE_FILE" up -d instrument-service analytics-service trading-service news-service
    fi
    
    # Start API gateway last
    log_info "Starting API gateway..."
    docker compose -f "$COMPOSE_FILE" up -d api-gateway
    
    # Start monitoring (optional)
    if [[ "$ENVIRONMENT" == "production" ]]; then
        log_info "Starting monitoring services..."
        docker compose -f "$COMPOSE_FILE" up -d prometheus grafana
    fi
    
    log_success "Services started successfully"
}

# Stop services
stop_services() {
    log_info "Stopping ATS services..."
    
    if [[ -n "$SERVICE" ]]; then
        docker compose -f "$COMPOSE_FILE" stop "$SERVICE"
        log_success "$SERVICE stopped"
    else
        docker compose -f "$COMPOSE_FILE" down
        log_success "All services stopped"
    fi
}

# Restart services
restart_services() {
    log_info "Restarting services..."
    stop_services
    sleep 5
    start_services
}

# Show service status
show_status() {
    log_info "Service status:"
    docker compose -f "$COMPOSE_FILE" ps
    
    echo ""
    log_info "Resource usage:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
}

# Show service logs
show_logs() {
    log_info "Showing service logs..."
    
    local log_args=""
    if [[ "$FOLLOW_LOGS" == true ]]; then
        log_args="-f"
    fi
    
    if [[ -n "$SERVICE" ]]; then
        docker compose -f "$COMPOSE_FILE" logs $log_args "$SERVICE"
    else
        docker compose -f "$COMPOSE_FILE" logs $log_args
    fi
}

# Scale services
scale_services() {
    if [[ -z "$SERVICE" ]]; then
        log_error "Service name required for scaling"
        exit 1
    fi
    
    if [[ -z "$REPLICAS" ]]; then
        log_error "Number of replicas required for scaling"
        exit 1
    fi
    
    log_info "Scaling $SERVICE to $REPLICAS replicas..."
    docker compose -f "$COMPOSE_FILE" up -d --scale "$SERVICE=$REPLICAS" "$SERVICE"
    
    log_success "$SERVICE scaled to $REPLICAS replicas"
}

# Health check
health_check() {
    log_info "Performing health checks..."
    
    local services=(
        "service-registry:8500"
        "instrument-service:8001"
        "analytics-service:8002" 
        "trading-service:8003"
        "news-service:8004"
        "api-gateway:8000"
    )
    
    local healthy_count=0
    local total_count=${#services[@]}
    
    for service_port in "${services[@]}"; do
        local service="${service_port%%:*}"
        local port="${service_port##*:}"
        
        log_info "Checking $service health..."
        
        if curl -f -s -m 10 "http://localhost:$port/health" > /dev/null; then
            log_success "$service is healthy"
            ((healthy_count++))
        else
            log_error "$service is unhealthy or not responding"
        fi
    done
    
    echo ""
    log_info "Health check summary: $healthy_count/$total_count services healthy"
    
    if [[ $healthy_count -eq $total_count ]]; then
        log_success "All services are healthy"
        return 0
    else
        log_warning "Some services are unhealthy"
        return 1
    fi
}

# Clean up resources
cleanup() {
    log_info "Cleaning up resources..."
    
    # Stop and remove containers
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    
    # Remove unused images (optional)
    if [[ "$ENVIRONMENT" == "dev" ]]; then
        log_info "Removing unused Docker images..."
        docker image prune -f
    fi
    
    # Remove unused volumes (be careful!)
    if [[ "$1" == "--volumes" ]]; then
        log_warning "Removing Docker volumes (this will delete data!)"
        read -p "Are you sure? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker volume prune -f
        fi
    fi
    
    log_success "Cleanup completed"
}

# Full deployment
full_deploy() {
    log_info "Starting full deployment..."
    
    build_images
    start_services
    
    # Wait for services to start
    sleep 30
    
    # Perform health check
    if health_check; then
        log_success "Deployment completed successfully"
    else
        log_error "Deployment completed but some services are unhealthy"
        exit 1
    fi
}

# Main execution
main() {
    set_environment_config
    check_prerequisites
    
    case "$COMMAND" in
        start)
            start_services
            ;;
        stop)
            stop_services
            ;;
        restart)
            restart_services
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs
            ;;
        scale)
            scale_services
            ;;
        health)
            health_check
            ;;
        clean)
            cleanup "$@"
            ;;
        build)
            build_images
            ;;
        deploy)
            full_deploy
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"