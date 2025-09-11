#!/bin/bash
#
# Mandatory Deployment Checklist
# Enforces validation before any deployment
#
set -e

# Configuration
ENVIRONMENT="${ENVIRONMENT:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[CHECKLIST]${NC} $1"
}

success() {
    echo -e "${GREEN}[✅]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[⚠️]${NC} $1"
}

error() {
    echo -e "${RED}[❌]${NC} $1"
}

# Check functions
check_environment_variable() {
    log "Checking ENVIRONMENT variable..."
    
    if [ -z "$ENVIRONMENT" ]; then
        error "ENVIRONMENT variable not set"
        echo "  Expected values: dev, intg, prod"
        echo "  Set with: export ENVIRONMENT=intg"
        return 1
    fi
    
    case "$ENVIRONMENT" in
        dev|intg|prod)
            success "ENVIRONMENT set to: $ENVIRONMENT"
            return 0
            ;;
        *)
            error "Invalid ENVIRONMENT value: $ENVIRONMENT"
            echo "  Valid values: dev, intg, prod"
            return 1
            ;;
    esac
}

check_docker_daemon() {
    log "Checking Docker daemon..."
    
    if ! docker info >/dev/null 2>&1; then
        error "Docker daemon not running"
        echo "  Start Docker with: sudo systemctl start docker"
        return 1
    fi
    
    success "Docker daemon running"
    return 0
}

check_docker_network() {
    log "Checking Docker network for $ENVIRONMENT..."
    
    local network_name="ats-${ENVIRONMENT}-network"
    
    if ! docker network inspect "$network_name" >/dev/null 2>&1; then
        warning "Network $network_name does not exist"
        echo "  Will be created during deployment"
        return 0
    fi
    
    success "Network $network_name exists"
    return 0
}

check_compose_file() {
    log "Checking Docker Compose file..."
    
    local compose_file="$PROJECT_ROOT/docker-compose.${ENVIRONMENT}.yml"
    
    if [ ! -f "$compose_file" ]; then
        error "Docker Compose file not found: $compose_file"
        echo "  Expected location: docker-compose.${ENVIRONMENT}.yml"
        return 1
    fi
    
    # Validate compose file syntax
    if ! docker-compose -f "$compose_file" config >/dev/null 2>&1; then
        error "Invalid Docker Compose file syntax"
        echo "  Check syntax with: docker-compose -f $compose_file config"
        return 1
    fi
    
    success "Docker Compose file valid: $compose_file"
    return 0
}

check_port_conflicts() {
    log "Checking for port conflicts..."
    
    local ports_to_check
    case "$ENVIRONMENT" in
        dev)
            ports_to_check="3000 5432"
            ;;
        intg)
            ports_to_check="4000 4432"
            ;;
        prod)
            ports_to_check="4000 4432"
            ;;
    esac
    
    local conflicts=()
    for port in $ports_to_check; do
        if netstat -tuln 2>/dev/null | grep -q ":${port} " || ss -tuln 2>/dev/null | grep -q ":${port} "; then
            conflicts+=("$port")
        fi
    done
    
    if [ ${#conflicts[@]} -gt 0 ]; then
        warning "Port conflicts detected: ${conflicts[*]}"
        echo "  These ports are already in use"
        echo "  Deployment will attempt to stop conflicting containers"
        return 0
    fi
    
    success "No port conflicts detected"
    return 0
}

check_existing_containers() {
    log "Checking for existing containers..."
    
    local existing_containers
    existing_containers=$(docker ps --format "{{.Names}}" | grep "ats-${ENVIRONMENT}-" || true)
    
    if [ -n "$existing_containers" ]; then
        warning "Existing containers found:"
        echo "$existing_containers" | sed 's/^/    /'
        echo "  These will be stopped and recreated"
        return 0
    fi
    
    success "No existing containers found"
    return 0
}

check_disk_space() {
    log "Checking disk space..."
    
    local available_space
    available_space=$(df /var/lib/docker --output=avail | tail -1 | sed 's/[^0-9]//g')
    local required_space=2097152  # 2GB in KB
    
    if [ "$available_space" -lt "$required_space" ]; then
        error "Insufficient disk space"
        echo "  Available: $(( available_space / 1024 / 1024 ))GB"
        echo "  Required: 2GB minimum"
        return 1
    fi
    
    success "Sufficient disk space available"
    return 0
}

check_environment_validator() {
    log "Checking environment validator..."
    
    local validator_file="$PROJECT_ROOT/src/core/platform/config/environment_validator.py"
    
    if [ ! -f "$validator_file" ]; then
        warning "Environment validator not found"
        echo "  Advanced validation will be skipped"
        return 0
    fi
    
    success "Environment validator available"
    return 0
}

check_image_availability() {
    log "Checking Docker image availability..."
    
    local required_images=("timescale/timescaledb:latest-pg13" "dragonflyer762/ats-genai:latest")
    
    for image in "${required_images[@]}"; do
        if ! docker image inspect "$image" >/dev/null 2>&1; then
            warning "Image not available locally: $image"
            echo "  Will be pulled during deployment"
        else
            success "Image available: $image"
        fi
    done
    
    return 0
}

# Main checklist execution
run_checklist() {
    echo "🔍 ATS Deployment Checklist - $(date)"
    echo "Environment: ${ENVIRONMENT:-NOT_SET}"
    echo "Project: $PROJECT_ROOT"
    echo
    
    local checks=(
        "check_environment_variable"
        "check_docker_daemon"
        "check_docker_network"
        "check_compose_file"
        "check_port_conflicts"
        "check_existing_containers"
        "check_disk_space"
        "check_environment_validator"
        "check_image_availability"
    )
    
    local failed_checks=()
    local warning_checks=()
    
    for check in "${checks[@]}"; do
        if ! $check; then
            failed_checks+=("$check")
        fi
    done
    
    echo
    echo "📊 Checklist Summary:"
    echo "  Total checks: ${#checks[@]}"
    echo "  Failed checks: ${#failed_checks[@]}"
    
    if [ ${#failed_checks[@]} -gt 0 ]; then
        echo
        error "Deployment checklist FAILED"
        echo "Failed checks:"
        for check in "${failed_checks[@]}"; do
            echo "  - $check"
        done
        echo
        echo "Fix the above issues before deploying."
        return 1
    else
        echo
        success "✅ Deployment checklist PASSED"
        echo "Environment $ENVIRONMENT is ready for deployment."
        return 0
    fi
}

# CLI usage
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Usage: $0 [ENVIRONMENT]"
    echo
    echo "Validates environment configuration before deployment."
    echo
    echo "Arguments:"
    echo "  ENVIRONMENT    Target environment (dev/intg/prod)"
    echo "                 Can also be set via ENVIRONMENT variable"
    echo
    echo "Examples:"
    echo "  $0 intg"
    echo "  ENVIRONMENT=intg $0"
    echo
    exit 0
fi

# Set environment from argument if provided
if [ -n "$1" ]; then
    ENVIRONMENT="$1"
fi

# Run the checklist
run_checklist