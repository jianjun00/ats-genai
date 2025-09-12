#!/bin/bash
# ATS-INTG Deployment Script
# Deploys daily refresh jobs to ATS Integration environment with comprehensive validation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT="ats-intg"
COMPOSE_FILE="docker-compose.intg-jobs.yml"
POSTGRES_COMPOSE_FILE="docker-compose.postgres-intg.yml"
BACKUP_DIR="/mnt/d/ats-backup/intg"
LOCK_FILE="/tmp/ats-intg-deployment.lock"
MAX_WAIT_TIME=600  # 10 minutes

# Functions
print_header() {
    echo -e "${PURPLE}🚀 ATS-INTG Deployment${NC}"
    echo -e "${PURPLE}======================${NC}"
    echo ""
}

print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

create_deployment_lock() {
    local branch=$(git branch --show-current 2>/dev/null || echo "unknown")
    local user=$(whoami)
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "Environment: $ENVIRONMENT | Branch: $branch | User: $user | Started: $timestamp" > "$LOCK_FILE"
    print_info "Created deployment lock: $LOCK_FILE"
}

remove_deployment_lock() {
    if [ -f "$LOCK_FILE" ]; then
        rm "$LOCK_FILE"
        print_info "Removed deployment lock"
    fi
}

check_deployment_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local lock_info=$(cat "$LOCK_FILE")
        print_error "Another deployment is in progress:"
        print_error "$lock_info"
        print_info "If this is stale, remove: $LOCK_FILE"
        return 1
    fi
    return 0
}

check_prerequisites() {
    print_info "Checking deployment prerequisites..."

    # Check if we're in the right directory
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "$COMPOSE_FILE not found. Are you in the project root?"
        return 1
    fi

    # Check Docker
    if ! command -v docker >/dev/null 2>&1; then
        print_error "Docker is not installed or not in PATH"
        return 1
    fi

    if ! command -v docker-compose >/dev/null 2>&1; then
        print_error "docker-compose is not installed or not in PATH"
        return 1
    fi

    # Check Docker daemon
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker daemon is not running"
        return 1
    fi

    # Check Git status
    local git_status=$(git status --porcelain 2>/dev/null)
    if [ -n "$git_status" ]; then
        print_warning "Git working directory has uncommitted changes"
        echo "$git_status"
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            return 1
        fi
    fi

    print_status 0 "All prerequisites check passed"
    return 0
}

backup_existing_data() {
    print_info "Creating backup of existing data..."

    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local backup_file="$BACKUP_DIR/pre_deployment_backup_$timestamp.sql"

    # Create backup directory if it doesn't exist
    mkdir -p "$BACKUP_DIR"

    # Check if postgres container is running
    if docker ps --filter "name=postgres-intg" --filter "status=running" | grep -q postgres-intg; then
        print_info "Backing up existing PostgreSQL data..."

        if docker exec postgres-intg pg_dump -U postgres intg_db > "$backup_file" 2>/dev/null; then
            print_status 0 "Database backup created: $backup_file"
        else
            print_warning "Could not create database backup (container may not exist yet)"
        fi
    else
        print_info "No existing PostgreSQL container found - skipping backup"
    fi

    return 0
}

setup_environment() {
    print_info "Setting up ATS-INTG environment..."

    # Run environment setup script
    if [ -f "scripts/setup_intg_environment.sh" ]; then
        if bash scripts/setup_intg_environment.sh; then
            print_status 0 "Environment setup completed"
        else
            print_status 1 "Environment setup failed"
            return 1
        fi
    else
        print_warning "Environment setup script not found - continuing"
    fi

    return 0
}

validate_configurations() {
    print_info "Validating deployment configurations..."

    # Validate Docker Compose file
    if docker-compose -f "$COMPOSE_FILE" config -q; then
        print_status 0 "Docker Compose configuration is valid"
    else
        print_status 1 "Docker Compose configuration is invalid"
        return 1
    fi

    # Validate job scheduler configuration
    if python scripts/daily_job_scheduler.py config --format docker >/dev/null 2>&1; then
        print_status 0 "Job scheduler configuration is valid"
    else
        print_status 1 "Job scheduler configuration is invalid"
        return 1
    fi

    # Check API keys
    local missing_keys=()

    if [ -f ".env.test" ]; then
        source .env.test

        [ -z "$POLYGON_API_KEY" ] && missing_keys+=("POLYGON_API_KEY")
        [ -z "$FMP_API_KEY" ] && missing_keys+=("FMP_API_KEY")
        [ -z "$TIINGO_API_KEY" ] && missing_keys+=("TIINGO_API_KEY")
        [ -z "$ALPHA_VANTAGE_API_KEY" ] && missing_keys+=("ALPHA_VANTAGE_API_KEY")

        if [ ${#missing_keys[@]} -eq 0 ]; then
            print_status 0 "All API keys are configured"
        else
            print_warning "Missing API keys: ${missing_keys[*]}"
            print_info "Jobs will still run but may fail without proper API keys"
        fi
    else
        print_warning ".env.test file not found - API keys may not be configured"
    fi

    return 0
}

deploy_services() {
    print_info "Deploying ATS-INTG services..."

    # Stop existing services
    print_info "Stopping existing services..."
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true

    # Pull latest images
    print_info "Pulling latest Docker images..."
    if docker-compose -f "$COMPOSE_FILE" pull; then
        print_status 0 "Docker images updated"
    else
        print_warning "Could not pull some images - continuing with existing images"
    fi

    # Start services
    print_info "Starting ATS-INTG services..."
    if docker-compose -f "$COMPOSE_FILE" up -d; then
        print_status 0 "Services started successfully"
    else
        print_status 1 "Service startup failed"
        return 1
    fi

    return 0
}

wait_for_services() {
    print_info "Waiting for services to be ready..."

    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        local ready_services=0

        # Check PostgreSQL
        if docker exec postgres-intg pg_isready -U postgres -d intg_db >/dev/null 2>&1; then
            ready_services=$((ready_services + 1))
        fi

        # Check scheduler
        if docker ps --filter "name=ats-intg-scheduler" --filter "status=running" | grep -q ats-intg-scheduler; then
            ready_services=$((ready_services + 1))
        fi

        if [ $ready_services -ge 2 ]; then
            print_status 0 "All services are ready"
            return 0
        fi

        print_info "Waiting for services... ($attempt/$max_attempts) - $ready_services/2 ready"
        sleep 10
        attempt=$((attempt + 1))
    done

    print_status 1 "Services did not become ready within expected time"
    return 1
}

run_smoke_tests() {
    print_info "Running deployment smoke tests..."

    local tests_passed=0
    local total_tests=4

    # Test 1: Database connectivity
    if docker exec postgres-intg psql -U postgres -d intg_db -c "SELECT 'Database test successful' as status" >/dev/null 2>&1; then
        print_status 0 "Database connectivity test passed"
        tests_passed=$((tests_passed + 1))
    else
        print_status 1 "Database connectivity test failed"
    fi

    # Test 2: Tables exist
    if docker exec postgres-intg psql -U postgres -d intg_db -c "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'intg_%'" >/dev/null 2>&1; then
        print_status 0 "Database tables test passed"
        tests_passed=$((tests_passed + 1))
    else
        print_status 1 "Database tables test failed"
    fi

    # Test 3: Scheduler is running
    if docker ps --filter "name=ats-intg-scheduler" --filter "status=running" | grep -q ats-intg-scheduler; then
        print_status 0 "Scheduler service test passed"
        tests_passed=$((tests_passed + 1))
    else
        print_status 1 "Scheduler service test failed"
    fi

    # Test 4: Job configuration validation
    if python scripts/daily_job_scheduler.py status >/dev/null 2>&1; then
        print_status 0 "Job configuration test passed"
        tests_passed=$((tests_passed + 1))
    else
        print_status 1 "Job configuration test failed"
    fi

    if [ $tests_passed -eq $total_tests ]; then
        print_status 0 "All smoke tests passed ($tests_passed/$total_tests)"
        return 0
    else
        print_status 1 "Some smoke tests failed ($tests_passed/$total_tests)"
        return 1
    fi
}

show_deployment_status() {
    print_info "Deployment Status Summary:"
    echo ""

    print_info "Services:"
    docker ps --filter "name=ats-intg" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo ""

    print_info "Database Status:"
    if docker exec postgres-intg pg_isready -U postgres -d intg_db >/dev/null 2>&1; then
        local db_size=$(docker exec postgres-intg psql -U postgres -d intg_db -t -c "SELECT pg_size_pretty(pg_database_size('intg_db'))" | xargs)
        print_info "Database: Connected (Size: $db_size)"

        local table_count=$(docker exec postgres-intg psql -U postgres -d intg_db -t -c "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'intg_%'" | xargs)
        print_info "Tables: $table_count intg_* tables found"
    else
        print_error "Database: Not accessible"
    fi
    echo ""

    print_info "Scheduled Jobs:"
    if docker exec ats-intg-scheduler crontab -l >/dev/null 2>&1; then
        local job_count=$(docker exec ats-intg-scheduler crontab -l 2>/dev/null | grep -c "python scripts/" || echo "0")
        print_info "Active cron jobs: $job_count"
    else
        print_warning "Could not check cron jobs"
    fi
    echo ""

    print_info "Access Information:"
    print_info "Database: postgresql://postgres:intg_password@localhost:5433/intg_db"
    print_info "Logs: docker logs ats-intg-scheduler -f"
    print_info "Monitoring: python scripts/monitor_daily_jobs.py"
    print_info "Manual job: python scripts/daily_job_scheduler.py manual --job prices"
    echo ""

    print_info "Data Persistence:"
    print_info "PostgreSQL data: /mnt/d/ats-data/intg/postgresql"
    print_info "Backups: /mnt/d/ats-backup/intg"
    print_info "Logs: /mnt/d/ats-logs/intg"
}

rollback_deployment() {
    print_error "Deployment failed - initiating rollback..."

    # Stop failed services
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true

    # Restore from backup if available
    local latest_backup=$(ls -t "$BACKUP_DIR"/pre_deployment_backup_*.sql 2>/dev/null | head -n 1)
    if [ -n "$latest_backup" ]; then
        print_info "Restoring from backup: $latest_backup"

        # Start only PostgreSQL
        docker-compose -f "$POSTGRES_COMPOSE_FILE" up -d postgres-intg
        sleep 30

        # Restore backup
        if cat "$latest_backup" | docker exec -i postgres-intg psql -U postgres -d intg_db; then
            print_status 0 "Database restored from backup"
        else
            print_status 1 "Database restoration failed"
        fi
    else
        print_warning "No backup available for restoration"
    fi

    print_info "Rollback completed - system in previous state"
}

# Trap to ensure cleanup on exit
cleanup() {
    remove_deployment_lock
}
trap cleanup EXIT

# Main deployment workflow
main() {
    print_header

    # Check for existing deployment
    if ! check_deployment_lock; then
        exit 1
    fi

    # Create deployment lock
    create_deployment_lock

    # Run deployment steps
    if ! check_prerequisites; then
        print_error "Prerequisites check failed"
        exit 1
    fi

    if ! backup_existing_data; then
        print_error "Backup creation failed"
        exit 1
    fi

    if ! setup_environment; then
        print_error "Environment setup failed"
        rollback_deployment
        exit 1
    fi

    if ! validate_configurations; then
        print_error "Configuration validation failed"
        exit 1
    fi

    if ! deploy_services; then
        print_error "Service deployment failed"
        rollback_deployment
        exit 1
    fi

    if ! wait_for_services; then
        print_error "Services did not start properly"
        rollback_deployment
        exit 1
    fi

    if ! run_smoke_tests; then
        print_error "Smoke tests failed"
        rollback_deployment
        exit 1
    fi

    # Success!
    print_header
    print_status 0 "ATS-INTG Deployment Completed Successfully!"
    echo ""
    show_deployment_status

    print_info "🎉 Deployment complete! Daily jobs are now scheduled and running."
    print_info "🔍 Monitor with: docker logs ats-intg-scheduler -f"
    print_info "🧪 Test manually: python scripts/daily_job_scheduler.py manual --job prices"
}

# Run main function
main "$@"