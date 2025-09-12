#!/bin/bash
# Production Deployment Script for News Collection System
# Deploys the complete news collection infrastructure with monitoring

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT="${1:-prod}"
BACKUP_DIR="/mnt/d/ats-backup/${ENVIRONMENT}"
LOG_DIR="/mnt/d/ats-logs/${ENVIRONMENT}"
DEPLOYMENT_LOG="$LOG_DIR/deployment_$(date +%Y%m%d_%H%M%S).log"

# Functions
print_header() {
    echo -e "${PURPLE}🚀 News Collection Production Deployment${NC}"
    echo -e "${PURPLE}======================================${NC}"
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

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$DEPLOYMENT_LOG"
}

# Prerequisites check
check_prerequisites() {
    print_info "Checking deployment prerequisites..."

    local errors=0

    # Check Docker
    if ! command -v docker >/dev/null 2>&1; then
        print_error "Docker is not installed"
        ((errors++))
    fi

    # Check Docker Compose
    if ! command -v docker-compose >/dev/null 2>&1; then
        print_error "Docker Compose is not installed"
        ((errors++))
    fi

    # Check required directories
    if [ ! -d "/home/jianjun/ats-genai-data" ]; then
        print_error "ATS source directory not found"
        ((errors++))
    fi

    # Check API key
    if [ -z "$POLYGON_API_KEY" ]; then
        print_warning "POLYGON_API_KEY not set - will need to be configured"
    fi

    # Check Slack webhook
    if [ -z "$SLACK_WEBHOOK_URL" ]; then
        print_warning "SLACK_WEBHOOK_URL not set - alerts will be disabled"
    fi

    if [ $errors -gt 0 ]; then
        print_error "Prerequisites check failed with $errors errors"
        return 1
    fi

    print_status 0 "Prerequisites check passed"
    return 0
}

# Environment setup
setup_environment() {
    print_info "Setting up $ENVIRONMENT environment..."

    # Create required directories
    mkdir -p "$BACKUP_DIR"
    mkdir -p "$LOG_DIR"
    mkdir -p "/mnt/d/ats-data"

    # Set proper permissions
    chmod 755 "$LOG_DIR"
    chmod 755 "$BACKUP_DIR"

    print_status 0 "Environment directories created"
}

# Database setup
setup_database() {
    print_info "Setting up database for $ENVIRONMENT..."

    # Check if database container is running
    if ! docker ps --filter "name=ats-${ENVIRONMENT}-postgres" --filter "status=running" | grep -q postgres; then
        print_info "Starting PostgreSQL container for $ENVIRONMENT..."

        # Start database (assuming docker-compose setup exists)
        if [ -f "docker-compose.${ENVIRONMENT}.yml" ]; then
            docker-compose -f "docker-compose.${ENVIRONMENT}.yml" up -d postgres
        else
            print_warning "No docker-compose file found - manual database setup required"
        fi

        # Wait for database to be ready
        local max_wait=60
        local wait_count=0
        while [ $wait_count -lt $max_wait ]; do
            if docker exec ats-${ENVIRONMENT}-postgres pg_isready -U postgres >/dev/null 2>&1; then
                break
            fi
            sleep 2
            ((wait_count += 2))
        done

        if [ $wait_count -ge $max_wait ]; then
            print_error "Database did not become ready within $max_wait seconds"
            return 1
        fi
    fi

    print_status 0 "Database is ready"
}

# Deploy scripts and configuration
deploy_scripts() {
    print_info "Deploying news collection scripts..."

    # Verify critical files exist
    local required_files=(
        "scripts/polygon_news_backfill.py"
        "scripts/cron/daily_news_collection.sh"
        "scripts/cron/news_health_monitor_simple.sh"
        "tests/monitoring/test_news_data_monitoring.py"
    )

    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            print_error "Required file missing: $file"
            return 1
        fi
    done

    # Make scripts executable
    chmod +x scripts/cron/*.sh
    chmod +x scripts/*.py 2>/dev/null || true

    print_status 0 "Scripts deployed and configured"
}

# Install cron jobs
install_cron_jobs() {
    print_info "Installing cron jobs for $ENVIRONMENT..."

    # Backup existing crontab
    crontab -l > "${BACKUP_DIR}/crontab_backup_$(date +%Y%m%d_%H%M%S)" 2>/dev/null || echo "No existing crontab"

    # API key for cron
    local api_key="${POLYGON_API_KEY:-YOUR_POLYGON_API_KEY_HERE}"

    # Create cron entries
    local cron_entries="
# ATS News Collection - $ENVIRONMENT Environment
# Daily collection at 8 AM
0 8 * * * ENVIRONMENT=$ENVIRONMENT POLYGON_API_KEY=\"$api_key\" /home/jianjun/ats-genai-data/scripts/cron/daily_news_collection.sh >> $LOG_DIR/cron.log 2>&1

# Health monitoring every 4 hours
0 */4 * * * ENVIRONMENT=$ENVIRONMENT SLACK_WEBHOOK_URL=\"${SLACK_WEBHOOK_URL:-}\" /home/jianjun/ats-genai-data/scripts/cron/news_health_monitor_simple.sh >> $LOG_DIR/health.log 2>&1

# Weekly log cleanup on Sundays at 2 AM
0 2 * * 0 find $LOG_DIR -name \"*.log\" -mtime +30 -delete

# Monthly backup cleanup - keep 3 months
0 3 1 * * find $BACKUP_DIR -name \"*.sql\" -mtime +90 -delete
"

    # Install cron jobs (remove existing news collection jobs first)
    (crontab -l 2>/dev/null | grep -v "ATS News Collection\|daily_news_collection\|news_health_monitor" || true; echo "$cron_entries") | crontab -

    print_status 0 "Cron jobs installed successfully"

    # Show installed jobs
    print_info "Installed cron schedule:"
    crontab -l | grep -E "(daily_news_collection|news_health_monitor|Weekly.*cleanup|Monthly.*backup)" || echo "No news cron jobs found"
}

# Initial data validation
validate_deployment() {
    print_info "Validating deployment..."

    # Test database connectivity
    if ! docker exec ats-${ENVIRONMENT}-postgres pg_isready -U postgres >/dev/null 2>&1; then
        print_error "Database connectivity test failed"
        return 1
    fi

    # Test monitoring script
    print_info "Testing health monitoring..."
    if ENVIRONMENT="$ENVIRONMENT" ./scripts/cron/news_health_monitor_simple.sh >/dev/null 2>&1; then
        print_status 0 "Health monitoring test passed"
    else
        print_warning "Health monitoring test failed - check logs"
    fi

    # Test API connectivity (if key provided)
    if [ -n "$POLYGON_API_KEY" ]; then
        print_info "Testing API connectivity..."
        local test_result=$(curl -s "https://api.polygon.io/v2/reference/news?limit=1&apikey=$POLYGON_API_KEY" | grep -c '"status":"OK"' || echo "0")
        if [ "$test_result" -gt 0 ]; then
            print_status 0 "API connectivity test passed"
        else
            print_warning "API connectivity test failed - check API key"
        fi
    fi

    print_status 0 "Deployment validation completed"
}

# Setup monitoring dashboard integration
setup_monitoring_integration() {
    print_info "Setting up monitoring integration..."

    # Create monitoring endpoint script
    cat > "/home/jianjun/ats-genai-data/scripts/monitoring_endpoint.sh" <<EOF
#!/bin/bash
# Health monitoring endpoint for external monitoring systems

ENVIRONMENT="\${1:-$ENVIRONMENT}"

# Run health check and return JSON
docker run --rm \\
    --network ats-\${ENVIRONMENT}-network \\
    -e PYTHONPATH="/workspace/src" \\
    -e DB_HOST="ats-\${ENVIRONMENT}-postgres" \\
    -e DB_PORT="5432" \\
    -e DB_USER="postgres" \\
    -e DB_PASSWORD="\${ENVIRONMENT}_password" \\
    -e DB_NAME="\${ENVIRONMENT}_db" \\
    -v /home/jianjun/ats-genai-data:/workspace \\
    -w /workspace \\
    dragonflyer762/ats-genai:latest \\
    python3 tests/monitoring/test_news_data_monitoring.py \\
    --environment "\$ENVIRONMENT" \\
    --output json 2>/dev/null | sed -n '/^{/,\$p'
EOF

    chmod +x "/home/jianjun/ats-genai-data/scripts/monitoring_endpoint.sh"

    print_status 0 "Monitoring integration setup completed"
}

# Backup current state
create_deployment_backup() {
    print_info "Creating pre-deployment backup..."

    local backup_file="$BACKUP_DIR/pre_deployment_$(date +%Y%m%d_%H%M%S).sql"

    if docker exec ats-${ENVIRONMENT}-postgres pg_dump -U postgres ${ENVIRONMENT}_db > "$backup_file" 2>/dev/null; then
        print_status 0 "Backup created: $backup_file"
    else
        print_warning "Backup creation failed - continuing deployment"
    fi
}

# Generate deployment summary
generate_summary() {
    print_info "Deployment Summary:"
    echo ""

    print_info "📋 Configuration:"
    echo "   Environment: $ENVIRONMENT"
    echo "   Log Directory: $LOG_DIR"
    echo "   Backup Directory: $BACKUP_DIR"
    echo "   Deployment Log: $DEPLOYMENT_LOG"
    echo ""

    print_info "🔑 API Configuration:"
    if [ -n "$POLYGON_API_KEY" ]; then
        echo "   Polygon API Key: Configured ✅"
    else
        echo "   Polygon API Key: NOT CONFIGURED ⚠️"
        echo "   → Set POLYGON_API_KEY environment variable"
    fi
    echo ""

    print_info "📢 Alert Configuration:"
    if [ -n "$SLACK_WEBHOOK_URL" ]; then
        echo "   Slack Alerts: Configured ✅"
    else
        echo "   Slack Alerts: NOT CONFIGURED ⚠️"
        echo "   → Run: ./scripts/setup_slack_alerts.sh"
    fi
    echo ""

    print_info "⏰ Scheduled Jobs:"
    echo "   Daily Collection: 8:00 AM (collects previous day)"
    echo "   Health Monitoring: Every 4 hours"
    echo "   Log Cleanup: Weekly (Sundays 2:00 AM)"
    echo "   Backup Cleanup: Monthly (keep 3 months)"
    echo ""

    print_info "🔧 Manual Operations:"
    echo "   Run health check: ENVIRONMENT=$ENVIRONMENT ./scripts/cron/news_health_monitor_simple.sh"
    echo "   Manual collection: ENVIRONMENT=$ENVIRONMENT ./scripts/cron/daily_news_collection.sh"
    echo "   View monitoring: ./scripts/monitoring_endpoint.sh $ENVIRONMENT | jq"
    echo "   Check logs: tail -f $LOG_DIR/health_monitor_$(date +%Y%m%d).log"
    echo ""

    print_info "📊 Health Dashboard:"
    echo "   Analytics Service: http://localhost:$([ "$ENVIRONMENT" = "intg" ] && echo "4000" || echo "3000")"
    echo "   Grafana: http://localhost:$([ "$ENVIRONMENT" = "intg" ] && echo "4002" || echo "3001")"
    echo ""

    print_info "🚨 Alert Thresholds:"
    echo "   Data Freshness: > $([ "$ENVIRONMENT" = "prod" ] && echo "24" || echo "48") hours (Critical)"
    echo "   Daily Volume: < 10 articles on weekdays (Warning)"
    echo "   Database: Connection failures (Critical)"
    echo "   API Errors: > 50% failure rate (Critical)"
}

# Main deployment function
main() {
    print_header

    log "Starting deployment for environment: $ENVIRONMENT"

    # Setup logging
    mkdir -p "$LOG_DIR"

    # Run deployment steps
    if ! check_prerequisites; then
        print_error "Prerequisites check failed"
        exit 1
    fi

    setup_environment

    if ! setup_database; then
        print_error "Database setup failed"
        exit 1
    fi

    create_deployment_backup

    if ! deploy_scripts; then
        print_error "Script deployment failed"
        exit 1
    fi

    if ! install_cron_jobs; then
        print_error "Cron job installation failed"
        exit 1
    fi

    setup_monitoring_integration

    if ! validate_deployment; then
        print_error "Deployment validation failed"
        exit 1
    fi

    # Success!
    print_header
    print_status 0 "News Collection Production Deployment Completed!"
    echo ""

    generate_summary

    print_info "🎉 Deployment successful! News collection system is now active."
    log "Deployment completed successfully"
}

# Show usage if no environment specified
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Usage: $0 [environment]"
    echo ""
    echo "Deploys the complete news collection system with monitoring."
    echo ""
    echo "Arguments:"
    echo "  environment    Target environment (default: prod)"
    echo "                 Options: dev, intg, prod"
    echo ""
    echo "Environment Variables:"
    echo "  POLYGON_API_KEY     Polygon.io API key (required)"
    echo "  SLACK_WEBHOOK_URL   Slack webhook for alerts (optional)"
    echo ""
    echo "Examples:"
    echo "  $0 prod                    # Deploy to production"
    echo "  $0 intg                    # Deploy to integration"
    echo "  POLYGON_API_KEY=\"xxx\" $0  # Deploy with API key"
    echo ""
    exit 0
fi

# Run main deployment
main "$@"