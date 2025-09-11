#!/bin/bash
# Daily News Collection Script for Production
# Runs daily to collect news from previous day and ensure continuous coverage

set -e

# Configuration
ENVIRONMENT="${ENVIRONMENT:-intg}"
LOG_DIR="/mnt/d/ats-logs/${ENVIRONMENT}"
BACKUP_DIR="/mnt/d/ats-backup/${ENVIRONMENT}"
LOCK_FILE="/tmp/daily_news_${ENVIRONMENT}.lock"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging setup
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_news_$(date +%Y%m%d).log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - ${RED}ERROR${NC} - $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - ${GREEN}SUCCESS${NC} - $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - ${YELLOW}WARNING${NC} - $1" | tee -a "$LOG_FILE"
}

# Check for existing process
check_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_error "Daily news collection already running (PID: $pid)"
            exit 1
        else
            log_warning "Removing stale lock file"
            rm -f "$LOCK_FILE"
        fi
    fi
    echo $$ > "$LOCK_FILE"
}

# Cleanup function
cleanup() {
    rm -f "$LOCK_FILE"
}
trap cleanup EXIT

# Main collection function
run_daily_collection() {
    log "🚀 Starting daily news collection for $ENVIRONMENT"
    
    # Calculate date range (collect previous day to cover any gaps)
    local start_date=$(date -d "2 days ago" +%Y-%m-%d)
    local end_date=$(date -d "1 day ago" +%Y-%m-%d)
    
    log "📅 Collecting news from $start_date to $end_date"
    
    # Run the fixed backfill script
    local success=true
    if ! docker run --rm \
        --network ats-${ENVIRONMENT}-network \
        -e POLYGON_API_KEY="$POLYGON_API_KEY" \
        -e PYTHONPATH="/workspace/src" \
        -e DB_HOST="ats-${ENVIRONMENT}-postgres" \
        -e DB_PORT="5432" \
        -e DB_USER="postgres" \
        -e DB_PASSWORD="${ENVIRONMENT}_password" \
        -e DB_NAME="${ENVIRONMENT}_db" \
        -v /home/jianjun/ats-genai-data:/workspace \
        -w /workspace \
        dragonflyer762/ats-genai:latest \
        python3 scripts/polygon_news_backfill.py \
        --start-date "$start_date" \
        --end-date "$end_date" \
        --environment "$ENVIRONMENT" \
        --limit-per-request 100 \
        --max-requests 20 \
        >> "$LOG_FILE" 2>&1; then
        
        success=false
        log_error "News collection failed"
    fi
    
    if $success; then
        log_success "News collection completed successfully"
        
        # Run health check
        log "🏥 Running post-collection health check..."
        run_health_check
    else
        log_error "News collection failed - check logs for details"
        return 1
    fi
}

# Health check after collection
run_health_check() {
    if docker run --rm \
        --network ats-${ENVIRONMENT}-network \
        -e PYTHONPATH="/workspace/src" \
        -e DB_HOST="ats-${ENVIRONMENT}-postgres" \
        -e DB_PORT="5432" \
        -e DB_USER="postgres" \
        -e DB_PASSWORD="${ENVIRONMENT}_password" \
        -e DB_NAME="${ENVIRONMENT}_db" \
        -v /home/jianjun/ats-genai-data:/workspace \
        -w /workspace \
        dragonflyer762/ats-genai:latest \
        python3 tests/monitoring/test_news_data_monitoring.py \
        --environment "$ENVIRONMENT" \
        --output json \
        >> "$LOG_FILE" 2>&1; then
        
        log_success "Health check passed"
    else
        log_warning "Health check detected issues - see logs"
    fi
}

# Backup function (weekly)
run_weekly_backup() {
    if [ "$(date +%w)" = "0" ]; then  # Sunday
        log "📦 Running weekly backup..."
        local backup_file="$BACKUP_DIR/weekly_news_backup_$(date +%Y%m%d).sql"
        mkdir -p "$BACKUP_DIR"
        
        if docker exec ats-${ENVIRONMENT}-postgres pg_dump -U postgres ${ENVIRONMENT}_db > "$backup_file" 2>/dev/null; then
            log_success "Weekly backup created: $backup_file"
            
            # Keep only last 4 weekly backups
            find "$BACKUP_DIR" -name "weekly_news_backup_*.sql" -mtime +28 -delete
        else
            log_warning "Weekly backup failed"
        fi
    fi
}

# Statistics reporting
report_collection_stats() {
    log "📊 Collection Statistics:"
    
    # Get recent article counts
    local today_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon 
        WHERE DATE(published_utc) = CURRENT_DATE - INTERVAL '1 day'
    " 2>/dev/null | xargs || echo "0")
    
    local week_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon 
        WHERE published_utc >= CURRENT_DATE - INTERVAL '7 days'
    " 2>/dev/null | xargs || echo "0")
    
    log "   Yesterday: $today_count articles"
    log "   Last 7 days: $week_count articles"
    
    # Check data freshness
    local latest_article=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT MAX(published_utc) FROM ${ENVIRONMENT}_news_polygon
    " 2>/dev/null | xargs || echo "unknown")
    
    log "   Latest article: $latest_article"
}

# Main execution
main() {
    log "=" * 60
    log "🗞️  Daily News Collection - $(date)"
    log "Environment: $ENVIRONMENT"
    
    check_lock
    
    # Check required environment variables
    if [ -z "$POLYGON_API_KEY" ]; then
        log_error "POLYGON_API_KEY not set"
        exit 1
    fi
    
    # Run collection
    if run_daily_collection; then
        run_weekly_backup
        report_collection_stats
        log_success "Daily news collection completed successfully"
    else
        log_error "Daily news collection failed"
        exit 1
    fi
    
    log "=" * 60
}

# Execute main function
main "$@"