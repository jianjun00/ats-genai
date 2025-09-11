#!/bin/bash
# Simplified News Data Health Monitoring Script

set -e

# Configuration
ENVIRONMENT="${ENVIRONMENT:-intg}"
LOG_DIR="/mnt/d/ats-logs/${ENVIRONMENT}"

# Setup logging
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/health_monitor_$(date +%Y%m%d).log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - \033[0;32mSUCCESS\033[0m - $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - \033[1;33mWARNING\033[0m - $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - \033[0;31mERROR\033[0m - $1" | tee -a "$LOG_FILE"
}

# Run health monitoring
run_health_check() {
    log "🏥 Starting news data health check for $ENVIRONMENT"
    
    local temp_results="/tmp/news_health_${ENVIRONMENT}_$$.json"
    local temp_errors="/tmp/news_health_${ENVIRONMENT}_$$.err"
    
    # Run monitoring script and capture both stdout and stderr
    docker run --rm \
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
        > "$temp_results" 2> "$temp_errors"
    
    local exit_code=$?
    
    # Check if we got valid JSON output regardless of exit code
    if [ -s "$temp_results" ] && grep -q '"overall_health"' "$temp_results" 2>/dev/null; then
        # Parse results successfully
        local overall_health=$(python3 -c "
import json, sys
try:
    with open('$temp_results') as f: 
        content = f.read().strip()
        # Skip GIN DEBUG lines
        json_start = content.find('{')
        if json_start >= 0:
            content = content[json_start:]
        data = json.loads(content)
        print(data.get('overall_health', 'UNKNOWN'))
except: 
    print('PARSE_ERROR')
")
        
        local alert_count=$(python3 -c "
import json, sys
try:
    with open('$temp_results') as f: 
        content = f.read().strip()
        json_start = content.find('{')
        if json_start >= 0:
            content = content[json_start:]
        data = json.loads(content)
        print(len(data.get('alerts', [])))
except: 
    print('0')
")
        
        log "Health check completed: $overall_health ($alert_count alerts)"
        
        # Report based on health status
        if [ "$overall_health" = "HEALTHY" ]; then
            log_success "All health checks passed"
        elif [ "$alert_count" -gt 0 ]; then
            log_warning "Found $alert_count alerts - system may need attention"
        else
            log_warning "Health status: $overall_health"
        fi
        
        # Cleanup
        rm -f "$temp_results" "$temp_errors"
        return 0
    else
        log_error "Health check script failed to produce valid results"
        if [ -f "$temp_errors" ] && [ -s "$temp_errors" ]; then
            log_error "Error details: $(tail -3 "$temp_errors" | tr '\n' ' ')"
        fi
        rm -f "$temp_results" "$temp_errors"
        return 1
    fi
}

# Quick database stats
report_quick_stats() {
    log "📊 Quick Statistics:"
    
    local yesterday_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon 
        WHERE DATE(published_utc) = CURRENT_DATE - INTERVAL '1 day'
    " 2>/dev/null | xargs || echo "0")
    
    local total_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon
    " 2>/dev/null | xargs || echo "0")
    
    log "   Yesterday: $yesterday_count articles"
    log "   Total: $total_count articles"
}

# Main execution
main() {
    log "🔍 News Health Monitor - $(date)"
    log "Environment: $ENVIRONMENT"
    
    # Quick connectivity test
    if ! docker exec ats-${ENVIRONMENT}-postgres pg_isready -U postgres >/dev/null 2>&1; then
        log_error "Database not accessible"
        exit 1
    fi
    
    # Run health check
    if run_health_check; then
        report_quick_stats
        log_success "Health monitoring completed"
    else
        log_error "Health monitoring failed"
        exit 1
    fi
}

# Execute main function
main "$@"