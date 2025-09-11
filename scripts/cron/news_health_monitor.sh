#!/bin/bash
# News Data Health Monitoring Script
# Runs every 4 hours to check news collection health and alert on issues

set -e

# Configuration
ENVIRONMENT="${ENVIRONMENT:-intg}"
LOG_DIR="/mnt/d/ats-logs/${ENVIRONMENT}"
ALERT_WEBHOOK="${SLACK_WEBHOOK_URL:-}"

# Colors and logging
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Setup logging
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/health_monitor_$(date +%Y%m%d).log"

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

# Send Slack alert (if webhook configured)
send_slack_alert() {
    local severity="$1"
    local message="$2"
    
    if [ -n "$ALERT_WEBHOOK" ]; then
        local emoji="🔴"
        [ "$severity" = "warning" ] && emoji="🟡"
        [ "$severity" = "info" ] && emoji="🔵"
        
        local payload=$(cat <<EOF
{
    "text": "${emoji} News Collection Alert - ${ENVIRONMENT^^}",
    "attachments": [
        {
            "color": "$( [ "$severity" = "critical" ] && echo "danger" || echo "warning" )",
            "fields": [
                {
                    "title": "Environment",
                    "value": "${ENVIRONMENT}",
                    "short": true
                },
                {
                    "title": "Severity", 
                    "value": "${severity}",
                    "short": true
                },
                {
                    "title": "Message",
                    "value": "${message}",
                    "short": false
                },
                {
                    "title": "Time",
                    "value": "$(date)",
                    "short": true
                }
            ]
        }
    ]
}
EOF
        )
        
        curl -X POST -H 'Content-type: application/json' \
             --data "$payload" \
             "$ALERT_WEBHOOK" \
             &>/dev/null || log_warning "Failed to send Slack alert"
    fi
}

# Run health monitoring
run_health_check() {
    log "🏥 Starting news data health check for $ENVIRONMENT"
    
    # Create temporary file for results
    local temp_results="/tmp/news_health_${ENVIRONMENT}_$$.json"
    
    # Run monitoring script
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
        > "$temp_results" 2>"$temp_results.err"
    
    # Check if we got JSON output (the script may exit 1 for UNHEALTHY but still produce valid results)
    if [ -s "$temp_results" ] && grep -q '"overall_health"' "$temp_results" 2>/dev/null; then
        
        # Parse results
        local overall_health=$(cat "$temp_results" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('overall_health', 'UNKNOWN'))")
        local alert_count=$(cat "$temp_results" | python3 -c "import json, sys; data=json.load(sys.stdin); print(len(data.get('alerts', [])))")
        
        log "Health check completed: $overall_health ($alert_count alerts)"
        
        # Process alerts
        if [ "$alert_count" -gt 0 ]; then
            local critical_alerts=$(cat "$temp_results" | python3 -c "
import json, sys
data = json.load(sys.stdin)
critical = [a for a in data.get('alerts', []) if a.get('severity') == 'critical']
print(len(critical))
")
            
            if [ "$critical_alerts" -gt 0 ]; then
                log_error "Found $critical_alerts critical alerts"
                
                # Get first critical alert message
                local first_alert=$(cat "$temp_results" | python3 -c "
import json, sys
data = json.load(sys.stdin)
critical = [a for a in data.get('alerts', []) if a.get('severity') == 'critical']
if critical:
    print(f\"{critical[0]['check']}: {critical[0]['message']}\")
else:
    print('Unknown critical issue')
")
                
                send_slack_alert "critical" "$first_alert"
            else
                log_warning "Found $alert_count non-critical alerts"
                
                # Get first warning
                local first_warning=$(cat "$temp_results" | python3 -c "
import json, sys
data = json.load(sys.stdin)
warnings = [a for a in data.get('alerts', []) if a.get('severity') == 'warning']
if warnings:
    print(f\"{warnings[0]['check']}: {warnings[0]['message']}\")
else:
    print('Multiple minor issues detected')
")
                
                send_slack_alert "warning" "$first_warning"
            fi
        else
            log_success "All health checks passed"
        fi
        
        # Cleanup
        rm -f "$temp_results"
        
        return 0
    else
        log_error "Health check script failed to run"
        if [ -f "$temp_results.err" ]; then
            log_error "Error details: $(cat "$temp_results.err" | head -5)"
        fi
        send_slack_alert "critical" "Health monitoring script failed to execute"
        rm -f "$temp_results" "$temp_results.err"
        return 1
    fi
}

# Quick stats summary
report_quick_stats() {
    log "📊 Quick Statistics:"
    
    # Get basic counts
    local today_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon 
        WHERE DATE(published_utc) = CURRENT_DATE
    " 2>/dev/null | xargs || echo "0")
    
    local yesterday_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon 
        WHERE DATE(published_utc) = CURRENT_DATE - INTERVAL '1 day'
    " 2>/dev/null | xargs || echo "0")
    
    local total_count=$(docker exec ats-${ENVIRONMENT}-postgres psql -U postgres -d ${ENVIRONMENT}_db -t -c "
        SELECT COUNT(*) FROM ${ENVIRONMENT}_news_polygon
    " 2>/dev/null | xargs || echo "0")
    
    log "   Today: $today_count articles"
    log "   Yesterday: $yesterday_count articles"  
    log "   Total: $total_count articles"
    
    # Alert if very low daily volume
    if [ "$yesterday_count" -lt 10 ] && [ "$(date +%w)" != "0" ] && [ "$(date +%w)" != "6" ]; then
        log_warning "Low article volume detected for yesterday: $yesterday_count articles"
        send_slack_alert "warning" "Low news volume: only $yesterday_count articles collected yesterday"
    fi
}

# Main execution
main() {
    log "🔍 News Health Monitor - $(date)"
    log "Environment: $ENVIRONMENT"
    
    # Quick connectivity test
    if ! docker exec ats-${ENVIRONMENT}-postgres pg_isready -U postgres >/dev/null 2>&1; then
        log_error "Database not accessible"
        send_slack_alert "critical" "Database connection failed"
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