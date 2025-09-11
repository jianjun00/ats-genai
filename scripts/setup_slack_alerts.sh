#!/bin/bash
# Setup Slack Alerts for News Collection Monitoring

set -e

echo "📢 Setting up Slack alerts for news collection monitoring"
echo ""

# Function to test Slack webhook
test_slack_webhook() {
    local webhook_url="$1"
    local test_message="🧪 Test alert from ATS News Collection Monitoring - Setup Complete!"
    
    echo "Testing Slack webhook..."
    
    local payload=$(cat <<EOF
{
    "text": "$test_message",
    "attachments": [
        {
            "color": "good",
            "fields": [
                {
                    "title": "Environment",
                    "value": "Test",
                    "short": true
                },
                {
                    "title": "Status",
                    "value": "Setup Successful",
                    "short": true
                },
                {
                    "title": "Time",
                    "value": "$(date)",
                    "short": false
                }
            ]
        }
    ]
}
EOF
    )
    
    if curl -s -X POST -H 'Content-type: application/json' \
             --data "$payload" \
             "$webhook_url" | grep -q "ok"; then
        echo "✅ Slack webhook test successful!"
        return 0
    else
        echo "❌ Slack webhook test failed"
        return 1
    fi
}

# Get webhook URL from user
get_webhook_url() {
    echo "To set up Slack alerts, you need a Slack webhook URL."
    echo ""
    echo "📋 Steps to create a Slack webhook:"
    echo "1. Go to https://api.slack.com/apps"
    echo "2. Create a new app or select existing app"
    echo "3. Go to 'Incoming Webhooks' and activate webhooks"
    echo "4. Click 'Add New Webhook to Workspace'"
    echo "5. Select the channel for news alerts"
    echo "6. Copy the webhook URL"
    echo ""
    
    read -p "Enter your Slack webhook URL: " webhook_url
    
    if [[ ! "$webhook_url" =~ ^https://hooks\.slack\.com/services/ ]]; then
        echo "❌ Invalid webhook URL format. Should start with https://hooks.slack.com/services/"
        return 1
    fi
    
    echo "$webhook_url"
}

# Setup environment variables
setup_environment_variables() {
    local webhook_url="$1"
    local environment="${2:-intg}"
    
    echo "Setting up environment variables..."
    
    # Create environment file
    local env_file="/home/jianjun/ats-genai-data/.env.alerts"
    
    cat > "$env_file" <<EOF
# Slack Webhook Configuration for News Collection Alerts
export SLACK_WEBHOOK_URL="$webhook_url"
export ENVIRONMENT="$environment"

# Alert Configuration
export ALERT_CRITICAL_ENABLED="true"
export ALERT_WARNING_ENABLED="true"
export ALERT_INFO_ENABLED="false"

# Notification Settings
export ALERT_CHANNEL_PREFIX="[ATS-NEWS]"
export ALERT_MENTION_ON_CRITICAL="@channel"
export ALERT_QUIET_HOURS="22:00-06:00"  # No alerts during these hours (24h format)
EOF
    
    echo "✅ Environment variables saved to $env_file"
    echo ""
    echo "To use these settings, run:"
    echo "  source $env_file"
    echo ""
    echo "Or add to your ~/.bashrc:"
    echo "  echo 'source $env_file' >> ~/.bashrc"
}

# Update health monitor to use Slack alerts
update_health_monitor() {
    echo "Updating health monitor script with Slack integration..."
    
    # Replace the simple health monitor with Slack-enabled version
    cat > "/home/jianjun/ats-genai-data/scripts/cron/news_health_monitor.sh" <<'EOF'
#!/bin/bash
# News Data Health Monitoring Script with Slack Integration

set -e

# Configuration
ENVIRONMENT="${ENVIRONMENT:-intg}"
LOG_DIR="/mnt/d/ats-logs/${ENVIRONMENT}"
SLACK_WEBHOOK="${SLACK_WEBHOOK_URL:-}"

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

# Send Slack alert
send_slack_alert() {
    local severity="$1"
    local message="$2"
    local details="${3:-}"
    
    if [ -z "$SLACK_WEBHOOK" ]; then
        log_warning "Slack webhook not configured - skipping alert"
        return 0
    fi
    
    # Check quiet hours
    local current_hour=$(date +%H)
    if [[ "$current_hour" -ge 22 || "$current_hour" -lt 6 ]]; then
        log "Quiet hours - skipping Slack alert"
        return 0
    fi
    
    local emoji="🔴"
    local color="danger"
    local mention=""
    
    case "$severity" in
        "critical")
            emoji="🔴"
            color="danger"
            mention="${ALERT_MENTION_ON_CRITICAL:-}"
            ;;
        "warning")
            emoji="🟡"
            color="warning"
            ;;
        "info")
            emoji="🔵"
            color="good"
            ;;
    esac
    
    local payload=$(cat <<EOF
{
    "text": "${mention} ${emoji} News Collection Alert - ${ENVIRONMENT^^}",
    "attachments": [
        {
            "color": "$color",
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
    
    if [ -n "$details" ]; then
        # Add details field
        payload=$(echo "$payload" | sed 's/}$/,{\"title\":\"Details\",\"value\":\"'"$details"'\",\"short\":false}]}/')
    fi
    
    if curl -s -X POST -H 'Content-type: application/json' \
             --data "$payload" \
             "$SLACK_WEBHOOK" >/dev/null 2>&1; then
        log "Slack alert sent successfully"
    else
        log_warning "Failed to send Slack alert"
    fi
}

# Run health monitoring with Slack integration
run_health_check() {
    log "🏥 Starting news data health check for $ENVIRONMENT"
    
    local temp_results="/tmp/news_health_${ENVIRONMENT}_$$.json"
    
    # Run monitoring script
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
        > "$temp_results" 2>/dev/null
    
    # Parse results
    if [ -s "$temp_results" ] && grep -q '"overall_health"' "$temp_results" 2>/dev/null; then
        local overall_health=$(python3 -c "
import json, sys
try:
    with open('$temp_results') as f: 
        content = f.read().strip()
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
        
        local critical_alerts=$(python3 -c "
import json, sys
try:
    with open('$temp_results') as f: 
        content = f.read().strip()
        json_start = content.find('{')
        if json_start >= 0:
            content = content[json_start:]
        data = json.loads(content)
        critical = [a for a in data.get('alerts', []) if a.get('severity') == 'critical']
        print(len(critical))
except: 
    print('0')
")
        
        log "Health check completed: $overall_health ($alert_count alerts, $critical_alerts critical)"
        
        # Send alerts based on severity
        if [ "$critical_alerts" -gt 0 ]; then
            local first_critical=$(python3 -c "
import json, sys
try:
    with open('$temp_results') as f: 
        content = f.read().strip()
        json_start = content.find('{')
        if json_start >= 0:
            content = content[json_start:]
        data = json.loads(content)
        critical = [a for a in data.get('alerts', []) if a.get('severity') == 'critical']
        if critical:
            print(f\"{critical[0]['check']}: {critical[0]['message']}\")
        else:
            print('Unknown critical issue')
except: 
    print('Critical health check issue')
")
            
            log_error "Critical alerts detected"
            send_slack_alert "critical" "$first_critical" "$critical_alerts critical alerts found"
            
        elif [ "$alert_count" -gt 0 ]; then
            local first_warning=$(python3 -c "
import json, sys
try:
    with open('$temp_results') as f: 
        content = f.read().strip()
        json_start = content.find('{')
        if json_start >= 0:
            content = content[json_start:]
        data = json.loads(content)
        warnings = [a for a in data.get('alerts', []) if a.get('severity') == 'warning']
        if warnings:
            print(f\"{warnings[0]['check']}: {warnings[0]['message']}\")
        else:
            print('Minor issues detected')
except: 
    print('Warning level issues')
")
            
            log_warning "Warning alerts detected"
            # Only send warning alerts every 6 hours to avoid spam
            local last_warning_file="/tmp/last_news_warning_${ENVIRONMENT}"
            local current_time=$(date +%s)
            local last_warning_time=0
            
            if [ -f "$last_warning_file" ]; then
                last_warning_time=$(cat "$last_warning_file" 2>/dev/null || echo "0")
            fi
            
            if [ $((current_time - last_warning_time)) -gt 21600 ]; then  # 6 hours
                send_slack_alert "warning" "$first_warning" "$alert_count warning alerts"
                echo "$current_time" > "$last_warning_file"
            else
                log "Warning alert suppressed (too recent)"
            fi
        else
            log_success "All health checks passed"
        fi
        
        rm -f "$temp_results"
        return 0
    else
        log_error "Health check script failed"
        send_slack_alert "critical" "Health monitoring script failed to execute"
        rm -f "$temp_results"
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
    
    # Alert if very low volume on weekdays
    if [ "$yesterday_count" -lt 10 ] && [ "$(date +%w)" != "0" ] && [ "$(date +%w)" != "6" ]; then
        log_warning "Low article volume detected: $yesterday_count articles"
        send_slack_alert "warning" "Low news volume: only $yesterday_count articles collected yesterday"
    fi
}

# Main execution
main() {
    log "🔍 News Health Monitor - $(date)"
    
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

main "$@"
EOF
    
    chmod +x "/home/jianjun/ats-genai-data/scripts/cron/news_health_monitor.sh"
    echo "✅ Health monitor updated with Slack integration"
}

# Main setup function
main() {
    echo "🚀 ATS News Collection - Slack Alerts Setup"
    echo "============================================"
    echo ""
    
    # Get webhook URL
    webhook_url=$(get_webhook_url)
    if [ $? -ne 0 ]; then
        echo "❌ Setup cancelled"
        exit 1
    fi
    
    # Test webhook
    if ! test_slack_webhook "$webhook_url"; then
        echo "❌ Webhook test failed. Please check the URL and try again."
        exit 1
    fi
    
    # Setup environment
    environment="${1:-intg}"
    setup_environment_variables "$webhook_url" "$environment"
    
    # Update health monitor
    update_health_monitor
    
    echo ""
    echo "✅ Slack alerts setup completed!"
    echo ""
    echo "📋 Next steps:"
    echo "1. Source the environment file: source /home/jianjun/ats-genai-data/.env.alerts"
    echo "2. Test the health monitor: ENVIRONMENT=$environment ./scripts/cron/news_health_monitor.sh"
    echo "3. The cron job will automatically use these settings"
    echo ""
    echo "🔧 Configuration:"
    echo "- Webhook URL: ${webhook_url:0:50}..."
    echo "- Environment: $environment"
    echo "- Quiet hours: 22:00-06:00 (no alerts during these times)"
    echo "- Warning throttling: Max 1 warning alert per 6 hours"
    echo ""
    echo "📱 You should receive alerts for:"
    echo "- 🔴 Critical: Database failures, script crashes, severe data issues"
    echo "- 🟡 Warning: Data gaps, low volume, quality issues (throttled)"
    echo ""
}

# Run main setup
main "$@"