#!/bin/bash
# Minikube Fault Tolerance and Monitoring Script
# This script implements comprehensive fault tolerance for minikube

set -euo pipefail

# Configuration
MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-minikube}
LOG_DIR="/tmp/minikube-monitoring"
HEALTH_CHECK_INTERVAL=30
RESTART_THRESHOLD=3
DISK_WARNING_THRESHOLD=85
MEMORY_WARNING_THRESHOLD=80

# Slack Configuration
SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL:-"https://hooks.slack.com/services/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"}
SLACK_CHANNEL=${SLACK_CHANNEL:-"#minikube-alerts"}

# Create log directory
mkdir -p "$LOG_DIR"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_DIR/minikube-monitor.log"
}

# Slack notification function
send_slack_alert() {
    local title="$1"
    local message="$2"
    local color="${3:-#ff9900}"  # Default orange
    local priority="${4:-medium}"
    
    if [[ -z "$SLACK_WEBHOOK_URL" ]]; then
        log "WARNING: SLACK_WEBHOOK_URL not configured. Skipping Slack notification."
        return 0
    fi
    
    local hostname
    hostname=$(hostname)
    
    local payload
    payload=$(cat <<EOF
{
    "channel": "$SLACK_CHANNEL",
    "username": "Minikube Monitor",
    "icon_emoji": ":warning:",
    "attachments": [
        {
            "color": "$color",
            "title": "$title",
            "text": "$message",
            "fields": [
                {
                    "title": "Host",
                    "value": "$hostname",
                    "short": true
                },
                {
                    "title": "Priority",
                    "value": "$priority",
                    "short": true
                },
                {
                    "title": "Timestamp",
                    "value": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
                    "short": true
                }
            ],
            "footer": "Minikube Fault Tolerance Monitor"
        }
    ]
}
EOF
)
    
    if curl -X POST -H 'Content-type: application/json' \
            --data "$payload" \
            "$SLACK_WEBHOOK_URL" \
            --silent --fail >/dev/null 2>&1; then
        log "Slack alert sent: $title"
    else
        log "ERROR: Failed to send Slack alert: $title"
    fi
}

# Resource monitoring
check_resources() {
    log "Checking system resources..."
    
    # Check disk space
    local disk_usage
    disk_usage=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ "$disk_usage" -gt "$DISK_WARNING_THRESHOLD" ]; then
        log "WARNING: Disk usage is ${disk_usage}% (threshold: ${DISK_WARNING_THRESHOLD}%)"
        send_slack_alert "⚠️ High Disk Usage Detected" \
            "Disk usage is at ${disk_usage}% (threshold: ${DISK_WARNING_THRESHOLD}%). Automatic cleanup will be initiated." \
            "#ff9900" "high"
        cleanup_docker_resources
        
        # If still high after cleanup, kill non-essential pods
        local new_disk_usage
        new_disk_usage=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
        if [ "$new_disk_usage" -gt "$DISK_WARNING_THRESHOLD" ]; then
            manage_resource_pressure "disk" "$new_disk_usage" "$DISK_WARNING_THRESHOLD"
        fi
    fi
    
    # Check memory
    local memory_usage
    memory_usage=$(free | awk 'NR==2{printf "%.0f", $3*100/$2}')
    if [ "$memory_usage" -gt "$MEMORY_WARNING_THRESHOLD" ]; then
        log "WARNING: Memory usage is ${memory_usage}% (threshold: ${MEMORY_WARNING_THRESHOLD}%)"
        send_slack_alert "⚠️ High Memory Usage Detected" \
            "Memory usage is at ${memory_usage}% (threshold: ${MEMORY_WARNING_THRESHOLD}%). Will kill non-essential pods if needed." \
            "#ff9900" "medium"
        manage_resource_pressure "memory" "$memory_usage" "$MEMORY_WARNING_THRESHOLD"
    fi
    
    # Check CPU usage
    local cpu_usage
    cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | sed 's/%us,//' | sed 's/\..*//' || echo "0")
    if [[ "$cpu_usage" =~ ^[0-9]+$ ]] && [ "$cpu_usage" -gt 90 ]; then
        log "WARNING: CPU usage is ${cpu_usage}% (threshold: 90%)"
        send_slack_alert "⚠️ High CPU Usage Detected" \
            "CPU usage is at ${cpu_usage}% (threshold: 90%). Will kill non-essential pods if needed." \
            "#ff9900" "medium"
        manage_resource_pressure "cpu" "$cpu_usage" "90"
    fi
    
    # Check Docker resources
    local docker_images_size
    docker_images_size=$(docker system df --format "table {{.TotalCount}}\t{{.Size}}" | awk 'NR==2 {print $2}' || echo "0B")
    log "Docker images using: $docker_images_size"
}

# Docker cleanup
cleanup_docker_resources() {
    log "Cleaning up Docker resources..."
    
    # Remove unused images
    docker image prune -af --filter "until=24h" 2>/dev/null || true
    
    # Remove unused containers
    docker container prune -f 2>/dev/null || true
    
    # Remove unused volumes (be careful with this)
    docker volume prune -f 2>/dev/null || true
    
    # Remove build cache
    docker builder prune -af 2>/dev/null || true
    
    log "Docker cleanup completed"
    send_slack_alert "🧹 Docker Cleanup Completed" \
        "Docker resources have been cleaned up to free disk space." \
        "#36a64f" "info"
}

# Resource pressure management - kill non-essential pods when under pressure
manage_resource_pressure() {
    local pressure_type="$1"  # disk, memory, or cpu
    local current_usage="$2"
    local threshold="$3"
    
    log "Managing $pressure_type pressure: ${current_usage}% (threshold: ${threshold}%)"
    
    # Define non-essential pods that can be killed (excluding postgres and critical services)
    local non_essential_labels=(
        "job-name=polygon-instruments"
        "job-name=tiingo-instruments" 
        "job-name=query-*"
        "app=training-data-webapp"
        "app=portfolio-webapp"
        "job-name=enhanced-training"
        "job-name=price-unification"
    )
    
    local killed_pods=0
    
    for label in "${non_essential_labels[@]}"; do
        # Get non-essential pods
        local pods
        pods=$(kubectl get pods -n ats-dev -l "$label" --no-headers 2>/dev/null | awk '{print $1}' || true)
        
        if [[ -n "$pods" ]]; then
            for pod in $pods; do
                if [[ $killed_pods -lt 3 ]]; then  # Limit to 3 pods per pressure event
                    log "Killing non-essential pod under $pressure_type pressure: $pod"
                    kubectl delete pod "$pod" -n ats-dev --grace-period=30 2>/dev/null || true
                    ((killed_pods++))
                fi
            done
        fi
    done
    
    if [[ $killed_pods -gt 0 ]]; then
        send_slack_alert "⚡ Resource Pressure Management" \
            "Killed $killed_pods non-essential pods due to high $pressure_type usage (${current_usage}%). Critical services (PostgreSQL) preserved." \
            "#ff9900" "high"
    fi
    
    # For extreme pressure, also consider scaling down deployments
    if [[ "$current_usage" -gt $((threshold + 10)) ]]; then
        log "Extreme $pressure_type pressure detected. Scaling down non-essential deployments..."
        
        # Scale down non-essential deployments
        kubectl scale deployment training-data-webapp --replicas=0 -n ats-dev 2>/dev/null || true
        kubectl scale deployment portfolio-webapp --replicas=0 -n ats-dev 2>/dev/null || true
        
        send_slack_alert "🔻 Extreme Resource Pressure" \
            "Scaled down non-essential deployments due to extreme $pressure_type pressure (${current_usage}%). System in emergency mode." \
            "#ff0000" "critical"
    fi
}

# Health check function
check_minikube_health() {
    local status
    status=$(minikube status --profile="$MINIKUBE_PROFILE" --output=json 2>/dev/null || echo '{"Host":"Stopped"}')
    
    # Parse JSON status
    local host_status
    host_status=$(echo "$status" | jq -r '.Host // "Unknown"' 2>/dev/null || echo "Unknown")
    
    if [ "$host_status" != "Running" ]; then
        log "CRITICAL: Minikube host status: $host_status"
        send_slack_alert "🚨 Minikube Cluster Down" \
            "Minikube host status: $host_status. Cluster is not running!" \
            "#ff0000" "critical"
        return 1
    fi
    
    # Check if kubectl works
    if ! kubectl get nodes --request-timeout=10s >/dev/null 2>&1; then
        log "CRITICAL: kubectl not responding"
        send_slack_alert "🚨 Kubernetes API Not Responding" \
            "kubectl is not responding. Kubernetes API server may be down." \
            "#ff0000" "critical"
        return 1
    fi
    
    # Check PostgreSQL persistence
    if ! kubectl get pod -n ats-dev -l app=postgres --no-headers 2>/dev/null | grep -q "Running"; then
        log "CRITICAL: PostgreSQL pod not running"
        send_slack_alert "🗄️ PostgreSQL Pod Down" \
            "PostgreSQL pod in ats-dev namespace is not running. Data persistence may be affected." \
            "#ff0000" "critical"
        return 1
    fi
    
    log "Health check passed"
    return 0
}

# Backup persistent data
backup_persistent_data() {
    log "Creating backup of persistent data..."
    
    local backup_dir="$LOG_DIR/backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$backup_dir"
    
    # Backup persistent volumes
    kubectl get pv -o yaml > "$backup_dir/persistent-volumes.yaml" 2>/dev/null || true
    kubectl get pvc --all-namespaces -o yaml > "$backup_dir/persistent-volume-claims.yaml" 2>/dev/null || true
    
    # Backup important namespaces
    for ns in ats-dev kube-system; do
        if kubectl get namespace "$ns" >/dev/null 2>&1; then
            kubectl get all -n "$ns" -o yaml > "$backup_dir/namespace-$ns.yaml" 2>/dev/null || true
        fi
    done
    
    log "Backup completed: $backup_dir"
    echo "$backup_dir"
}

# Smart restart with backup
smart_restart() {
    log "Initiating smart restart..."
    
    # Create backup before restart
    local backup_path
    backup_path=$(backup_persistent_data)
    
    # Stop minikube gracefully
    log "Stopping minikube gracefully..."
    minikube stop --profile="$MINIKUBE_PROFILE" 2>/dev/null || true
    
    # Clean up resources
    cleanup_docker_resources
    
    # Start minikube with optimal settings
    log "Starting minikube with fault-tolerant configuration..."
    minikube start --profile="$MINIKUBE_PROFILE" \
        --driver=docker \
        --memory=8192 \
        --cpus=4 \
        --disk-size=40g \
        --kubernetes-version=stable \
        --extra-config=kubelet.housekeeping-interval=10s \
        --extra-config=kubelet.image-gc-high-threshold=85 \
        --extra-config=kubelet.image-gc-low-threshold=80 \
        --extra-config=kubelet.minimum-image-ttl-duration=120s \
        2>&1 | tee "$LOG_DIR/restart-$(date +%Y%m%d_%H%M%S).log"
    
    log "Minikube restart completed. Backup available at: $backup_path"
}

# Monitoring loop
monitor_loop() {
    local failure_count=0
    
    log "Starting minikube monitoring loop..."
    send_slack_alert "🎯 Minikube Monitoring Started" \
        "Fault tolerance monitoring initiated. Health checks every 30s with automatic pod management during resource pressure." \
        "#36a64f" "info"
    
    while true; do
        check_resources
        
        if check_minikube_health; then
            failure_count=0
            log "Minikube is healthy"
        else
            ((failure_count++))
            log "Health check failed ($failure_count/$RESTART_THRESHOLD)"
            
            if [ $failure_count -ge $RESTART_THRESHOLD ]; then
                log "Failure threshold reached. Initiating smart restart..."
                send_slack_alert "🔄 Minikube Auto-Restart Initiated" \
                    "Minikube failed health checks $failure_count times. Auto-restart initiated with data backup." \
                    "#ff9900" "critical"
                smart_restart
                failure_count=0
            fi
        fi
        
        sleep $HEALTH_CHECK_INTERVAL
    done
}

# Install monitoring as systemd service (if running on systemd)
install_service() {
    if command -v systemctl >/dev/null 2>&1; then
        log "Installing minikube monitoring service..."
        
        cat > /tmp/minikube-monitor.service << 'EOF'
[Unit]
Description=Minikube Fault Tolerance Monitor
After=docker.service
Wants=docker.service

[Service]
Type=simple
User=jianjun
ExecStart=/home/jianjun/ats-genai-data/scripts/minikube-fault-tolerance.sh monitor
Restart=always
RestartSec=30
Environment=HOME=/home/jianjun
Environment=PATH=/usr/local/bin:/usr/bin:/bin

[Install]
WantedBy=multi-user.target
EOF
        
        sudo mv /tmp/minikube-monitor.service /etc/systemd/system/
        sudo systemctl daemon-reload
        sudo systemctl enable minikube-monitor.service
        
        log "Service installed. Start with: sudo systemctl start minikube-monitor"
    else
        log "Systemd not available. Run manually or set up your own process manager."
    fi
}

# Main function
main() {
    case "${1:-help}" in
        "monitor")
            monitor_loop
            ;;
        "check")
            check_resources
            check_minikube_health
            ;;
        "restart")
            smart_restart
            ;;
        "backup")
            backup_persistent_data
            ;;
        "cleanup")
            cleanup_docker_resources
            ;;
        "install")
            install_service
            ;;
        "test")
            # Test Slack integration and data persistence
            send_slack_alert "🧪 Test Alert" \
                "This is a test alert from the Minikube fault tolerance system." \
                "#0066cc" "info"
            check_resources
            check_minikube_health
            ;;
        *)
            echo "Usage: $0 {monitor|check|restart|backup|cleanup|install|test}"
            echo ""
            echo "Commands:"
            echo "  monitor  - Start continuous monitoring loop"
            echo "  check    - Run one-time health check"
            echo "  restart  - Smart restart with backup"
            echo "  backup   - Backup persistent data"
            echo "  cleanup  - Clean Docker resources"
            echo "  install  - Install as systemd service"
            echo "  test     - Test Slack integration and health checks"
            exit 1
            ;;
    esac
}

main "$@"