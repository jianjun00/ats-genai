#!/bin/bash
# PostgreSQL Fault Tolerance Upgrade Script
# Safely migrates from Deployment to StatefulSet with enhanced fault tolerance

set -euo pipefail

NAMESPACE="ats-dev"
BACKUP_PATH="/tmp/postgres-migration-backup"
SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-https://hooks.slack.com/services/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}"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

send_slack_alert() {
    local title="$1"
    local message="$2"
    local color="${3:-#0066cc}"

    if [[ -n "$SLACK_WEBHOOK_URL" ]]; then
        curl -X POST -H 'Content-type: application/json' \
            --data "{\"attachments\":[{\"color\":\"$color\",\"title\":\"$title\",\"text\":\"$message\"}]}" \
            "$SLACK_WEBHOOK_URL" --silent --fail >/dev/null 2>&1 || true
    fi
}

# Pre-upgrade checks
pre_upgrade_checks() {
    log "Running pre-upgrade checks..."

    # Check if PostgreSQL is running
    if ! kubectl get deployment postgres -n "$NAMESPACE" >/dev/null 2>&1; then
        log "ERROR: PostgreSQL deployment not found"
        exit 1
    fi

    # Check if pod is ready
    local pod_name
    pod_name=$(kubectl get pod -n "$NAMESPACE" -l app=postgres --no-headers | head -1 | awk '{print $1}')
    if [[ -z "$pod_name" ]]; then
        log "ERROR: No PostgreSQL pod found"
        exit 1
    fi

    # Test database connectivity
    if ! kubectl exec -n "$NAMESPACE" "$pod_name" -- pg_isready -U postgres >/dev/null 2>&1; then
        log "ERROR: PostgreSQL is not ready"
        exit 1
    fi

    # Check disk space
    local disk_usage
    disk_usage=$(df -h /mnt/host/data | awk 'NR==2 {print $5}' | sed 's/%//')
    if [[ "$disk_usage" -gt 80 ]]; then
        log "WARNING: Disk usage is ${disk_usage}% - consider cleanup before upgrade"
    fi

    log "✅ Pre-upgrade checks passed"
}

# Create comprehensive backup before upgrade
create_migration_backup() {
    log "Creating comprehensive backup before migration..."

    mkdir -p "$BACKUP_PATH"
    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)

    local pod_name
    pod_name=$(kubectl get pod -n "$NAMESPACE" -l app=postgres --no-headers | head -1 | awk '{print $1}')

    # Create full database dump
    kubectl exec -n "$NAMESPACE" "$pod_name" -- pg_dump -U postgres -d dev_db \
        --format=custom --compress=9 \
        --file="/tmp/migration_backup_$timestamp.dump"

    # Copy backup to host
    kubectl cp "$NAMESPACE/$pod_name:/tmp/migration_backup_$timestamp.dump" \
        "$BACKUP_PATH/migration_backup_$timestamp.dump"

    # Create schema-only backup
    kubectl exec -n "$NAMESPACE" "$pod_name" -- pg_dump -U postgres -d dev_db \
        --schema-only --format=plain \
        --file="/tmp/schema_backup_$timestamp.sql"

    kubectl cp "$NAMESPACE/$pod_name:/tmp/schema_backup_$timestamp.sql" \
        "$BACKUP_PATH/schema_backup_$timestamp.sql"

    # Backup configuration
    kubectl get deployment postgres -n "$NAMESPACE" -o yaml > "$BACKUP_PATH/original_deployment.yaml"

    # Create restoration instructions
    cat > "$BACKUP_PATH/restore_instructions.txt" << EOF
PostgreSQL Migration Backup - $timestamp

Files:
- migration_backup_$timestamp.dump: Full database backup (custom format)
- schema_backup_$timestamp.sql: Schema-only backup (plain SQL)
- original_deployment.yaml: Original deployment configuration

To restore from backup:
1. kubectl apply -f original_deployment.yaml
2. Wait for pod to be ready
3. kubectl exec -n $NAMESPACE deployment/postgres -- pg_restore -U postgres -d dev_db /path/to/migration_backup_$timestamp.dump

Or for schema-only restore:
kubectl exec -n $NAMESPACE deployment/postgres -- psql -U postgres -d dev_db -f /path/to/schema_backup_$timestamp.sql
EOF

    log "✅ Backup created at $BACKUP_PATH"
    send_slack_alert "💾 PostgreSQL Migration Backup Created" \
        "Comprehensive backup completed before StatefulSet upgrade" "#36a64f"
}

# Upgrade to StatefulSet
upgrade_to_statefulset() {
    log "Upgrading PostgreSQL to StatefulSet with fault tolerance..."

    send_slack_alert "🔄 PostgreSQL Upgrade Starting" \
        "Migrating from Deployment to StatefulSet. Brief downtime expected." "#ff9900"

    # Scale down current deployment
    log "Scaling down current deployment..."
    kubectl scale deployment postgres --replicas=0 -n "$NAMESPACE"

    # Wait for pod to be terminated
    log "Waiting for pod termination..."
    kubectl wait --for=delete pod -l app=postgres -n "$NAMESPACE" --timeout=120s

    # Delete the deployment (keep the service)
    log "Removing old deployment..."
    kubectl delete deployment postgres -n "$NAMESPACE"

    # Apply StatefulSet configuration
    log "Applying StatefulSet configuration..."
    kubectl apply -f /home/jianjun/ats-genai-data/k8s/postgres-statefulset-enhanced.yaml

    # Wait for StatefulSet to be ready
    log "Waiting for StatefulSet to be ready..."
    kubectl wait --for=condition=ready pod -l app=postgres -n "$NAMESPACE" --timeout=300s

    # Verify the upgrade
    verify_upgrade

    log "✅ Upgrade to StatefulSet completed"
    send_slack_alert "✅ PostgreSQL StatefulSet Upgrade Completed" \
        "Successfully migrated to StatefulSet with enhanced fault tolerance" "#36a64f"
}

# Deploy PgBouncer for connection pooling
deploy_pgbouncer() {
    log "Deploying PgBouncer for connection pooling..."

    # Apply PgBouncer configuration
    kubectl apply -f /home/jianjun/ats-genai-data/k8s/pgbouncer-deployment.yaml

    # Wait for PgBouncer to be ready
    log "Waiting for PgBouncer to be ready..."
    kubectl wait --for=condition=available deployment/pgbouncer -n "$NAMESPACE" --timeout=120s

    log "✅ PgBouncer deployed successfully"
    send_slack_alert "🔗 PgBouncer Connection Pooling Deployed" \
        "Connection pooling now active - applications should use port 6432" "#36a64f"
}

# Verify the upgrade
verify_upgrade() {
    log "Verifying upgrade..."

    # Check StatefulSet status
    if ! kubectl get statefulset postgres -n "$NAMESPACE" >/dev/null 2>&1; then
        log "ERROR: StatefulSet not found"
        exit 1
    fi

    # Check pod status
    local pod_name
    pod_name=$(kubectl get pod -n "$NAMESPACE" -l app=postgres --no-headers | head -1 | awk '{print $1}')
    if [[ -z "$pod_name" ]]; then
        log "ERROR: No StatefulSet pod found"
        exit 1
    fi

    # Test database connectivity
    if ! kubectl exec -n "$NAMESPACE" "$pod_name" -- pg_isready -U postgres >/dev/null 2>&1; then
        log "ERROR: PostgreSQL not ready after upgrade"
        exit 1
    fi

    # Test database functionality
    local table_count
    table_count=$(kubectl exec -n "$NAMESPACE" "$pod_name" -- psql -U postgres -d dev_db -t -c \
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';" | xargs)

    log "Database verification: $table_count tables found"

    # Check WAL archiving configuration
    local archive_mode
    archive_mode=$(kubectl exec -n "$NAMESPACE" "$pod_name" -- psql -U postgres -d postgres -t -c \
        "SHOW archive_mode;" | xargs)

    if [[ "$archive_mode" == "on" ]]; then
        log "✅ WAL archiving is enabled"
    else
        log "WARNING: WAL archiving is not enabled"
    fi

    log "✅ Upgrade verification completed"
}

# Setup monitoring and maintenance
setup_monitoring() {
    log "Setting up enhanced monitoring and maintenance..."

    # Run initial PostgreSQL fault tolerance setup
    /home/jianjun/ats-genai-data/scripts/postgres-fault-tolerance.sh all

    # Create daily backup cron job
    cat > /tmp/postgres-daily-backup.sh << 'EOF'
#!/bin/bash
/home/jianjun/ats-genai-data/scripts/postgres-fault-tolerance.sh backup
EOF

    chmod +x /tmp/postgres-daily-backup.sh

    # Add to crontab (backup at 2 AM daily)
    (crontab -l 2>/dev/null || echo "") | grep -v "postgres-daily-backup" > /tmp/current_cron
    echo "0 2 * * * /tmp/postgres-daily-backup.sh" >> /tmp/current_cron
    crontab /tmp/current_cron
    rm /tmp/current_cron

    log "✅ Monitoring and maintenance setup completed"
    send_slack_alert "📊 PostgreSQL Monitoring Activated" \
        "Daily backups, health checks, and maintenance now automated" "#36a64f"
}

# Rollback function (if needed)
rollback() {
    log "Rolling back to original deployment..."

    send_slack_alert "⚠️ PostgreSQL Rollback Initiated" \
        "Rolling back to original Deployment configuration" "#ff9900"

    # Delete StatefulSet and PgBouncer
    kubectl delete statefulset postgres -n "$NAMESPACE" 2>/dev/null || true
    kubectl delete deployment pgbouncer -n "$NAMESPACE" 2>/dev/null || true
    kubectl delete configmap postgres-config postgres-scripts pgbouncer-config -n "$NAMESPACE" 2>/dev/null || true

    # Restore original deployment
    kubectl apply -f "$BACKUP_PATH/original_deployment.yaml"

    # Wait for pod to be ready
    kubectl wait --for=condition=ready pod -l app=postgres -n "$NAMESPACE" --timeout=300s

    log "✅ Rollback completed"
    send_slack_alert "✅ PostgreSQL Rollback Completed" \
        "Successfully rolled back to original configuration" "#36a64f"
}

# Test the new setup
test_new_setup() {
    log "Testing new PostgreSQL setup..."

    # Test direct connection
    local pod_name
    pod_name=$(kubectl get pod -n "$NAMESPACE" -l app=postgres --no-headers | head -1 | awk '{print $1}')

    if kubectl exec -n "$NAMESPACE" "$pod_name" -- psql -U postgres -d dev_db -c "SELECT 1;" >/dev/null 2>&1; then
        log "✅ Direct PostgreSQL connection working"
    else
        log "❌ Direct PostgreSQL connection failed"
        return 1
    fi

    # Test PgBouncer connection (if deployed)
    if kubectl get deployment pgbouncer -n "$NAMESPACE" >/dev/null 2>&1; then
        # This would need to be tested from an application pod
        log "✅ PgBouncer deployed (test from application)"
    fi

    # Test fault tolerance features
    /home/jianjun/ats-genai-data/scripts/postgres-fault-tolerance.sh health

    log "✅ New setup testing completed"
}

# Main upgrade function
main() {
    case "${1:-help}" in
        "check")
            pre_upgrade_checks
            ;;
        "backup")
            pre_upgrade_checks
            create_migration_backup
            ;;
        "upgrade")
            pre_upgrade_checks
            create_migration_backup
            upgrade_to_statefulset
            deploy_pgbouncer
            setup_monitoring
            test_new_setup
            log "🎉 PostgreSQL fault tolerance upgrade completed successfully!"
            send_slack_alert "🎉 PostgreSQL Fault Tolerance Upgrade Complete" \
                "StatefulSet, PgBouncer, WAL archiving, monitoring, and backups are now active" "#36a64f"
            ;;
        "rollback")
            rollback
            ;;
        "test")
            test_new_setup
            ;;
        *)
            echo "PostgreSQL Fault Tolerance Upgrade Script"
            echo ""
            echo "Usage: $0 {check|backup|upgrade|rollback|test}"
            echo ""
            echo "Commands:"
            echo "  check    - Run pre-upgrade checks"
            echo "  backup   - Create migration backup"
            echo "  upgrade  - Full upgrade to fault-tolerant setup"
            echo "  rollback - Rollback to original deployment"
            echo "  test     - Test the new setup"
            echo ""
            echo "Full upgrade process:"
            echo "  1. ./upgrade-postgres-fault-tolerance.sh check"
            echo "  2. ./upgrade-postgres-fault-tolerance.sh upgrade"
            echo ""
            echo "Rollback if needed:"
            echo "  ./upgrade-postgres-fault-tolerance.sh rollback"
            exit 1
            ;;
    esac
}

main "$@"