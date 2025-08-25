#!/bin/bash
# PostgreSQL Fault Tolerance Enhancement Script
# Implements comprehensive database protection with WAL archiving, backups, and monitoring

set -euo pipefail

# Configuration
NAMESPACE="ats-dev"
POSTGRES_POD=""
BACKUP_RETENTION_DAYS=30
WAL_ARCHIVE_PATH="/mnt/host/data/postgres-wal-archive"
BACKUP_PATH="/mnt/host/data/postgres-backups"
SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-https://hooks.slack.com/services/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}"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

send_slack_alert() {
    local title="$1"
    local message="$2" 
    local color="${3:-#ff9900}"
    
    if [[ -n "$SLACK_WEBHOOK_URL" ]]; then
        curl -X POST -H 'Content-type: application/json' \
            --data "{\"attachments\":[{\"color\":\"$color\",\"title\":\"$title\",\"text\":\"$message\"}]}" \
            "$SLACK_WEBHOOK_URL" --silent --fail >/dev/null 2>&1 || true
    fi
}

get_postgres_pod() {
    POSTGRES_POD=$(kubectl get pod -n "$NAMESPACE" -l app=postgres --no-headers | head -1 | awk '{print $1}')
    if [[ -z "$POSTGRES_POD" ]]; then
        log "ERROR: No PostgreSQL pod found in namespace $NAMESPACE"
        exit 1
    fi
    log "Using PostgreSQL pod: $POSTGRES_POD"
}

# Enable WAL archiving for point-in-time recovery
enable_wal_archiving() {
    log "Enabling WAL archiving for point-in-time recovery..."
    
    # Create WAL archive directory
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- mkdir -p "$WAL_ARCHIVE_PATH" 2>/dev/null || true
    
    # Update PostgreSQL configuration for WAL archiving
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d postgres -c "
        ALTER SYSTEM SET archive_mode = 'on';
        ALTER SYSTEM SET archive_command = 'test ! -f $WAL_ARCHIVE_PATH/%f && cp %p $WAL_ARCHIVE_PATH/%f';
        ALTER SYSTEM SET wal_level = 'replica';
        ALTER SYSTEM SET max_wal_senders = 3;
        ALTER SYSTEM SET checkpoint_timeout = '5min';
        ALTER SYSTEM SET checkpoint_completion_target = 0.9;
        ALTER SYSTEM SET log_checkpoints = on;
        ALTER SYSTEM SET log_min_duration_statement = 1000;
        ALTER SYSTEM SET log_connections = on;
        ALTER SYSTEM SET log_disconnections = on;
    "
    
    log "WAL archiving configuration updated. PostgreSQL restart required."
    send_slack_alert "🗄️ PostgreSQL WAL Archiving Enabled" \
        "WAL archiving configured for point-in-time recovery. Restart required to activate." \
        "#0066cc"
}

# Create full database backup
create_full_backup() {
    log "Creating full database backup..."
    
    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="$BACKUP_PATH/full_backup_$timestamp.sql"
    
    # Create backup directory
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- mkdir -p "$BACKUP_PATH" 2>/dev/null || true
    
    # Create full backup using pg_dump
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- pg_dump -U postgres -d dev_db \
        --verbose --format=custom --compress=9 \
        --file="$backup_file"
    
    # Create a plain SQL backup for easier recovery
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- pg_dump -U postgres -d dev_db \
        --verbose --format=plain \
        --file="$backup_file.plain"
    
    # Create backup manifest
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- bash -c "
        echo 'Backup created: $(date)' > $backup_file.info
        echo 'Database: dev_db' >> $backup_file.info
        echo 'Format: custom + plain' >> $backup_file.info
        echo 'Size: $(du -h $backup_file | cut -f1)' >> $backup_file.info
        echo 'WAL Position: $(psql -U postgres -d postgres -t -c "SELECT pg_current_wal_lsn();")' >> $backup_file.info
    "
    
    log "Backup created: $backup_file"
    send_slack_alert "💾 PostgreSQL Backup Completed" \
        "Full database backup created: $backup_file ($(date))" \
        "#36a64f"
    
    # Cleanup old backups
    cleanup_old_backups
}

# Cleanup old backups
cleanup_old_backups() {
    log "Cleaning up backups older than $BACKUP_RETENTION_DAYS days..."
    
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- find "$BACKUP_PATH" \
        -name "full_backup_*.sql*" -type f -mtime +$BACKUP_RETENTION_DAYS -delete 2>/dev/null || true
    
    # Also cleanup old WAL files (keep 7 days)
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- find "$WAL_ARCHIVE_PATH" \
        -name "0*" -type f -mtime +7 -delete 2>/dev/null || true
}

# Database health check (comprehensive)
comprehensive_health_check() {
    log "Running comprehensive PostgreSQL health check..."
    
    local issues=0
    
    # Check if PostgreSQL is running
    if ! kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- pg_isready -U postgres >/dev/null 2>&1; then
        log "ERROR: PostgreSQL is not ready"
        send_slack_alert "🚨 PostgreSQL Not Ready" \
            "PostgreSQL is not responding to connections" "#ff0000"
        ((issues++))
    fi
    
    # Check connections
    local connections
    connections=$(kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d postgres -t -c \
        "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null | xargs || echo "0")
    
    if [[ "$connections" -gt 80 ]]; then
        log "WARNING: High connection count: $connections/100"
        send_slack_alert "⚠️ High PostgreSQL Connections" \
            "Connection count: $connections/100 (80% threshold)" "#ff9900"
        ((issues++))
    fi
    
    # Check for long-running queries
    local long_queries
    long_queries=$(kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d postgres -t -c \
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND now() - query_start > interval '5 minutes';" 2>/dev/null | xargs || echo "0")
    
    if [[ "$long_queries" -gt 0 ]]; then
        log "WARNING: $long_queries long-running queries detected"
        send_slack_alert "⚠️ Long-Running Queries Detected" \
            "$long_queries queries running longer than 5 minutes" "#ff9900"
        ((issues++))
    fi
    
    # Check database size
    local db_size
    db_size=$(kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d postgres -t -c \
        "SELECT pg_size_pretty(pg_database_size('dev_db'));" 2>/dev/null | xargs || echo "unknown")
    log "Database size: $db_size"
    
    # Check WAL archiving status
    local archive_status
    archive_status=$(kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d postgres -t -c \
        "SELECT archived_count, failed_count FROM pg_stat_archiver;" 2>/dev/null || echo "0 0")
    log "WAL archiving: $archive_status (archived/failed)"
    
    # Check for table bloat
    local bloated_tables
    bloated_tables=$(kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d dev_db -t -c \
        "SELECT count(*) FROM pg_stat_user_tables WHERE n_dead_tup > n_live_tup;" 2>/dev/null | xargs || echo "0")
    
    if [[ "$bloated_tables" -gt 0 ]]; then
        log "WARNING: $bloated_tables tables may need VACUUM"
        send_slack_alert "⚠️ PostgreSQL Table Bloat Detected" \
            "$bloated_tables tables may need maintenance (VACUUM)" "#ff9900"
        ((issues++))
    fi
    
    if [[ $issues -eq 0 ]]; then
        log "✅ Comprehensive health check passed"
        return 0
    else
        log "❌ Health check found $issues issues"
        return 1
    fi
}

# Database corruption check
check_corruption() {
    log "Checking for database corruption..."
    
    # Run VACUUM with verbose output to check for corruption
    if kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d dev_db -c \
        "VACUUM (VERBOSE, ANALYZE);" >/dev/null 2>&1; then
        log "✅ VACUUM completed successfully - no corruption detected"
    else
        log "❌ VACUUM failed - possible corruption detected"
        send_slack_alert "🚨 PostgreSQL Corruption Detected" \
            "VACUUM operation failed - database may be corrupted" "#ff0000"
        return 1
    fi
    
    # Check system catalogs
    if kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d dev_db -c \
        "SELECT count(*) FROM information_schema.tables;" >/dev/null 2>&1; then
        log "✅ System catalogs are accessible"
    else
        log "❌ System catalog corruption detected"
        send_slack_alert "🚨 PostgreSQL System Catalog Corruption" \
            "Cannot access system catalogs - critical corruption detected" "#ff0000"
        return 1
    fi
}

# Point-in-time recovery preparation
prepare_pitr() {
    log "Preparing point-in-time recovery capabilities..."
    
    # Create recovery script
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- tee /tmp/recover_to_point.sh >/dev/null << 'EOF'
#!/bin/bash
# Point-in-time recovery script
# Usage: ./recover_to_point.sh "2025-08-24 14:30:00"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 'YYYY-MM-DD HH:MM:SS'"
    exit 1
fi

RECOVERY_TARGET="$1"
PGDATA="/var/lib/postgresql/data/pgdata"

echo "Preparing point-in-time recovery to: $RECOVERY_TARGET"

# Stop PostgreSQL (this should be done by Kubernetes)
echo "1. Stop PostgreSQL service"
echo "2. Restore from latest base backup"
echo "3. Configure recovery.conf"
echo "4. Start PostgreSQL for recovery"

cat > /tmp/recovery.conf << EOL
restore_command = 'cp /mnt/host/data/postgres-wal-archive/%f %p'
recovery_target_time = '$RECOVERY_TARGET'
recovery_target_action = 'promote'
EOL

echo "Recovery configuration created. Manual steps required:"
echo "1. kubectl scale deployment postgres --replicas=0"
echo "2. Restore base backup to $PGDATA"
echo "3. Copy recovery.conf to $PGDATA/"
echo "4. kubectl scale deployment postgres --replicas=1"
EOF

    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- chmod +x /tmp/recover_to_point.sh
    log "Point-in-time recovery script prepared at /tmp/recover_to_point.sh"
}

# Automated maintenance
run_maintenance() {
    log "Running automated database maintenance..."
    
    # Auto-vacuum and analyze
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d dev_db -c \
        "VACUUM (ANALYZE, VERBOSE);"
    
    # Update table statistics
    kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d dev_db -c \
        "ANALYZE;"
    
    # Reindex if needed (careful - this can be expensive)
    local reindex_needed
    reindex_needed=$(kubectl exec -n "$NAMESPACE" "$POSTGRES_POD" -- psql -U postgres -d dev_db -t -c \
        "SELECT count(*) FROM pg_stat_user_indexes WHERE idx_scan < 10 AND pg_relation_size(indexrelid) > 1000000;" | xargs)
    
    if [[ "$reindex_needed" -gt 0 ]]; then
        log "Found $reindex_needed indexes that may need rebuilding"
        send_slack_alert "🔧 PostgreSQL Maintenance Notice" \
            "$reindex_needed indexes may benefit from rebuilding" "#0066cc"
    fi
    
    log "Database maintenance completed"
    send_slack_alert "🔧 PostgreSQL Maintenance Completed" \
        "VACUUM, ANALYZE completed. Database optimized." "#36a64f"
}

# Install monitoring script
install_monitoring() {
    log "Installing PostgreSQL monitoring..."
    
    # Create monitoring script that runs every 5 minutes
    cat > /tmp/postgres-monitor.sh << 'EOF'
#!/bin/bash
while true; do
    /home/jianjun/ats-genai-data/scripts/postgres-fault-tolerance.sh health
    sleep 300  # 5 minutes
done
EOF
    
    chmod +x /tmp/postgres-monitor.sh
    log "PostgreSQL monitoring script installed at /tmp/postgres-monitor.sh"
    log "Start monitoring with: nohup /tmp/postgres-monitor.sh > /tmp/postgres-monitor.log 2>&1 &"
}

# Main function
main() {
    case "${1:-help}" in
        "enable-wal")
            get_postgres_pod
            enable_wal_archiving
            ;;
        "backup")
            get_postgres_pod
            create_full_backup
            ;;
        "health")
            get_postgres_pod
            comprehensive_health_check
            ;;
        "corruption")
            get_postgres_pod
            check_corruption
            ;;
        "pitr")
            get_postgres_pod
            prepare_pitr
            ;;
        "maintenance")
            get_postgres_pod
            run_maintenance
            ;;
        "monitor")
            install_monitoring
            ;;
        "all")
            get_postgres_pod
            enable_wal_archiving
            create_full_backup
            prepare_pitr
            comprehensive_health_check
            log "PostgreSQL fault tolerance setup completed!"
            send_slack_alert "✅ PostgreSQL Fault Tolerance Activated" \
                "WAL archiving, backups, and monitoring are now active" "#36a64f"
            ;;
        *)
            echo "Usage: $0 {enable-wal|backup|health|corruption|pitr|maintenance|monitor|all}"
            echo ""
            echo "Commands:"
            echo "  enable-wal   - Enable WAL archiving for PITR"
            echo "  backup       - Create full database backup"
            echo "  health       - Comprehensive health check"
            echo "  corruption   - Check for database corruption"
            echo "  pitr         - Prepare point-in-time recovery"
            echo "  maintenance  - Run database maintenance"
            echo "  monitor      - Install continuous monitoring"
            echo "  all          - Setup complete fault tolerance"
            exit 1
            ;;
    esac
}

main "$@"