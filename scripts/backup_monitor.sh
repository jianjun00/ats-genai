#!/bin/bash
#
# ATS Database Backup Monitoring Script
# Monitors backup health, sends alerts, and provides status reporting
#

set -euo pipefail

# Configuration
DEV_BACKUP_DIR="/mnt/d/ats-backup/dev"
INTG_BACKUP_DIR="/mnt/d/ats-backup/intg"
LOG_FILE="/mnt/d/ats-logs/backup-monitor.log"
ALERT_FILE="/tmp/backup_alerts.txt"
MAX_BACKUP_AGE_HOURS=26  # Alert if backup older than 26 hours

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

send_alert() {
    local message="$1"
    log "🚨 ALERT: $message"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALERT: $message" >> "$ALERT_FILE"
}

check_backup_freshness() {
    local backup_dir="$1"
    local env_name="$2"

    local latest_backup
    latest_backup=$(find "$backup_dir" -name "daily_backup_*.sql" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)

    if [[ -z "$latest_backup" ]]; then
        send_alert "No daily backups found for $env_name"
        return 1
    fi

    local backup_age_seconds
    backup_age_seconds=$(( $(date +%s) - $(stat -c %Y "$latest_backup" 2>/dev/null || echo 0) ))
    local backup_age_hours=$(( backup_age_seconds / 3600 ))

    local backup_size
    backup_size=$(du -h "$latest_backup" 2>/dev/null | cut -f1 || echo "Unknown")

    log "📊 $env_name latest backup: $(basename "$latest_backup") (${backup_age_hours}h old, $backup_size)"

    if [[ $backup_age_hours -gt $MAX_BACKUP_AGE_HOURS ]]; then
        send_alert "$env_name backup is ${backup_age_hours} hours old (exceeds ${MAX_BACKUP_AGE_HOURS}h threshold)"
        return 1
    fi

    return 0
}

check_disk_space() {
    local backup_dir="$1"
    local env_name="$2"

    if [[ ! -d "$backup_dir" ]]; then
        send_alert "$env_name backup directory does not exist: $backup_dir"
        return 1
    fi

    local disk_usage
    disk_usage=$(df -h "$backup_dir" | awk 'NR==2 {print $5}' | sed 's/%//')

    log "💾 $env_name backup disk usage: ${disk_usage}%"

    if [[ $disk_usage -gt 90 ]]; then
        send_alert "$env_name backup disk usage critical: ${disk_usage}%"
        return 1
    elif [[ $disk_usage -gt 80 ]]; then
        log "⚠️  $env_name backup disk usage high: ${disk_usage}%"
    fi

    return 0
}

check_backup_integrity() {
    local backup_dir="$1"
    local env_name="$2"

    local latest_backup
    latest_backup=$(find "$backup_dir" -name "daily_backup_*.sql" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)

    if [[ -z "$latest_backup" ]]; then
        return 1
    fi

    if ! grep -q "PostgreSQL database dump complete" "$latest_backup" 2>/dev/null; then
        send_alert "$env_name latest backup appears corrupted (missing completion marker)"
        return 1
    fi

    local backup_lines
    backup_lines=$(wc -l < "$latest_backup" 2>/dev/null || echo 0)

    if [[ $backup_lines -lt 100 ]]; then
        send_alert "$env_name backup suspiciously small: $backup_lines lines"
        return 1
    fi

    log "✅ $env_name backup integrity check passed ($backup_lines lines)"
    return 0
}

generate_summary_report() {
    log "📋 BACKUP MONITORING SUMMARY"
    log "================================"

    # Count total backups
    local dev_backups
    dev_backups=$(find "$DEV_BACKUP_DIR" -name "*.sql" -type f 2>/dev/null | wc -l)
    local intg_backups
    intg_backups=$(find "$INTG_BACKUP_DIR" -name "*.sql" -type f 2>/dev/null | wc -l)

    # Calculate total backup sizes
    local dev_size
    dev_size=$(du -sh "$DEV_BACKUP_DIR" 2>/dev/null | cut -f1 || echo "Unknown")
    local intg_size
    intg_size=$(du -sh "$INTG_BACKUP_DIR" 2>/dev/null | cut -f1 || echo "Unknown")

    log "📊 ATS-DEV: $dev_backups backups, $dev_size total"
    log "📊 ATS-INTG: $intg_backups backups, $intg_size total"

    # Check for recent alerts
    local alert_count
    alert_count=$(grep -c "$(date '+%Y-%m-%d')" "$ALERT_FILE" 2>/dev/null || echo 0)
    log "🚨 Today's alerts: $alert_count"

    # Database status if containers running
    if docker ps | grep -q "ats-dev-postgres"; then
        log "🟢 ATS-DEV database: Running"
    else
        log "🔴 ATS-DEV database: Not running"
    fi

    if docker ps | grep -q "ats-intg-postgres"; then
        log "🟢 ATS-INTG database: Running"
    else
        log "🔴 ATS-INTG database: Not running"
    fi
}

main() {
    log "🔍 Starting backup monitoring check"

    # Ensure log directories exist
    mkdir -p "$(dirname "$LOG_FILE")"
    mkdir -p "$DEV_BACKUP_DIR"
    mkdir -p "$INTG_BACKUP_DIR"

    local status=0

    # Check ATS-DEV backups
    log "🔍 Checking ATS-DEV backups..."
    if ! check_backup_freshness "$DEV_BACKUP_DIR" "ATS-DEV"; then
        status=1
    fi
    if ! check_disk_space "$DEV_BACKUP_DIR" "ATS-DEV"; then
        status=1
    fi
    if ! check_backup_integrity "$DEV_BACKUP_DIR" "ATS-DEV"; then
        status=1
    fi

    # Check ATS-INTG backups
    log "🔍 Checking ATS-INTG backups..."
    if ! check_backup_freshness "$INTG_BACKUP_DIR" "ATS-INTG"; then
        status=1
    fi
    if ! check_disk_space "$INTG_BACKUP_DIR" "ATS-INTG"; then
        status=1
    fi
    if ! check_backup_integrity "$INTG_BACKUP_DIR" "ATS-INTG"; then
        status=1
    fi

    # Generate summary
    generate_summary_report

    if [[ $status -eq 0 ]]; then
        log "✅ All backup checks passed"
    else
        log "❌ Some backup checks failed - see alerts"
    fi

    log "🏁 Backup monitoring check completed"
    return $status
}

# Run monitoring if called directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi