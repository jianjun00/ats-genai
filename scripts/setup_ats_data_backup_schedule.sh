#!/bin/bash
#
# ATS Data Backup Scheduling Setup Script
# Sets up automated cron jobs for full snapshot and incremental backups
#
set -euo pipefail

SCRIPT_DIR="/home/jianjun/ats-genai-admin/scripts"
CURRENT_USER=$(whoami)

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

show_help() {
    cat << EOF
ATS Data Backup Scheduling Setup

Usage: $0 [COMMAND]

Commands:
    install     Install backup cron jobs
    uninstall   Remove backup cron jobs
    status      Show current cron jobs
    test        Run test backups to verify setup

Backup Schedule:
    Full Snapshot:     Sundays at 01:00 AM (weekly)
    Incremental Sync:  Daily at 04:00 AM
    Database Backups:  Daily at 02:00 AM (dev) and 03:00 AM (intg)
EOF
}

install_crontab() {
    log "📅 Installing ATS data backup cron jobs..."

    # Get current crontab
    TEMP_CRON=$(mktemp)
    crontab -l 2>/dev/null > "$TEMP_CRON" || true

    # Remove any existing ATS data backup entries
    sed -i '/# ATS Data Backup/d' "$TEMP_CRON"
    sed -i '/full_snapshot_ats_data.sh/d' "$TEMP_CRON"
    sed -i '/incremental_sync_ats_data.sh/d' "$TEMP_CRON"

    # Add new backup entries
    cat >> "$TEMP_CRON" << EOF

# ATS Data Backup Jobs
# Full snapshot backup - Sundays at 1 AM
0 1 * * 0 $SCRIPT_DIR/full_snapshot_ats_data.sh >> /mnt/d/ats-logs/cron-data-backup.log 2>&1

# Incremental sync backup - Daily at 4 AM
0 4 * * * $SCRIPT_DIR/incremental_sync_ats_data.sh >> /mnt/d/ats-logs/cron-data-backup.log 2>&1

# Backup cleanup - Daily at 5 AM (after incremental)
0 5 * * * $SCRIPT_DIR/manage_ats_data_backups.sh cleanup >> /mnt/d/ats-logs/cron-data-backup.log 2>&1
EOF

    # Install the new crontab
    crontab "$TEMP_CRON"
    rm "$TEMP_CRON"

    log "✅ Cron jobs installed successfully"
    log "📊 Current crontab:"
    crontab -l | grep -A 10 "ATS Data Backup"
}

uninstall_crontab() {
    log "🗑️  Removing ATS data backup cron jobs..."

    TEMP_CRON=$(mktemp)
    crontab -l 2>/dev/null > "$TEMP_CRON" || true

    # Remove ATS data backup entries
    sed -i '/# ATS Data Backup/d' "$TEMP_CRON"
    sed -i '/full_snapshot_ats_data.sh/d' "$TEMP_CRON"
    sed -i '/incremental_sync_ats_data.sh/d' "$TEMP_CRON"
    sed -i '/manage_ats_data_backups.sh cleanup/d' "$TEMP_CRON"

    # Remove empty lines
    sed -i '/^$/N;/^\n$/d' "$TEMP_CRON"

    crontab "$TEMP_CRON"
    rm "$TEMP_CRON"

    log "✅ Cron jobs removed successfully"
}

show_status() {
    log "📊 Current ATS backup cron job status:"
    echo "=================================================="

    # Show relevant cron jobs
    echo "🔍 Active Cron Jobs:"
    crontab -l 2>/dev/null | grep -E "(ats_data|data.*backup|full_snapshot|incremental_sync)" || echo "  No ATS data backup jobs found"

    echo ""
    echo "📋 All Database Backup Jobs:"
    crontab -l 2>/dev/null | grep -E "(backup|daily_backup)" || echo "  No backup jobs found"

    echo ""
    echo "⏰ Next Scheduled Runs:"
    echo "  Full Snapshot: Next Sunday at 01:00 AM"
    echo "  Incremental: Daily at 04:00 AM (next: $(date -d 'tomorrow 04:00' '+%Y-%m-%d %H:%M'))"
    echo "  Cleanup: Daily at 05:00 AM (next: $(date -d 'tomorrow 05:00' '+%Y-%m-%d %H:%M'))"

    echo ""
    echo "📁 Log Files:"
    echo "  Data backup logs: /mnt/d/ats-logs/data-backup-*.log"
    echo "  Cron logs: /mnt/d/ats-logs/cron-data-backup.log"

    echo "=================================================="
}

run_test() {
    log "🧪 Running test backups to verify setup..."

    # Check script permissions
    echo "🔍 Checking script permissions:"
    for script in "full_snapshot_ats_data.sh" "incremental_sync_ats_data.sh" "manage_ats_data_backups.sh"; do
        script_path="$SCRIPT_DIR/$script"
        if [[ -x "$script_path" ]]; then
            echo "  ✅ $script is executable"
        else
            echo "  ❌ $script is NOT executable"
            log "🔧 Making $script executable..."
            chmod +x "$script_path"
        fi
    done

    # Check directory permissions
    echo ""
    echo "🔍 Checking directory permissions:"
    for dir in "/mnt/d/ats-archive" "/mnt/d/ats-logs" "/mnt/d/ats-data"; do
        if [[ -d "$dir" && -r "$dir" && -w "$dir" ]]; then
            echo "  ✅ $dir is accessible"
        else
            echo "  ❌ $dir has permission issues"
        fi
    done

    # Test incremental backup (quick)
    echo ""
    echo "🧪 Running test incremental backup..."
    if "$SCRIPT_DIR/incremental_sync_ats_data.sh"; then
        echo "  ✅ Test incremental backup completed successfully"
    else
        echo "  ❌ Test incremental backup failed"
        return 1
    fi

    # Test management script
    echo ""
    echo "🧪 Testing backup management script..."
    if "$SCRIPT_DIR/manage_ats_data_backups.sh" status > /dev/null; then
        echo "  ✅ Management script working correctly"
    else
        echo "  ❌ Management script failed"
        return 1
    fi

    log "✅ All tests passed - backup system is ready!"
}

# Main script logic
case "${1:-help}" in
    install)
        # Verify scripts exist before installing
        for script in "full_snapshot_ats_data.sh" "incremental_sync_ats_data.sh" "manage_ats_data_backups.sh"; do
            if [[ ! -f "$SCRIPT_DIR/$script" ]]; then
                log "❌ ERROR: Required script not found: $SCRIPT_DIR/$script"
                exit 1
            fi
        done
        install_crontab
        ;;
    uninstall)
        uninstall_crontab
        ;;
    status)
        show_status
        ;;
    test)
        run_test
        ;;
    -h|--help|help)
        show_help
        ;;
    *)
        echo "❌ ERROR: Unknown command: $1"
        show_help
        exit 1
        ;;
esac