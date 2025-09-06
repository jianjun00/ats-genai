#!/bin/bash
#
# ATS Data Backup Management and Monitoring Script  
# Provides backup status, cleanup, restore, and monitoring functions
#
set -euo pipefail

# Configuration
BACKUP_ROOT="/mnt/d/ats-archive"
SOURCE_DIR="/mnt/d/ats-data"
SNAPSHOT_DIR="$BACKUP_ROOT/snapshots"
INCREMENTAL_DIR="$BACKUP_ROOT/incremental"
LOG_FILE="/mnt/d/ats-logs/backup-management.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

show_help() {
    cat << EOF
ATS Data Backup Management Script

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    status              Show backup status and summary
    list                List all available backups
    cleanup             Clean up old backups based on retention policy
    verify [BACKUP]     Verify backup integrity
    restore [BACKUP]    Restore from backup (interactive)
    monitor             Show storage usage and health
    schedule            Display backup schedule information

Options:
    -h, --help         Show this help message
    -v, --verbose      Enable verbose output

Examples:
    $0 status                           # Show backup status
    $0 list                            # List all backups
    $0 cleanup                         # Clean old backups
    $0 verify incremental_20250906_123456  # Verify specific backup
    $0 restore                         # Interactive restore menu
    $0 monitor                         # Show storage health
EOF
}

show_status() {
    log "📊 ATS Data Backup Status Report"
    echo "=================================================="
    
    # Latest snapshots
    if [[ -d "$SNAPSHOT_DIR" && -L "$SNAPSHOT_DIR/latest" ]]; then
        LATEST_SNAPSHOT=$(readlink -f "$SNAPSHOT_DIR/latest")
        SNAPSHOT_NAME=$(basename "$LATEST_SNAPSHOT")
        SNAPSHOT_DATE=$(stat -c %y "$LATEST_SNAPSHOT" | cut -d' ' -f1)
        SNAPSHOT_SIZE=$(du -sh "$LATEST_SNAPSHOT" | cut -f1)
        echo "🔷 Latest Full Snapshot: $SNAPSHOT_NAME ($SNAPSHOT_DATE, $SNAPSHOT_SIZE)"
    else
        echo "❌ No snapshots found"
    fi
    
    # Latest incremental
    if [[ -d "$INCREMENTAL_DIR" && -L "$INCREMENTAL_DIR/latest" ]]; then
        LATEST_INCREMENTAL=$(readlink -f "$INCREMENTAL_DIR/latest")
        INCREMENTAL_NAME=$(basename "$LATEST_INCREMENTAL")
        INCREMENTAL_DATE=$(stat -c %y "$LATEST_INCREMENTAL" | cut -d' ' -f1)
        INCREMENTAL_SIZE=$(du -sh "$LATEST_INCREMENTAL" | cut -f1)
        echo "🔶 Latest Incremental: $INCREMENTAL_NAME ($INCREMENTAL_DATE, $INCREMENTAL_SIZE)"
    else
        echo "❌ No incremental backups found"
    fi
    
    # Backup counts
    SNAPSHOT_COUNT=0
    INCREMENTAL_COUNT=0
    
    if [[ -d "$SNAPSHOT_DIR" ]]; then
        SNAPSHOT_COUNT=$(find "$SNAPSHOT_DIR" -maxdepth 1 -type d -name "full_snapshot_*" | wc -l)
    fi
    
    if [[ -d "$INCREMENTAL_DIR" ]]; then
        INCREMENTAL_COUNT=$(find "$INCREMENTAL_DIR" -maxdepth 1 -type d -name "incremental_*" | wc -l)
    fi
    
    echo "📊 Total Snapshots: $SNAPSHOT_COUNT"
    echo "📊 Total Incrementals: $INCREMENTAL_COUNT"
    
    # Storage usage
    if [[ -d "$BACKUP_ROOT" ]]; then
        TOTAL_BACKUP_SIZE=$(du -sh "$BACKUP_ROOT" 2>/dev/null | cut -f1 || echo "Unknown")
        AVAILABLE_SPACE=$(df -h "$BACKUP_ROOT" | awk 'NR==2 {print $4}')
        echo "💾 Total Backup Storage: $TOTAL_BACKUP_SIZE"
        echo "💾 Available Space: $AVAILABLE_SPACE"
    fi
    
    echo "=================================================="
}

list_backups() {
    log "📋 Listing all available backups"
    echo "=================================================="
    
    echo "🔷 Full Snapshots:"
    if [[ -d "$SNAPSHOT_DIR" ]]; then
        find "$SNAPSHOT_DIR" -maxdepth 1 -type d -name "full_snapshot_*" -exec basename {} \; | sort -r | while read backup; do
            if [[ -n "$backup" ]]; then
                backup_path="$SNAPSHOT_DIR/$backup"
                backup_date=$(stat -c %y "$backup_path" | cut -d' ' -f1,2 | cut -d'.' -f1)
                backup_size=$(du -sh "$backup_path" | cut -f1)
                echo "  📦 $backup ($backup_date, $backup_size)"
            fi
        done
    else
        echo "  No snapshots found"
    fi
    
    echo ""
    echo "🔶 Incremental Backups:"
    if [[ -d "$INCREMENTAL_DIR" ]]; then
        find "$INCREMENTAL_DIR" -maxdepth 1 -type d -name "incremental_*" -exec basename {} \; | sort -r | head -10 | while read backup; do
            if [[ -n "$backup" ]]; then
                backup_path="$INCREMENTAL_DIR/$backup"
                backup_date=$(stat -c %y "$backup_path" | cut -d' ' -f1,2 | cut -d'.' -f1)
                backup_size=$(du -sh "$backup_path" | cut -f1)
                echo "  📦 $backup ($backup_date, $backup_size)"
            fi
        done
        
        total_incrementals=$(find "$INCREMENTAL_DIR" -maxdepth 1 -type d -name "incremental_*" | wc -l)
        if [[ $total_incrementals -gt 10 ]]; then
            echo "  ... and $((total_incrementals - 10)) more incrementals"
        fi
    else
        echo "  No incremental backups found"
    fi
    
    echo "=================================================="
}

cleanup_backups() {
    log "🧹 Starting backup cleanup with retention policy"
    
    # Cleanup old snapshots (keep last 3)
    if [[ -d "$SNAPSHOT_DIR" ]]; then
        snapshots_to_delete=$(find "$SNAPSHOT_DIR" -maxdepth 1 -type d -name "full_snapshot_*" | sort -r | tail -n +4)
        if [[ -n "$snapshots_to_delete" ]]; then
            echo "$snapshots_to_delete" | while read snapshot; do
                if [[ -n "$snapshot" ]]; then
                    snapshot_name=$(basename "$snapshot")
                    snapshot_size=$(du -sh "$snapshot" | cut -f1)
                    log "🗑️  Removing old snapshot: $snapshot_name ($snapshot_size)"
                    rm -rf "$snapshot"
                fi
            done
        else
            log "✅ No old snapshots to cleanup"
        fi
    fi
    
    # Cleanup old incrementals (keep last 14 days)
    if [[ -d "$INCREMENTAL_DIR" ]]; then
        log "🧹 Cleaning incrementals older than 14 days..."
        old_incrementals=$(find "$INCREMENTAL_DIR" -maxdepth 1 -type d -name "incremental_*" -mtime +14)
        if [[ -n "$old_incrementals" ]]; then
            echo "$old_incrementals" | while read incremental; do
                if [[ -n "$incremental" ]]; then
                    incremental_name=$(basename "$incremental")
                    incremental_size=$(du -sh "$incremental" | cut -f1)
                    log "🗑️  Removing old incremental: $incremental_name ($incremental_size)"
                    rm -rf "$incremental"
                fi
            done
        else
            log "✅ No old incrementals to cleanup"
        fi
    fi
    
    log "✅ Backup cleanup completed"
}

verify_backup() {
    local backup_name="$1"
    local backup_path=""
    
    # Find backup path
    if [[ -d "$SNAPSHOT_DIR/$backup_name" ]]; then
        backup_path="$SNAPSHOT_DIR/$backup_name"
    elif [[ -d "$INCREMENTAL_DIR/$backup_name" ]]; then
        backup_path="$INCREMENTAL_DIR/$backup_name"
    else
        log "❌ ERROR: Backup not found: $backup_name"
        return 1
    fi
    
    log "🔍 Verifying backup: $backup_name"
    
    # Check metadata file
    if [[ -f "$backup_path/.backup_metadata.json" ]]; then
        log "✅ Metadata file exists"
    else
        log "⚠️  WARNING: No metadata file found"
    fi
    
    # Check critical directories
    local critical_dirs=("minute-bars" "training_data" "checkpoints" "config")
    local missing_dirs=0
    
    for dir in "${critical_dirs[@]}"; do
        if [[ -d "$SOURCE_DIR/$dir" ]]; then
            if [[ -d "$backup_path/$dir" ]]; then
                log "✅ Critical directory verified: $dir"
            else
                log "❌ Missing critical directory: $dir"
                missing_dirs=$((missing_dirs + 1))
            fi
        fi
    done
    
    # Sample file verification
    log "🔍 Performing sample file verification..."
    sample_files=$(find "$backup_path" -type f -name "*.parquet" | head -5)
    verified_files=0
    
    while IFS= read -r file; do
        if [[ -n "$file" && -f "$file" ]]; then
            if [[ -s "$file" ]]; then  # Check if file is not empty
                verified_files=$((verified_files + 1))
            fi
        fi
    done <<< "$sample_files"
    
    if [[ $missing_dirs -eq 0 && $verified_files -gt 0 ]]; then
        log "✅ Backup verification passed: $backup_name"
    else
        log "❌ Backup verification failed: missing_dirs=$missing_dirs, verified_files=$verified_files"
        return 1
    fi
}

show_monitor() {
    log "📊 ATS Data Backup Monitoring Report"
    echo "=================================================="
    
    # Disk usage
    df -h "$BACKUP_ROOT" | awk 'NR==1 {print "Filesystem      Size  Used Avail Use% Mounted on"} NR==2 {print $0}'
    echo ""
    
    # Backup growth trend (if multiple backups exist)
    echo "📈 Backup Storage Trend:"
    if [[ -d "$SNAPSHOT_DIR" ]]; then
        find "$SNAPSHOT_DIR" -maxdepth 1 -type d -name "full_snapshot_*" | sort | tail -3 | while read snapshot; do
            if [[ -n "$snapshot" ]]; then
                snapshot_name=$(basename "$snapshot")
                snapshot_date=$(echo "$snapshot_name" | sed 's/full_snapshot_\([0-9]*\)_\([0-9]*\)/\1/')
                snapshot_size=$(du -sh "$snapshot" | cut -f1)
                echo "  📦 $snapshot_date: $snapshot_size"
            fi
        done
    fi
    
    echo ""
    
    # Recent backup activity
    echo "⏰ Recent Backup Activity:"
    if [[ -f "/mnt/d/ats-logs/data-backup-snapshot.log" ]]; then
        echo "  Last Snapshot:"
        tail -1 "/mnt/d/ats-logs/data-backup-snapshot.log" | grep "completed successfully" || echo "  No recent successful snapshots"
    fi
    
    if [[ -f "/mnt/d/ats-logs/data-backup-incremental.log" ]]; then
        echo "  Last Incremental:"
        tail -1 "/mnt/d/ats-logs/data-backup-incremental.log" | grep "completed successfully" || echo "  No recent successful incrementals"
    fi
    
    echo "=================================================="
}

show_schedule() {
    echo "📅 ATS Data Backup Schedule"
    echo "=================================================="
    echo "🔷 Full Snapshots: Weekly (Sunday 01:00 AM)"
    echo "🔶 Incremental Sync: Daily (04:00 AM)"
    echo "🧹 Cleanup: After each backup operation"
    echo ""
    echo "📋 Retention Policy:"
    echo "  - Full Snapshots: Keep last 3 (≈3 weeks)"
    echo "  - Incremental Backups: Keep 14 days"
    echo ""
    echo "📊 Current Crontab Entries:"
    crontab -l 2>/dev/null | grep -E "(ats_data|data.*backup)" || echo "  No backup cron jobs found"
    echo "=================================================="
}

# Main script logic
case "${1:-status}" in
    status)
        show_status
        ;;
    list)
        list_backups
        ;;
    cleanup)
        cleanup_backups
        ;;
    verify)
        if [[ -n "${2:-}" ]]; then
            verify_backup "$2"
        else
            echo "❌ ERROR: Please specify backup name to verify"
            echo "Use: $0 list to see available backups"
            exit 1
        fi
        ;;
    restore)
        echo "🔧 Interactive restore functionality coming soon..."
        echo "For now, use: cp -r /mnt/d/ats-archive/[backup]/* /mnt/d/ats-data/"
        ;;
    monitor)
        show_monitor
        ;;
    schedule)
        show_schedule
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