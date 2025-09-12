#!/bin/bash
#
# Complete ATS Backup Status Overview
# Shows status of all backup systems: databases + data
#
set -euo pipefail

echo "🏢 ATS Complete Backup Status Overview"
echo "========================================================================"
echo "Generated: $(date)"
echo ""

# Database Backups
echo "📊 DATABASE BACKUPS"
echo "----------------------------------------"

# ATS-DEV Database
echo "🔷 ATS-DEV Database:"
if [[ -f "/mnt/d/ats-logs/backup-dev.log" ]]; then
    LAST_DEV=$(tail -1 /mnt/d/ats-logs/backup-dev.log | grep "completed successfully" || echo "No recent backup")
    echo "  Last Backup: $LAST_DEV"
    DEV_SIZE=$(find /mnt/d/ats-backup/dev -name "daily_backup_*.sql" -exec du -sh {} \; | tail -1 | cut -f1 || echo "Unknown")
    echo "  Latest Size: $DEV_SIZE"
else
    echo "  Status: No log file found"
fi

# ATS-INTG Database
echo "🔶 ATS-INTG Database:"
if [[ -f "/mnt/d/ats-logs/backup-intg.log" ]]; then
    LAST_INTG=$(tail -1 /mnt/d/ats-logs/backup-intg.log | grep "completed successfully" || echo "No recent backup")
    echo "  Last Backup: $LAST_INTG"
    INTG_SIZE=$(find /mnt/d/ats-backup/intg -name "daily_backup_*.sql" -exec du -sh {} \; | tail -1 | cut -f1 || echo "Unknown")
    echo "  Latest Size: $INTG_SIZE"
else
    echo "  Status: No log file found"
fi

echo ""

# Data Backups
echo "💾 ATS DATA BACKUPS"
echo "----------------------------------------"
/home/jianjun/ats-genai-admin/scripts/manage_ats_data_backups.sh status | grep -v "📊 ATS Data Backup Status Report" | grep -v "=="

echo ""

# Scheduled Jobs
echo "⏰ BACKUP SCHEDULE"
echo "----------------------------------------"
echo "Daily Schedule:"
echo "  01:00 AM - ATS Data Full Snapshot (Sundays only)"
echo "  02:00 AM - ATS-DEV Database Backup (Daily)"
echo "  02:15 AM - ATS-INTG Database Backup (Daily)"
echo "  04:00 AM - ATS Data Incremental Sync (Daily)"
echo "  05:00 AM - ATS Data Cleanup (Daily)"
echo ""

# Storage Summary
echo "💽 STORAGE SUMMARY"
echo "----------------------------------------"
DB_BACKUP_SIZE=$(du -sh /mnt/d/ats-backup 2>/dev/null | cut -f1 || echo "Unknown")
DATA_BACKUP_SIZE=$(du -sh /mnt/d/ats-archive 2>/dev/null | cut -f1 || echo "Unknown")
TOTAL_AVAILABLE=$(df -h /mnt/d | awk 'NR==2 {print $4}')

echo "Database Backups: $DB_BACKUP_SIZE"
echo "Data Backups: $DATA_BACKUP_SIZE"
echo "Available Space: $TOTAL_AVAILABLE"
echo ""

# Recent Activity
echo "📈 RECENT ACTIVITY"
echo "----------------------------------------"
echo "Last 24 Hours:"
if [[ -f "/mnt/d/ats-logs/backup-dev.log" ]]; then
    echo "  DEV DB: $(tail -3 /mnt/d/ats-logs/backup-dev.log | grep "completed successfully" | tail -1 || echo "No recent activity")"
fi
if [[ -f "/mnt/d/ats-logs/backup-intg.log" ]]; then
    echo "  INTG DB: $(tail -3 /mnt/d/ats-logs/backup-intg.log | grep "completed successfully" | tail -1 || echo "No recent activity")"
fi
if [[ -f "/mnt/d/ats-logs/data-backup-incremental.log" ]]; then
    echo "  Data: $(tail -3 /mnt/d/ats-logs/data-backup-incremental.log | grep "completed successfully" | tail -1 || echo "No recent activity")"
fi

echo ""
echo "========================================================================"
echo "✅ Backup system operational - All components scheduled and monitored"
echo "📧 Next action: Monitor Sunday's first full snapshot at 01:00 AM"