#!/bin/bash
#
# ATS-DEV Daily Database Backup Script
# Automated daily backup for ATS-DEV PostgreSQL database with retention policy
#

set -euo pipefail

# Configuration
BACKUP_DIR="/mnt/d/ats-backup/dev"
RETENTION_DAYS=7
DB_NAME="dev_db"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
CONTAINER_NAME="ats-dev-postgres"

# Logging
LOG_FILE="/mnt/d/ats-logs/backup-dev.log"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
BACKUP_FILE="$BACKUP_DIR/daily_backup_$TIMESTAMP.sql"

# Ensure directories exist
mkdir -p "$BACKUP_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "🚀 Starting ATS-DEV daily backup: $BACKUP_FILE"

# Check if container is running
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    log "❌ ERROR: $CONTAINER_NAME container is not running"
    exit 1
fi

# Perform backup
log "📊 Creating database backup..."
if docker exec "$CONTAINER_NAME" pg_dump -U "$DB_USER" "$DB_NAME" > "$BACKUP_FILE" 2>>"$LOG_FILE"; then
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    log "✅ Backup completed successfully: $BACKUP_SIZE"

    # Verify backup integrity
    if grep -q "PostgreSQL database dump complete" "$BACKUP_FILE"; then
        log "✅ Backup integrity verified"
    else
        log "⚠️  WARNING: Backup integrity check failed"
    fi
else
    log "❌ ERROR: Backup failed"
    exit 1
fi

# Cleanup old backups (retention policy)
log "🧹 Cleaning up backups older than $RETENTION_DAYS days..."
find "$BACKUP_DIR" -name "daily_backup_*.sql" -type f -mtime +$RETENTION_DAYS -delete 2>>"$LOG_FILE" || true

# Count remaining backups
BACKUP_COUNT=$(find "$BACKUP_DIR" -name "daily_backup_*.sql" -type f | wc -l)
log "📊 Retained backups: $BACKUP_COUNT"

# Create latest symlink
ln -sf "$BACKUP_FILE" "$BACKUP_DIR/latest_daily_backup.sql" 2>>"$LOG_FILE" || true

# Disk space check
DISK_USAGE=$(df -h "$BACKUP_DIR" | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 85 ]; then
    log "⚠️  WARNING: Backup disk usage high: ${DISK_USAGE}%"
fi

# Final summary
TOTAL_BACKUPS=$(find "$BACKUP_DIR" -name "*.sql" -type f | wc -l)
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
log "🎯 Backup summary: $TOTAL_BACKUPS total backups, $TOTAL_SIZE total size"
log "✅ ATS-DEV daily backup completed successfully"

# Send notification (optional - can be extended)
echo "ATS-DEV backup completed: $BACKUP_SIZE at $(date)" >> "/tmp/backup_notifications.txt" 2>/dev/null || true