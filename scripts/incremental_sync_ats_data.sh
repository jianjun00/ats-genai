#!/bin/bash
#
# ATS Data Incremental Sync Backup Script
# Performs incremental backup using rsync with hard links for space efficiency
#
set -euo pipefail

# Configuration
SOURCE_DIR="/mnt/d/ats-data"
BACKUP_ROOT="/mnt/d/ats-archive"
INCREMENTAL_DIR="$BACKUP_ROOT/incremental"
LOG_FILE="/mnt/d/ats-logs/data-backup-incremental.log"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
BACKUP_NAME="incremental_$TIMESTAMP"
BACKUP_PATH="$INCREMENTAL_DIR/$BACKUP_NAME"

# Ensure directories exist
mkdir -p "$INCREMENTAL_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "🔄 Starting ATS-DATA incremental sync backup: $BACKUP_NAME"

# Pre-flight checks
log "🔍 Running pre-flight checks..."

# Check source directory exists
if [[ ! -d "$SOURCE_DIR" ]]; then
    log "❌ ERROR: Source directory $SOURCE_DIR does not exist"
    exit 1
fi

# Find the most recent backup to link against
LINK_DEST=""
LATEST_SNAPSHOT="$BACKUP_ROOT/snapshots/latest"
LATEST_INCREMENTAL="$INCREMENTAL_DIR/latest"

# Prefer latest incremental, fall back to latest snapshot
if [[ -L "$LATEST_INCREMENTAL" && -d "$LATEST_INCREMENTAL" ]]; then
    LINK_DEST=$(readlink -f "$LATEST_INCREMENTAL")
    log "📎 Using latest incremental as link destination: $(basename "$LINK_DEST")"
elif [[ -L "$LATEST_SNAPSHOT" && -d "$LATEST_SNAPSHOT" ]]; then
    LINK_DEST=$(readlink -f "$LATEST_SNAPSHOT")
    log "📎 Using latest snapshot as link destination: $(basename "$LINK_DEST")"
else
    log "⚠️  WARNING: No previous backup found. This will be a full backup."
fi

# Check available disk space
AVAILABLE_SPACE=$(df "$BACKUP_ROOT" | awk 'NR==2 {print $4}')
if [[ $AVAILABLE_SPACE -lt 10485760 ]]; then  # Less than 10GB
    AVAILABLE_GB=$((AVAILABLE_SPACE / 1024 / 1024))
    log "⚠️  WARNING: Low disk space available: ${AVAILABLE_GB}GB"
    if [[ $AVAILABLE_GB -lt 5 ]]; then
        log "❌ ERROR: Insufficient disk space for backup"
        exit 1
    fi
fi

# Create backup directory
log "📁 Creating incremental backup directory: $BACKUP_PATH"
mkdir -p "$BACKUP_PATH"

# Build rsync command
RSYNC_CMD="rsync -avH --progress --stats --delete"
RSYNC_CMD="$RSYNC_CMD --exclude='*.tmp' --exclude='*.lock' --exclude='lost+found'"
RSYNC_CMD="$RSYNC_CMD --exclude='.DS_Store' --exclude='Thumbs.db'"
RSYNC_CMD="$RSYNC_CMD --log-file=$LOG_FILE.rsync"

if [[ -n "$LINK_DEST" ]]; then
    RSYNC_CMD="$RSYNC_CMD --link-dest=$LINK_DEST"
fi

RSYNC_CMD="$RSYNC_CMD $SOURCE_DIR/ $BACKUP_PATH/"

# Perform incremental backup
log "📦 Starting incremental backup..."
START_TIME=$(date +%s)

if eval "$RSYNC_CMD" 2>>"$LOG_FILE"; then
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    HOURS=$((DURATION / 3600))
    MINUTES=$(((DURATION % 3600) / 60))
    SECONDS_REMAINING=$((DURATION % 60))
    
    BACKUP_SIZE=$(du -sh "$BACKUP_PATH" | cut -f1)
    BACKUP_SIZE_BYTES=$(du -sb "$BACKUP_PATH" | cut -f1)
    
    # Calculate space saved through hard links (if applicable)
    if [[ -n "$LINK_DEST" ]]; then
        BACKUP_SIZE_ACTUAL=$(du -sh --apparent-size "$BACKUP_PATH" | cut -f1)
        log "✅ Incremental backup completed successfully"
        log "📊 Apparent size: $BACKUP_SIZE_ACTUAL (actual disk usage: $BACKUP_SIZE)"
    else
        log "✅ Initial full backup completed successfully"
        log "📊 Backup size: $BACKUP_SIZE"
    fi
    
    log "⏱️  Duration: ${HOURS}h ${MINUTES}m ${SECONDS_REMAINING}s"
    
    # Extract rsync statistics
    if [[ -f "$LOG_FILE.rsync" ]]; then
        TRANSFERRED=$(grep "Total transferred file size:" "$LOG_FILE.rsync" | tail -1 | awk '{print $5, $6}' || echo "unknown")
        FILES_TRANSFERRED=$(grep "Number of files transferred:" "$LOG_FILE.rsync" | tail -1 | awk '{print $5}' || echo "unknown")
        FILES_CREATED=$(grep "Number of created files:" "$LOG_FILE.rsync" | tail -1 | awk '{print $5}' || echo "unknown")
        FILES_DELETED=$(grep "Number of deleted files:" "$LOG_FILE.rsync" | tail -1 | awk '{print $5}' || echo "unknown")
        
        log "📊 Files transferred: $FILES_TRANSFERRED"
        log "📊 Files created: $FILES_CREATED"
        log "📊 Files deleted: $FILES_DELETED"
        log "📊 Data transferred: $TRANSFERRED"
    fi
    
    # Create metadata file
    cat > "$BACKUP_PATH/.backup_metadata.json" << EOF
{
    "backup_type": "incremental_sync",
    "timestamp": "$TIMESTAMP",
    "source_directory": "$SOURCE_DIR",
    "backup_name": "$BACKUP_NAME",
    "creation_date": "$(date -Iseconds)",
    "link_destination": "$LINK_DEST",
    "backup_size": "$BACKUP_SIZE",
    "backup_size_bytes": $BACKUP_SIZE_BYTES,
    "duration_seconds": $DURATION,
    "files_transferred": "$FILES_TRANSFERRED",
    "files_created": "$FILES_CREATED", 
    "files_deleted": "$FILES_DELETED",
    "data_transferred": "$TRANSFERRED",
    "rsync_log": "$LOG_FILE.rsync"
}
EOF
    
    # Update latest incremental symlink
    ln -sfn "$BACKUP_NAME" "$INCREMENTAL_DIR/latest"
    log "🔗 Updated latest incremental symlink"
    
    # Verify backup integrity (quick check)
    log "🔍 Performing quick integrity check..."
    SAMPLE_CHECK_PASSED=true
    
    # Check a few critical directories exist
    for critical_dir in "minute-bars/firstrate" "training_data" "checkpoints" "config"; do
        if [[ -d "$SOURCE_DIR/$critical_dir" ]] && [[ ! -d "$BACKUP_PATH/$critical_dir" ]]; then
            log "⚠️  WARNING: Critical directory missing in backup: $critical_dir"
            SAMPLE_CHECK_PASSED=false
        fi
    done
    
    if [[ "$SAMPLE_CHECK_PASSED" == "true" ]]; then
        log "✅ Basic integrity check passed"
    else
        log "⚠️  WARNING: Some integrity checks failed"
    fi
    
else
    log "❌ ERROR: Incremental backup failed"
    # Clean up failed backup
    if [[ -d "$BACKUP_PATH" ]]; then
        log "🧹 Cleaning up failed backup directory"
        rm -rf "$BACKUP_PATH"
    fi
    exit 1
fi

# Cleanup old incremental backups (keep last 14 days)
log "🧹 Cleaning up old incremental backups (keeping 14 days)..."
find "$INCREMENTAL_DIR" -maxdepth 1 -type d -name "incremental_*" -mtime +14 -exec rm -rf {} \; 2>/dev/null || true

# Count remaining backups
INCREMENTAL_COUNT=$(find "$INCREMENTAL_DIR" -maxdepth 1 -type d -name "incremental_*" | wc -l)
log "📊 Incremental backups retained: $INCREMENTAL_COUNT"

# Disk usage summary
BACKUP_TOTAL_SIZE=$(du -sh "$BACKUP_ROOT" | cut -f1)
AVAILABLE_AFTER=$(df -h "$BACKUP_ROOT" | awk 'NR==2 {print $4}')
log "💾 Total backup storage used: $BACKUP_TOTAL_SIZE"
log "💾 Available space remaining: $AVAILABLE_AFTER"

# List recent incremental backups
log "📋 Recent incremental backups:"
ls -lt "$INCREMENTAL_DIR/" | grep "incremental_" | head -5 | awk '{print $9, $5, $6, $7, $8}' | while read line; do
    if [[ -n "$line" ]]; then
        log "   $line"
    fi
done

log "✅ ATS-DATA incremental sync backup completed successfully"
log "📍 Backup location: $BACKUP_PATH"

# Send completion notification
echo "ATS-DATA incremental backup completed: $BACKUP_SIZE at $(date)" >> "/tmp/backup_notifications.txt" 2>/dev/null || true