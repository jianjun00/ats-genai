#!/bin/bash
#
# ATS Data Full Snapshot Backup Script
# Creates complete backup of ats-data directory to ats-archive
#
set -euo pipefail

# Configuration
SOURCE_DIR="/mnt/d/ats-data"
BACKUP_ROOT="/mnt/d/ats-archive"
SNAPSHOT_DIR="$BACKUP_ROOT/snapshots"
LOG_FILE="/mnt/d/ats-logs/data-backup-snapshot.log"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
SNAPSHOT_NAME="full_snapshot_$TIMESTAMP"
SNAPSHOT_PATH="$SNAPSHOT_DIR/$SNAPSHOT_NAME"

# Ensure directories exist
mkdir -p "$SNAPSHOT_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "🚀 Starting ATS-DATA full snapshot backup: $SNAPSHOT_NAME"

# Pre-flight checks
log "🔍 Running pre-flight checks..."

# Check source directory exists
if [[ ! -d "$SOURCE_DIR" ]]; then
    log "❌ ERROR: Source directory $SOURCE_DIR does not exist"
    exit 1
fi

# Check available disk space (ensure at least 1TB free after backup)
AVAILABLE_SPACE=$(df "$BACKUP_ROOT" | awk 'NR==2 {print $4}')
SOURCE_SIZE=$(du -s "$SOURCE_DIR" | awk '{print $1}')
REQUIRED_SPACE=$((SOURCE_SIZE + 1048576000))  # Source size + 1TB buffer

if [[ $AVAILABLE_SPACE -lt $REQUIRED_SPACE ]]; then
    AVAILABLE_GB=$((AVAILABLE_SPACE / 1024 / 1024))
    REQUIRED_GB=$((REQUIRED_SPACE / 1024 / 1024))
    log "⚠️  WARNING: Low disk space. Available: ${AVAILABLE_GB}GB, Required: ${REQUIRED_GB}GB"
fi

# Calculate source directory size for progress tracking
log "📊 Calculating source directory size..."
SOURCE_SIZE_READABLE=$(du -sh "$SOURCE_DIR" | cut -f1)
log "📊 Source directory size: $SOURCE_SIZE_READABLE"

# Create snapshot directory
log "📁 Creating snapshot directory: $SNAPSHOT_PATH"
mkdir -p "$SNAPSHOT_PATH"

# Perform full snapshot using rsync
log "📦 Creating full snapshot (this may take several hours)..."
START_TIME=$(date +%s)

if rsync -avH \
    --progress \
    --stats \
    --exclude='*.tmp' \
    --exclude='*.lock' \
    --exclude='lost+found' \
    --exclude='.DS_Store' \
    --exclude='Thumbs.db' \
    --log-file="$LOG_FILE.rsync" \
    "$SOURCE_DIR/" "$SNAPSHOT_PATH/" 2>>"$LOG_FILE"; then
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    HOURS=$((DURATION / 3600))
    MINUTES=$(((DURATION % 3600) / 60))
    SECONDS=$((DURATION % 60))
    
    SNAPSHOT_SIZE=$(du -sh "$SNAPSHOT_PATH" | cut -f1)
    log "✅ Full snapshot completed successfully"
    log "📊 Snapshot size: $SNAPSHOT_SIZE"
    log "⏱️  Duration: ${HOURS}h ${MINUTES}m ${SECONDS}s"
    
    # Create metadata file
    cat > "$SNAPSHOT_PATH/.backup_metadata.json" << EOF
{
    "backup_type": "full_snapshot",
    "timestamp": "$TIMESTAMP",
    "source_directory": "$SOURCE_DIR",
    "snapshot_name": "$SNAPSHOT_NAME",
    "creation_date": "$(date -Iseconds)",
    "source_size": "$SOURCE_SIZE_READABLE",
    "snapshot_size": "$SNAPSHOT_SIZE",
    "duration_seconds": $DURATION,
    "rsync_log": "$LOG_FILE.rsync"
}
EOF
    
    # Create/update latest snapshot symlink
    ln -sfn "$SNAPSHOT_NAME" "$SNAPSHOT_DIR/latest"
    log "🔗 Updated latest snapshot symlink"
    
    # Verify snapshot integrity (sample check)
    log "🔍 Performing integrity verification..."
    SAMPLE_FILES=$(find "$SOURCE_DIR" -type f -name "*.parquet" | head -10)
    INTEGRITY_PASSED=true
    
    while IFS= read -r file; do
        if [[ -n "$file" ]]; then
            rel_path=${file#$SOURCE_DIR/}
            if [[ ! -f "$SNAPSHOT_PATH/$rel_path" ]]; then
                log "⚠️  WARNING: Missing file in snapshot: $rel_path"
                INTEGRITY_PASSED=false
            fi
        fi
    done <<< "$SAMPLE_FILES"
    
    if [[ "$INTEGRITY_PASSED" == "true" ]]; then
        log "✅ Snapshot integrity verification passed"
    else
        log "⚠️  WARNING: Some integrity checks failed - see log for details"
    fi
    
else
    log "❌ ERROR: Full snapshot failed"
    # Clean up failed snapshot
    if [[ -d "$SNAPSHOT_PATH" ]]; then
        log "🧹 Cleaning up failed snapshot directory"
        rm -rf "$SNAPSHOT_PATH"
    fi
    exit 1
fi

# Disk usage summary
BACKUP_TOTAL_SIZE=$(du -sh "$BACKUP_ROOT" | cut -f1)
AVAILABLE_AFTER=$(df -h "$BACKUP_ROOT" | awk 'NR==2 {print $4}')
log "💾 Backup root total size: $BACKUP_TOTAL_SIZE"
log "💾 Available space after backup: $AVAILABLE_AFTER"

# List all snapshots
log "📋 Current snapshots:"
ls -la "$SNAPSHOT_DIR/" | grep "full_snapshot_" | awk '{print $9, $5, $6, $7, $8}' | while read line; do
    log "   $line"
done

log "✅ ATS-DATA full snapshot backup completed successfully"
log "📍 Snapshot location: $SNAPSHOT_PATH"

# Send completion notification
echo "ATS-DATA full snapshot completed: $SNAPSHOT_SIZE at $(date)" >> "/tmp/backup_notifications.txt" 2>/dev/null || true