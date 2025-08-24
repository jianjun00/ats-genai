#!/bin/bash

# Backup Sync Script
# Syncs backup files from Minikube VM to host system for persistence

set -e

BACKUP_SOURCE_INTG="/home/jianjun/ats-data/backups/ats-intg"
BACKUP_SOURCE_PROD="/home/jianjun/ats-data/backups/ats-prod"
HOST_BACKUP_DIR="/home/jianjun/ats-data/host-backups"

echo "🔄 Starting backup sync from Minikube to host system..."

# Create host backup directories if they don't exist
mkdir -p "${HOST_BACKUP_DIR}/ats-intg"
mkdir -p "${HOST_BACKUP_DIR}/ats-prod"

# Function to sync files from Minikube to host
sync_backup_files() {
    local env=$1
    local minikube_path=$2
    local host_path=$3
    
    echo "📁 Syncing $env backups..."
    
    # Get list of backup files in Minikube
    backup_files=$(minikube ssh "ls -1 ${minikube_path}/*.sql.custom ${minikube_path}/*.sql.gz 2>/dev/null || true")
    
    if [ -z "$backup_files" ]; then
        echo "⚠️  No backup files found for $env"
        return 0
    fi
    
    # Sync each file
    echo "$backup_files" | while read -r file; do
        if [ -n "$file" ]; then
            filename=$(basename "$file")
            echo "📥 Copying $filename..."
            minikube cp "minikube:${file}" "${host_path}/${filename}" || echo "❌ Failed to copy $filename"
        fi
    done
    
    echo "✅ $env backup sync complete"
}

# Sync both environments
sync_backup_files "ats-intg" "$BACKUP_SOURCE_INTG" "${HOST_BACKUP_DIR}/ats-intg"
sync_backup_files "ats-prod" "$BACKUP_SOURCE_PROD" "${HOST_BACKUP_DIR}/ats-prod"

# Show final status
echo "📊 Backup sync summary:"
echo "Host backup directory: $HOST_BACKUP_DIR"
echo "ats-intg files: $(ls -1 ${HOST_BACKUP_DIR}/ats-intg/ 2>/dev/null | wc -l) files"
echo "ats-prod files: $(ls -1 ${HOST_BACKUP_DIR}/ats-prod/ 2>/dev/null | wc -l) files"

echo "✅ Backup sync completed successfully!"

echo "💡 To restore these backups after Minikube restart:"
echo "   1. Start Minikube: minikube start"
echo "   2. Copy files back: minikube cp <host-backup-file> minikube:/path/to/restore/"
echo "   3. Run restore job with the backup file"