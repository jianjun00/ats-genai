#!/bin/bash
#
# Setup Daily Automated Backups for ATS Platform
# Configures cron jobs and permissions for automated database backups
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ATS_ROOT="$(dirname "$SCRIPT_DIR")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "🚀 Setting up automated daily backups for ATS platform..."

# Make backup scripts executable
chmod +x "$SCRIPT_DIR/daily_backup_ats_dev.sh"
chmod +x "$SCRIPT_DIR/daily_backup_ats_intg.sh"
chmod +x "$SCRIPT_DIR/backup_monitor.sh"

log "✅ Made backup scripts executable"

# Ensure backup directories exist
mkdir -p "/mnt/d/ats-backup/dev"
mkdir -p "/mnt/d/ats-backup/intg"
mkdir -p "/mnt/d/ats-logs"

log "✅ Created backup directories"

# Test backup scripts
log "🔍 Testing ATS-DEV backup script..."
if "$SCRIPT_DIR/daily_backup_ats_dev.sh"; then
    log "✅ ATS-DEV backup test successful"
else
    log "⚠️  ATS-DEV backup test failed - check if database is running"
fi

log "🔍 Testing ATS-INTG backup script..."
if "$SCRIPT_DIR/daily_backup_ats_intg.sh"; then
    log "✅ ATS-INTG backup test successful"
else
    log "⚠️  ATS-INTG backup test failed - check if database is running"
fi

log "🔍 Testing backup monitor..."
if "$SCRIPT_DIR/backup_monitor.sh"; then
    log "✅ Backup monitor test successful"
else
    log "⚠️  Backup monitor test had warnings - check logs"
fi

# Setup cron jobs
CRON_FILE="/tmp/ats_backup_cron"
cat > "$CRON_FILE" << EOF
# ATS Platform Daily Backups
# Run backups at 2 AM daily with staggered timing
0 2 * * * $SCRIPT_DIR/daily_backup_ats_dev.sh
15 2 * * * $SCRIPT_DIR/daily_backup_ats_intg.sh

# Monitor backups at 3 AM daily (after backups complete)
0 3 * * * $SCRIPT_DIR/backup_monitor.sh

# Additional monitoring at 6 PM daily
0 18 * * * $SCRIPT_DIR/backup_monitor.sh
EOF

# Install cron jobs
if command -v crontab &> /dev/null; then
    # Merge with existing crontab
    (crontab -l 2>/dev/null || echo "") | grep -v "ATS Platform Daily Backups" | grep -v "$SCRIPT_DIR" > /tmp/existing_cron || true
    cat /tmp/existing_cron "$CRON_FILE" | crontab -
    log "✅ Cron jobs installed successfully"
    
    # Show installed cron jobs
    log "📋 Installed cron schedule:"
    crontab -l | grep -A 10 "ATS Platform Daily Backups" || true
    
else
    log "⚠️  WARNING: crontab not available. Manual scheduling required:"
    log "   Add the following to your cron configuration:"
    cat "$CRON_FILE"
fi

# Create systemd timer as alternative (if systemd available)
if command -v systemctl &> /dev/null && [[ -d "/etc/systemd/system" ]] && [[ $EUID -eq 0 ]]; then
    log "🔧 Setting up systemd timers as alternative..."
    
    # Create service files
    cat > "/etc/systemd/system/ats-backup-dev.service" << EOF
[Unit]
Description=ATS-DEV Daily Database Backup
After=docker.service

[Service]
Type=oneshot
ExecStart=$SCRIPT_DIR/daily_backup_ats_dev.sh
User=$(whoami)
EOF

    cat > "/etc/systemd/system/ats-backup-dev.timer" << EOF
[Unit]
Description=ATS-DEV Daily Backup Timer
Requires=ats-backup-dev.service

[Timer]
OnCalendar=daily
Persistent=true
AccuracySec=1m
RandomizedDelaySec=5m

[Install]
WantedBy=timers.target
EOF

    systemctl daemon-reload
    systemctl enable ats-backup-dev.timer
    systemctl start ats-backup-dev.timer
    
    log "✅ Systemd timers configured"
fi

# Create backup management helper
cat > "$SCRIPT_DIR/manage_backups.sh" << 'EOF'
#!/bin/bash
"""
ATS Backup Management Helper
Common backup management operations
"""

case "${1:-}" in
    status)
        echo "📊 ATS Backup Status:"
        /home/jianjun/ats-genai-data/scripts/backup_monitor.sh
        ;;
    run-dev)
        echo "🚀 Running ATS-DEV backup..."
        /home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh
        ;;
    run-intg)
        echo "🚀 Running ATS-INTG backup..."
        /home/jianjun/ats-genai-data/scripts/daily_backup_ats_intg.sh
        ;;
    run-all)
        echo "🚀 Running all backups..."
        /home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh
        /home/jianjun/ats-genai-data/scripts/daily_backup_ats_intg.sh
        ;;
    cleanup)
        echo "🧹 Cleaning up old backups..."
        find /mnt/d/ats-backup -name "daily_backup_*.sql" -type f -mtime +7 -delete
        echo "✅ Cleanup completed"
        ;;
    logs)
        echo "📝 Recent backup logs:"
        tail -20 /mnt/d/ats-logs/backup-*.log 2>/dev/null || echo "No logs found"
        ;;
    *)
        echo "ATS Backup Management"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  status    - Check backup status and health"
        echo "  run-dev   - Run ATS-DEV backup manually"
        echo "  run-intg  - Run ATS-INTG backup manually"
        echo "  run-all   - Run all backups manually"
        echo "  cleanup   - Clean up old backups"
        echo "  logs      - Show recent backup logs"
        ;;
esac
EOF

chmod +x "$SCRIPT_DIR/manage_backups.sh"

log "✅ Created backup management helper: $SCRIPT_DIR/manage_backups.sh"

# Cleanup temporary files
rm -f "$CRON_FILE" "/tmp/existing_cron" 2>/dev/null || true

# Final summary
log ""
log "🎉 AUTOMATED DAILY BACKUPS SETUP COMPLETED!"
log "==========================================="
log "✅ ATS-DEV backup: Daily at 2:00 AM"
log "✅ ATS-INTG backup: Daily at 2:15 AM"
log "✅ Backup monitoring: Daily at 3:00 AM & 6:00 PM"
log "✅ Retention policy: 7 days"
log "✅ Backup locations:"
log "   - ATS-DEV: /mnt/d/ats-backup/dev/"
log "   - ATS-INTG: /mnt/d/ats-backup/intg/"
log ""
log "📋 Management commands:"
log "   ./scripts/manage_backups.sh status"
log "   ./scripts/manage_backups.sh run-all"
log "   ./scripts/manage_backups.sh logs"
log ""
log "🔍 Monitor logs at: /mnt/d/ats-logs/backup-*.log"
log "🚨 Alerts logged to: /tmp/backup_alerts.txt"