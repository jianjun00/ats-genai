#!/bin/bash
#
# ATS Backup Management Helper
# Common backup management operations
#

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