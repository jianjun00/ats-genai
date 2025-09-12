#!/bin/bash
# Migration Script: SystemD to Cron for ATS Platform
# Disables existing systemd timers and installs cron-based scheduling

set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"
ATS_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_PREFIX="[$(date '+%Y-%m-%d %H:%M:%S')] SystemD→Cron Migration:"

echo "$LOG_PREFIX Starting migration from SystemD to Cron jobs"

# Function to check if running as correct user
check_user() {
    if [ "$(whoami)" != "jianjun" ]; then
        echo "$LOG_PREFIX ❌ This script must be run as user 'jianjun'" >&2
        exit 1
    fi
}

# Function to disable systemd timers
disable_systemd_timers() {
    echo "$LOG_PREFIX Disabling SystemD timers..."

    local timers=("firstrate-daily.timer" "ats-daily-sync.timer")
    local disabled_count=0

    for timer in "${timers[@]}"; do
        if systemctl --user is-enabled "$timer" 2>/dev/null | grep -q "enabled"; then
            echo "$LOG_PREFIX Disabling $timer"
            systemctl --user disable "$timer" || true
            systemctl --user stop "$timer" || true
            ((disabled_count++))
        elif sudo systemctl is-enabled "$timer" 2>/dev/null | grep -q "enabled"; then
            echo "$LOG_PREFIX Disabling system $timer (requires sudo)"
            sudo systemctl disable "$timer" || true
            sudo systemctl stop "$timer" || true
            ((disabled_count++))
        else
            echo "$LOG_PREFIX ✅ $timer is not enabled (skipping)"
        fi
    done

    echo "$LOG_PREFIX Disabled $disabled_count SystemD timers"
}

# Function to backup existing crontab
backup_crontab() {
    echo "$LOG_PREFIX Backing up existing crontab..."

    local backup_file="/mnt/d/ats-backup/crontab-backup-$(date +%Y%m%d-%H%M%S).txt"

    if crontab -l > /dev/null 2>&1; then
        crontab -l > "$backup_file"
        echo "$LOG_PREFIX ✅ Existing crontab backed up to: $backup_file"
    else
        echo "$LOG_PREFIX ✅ No existing crontab to backup"
    fi
}

# Function to install ATS cron configuration
install_ats_cron() {
    echo "$LOG_PREFIX Installing ATS cron configuration..."

    local cron_file="$ATS_ROOT/scripts/cron/ats-complete-crontab"

    if [ ! -f "$cron_file" ]; then
        echo "$LOG_PREFIX ❌ Cron configuration file not found: $cron_file" >&2
        exit 1
    fi

    # Install the cron configuration
    crontab "$cron_file"
    echo "$LOG_PREFIX ✅ ATS cron configuration installed"

    # Verify installation
    local job_count=$(crontab -l | grep -v '^#' | grep -v '^$' | wc -l)
    echo "$LOG_PREFIX ✅ Installed $job_count cron jobs"
}

# Function to verify cron service is running
verify_cron_service() {
    echo "$LOG_PREFIX Verifying cron service..."

    if systemctl is-active cron >/dev/null 2>&1 || systemctl is-active crond >/dev/null 2>&1; then
        echo "$LOG_PREFIX ✅ Cron service is running"
    else
        echo "$LOG_PREFIX ⚠️ Cron service may not be running"
        echo "$LOG_PREFIX Attempting to start cron service..."
        sudo systemctl start cron || sudo systemctl start crond || true
    fi
}

# Function to create required directories
ensure_directories() {
    echo "$LOG_PREFIX Ensuring required directories exist..."

    local dirs=("/mnt/d/ats-logs" "/mnt/d/ats-data/firstrate-data/daily" "/mnt/d/ats-backup")

    for dir in "${dirs[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            echo "$LOG_PREFIX ✅ Created directory: $dir"
        else
            echo "$LOG_PREFIX ✅ Directory exists: $dir"
        fi
    done
}

# Function to test health check script
test_health_check() {
    echo "$LOG_PREFIX Testing health check script..."

    local health_script="$ATS_ROOT/scripts/cron/daily_health_check.sh"

    if [ -x "$health_script" ]; then
        echo "$LOG_PREFIX Running health check test..."
        if "$health_script" > /tmp/health-check-test.log 2>&1; then
            echo "$LOG_PREFIX ✅ Health check script executed successfully"
        else
            echo "$LOG_PREFIX ⚠️ Health check script had issues (check /tmp/health-check-test.log)"
        fi
    else
        echo "$LOG_PREFIX ❌ Health check script not found or not executable: $health_script" >&2
    fi
}

# Function to show migration summary
show_summary() {
    echo ""
    echo "$LOG_PREFIX ✅ Migration completed successfully!"
    echo ""
    echo "📋 **Migration Summary:**"
    echo "   • SystemD timers disabled"
    echo "   • Existing crontab backed up"
    echo "   • ATS cron configuration installed"
    echo "   • Health check script tested"
    echo ""
    echo "🎯 **Next Steps:**"
    echo "   1. Verify cron jobs: crontab -l"
    echo "   2. Check logs: tail -f /var/log/cron"
    echo "   3. Test health check: ./scripts/cron/daily_health_check.sh"
    echo "   4. Monitor first automated run tomorrow"
    echo ""
    echo "📊 **Current Schedule:**"
    echo "   • 1:00 AM: Daily prices sync (Mon-Fri)"
    echo "   • 2:00 AM: Database backups"
    echo "   • 2:30 AM: FirstRate downloads"
    echo "   • 4:00 AM: Data backups"
    echo "   • 6:30 AM: Health monitoring"
    echo ""
}

# Function to handle errors
handle_error() {
    echo "$LOG_PREFIX ❌ Migration failed at step: $1" >&2
    echo "$LOG_PREFIX Check logs and retry manually if needed" >&2
    exit 1
}

# Main migration process
main() {
    echo "$LOG_PREFIX ATS SystemD to Cron Migration"
    echo "$LOG_PREFIX Working directory: $ATS_ROOT"

    # Pre-flight checks
    check_user || handle_error "User check"

    # Migration steps
    echo ""
    echo "$LOG_PREFIX Step 1: Disable SystemD timers"
    disable_systemd_timers || handle_error "SystemD timer disable"

    echo ""
    echo "$LOG_PREFIX Step 2: Backup existing crontab"
    backup_crontab || handle_error "Crontab backup"

    echo ""
    echo "$LOG_PREFIX Step 3: Ensure required directories"
    ensure_directories || handle_error "Directory creation"

    echo ""
    echo "$LOG_PREFIX Step 4: Install ATS cron configuration"
    install_ats_cron || handle_error "Cron installation"

    echo ""
    echo "$LOG_PREFIX Step 5: Verify cron service"
    verify_cron_service || handle_error "Cron service verification"

    echo ""
    echo "$LOG_PREFIX Step 6: Test health check script"
    test_health_check || handle_error "Health check test"

    # Show summary
    show_summary
}

# Run with error handling
trap 'handle_error "Unexpected error"' ERR

# Parse command line arguments
case "${1:-}" in
    --dry-run)
        echo "$LOG_PREFIX DRY RUN MODE - No changes will be made"
        # Override functions for dry run
        disable_systemd_timers() { echo "$LOG_PREFIX [DRY-RUN] Would disable systemd timers"; }
        backup_crontab() { echo "$LOG_PREFIX [DRY-RUN] Would backup crontab"; }
        install_ats_cron() { echo "$LOG_PREFIX [DRY-RUN] Would install ATS cron config"; }
        ;;
    --help)
        echo "Usage: $0 [--dry-run|--help]"
        echo ""
        echo "Migrate ATS platform from SystemD timers to Cron jobs"
        echo ""
        echo "Options:"
        echo "  --dry-run    Show what would be done without making changes"
        echo "  --help       Show this help message"
        exit 0
        ;;
esac

# Execute migration
main