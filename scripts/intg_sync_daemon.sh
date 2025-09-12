#!/bin/bash
# ATS-INTG Sync Daemon
# Handles scheduled incremental synchronization from ATS-DEV to ATS-INTG

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
SYNC_LOG_DIR="/mnt/d/ats-logs/intg"
SYNC_LOG_FILE="$SYNC_LOG_DIR/incremental_sync.log"
PID_FILE="/tmp/intg_sync_daemon.pid"
LOCK_FILE="/tmp/intg_sync_daemon.lock"

# Functions
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    echo "$timestamp - $level - $message" | tee -a "$SYNC_LOG_FILE"
}

log_info() {
    log_message "INFO" "$1"
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    log_message "SUCCESS" "$1"
    echo -e "${GREEN}✅ $1${NC}"
}

log_error() {
    log_message "ERROR" "$1"
    echo -e "${RED}❌ $1${NC}"
}

log_warning() {
    log_message "WARNING" "$1"
    echo -e "${YELLOW}⚠️  $1${NC}"
}

check_prerequisites() {
    log_info "Checking sync daemon prerequisites..."

    # Create log directory
    mkdir -p "$SYNC_LOG_DIR"

    # Check if Python script exists
    if [ ! -f "scripts/intg_incremental_sync.py" ]; then
        log_error "Incremental sync script not found"
        return 1
    fi

    # Check database connections
    if ! python scripts/run_dev.py query --query "SELECT 1" >/dev/null 2>&1; then
        log_error "DEV database not accessible"
        return 1
    fi

    if ! python scripts/run_intg.py query --query "SELECT 1" >/dev/null 2>&1; then
        log_error "INTG database not accessible"
        return 1
    fi

    log_success "Prerequisites check passed"
    return 0
}

acquire_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local lock_pid=$(cat "$LOCK_FILE")
        if kill -0 "$lock_pid" 2>/dev/null; then
            log_warning "Another sync process is running (PID: $lock_pid)"
            return 1
        else
            log_warning "Stale lock file found, removing..."
            rm "$LOCK_FILE"
        fi
    fi

    echo $$ > "$LOCK_FILE"
    return 0
}

release_lock() {
    if [ -f "$LOCK_FILE" ]; then
        rm "$LOCK_FILE"
    fi
}

run_incremental_sync() {
    local sync_type="$1"
    local lookback_hours="$2"
    local extra_args="$3"

    log_info "Starting $sync_type incremental sync (lookback: ${lookback_hours}h)"

    if ! acquire_lock; then
        return 1
    fi

    # Create sync context
    local sync_start_time=$(date '+%Y-%m-%d %H:%M:%S')
    local sync_id="sync_$(date +%s)"

    log_info "[$sync_id] Starting sync operation: $sync_type"

    # Run the sync
    if python scripts/intg_incremental_sync.py sync --lookback-hours "$lookback_hours" $extra_args; then
        local duration=$(($(date +%s) - $(date -d "$sync_start_time" +%s)))
        log_success "[$sync_id] Sync completed successfully in ${duration}s"
        release_lock
        return 0
    else
        log_error "[$sync_id] Sync failed"
        release_lock
        return 1
    fi
}

run_price_sync() {
    log_info "🔄 Running price data sync..."
    run_incremental_sync "price_sync" 4 "--tables dev_daily_prices dev_tiingo_daily_prices dev_polygon_daily_prices dev_fmp_daily_prices"
}

run_instruments_sync() {
    log_info "🔄 Running instruments sync..."
    run_incremental_sync "instruments_sync" 24 "--tables dev_instruments"
}

run_fundamentals_sync() {
    log_info "🔄 Running fundamentals sync..."
    run_incremental_sync "fundamentals_sync" 48 "--tables dev_fundamentals_comprehensive"
}

run_comprehensive_sync() {
    log_info "🔄 Running comprehensive sync (all tables)..."
    run_incremental_sync "comprehensive_sync" 25 ""
}

check_sync_health() {
    log_info "🔍 Checking sync system health..."

    # Check recent sync history
    local health_report=$(python scripts/intg_incremental_sync.py status 2>/dev/null | head -20)

    if [ $? -eq 0 ]; then
        log_success "Sync system health check passed"
        return 0
    else
        log_error "Sync system health check failed"
        return 1
    fi
}

run_reconciliation() {
    log_info "🔄 Running weekly reconciliation..."
    run_incremental_sync "weekly_reconciliation" 168 ""  # 7 days lookback
}

cleanup_old_logs() {
    log_info "🧹 Cleaning up old sync logs..."

    # Keep logs for 30 days
    find "$SYNC_LOG_DIR" -name "*.log" -type f -mtime +30 -delete 2>/dev/null || true
    find "$SYNC_LOG_DIR" -name "INTG-SYNC-STATUS-*.md" -type f -mtime +7 -delete 2>/dev/null || true

    # Rotate current log if it's too large (>100MB)
    if [ -f "$SYNC_LOG_FILE" ]; then
        local log_size=$(stat -f%z "$SYNC_LOG_FILE" 2>/dev/null || stat -c%s "$SYNC_LOG_FILE" 2>/dev/null || echo 0)
        if [ "$log_size" -gt 104857600 ]; then  # 100MB
            local backup_log="${SYNC_LOG_FILE}.$(date +%Y%m%d_%H%M%S)"
            mv "$SYNC_LOG_FILE" "$backup_log"
            gzip "$backup_log" &
            log_info "Rotated large log file to $backup_log.gz"
        fi
    fi
}

start_daemon() {
    log_info "🚀 Starting ATS-INTG Sync Daemon..."

    if [ -f "$PID_FILE" ]; then
        local old_pid=$(cat "$PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            log_error "Sync daemon is already running (PID: $old_pid)"
            return 1
        else
            log_warning "Removing stale PID file"
            rm "$PID_FILE"
        fi
    fi

    # Store PID
    echo $$ > "$PID_FILE"

    # Setup trap for cleanup
    trap cleanup_and_exit EXIT INT TERM

    log_info "Sync daemon started (PID: $$)"

    # Main daemon loop
    while true; do
        local current_hour=$(date +%H)
        local current_minute=$(date +%M)

        # Market hours price sync (every 4 hours during 9 AM - 4 PM ET)
        if [[ "$current_hour" -ge 14 && "$current_hour" -le 21 ]] && [[ "$current_minute" -eq 0 ]]; then
            if [[ $((current_hour % 4)) -eq 2 ]]; then  # 10 AM, 2 PM, 6 PM ET (14, 18, 22 UTC)
                run_price_sync
            fi
        fi

        # Off-hours comprehensive sync (every 8 hours)
        if [[ "$current_minute" -eq 0 ]]; then
            if [[ $((current_hour % 8)) -eq 0 ]]; then  # 12 AM, 8 AM, 4 PM UTC
                run_comprehensive_sync
            fi
        fi

        # Daily instruments sync at 6 AM UTC
        if [[ "$current_hour" -eq 6 && "$current_minute" -eq 0 ]]; then
            run_instruments_sync
        fi

        # Daily fundamentals sync at 7 AM UTC
        if [[ "$current_hour" -eq 7 && "$current_minute" -eq 0 ]]; then
            run_fundamentals_sync
        fi

        # Health check every hour
        if [[ "$current_minute" -eq 30 ]]; then
            check_sync_health
        fi

        # Weekly reconciliation on Sundays at 2 AM UTC
        if [[ "$(date +%u)" -eq 7 && "$current_hour" -eq 2 && "$current_minute" -eq 0 ]]; then
            run_reconciliation
        fi

        # Daily log cleanup at 3 AM UTC
        if [[ "$current_hour" -eq 3 && "$current_minute" -eq 0 ]]; then
            cleanup_old_logs
        fi

        # Sleep for 1 minute before next check
        sleep 60
    done
}

stop_daemon() {
    log_info "🛑 Stopping ATS-INTG Sync Daemon..."

    if [ -f "$PID_FILE" ]; then
        local daemon_pid=$(cat "$PID_FILE")

        if kill -0 "$daemon_pid" 2>/dev/null; then
            log_info "Sending TERM signal to daemon (PID: $daemon_pid)"
            kill -TERM "$daemon_pid"

            # Wait up to 30 seconds for graceful shutdown
            local count=0
            while kill -0 "$daemon_pid" 2>/dev/null && [ $count -lt 30 ]; do
                sleep 1
                count=$((count + 1))
            done

            if kill -0 "$daemon_pid" 2>/dev/null; then
                log_warning "Daemon didn't stop gracefully, forcing shutdown"
                kill -KILL "$daemon_pid"
            fi

            log_success "Sync daemon stopped"
        else
            log_warning "Daemon PID file exists but process not running"
        fi

        rm -f "$PID_FILE"
    else
        log_warning "No daemon PID file found"
    fi

    # Clean up any stale locks
    release_lock
}

get_daemon_status() {
    if [ -f "$PID_FILE" ]; then
        local daemon_pid=$(cat "$PID_FILE")

        if kill -0 "$daemon_pid" 2>/dev/null; then
            log_success "Sync daemon is running (PID: $daemon_pid)"

            # Show recent activity
            if [ -f "$SYNC_LOG_FILE" ]; then
                log_info "Recent sync activity:"
                tail -n 10 "$SYNC_LOG_FILE"
            fi

            return 0
        else
            log_error "Daemon PID file exists but process not running"
            rm -f "$PID_FILE"
            return 1
        fi
    else
        log_info "Sync daemon is not running"
        return 1
    fi
}

cleanup_and_exit() {
    log_info "Cleaning up sync daemon..."
    release_lock
    rm -f "$PID_FILE"
    exit 0
}

install_cron_jobs() {
    log_info "📅 Installing cron jobs for ATS-INTG sync..."

    # Create cron jobs file
    cat > /tmp/intg_sync_cron << 'EOF'
# ATS-INTG Incremental Sync Jobs
PATH=/usr/local/bin:/usr/bin:/bin
SHELL=/bin/bash

# Price sync every 4 hours during market hours (9 AM - 4 PM ET = 14-21 UTC)
0 14,18,22 * * 1-5 cd /workspace && ./scripts/intg_sync_daemon.sh price-sync >> /mnt/d/ats-logs/intg/cron.log 2>&1

# Comprehensive sync every 8 hours
0 0,8,16 * * * cd /workspace && ./scripts/intg_sync_daemon.sh comprehensive-sync >> /mnt/d/ats-logs/intg/cron.log 2>&1

# Daily instruments sync at 6 AM UTC
0 6 * * * cd /workspace && ./scripts/intg_sync_daemon.sh instruments-sync >> /mnt/d/ats-logs/intg/cron.log 2>&1

# Daily fundamentals sync at 7 AM UTC
0 7 * * * cd /workspace && ./scripts/intg_sync_daemon.sh fundamentals-sync >> /mnt/d/ats-logs/intg/cron.log 2>&1

# Health check every 6 hours
30 */6 * * * cd /workspace && ./scripts/intg_sync_daemon.sh health-check >> /mnt/d/ats-logs/intg/cron.log 2>&1

# Weekly reconciliation on Sundays at 2 AM UTC
0 2 * * 0 cd /workspace && ./scripts/intg_sync_daemon.sh reconciliation >> /mnt/d/ats-logs/intg/cron.log 2>&1

# Daily log cleanup at 3 AM UTC
0 3 * * * cd /workspace && ./scripts/intg_sync_daemon.sh cleanup >> /mnt/d/ats-logs/intg/cron.log 2>&1
EOF

    # Install cron jobs
    crontab /tmp/intg_sync_cron
    rm /tmp/intg_sync_cron

    log_success "Cron jobs installed successfully"
    log_info "View installed jobs with: crontab -l"
}

# Main script logic
main() {
    local action="${1:-status}"

    # Ensure we're in the right directory
    if [ ! -f "scripts/intg_incremental_sync.py" ]; then
        echo "Error: Must be run from ATS project root directory"
        exit 1
    fi

    case "$action" in
        "start")
            check_prerequisites || exit 1
            start_daemon
            ;;

        "stop")
            stop_daemon
            ;;

        "restart")
            stop_daemon
            sleep 2
            check_prerequisites || exit 1
            start_daemon
            ;;

        "status")
            get_daemon_status
            ;;

        "install-cron")
            install_cron_jobs
            ;;

        "price-sync")
            check_prerequisites || exit 1
            run_price_sync
            ;;

        "instruments-sync")
            check_prerequisites || exit 1
            run_instruments_sync
            ;;

        "fundamentals-sync")
            check_prerequisites || exit 1
            run_fundamentals_sync
            ;;

        "comprehensive-sync")
            check_prerequisites || exit 1
            run_comprehensive_sync
            ;;

        "reconciliation")
            check_prerequisites || exit 1
            run_reconciliation
            ;;

        "health-check")
            check_prerequisites || exit 1
            check_sync_health
            ;;

        "cleanup")
            cleanup_old_logs
            ;;

        *)
            echo "Usage: $0 {start|stop|restart|status|install-cron|price-sync|instruments-sync|fundamentals-sync|comprehensive-sync|reconciliation|health-check|cleanup}"
            echo ""
            echo "Actions:"
            echo "  start              - Start sync daemon"
            echo "  stop               - Stop sync daemon"
            echo "  restart            - Restart sync daemon"
            echo "  status             - Show daemon status"
            echo "  install-cron       - Install cron jobs for scheduled sync"
            echo "  price-sync         - Run price data sync manually"
            echo "  instruments-sync   - Run instruments sync manually"
            echo "  fundamentals-sync  - Run fundamentals sync manually"
            echo "  comprehensive-sync - Run full sync manually"
            echo "  reconciliation     - Run weekly reconciliation manually"
            echo "  health-check       - Check sync system health"
            echo "  cleanup            - Clean up old log files"
            echo ""
            echo "Logs: $SYNC_LOG_FILE"
            echo "PID file: $PID_FILE"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"