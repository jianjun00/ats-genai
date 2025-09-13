#!/bin/bash
#
# Install FirstRate Daily Cron Jobs
#
# Installs comprehensive daily jobs for FirstRate data collection and processing
#

set -euo pipefail

# Configuration
PROJECT_DIR="/home/jianjun/ats-genai-data"
LOG_DIR="/mnt/d/ats-logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create the cron entries
create_firstrate_cron_entries() {
    cat << 'EOF'

# ============================================
# FIRSTRATE DAILY DATA COLLECTION & PROCESSING
# ============================================

# Daily FirstRate data download and processing - 6:00 AM EST/EDT
# (After FirstRate updates at 2:00 AM, allows time for data to be available)
0 6 * * 1-5 cd /home/jianjun/ats-genai-data && ./scripts/cron/firstrate_daily_complete.sh >> /mnt/d/ats-logs/firstrate-daily-cron.log 2>&1

# FirstRate coverage validation - 8:00 AM EST/EDT 
# (After daily processing, check data quality)
0 8 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/firstrate_quick_coverage_check.py >> /mnt/d/ats-logs/firstrate-coverage.log 2>&1

# FirstRate trading days validation - 9:00 AM EST/EDT
# (Comprehensive trading day coverage analysis)
0 9 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/firstrate_trading_days_validation.py --sample >> /mnt/d/ats-logs/firstrate-trading-days.log 2>&1

# Weekly FirstRate comprehensive validation - Saturdays at 7:00 AM
# (Full validation across all symbols)
0 7 * * 6 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/minute_bar_validation.py --days 7 --dry-run >> /mnt/d/ats-logs/firstrate-weekly-validation.log 2>&1

# FirstRate log cleanup - Daily at 11:00 PM
# (Clean up old log files to prevent disk space issues)
0 23 * * * find /mnt/d/ats-logs -name "firstrate-*.log" -mtime +30 -delete

EOF
}

# Install cron jobs
install_cron_jobs() {
    log "Installing FirstRate daily cron jobs..."
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    # Remove any existing FirstRate cron jobs
    log "Removing existing FirstRate cron jobs..."
    (crontab -l 2>/dev/null | grep -v "firstrate" | grep -v "FIRSTRATE" || true) | crontab -
    
    # Add new cron jobs
    log "Adding new FirstRate cron jobs..."
    (crontab -l 2>/dev/null; create_firstrate_cron_entries) | crontab -
    
    success "FirstRate cron jobs installed successfully"
}

# Show installed jobs
show_cron_jobs() {
    log "Current FirstRate cron jobs:"
    echo ""
    crontab -l | grep -A 20 -B 2 "FIRSTRATE DAILY DATA COLLECTION" || {
        warning "No FirstRate cron jobs found"
    }
}

# Test the daily script
test_daily_script() {
    log "Testing FirstRate daily script..."
    
    local test_script="$PROJECT_DIR/scripts/cron/firstrate_daily_complete.sh"
    
    if [[ ! -f "$test_script" ]]; then
        error "Daily script not found: $test_script"
        return 1
    fi
    
    if [[ ! -x "$test_script" ]]; then
        error "Daily script is not executable: $test_script"
        return 1
    fi
    
    # Quick validation - just check script syntax
    if bash -n "$test_script"; then
        success "Daily script syntax is valid"
    else
        error "Daily script has syntax errors"
        return 1
    fi
    
    log "Daily script is ready for execution"
    return 0
}

# Show schedule information
show_schedule() {
    log "FirstRate Daily Job Schedule:"
    cat << EOF

📅 SCHEDULE:
-----------
🕕 6:00 AM (Mon-Fri) - Daily data download & processing
🕗 8:00 AM (Mon-Fri) - Coverage validation  
🕘 9:00 AM (Mon-Fri) - Trading days validation
🕖 7:00 AM (Saturday) - Weekly comprehensive validation
🕚 11:00 PM (Daily) - Log cleanup

📁 DATA LOCATIONS:
------------------
• Raw data: /mnt/d/ats-data/firstrate-data/daily/
• Processed data: /mnt/d/ats-data/minute-bars/firstrate/
• Logs: /mnt/d/ats-logs/

🔧 WHAT RUNS DAILY:
-------------------
1. Download latest stock & ETF data from FirstRate API
2. Process zip files into monthly parquet format
3. Validate data coverage and quality
4. Generate reports and alerts
5. Clean up old log files

EOF
}

# Main execution
main() {
    log "Starting FirstRate daily cron job installation"
    
    # Change to project directory
    cd "$PROJECT_DIR" || {
        error "Failed to change to project directory: $PROJECT_DIR"
        exit 1
    }
    
    # Test the daily script first
    if ! test_daily_script; then
        error "Daily script test failed. Aborting installation."
        exit 1
    fi
    
    # Install the cron jobs
    install_cron_jobs
    
    # Show what was installed
    show_cron_jobs
    
    # Show schedule information
    show_schedule
    
    success "FirstRate daily cron job installation completed!"
    
    log "Jobs will start running tomorrow at 6:00 AM EST/EDT"
    log "Monitor logs in: $LOG_DIR"
}

# Parse arguments
case "${1:-install}" in
    "install")
        main
        ;;
    "remove")
        log "Removing FirstRate cron jobs..."
        (crontab -l 2>/dev/null | grep -v "firstrate" | grep -v "FIRSTRATE" || true) | crontab -
        success "FirstRate cron jobs removed"
        ;;
    "show")
        show_cron_jobs
        ;;
    "test")
        test_daily_script
        ;;
    "schedule")
        show_schedule
        ;;
    *)
        echo "Usage: $0 [install|remove|show|test|schedule]"
        exit 1
        ;;
esac