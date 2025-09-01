#!/bin/bash
#
# FirstRate Daily Job Setup Script
#
# Sets up automated daily downloads for FirstRate 1-minute bar data.
# Supports both cron and systemd timer scheduling.
#
# Usage:
#    ./scripts/setup_firstrate_daily_jobs.sh --method cron
#    ./scripts/setup_firstrate_daily_jobs.sh --method systemd
#    ./scripts/setup_firstrate_daily_jobs.sh --method both
#

set -euo pipefail

# Configuration
PROJECT_DIR="/home/jianjun/ats-genai-pm"
LOG_DIR="/mnt/d/ats-logs"
DATA_DIR="/mnt/d/ats-data/firstrate-data/daily"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if running as correct user
check_user() {
    if [[ "$USER" != "jianjun" ]]; then
        error "This script should be run as user 'jianjun'"
        exit 1
    fi
}

# Create necessary directories
setup_directories() {
    log "Creating necessary directories..."
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    chmod 755 "$LOG_DIR"
    
    # Create data directories
    mkdir -p "$DATA_DIR"/{stock,etf,fx}
    chmod 755 "$DATA_DIR"
    
    success "Directories created successfully"
}

# Test the download script
test_download_script() {
    log "Testing FirstRate download script..."
    
    cd "$PROJECT_DIR"
    
    # Test with a single asset type for yesterday (to ensure data exists)
    local test_date=$(date -d "yesterday" +%Y-%m-%d)
    
    if PYTHONPATH=src uv run python scripts/firstrate_daily_download.py \
        --asset-types stock --date "$test_date" --debug; then
        success "Download script test successful"
        return 0
    else
        error "Download script test failed"
        return 1
    fi
}

# Setup cron job
setup_cron() {
    log "Setting up cron job for FirstRate daily downloads..."
    
    # Install cron job for current user
    local cron_file="$PROJECT_DIR/scripts/cron/firstrate-daily.cron"
    
    if [[ -f "$cron_file" ]]; then
        # Add to user's crontab
        (crontab -l 2>/dev/null; echo ""; cat "$cron_file") | \
            grep -v "FirstRate Daily Download" | \
            crontab -
        
        success "Cron job installed successfully"
        log "Job scheduled to run at 2:30 AM EST/EDT daily"
    else
        error "Cron file not found: $cron_file"
        return 1
    fi
}

# Setup systemd timer
setup_systemd() {
    log "Setting up systemd timer for FirstRate daily downloads..."
    
    local service_file="$PROJECT_DIR/scripts/systemd/firstrate-daily.service"
    local timer_file="$PROJECT_DIR/scripts/systemd/firstrate-daily.timer"
    
    if [[ ! -f "$service_file" ]] || [[ ! -f "$timer_file" ]]; then
        error "Systemd files not found"
        return 1
    fi
    
    # Copy service and timer files
    sudo cp "$service_file" /etc/systemd/system/
    sudo cp "$timer_file" /etc/systemd/system/
    
    # Set correct permissions
    sudo chmod 644 /etc/systemd/system/firstrate-daily.{service,timer}
    
    # Reload systemd and enable timer
    sudo systemctl daemon-reload
    sudo systemctl enable firstrate-daily.timer
    sudo systemctl start firstrate-daily.timer
    
    success "Systemd timer installed and started"
    log "Job scheduled to run at 2:30 AM daily"
    
    # Show timer status
    systemctl status firstrate-daily.timer --no-pager
}

# Remove cron job
remove_cron() {
    log "Removing FirstRate cron job..."
    
    # Remove from crontab
    crontab -l 2>/dev/null | \
        grep -v "FirstRate Daily Download" | \
        grep -v "firstrate_daily_download.py" | \
        crontab -
    
    success "Cron job removed"
}

# Remove systemd timer
remove_systemd() {
    log "Removing FirstRate systemd timer..."
    
    # Stop and disable timer
    sudo systemctl stop firstrate-daily.timer 2>/dev/null || true
    sudo systemctl disable firstrate-daily.timer 2>/dev/null || true
    
    # Remove files
    sudo rm -f /etc/systemd/system/firstrate-daily.{service,timer}
    
    # Reload systemd
    sudo systemctl daemon-reload
    
    success "Systemd timer removed"
}

# Show job status
show_status() {
    log "FirstRate Daily Job Status:"
    
    echo -e "\n${BLUE}Cron Jobs:${NC}"
    if crontab -l 2>/dev/null | grep -q "firstrate_daily_download"; then
        echo "✅ Cron job is installed"
        crontab -l | grep "firstrate_daily_download" || true
    else
        echo "❌ No cron job found"
    fi
    
    echo -e "\n${BLUE}Systemd Timer:${NC}"
    if systemctl list-unit-files | grep -q "firstrate-daily.timer"; then
        echo "✅ Systemd timer is installed"
        systemctl status firstrate-daily.timer --no-pager || true
        echo -e "\n${BLUE}Next scheduled runs:${NC}"
        systemctl list-timers firstrate-daily.timer --no-pager || true
    else
        echo "❌ No systemd timer found"
    fi
    
    echo -e "\n${BLUE}Log Files:${NC}"
    echo "📝 Main log: $LOG_DIR/firstrate-daily.log"
    echo "📝 Error log: $LOG_DIR/firstrate-daily-error.log"
    
    echo -e "\n${BLUE}Data Directories:${NC}"
    for asset_type in stock etf fx; do
        local dir="$DATA_DIR/$asset_type"
        if [[ -d "$dir" ]]; then
            local count=$(find "$dir" -name "*.zip" 2>/dev/null | wc -l)
            echo "📁 $asset_type: $count files in $dir"
        else
            echo "📁 $asset_type: directory not found"
        fi
    done
}

# Show help
show_help() {
    cat << EOF
FirstRate Daily Job Setup Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    --method METHOD     Setup method: cron, systemd, or both (required)
    --test             Test the download script before setup
    --remove           Remove existing jobs instead of installing
    --status           Show current job status
    --help             Show this help message

EXAMPLES:
    # Setup using cron
    $0 --method cron

    # Setup using systemd timer
    $0 --method systemd

    # Setup using both methods
    $0 --method both

    # Test download script first
    $0 --test

    # Remove all jobs
    $0 --remove

    # Show current status
    $0 --status

DESCRIPTION:
    This script sets up automated daily downloads of 1-minute bar data 
    from FirstRate API for stocks, ETFs, and FX data. The jobs are 
    scheduled to run at 2:30 AM EST/EDT after FirstRate updates their 
    data at 2:00 AM.

    Data is stored in: $DATA_DIR
    Logs are stored in: $LOG_DIR
EOF
}

# Parse command line arguments
METHOD=""
TEST_ONLY=false
REMOVE_ONLY=false
STATUS_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --method)
            METHOD="$2"
            shift 2
            ;;
        --test)
            TEST_ONLY=true
            shift
            ;;
        --remove)
            REMOVE_ONLY=true
            shift
            ;;
        --status)
            STATUS_ONLY=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
main() {
    check_user
    
    if [[ "$STATUS_ONLY" == true ]]; then
        show_status
        exit 0
    fi
    
    if [[ "$TEST_ONLY" == true ]]; then
        setup_directories
        test_download_script
        exit $?
    fi
    
    if [[ "$REMOVE_ONLY" == true ]]; then
        log "Removing FirstRate daily jobs..."
        remove_cron
        remove_systemd
        success "All FirstRate daily jobs removed"
        exit 0
    fi
    
    # Validate method parameter
    if [[ -z "$METHOD" ]]; then
        error "Method is required. Use --method cron, --method systemd, or --method both"
        show_help
        exit 1
    fi
    
    if [[ "$METHOD" != "cron" && "$METHOD" != "systemd" && "$METHOD" != "both" ]]; then
        error "Invalid method: $METHOD. Use cron, systemd, or both"
        exit 1
    fi
    
    # Setup process
    log "Starting FirstRate daily job setup (method: $METHOD)"
    
    # Remove existing jobs first
    remove_cron
    remove_systemd
    
    # Create directories
    setup_directories
    
    # Test the script
    if ! test_download_script; then
        error "Download script test failed. Aborting setup."
        exit 1
    fi
    
    # Install jobs based on method
    case "$METHOD" in
        "cron")
            setup_cron
            ;;
        "systemd")
            setup_systemd
            ;;
        "both")
            setup_cron
            setup_systemd
            warning "Both cron and systemd methods installed. Disable one to avoid duplicate runs."
            ;;
    esac
    
    success "FirstRate daily job setup completed!"
    
    # Show final status
    echo -e "\n"
    show_status
    
    log "Setup complete. The job will run automatically at 2:30 AM EST/EDT daily."
}

# Run main function
main "$@"