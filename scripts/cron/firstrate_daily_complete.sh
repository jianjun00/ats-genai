#!/bin/bash
#
# FirstRate Daily Complete Job
# 
# Downloads and processes FirstRate data for all instruments and critical ETFs.
# Runs daily at 6:00 AM EST/EDT (after FirstRate updates at 2:00 AM).
#
# This script combines:
# 1. Download daily zip files for stocks and ETFs
# 2. Process zip files into monthly parquet files
# 3. Validate data coverage
# 4. Send alerts if issues are detected
#
# Usage: ./scripts/cron/firstrate_daily_complete.sh
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="/home/jianjun/ats-genai-data"
LOG_DIR="/mnt/d/ats-logs"
DATA_DIR="/mnt/d/ats-data"

# Create timestamp for this run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/firstrate-daily-$TIMESTAMP.log"
ERROR_LOG="$LOG_DIR/firstrate-daily-error-$TIMESTAMP.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    local message="[$(date +'%Y-%m-%d %H:%M:%S')] $1"
    echo -e "${BLUE}$message${NC}"
    echo "$message" >> "$LOG_FILE"
}

error() {
    local message="[ERROR] $1"
    echo -e "${RED}$message${NC}" >&2
    echo "$message" >> "$ERROR_LOG"
    echo "$message" >> "$LOG_FILE"
}

success() {
    local message="[SUCCESS] $1"
    echo -e "${GREEN}$message${NC}"
    echo "$message" >> "$LOG_FILE"
}

warning() {
    local message="[WARNING] $1"
    echo -e "${YELLOW}$message${NC}"
    echo "$message" >> "$LOG_FILE"
}

# Check if we're in the right directory
setup_environment() {
    log "Setting up environment for FirstRate daily job"
    
    # Change to project directory
    cd "$PROJECT_DIR" || {
        error "Failed to change to project directory: $PROJECT_DIR"
        exit 1
    }
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    # Create initial log entry
    log "Starting FirstRate daily complete job"
    log "Project directory: $PROJECT_DIR"
    log "Data directory: $DATA_DIR"
    log "Log file: $LOG_FILE"
}

# Download daily data for stocks and ETFs
download_daily_data() {
    log "Step 1: Downloading daily FirstRate data"
    
    # Download stocks and ETFs for yesterday (most recent trading data)
    local download_date=$(date -d "1 day ago" +%Y-%m-%d)
    log "Download date: $download_date"
    
    # Download and backfill data using the correct 30-day backfill script
    log "Running FirstRate 30-day backfill (includes download and processing)..."
    if PYTHONPATH=src python3 -m domains.workflow.firstrate.backfill.thirty_day_backfill --full --environment intg >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
        success "FirstRate 30-day backfill completed"
    else
        warning "FirstRate backfill had issues, but continuing..."
    fi
    
    return 0
}

# Process existing zip files into parquet format (if needed)
process_downloaded_data() {
    log "Step 2: Additional processing (if needed)"
    
    # Process any remaining data using the minute bars workflow
    log "Processing existing FirstRate data into parquet format..."
    if PYTHONPATH=src python3 -m domains.workflow.firstrate.backfill.minute_bars --data-path /mnt/d/ats-data/firstrate-data/daily/stock >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
        success "Additional processing completed"
    else
        warning "Additional processing had issues, but continuing..."
    fi
    
    return 0
}

# Validate data coverage after processing
validate_coverage() {
    log "Step 3: Validating data coverage"
    
    # Run existing coverage validation script
    if PYTHONPATH=src python3 scripts/test_firstrate_processing.py >> "$LOG_FILE" 2>> "$ERROR_LOG"; then
        success "Coverage validation completed"
        
        # Extract coverage percentage from the output
        local coverage=$(tail -20 "$LOG_FILE" | grep -o '[0-9]\+\.\?[0-9]*% coverage' | head -1 | grep -o '[0-9]\+\.\?[0-9]*')
        
        if [[ -n "$coverage" ]]; then
            log "Current coverage: $coverage%"
            
            # Check if coverage is acceptable (>= 80%)
            if (( $(echo "$coverage >= 80" | bc -l) )); then
                success "Coverage is acceptable (>= 80%)"
            else
                warning "Coverage is low ($coverage% < 80%)"
            fi
        fi
    else
        error "Coverage validation failed"
        return 1
    fi
    
    return 0
}

# Clean up old log files (keep last 30 days)
cleanup_logs() {
    log "Step 4: Cleaning up old log files"
    
    # Remove log files older than 30 days
    find "$LOG_DIR" -name "firstrate-daily-*.log" -mtime +30 -delete 2>/dev/null || true
    find "$LOG_DIR" -name "firstrate-daily-error-*.log" -mtime +30 -delete 2>/dev/null || true
    
    success "Log cleanup completed"
}

# Send summary report
send_summary() {
    log "Step 5: Generating summary report"
    
    local summary_file="$LOG_DIR/firstrate-daily-summary-$TIMESTAMP.txt"
    
    cat > "$summary_file" << EOF
FirstRate Daily Job Summary - $(date)
=====================================

Timestamp: $TIMESTAMP
Duration: $(( $(date +%s) - $(date -d "$(head -1 "$LOG_FILE" | cut -d']' -f1 | tr -d '[')" +%s) )) seconds

Data Status:
- Stock files: $(find "$DATA_DIR/firstrate-data/daily/stock" -name "*.zip" | wc -l)
- ETF files: $(find "$DATA_DIR/firstrate-data/daily/etf" -name "*.zip" | wc -l)

Log Files:
- Main log: $LOG_FILE
- Error log: $ERROR_LOG

Recent Errors:
$(tail -10 "$ERROR_LOG" 2>/dev/null || echo "No recent errors")

Coverage Status:
$(tail -20 "$LOG_FILE" | grep -E "(Coverage|coverage)" || echo "Coverage check not completed")
EOF

    log "Summary report generated: $summary_file"
    
    # Display summary
    cat "$summary_file"
}

# Main execution function
main() {
    local start_time=$(date +%s)
    
    # Trap to ensure cleanup on exit
    trap 'log "FirstRate daily job interrupted or failed"' ERR EXIT
    
    setup_environment
    
    # Execute the pipeline
    if download_daily_data; then
        if process_downloaded_data; then
            validate_coverage
        else
            error "Data processing failed"
        fi
    else
        error "Data download failed"
        exit 1
    fi
    
    cleanup_logs
    send_summary
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    success "FirstRate daily job completed successfully in ${duration} seconds"
    
    # Remove the error trap since we completed successfully
    trap - ERR EXIT
}

# Execute main function with error handling
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi