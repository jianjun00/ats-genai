#!/bin/bash
"""
Install Daily Validation Cron Job

Sets up automated daily data validation reports to run every morning at 7 AM EST.
"""

# Get the absolute path to the project
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Create cron job entry
CRON_JOB="0 7 * * * cd $PROJECT_DIR && PYTHONPATH=src /usr/bin/python3 scripts/monitoring/run_daily_validation.py >> /tmp/daily_validation_cron.log 2>&1"

# Add to crontab
echo "Installing daily validation cron job..."
echo "Job will run daily at 7:00 AM EST"
echo "Cron entry: $CRON_JOB"

# Check if job already exists
if crontab -l 2>/dev/null | grep -q "run_daily_validation.py"; then
    echo "Cron job already exists. Updating..."
    # Remove existing job and add new one
    (crontab -l 2>/dev/null | grep -v "run_daily_validation.py"; echo "$CRON_JOB") | crontab -
else
    echo "Adding new cron job..."
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
fi

echo "Cron job installed successfully!"
echo ""
echo "To verify installation, run: crontab -l"
echo "To remove the job, run: crontab -e and delete the line"
echo ""
echo "Logs will be written to:"
echo "  - /tmp/daily_validation.log (application log)"
echo "  - /tmp/daily_validation_cron.log (cron execution log)"