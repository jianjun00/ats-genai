#!/bin/bash
# AUTOMATED COMMENTED CODE CLEANUP
# Generated from static analysis - Review before running!

set -e

echo "Starting commented code cleanup..."
BACKUP_DIR="commented_code_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"


# Remove large comment block from src/secmaster/range_splits_polygon.py (lines 8-22)
echo "Processing comment block in src/secmaster/range_splits_polygon.py..."
cp "src/secmaster/range_splits_polygon.py" "$BACKUP_DIR/"

# Remove lines 8 to 22
sed -i '8,22d' "src/secmaster/range_splits_polygon.py"


# Remove large comment block from src/services/analytics_service.py (lines 1963-1970)
echo "Processing comment block in src/services/analytics_service.py..."
cp "src/services/analytics_service.py" "$BACKUP_DIR/"

# Remove lines 1963 to 1970
sed -i '1963,1970d' "src/services/analytics_service.py"


# Remove large comment block from src/frontfill/economic_events_frontfill.py (lines 218-222)
echo "Processing comment block in src/frontfill/economic_events_frontfill.py..."
cp "src/frontfill/economic_events_frontfill.py" "$BACKUP_DIR/"

# Remove lines 218 to 222
sed -i '218,222d' "src/frontfill/economic_events_frontfill.py"


# Remove large comment block from src/app/training_data_job_runner.py (lines 1483-1488)
echo "Processing comment block in src/app/training_data_job_runner.py..."
cp "src/app/training_data_job_runner.py" "$BACKUP_DIR/"

# Remove lines 1483 to 1488
sed -i '1483,1488d' "src/app/training_data_job_runner.py"


# Remove large comment block from src/market_data/utils/calculate_adjusted_prices.py (lines 37-41)
echo "Processing comment block in src/market_data/utils/calculate_adjusted_prices.py..."
cp "src/market_data/utils/calculate_adjusted_prices.py" "$BACKUP_DIR/"

# Remove lines 37 to 41
sed -i '37,41d' "src/market_data/utils/calculate_adjusted_prices.py"


# Remove large comment block from src/market_data/agent/alert_handlers.py (lines 123-129)
echo "Processing comment block in src/market_data/agent/alert_handlers.py..."
cp "src/market_data/agent/alert_handlers.py" "$BACKUP_DIR/"

# Remove lines 123 to 129
sed -i '123,129d' "src/market_data/agent/alert_handlers.py"


# Remove large comment block from src/market_data/agent/alert_handlers.py (lines 178-186)
echo "Processing comment block in src/market_data/agent/alert_handlers.py..."
cp "src/market_data/agent/alert_handlers.py" "$BACKUP_DIR/"

# Remove lines 178 to 186
sed -i '178,186d' "src/market_data/agent/alert_handlers.py"


# Remove large comment block from src/events/ingest/quandl_earnings.py (lines 14-28)
echo "Processing comment block in src/events/ingest/quandl_earnings.py..."
cp "src/events/ingest/quandl_earnings.py" "$BACKUP_DIR/"

# Remove lines 14 to 28
sed -i '14,28d' "src/events/ingest/quandl_earnings.py"


echo "Commented code cleanup completed."
echo "Backup files stored in: $BACKUP_DIR"
