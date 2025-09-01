#!/bin/bash
# AUTOMATED DEAD FUNCTIONS CLEANUP
# Generated from static analysis - Review before running!

set -e

echo "Starting dead functions cleanup..."
BACKUP_DIR="dead_functions_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"


# Remove dead function pytest_configure from src/intg_conftest.py
echo "Removing function pytest_configure from src/intg_conftest.py..."
cp "src/intg_conftest.py" "$BACKUP_DIR/"

# This is a placeholder - manual removal required for functions
echo "MANUAL ACTION REQUIRED: Remove function pytest_configure at line 4 in src/intg_conftest.py"


# Remove dead function _process_portfolio_breakdown_data from src/analytics_api_dynamic.py
echo "Removing function _process_portfolio_breakdown_data from src/analytics_api_dynamic.py..."
cp "src/analytics_api_dynamic.py" "$BACKUP_DIR/"

# This is a placeholder - manual removal required for functions
echo "MANUAL ACTION REQUIRED: Remove function _process_portfolio_breakdown_data at line 554 in src/analytics_api_dynamic.py"


# Remove dead function generate_backtest_report from src/ml/evaluation/sr_backtester.py
echo "Removing function generate_backtest_report from src/ml/evaluation/sr_backtester.py..."
cp "src/ml/evaluation/sr_backtester.py" "$BACKUP_DIR/"

# This is a placeholder - manual removal required for functions
echo "MANUAL ACTION REQUIRED: Remove function generate_backtest_report at line 650 in src/ml/evaluation/sr_backtester.py"


# Remove dead function get_latest_prices from src/dao/market_data/daily_prices_dao.py
echo "Removing function get_latest_prices from src/dao/market_data/daily_prices_dao.py..."
cp "src/dao/market_data/daily_prices_dao.py" "$BACKUP_DIR/"

# This is a placeholder - manual removal required for functions
echo "MANUAL ACTION REQUIRED: Remove function get_latest_prices at line 238 in src/dao/market_data/daily_prices_dao.py"


# Remove dead function get_latest_price from src/dao/base/vendor_dao.py
echo "Removing function get_latest_price from src/dao/base/vendor_dao.py..."
cp "src/dao/base/vendor_dao.py" "$BACKUP_DIR/"

# This is a placeholder - manual removal required for functions
echo "MANUAL ACTION REQUIRED: Remove function get_latest_price at line 307 in src/dao/base/vendor_dao.py"


echo "Dead functions analysis completed."
echo "Manual review required for function removals."
echo "Backup files stored in: $BACKUP_DIR"
