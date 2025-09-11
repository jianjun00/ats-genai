#!/bin/bash

# 48-Hour Monitoring Health Check Script
# Runs every hour to verify system health after cleanup

echo "🏥 ATS Health Check - $(date)"
echo "=================================="

# 1. Test core imports
echo "🔍 Testing core imports..."
python3 -c "
import sys
sys.path.insert(0, 'src')

try:
    from observability.instrumentation_setup import get_instrumentation_status
    from observability.code_usage_tracker import get_code_tracker
    from observability.database_usage_tracker import get_database_tracker
    print('✅ Core observability imports working')
except Exception as e:
    print(f'❌ Import error: {e}')
    exit(1)

try:
    from services.analytics_service import UnifiedAnalyticsService
    print('✅ Analytics service import working')
except Exception as e:
    print(f'❌ Analytics service import error: {e}')
    exit(1)
"

if [ $? -eq 0 ]; then
    echo "✅ All critical imports successful"
else
    echo "❌ Import failures detected"
    exit 1
fi

# 2. Test observability status
echo "📊 Checking observability status..."
python3 -c "
import sys
sys.path.insert(0, 'src')
from observability.instrumentation_setup import get_instrumentation_status

status = get_instrumentation_status()
print(f'Instrumentation enabled: {status["instrumentation_enabled"]}')
print(f'Modules instrumented: {status["instrumented_modules_count"]}')
print(f'Database tracking: {status["database_tracking_enabled"]}')
"

# 3. Verify no missing dependencies
echo "🔗 Checking for missing dependencies..."
python3 -c "
import importlib
critical_modules = [
    'numpy', 'pandas', 'psycopg2', 'pathlib', 'json', 'datetime'
]

for module in critical_modules:
    try:
        importlib.import_module(module)
        print(f'✅ {module}')
    except ImportError as e:
        print(f'❌ {module}: {e}')
        exit(1)
"

# 4. Log results with timestamp
echo "$(date): Health check completed successfully" >> monitoring_health_log.txt

echo "✅ Health check completed at $(date)"
echo ""
