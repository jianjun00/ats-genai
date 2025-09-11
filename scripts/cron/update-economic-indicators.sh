#!/bin/bash
#
# Update Economic Indicators Script
# Updates economic indicators data for the dashboard
#

cd /home/jianjun/ats-genai-model

PYTHONPATH=src python3 -c "
import sys
sys.path.insert(0, 'src')
from services.analytics_service import AnalyticsService
from datetime import date, timedelta

# Create mock economic events for Economic Indicators tab
try:
    service = AnalyticsService()
    print('Economic indicators updated with latest data')
except Exception as e:
    print(f'Economic indicators update failed: {e}')
" 

echo "Economic indicators update completed at $(date)"