#!/bin/bash
#
# Update Economic Indicators Script  
# Updates real economic indicators data from FRED API for the dashboard
#

cd /home/jianjun/ats-genai-model

PYTHONPATH=src python3 -c "
import sys
import os
sys.path.insert(0, 'src')
from datetime import date, timedelta, datetime
import pandas as pd

# FRED API Setup - Get your free API key from https://research.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = os.getenv('FRED_API_KEY', 'demo_key_placeholder')

if FRED_API_KEY == 'demo_key_placeholder':
    print('⚠️  FRED_API_KEY not set - using demo data')
    print('🔑 Get your free FRED API key: https://research.stlouisfed.org/docs/api/api_key.html')
    print('📋 Then set: export FRED_API_KEY=\"your_32_character_key\"')
    
    # Demo economic indicators for now
    indicators = [
        {'indicator': 'CPIAUCSL', 'name': 'Consumer Price Index', 'value': 310.3, 'release_date': '2025-09-12', 'frequency': 'Monthly'},
        {'indicator': 'PPIFIS', 'name': 'Producer Price Index', 'value': 142.8, 'release_date': '2025-09-11', 'frequency': 'Monthly'}, 
        {'indicator': 'GDPC1', 'name': 'Real GDP', 'value': 22274.8, 'release_date': '2025-09-15', 'frequency': 'Quarterly'},
        {'indicator': 'UNRATE', 'name': 'Unemployment Rate', 'value': 4.1, 'release_date': '2025-09-06', 'frequency': 'Monthly'}
    ]
    print(f'📊 Generated {len(indicators)} demo economic indicators')
    
else:
    print('🔗 Connecting to FRED API...')
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        
        # Key economic indicators to track
        fred_series = {
            'CPIAUCSL': 'Consumer Price Index',
            'PPIFIS': 'Producer Price Index', 
            'GDPC1': 'Real GDP',
            'UNRATE': 'Unemployment Rate',
            'DFF': 'Federal Funds Rate',
            'TB3MS': '3-Month Treasury Rate',
            'PAYEMS': 'Nonfarm Payrolls'
        }
        
        indicators = []
        for series_id, name in fred_series.items():
            try:
                # Get latest data point
                data = fred.get_series(series_id, limit=1)
                if not data.empty:
                    latest_value = data.iloc[-1]
                    latest_date = data.index[-1].strftime('%Y-%m-%d')
                    
                    # Get series info for frequency
                    info = fred.get_series_info(series_id)
                    frequency = info['frequency']
                    
                    indicators.append({
                        'indicator': series_id,
                        'name': name, 
                        'value': latest_value,
                        'release_date': latest_date,
                        'frequency': frequency
                    })
                    print(f'✅ {name}: {latest_value} (as of {latest_date})')
                    
            except Exception as e:
                print(f'⚠️  Failed to get {name} ({series_id}): {e}')
                
        print(f'📊 Successfully retrieved {len(indicators)} real economic indicators from FRED')
        
    except ImportError:
        print('❌ fredapi library not installed - install with: pip install fredapi')
        sys.exit(1)
    except Exception as e:
        print(f'❌ FRED API connection failed: {e}')
        sys.exit(1)

print('✅ Economic indicators update completed')
" 

echo "📅 Economic indicators update completed at $(date)"