#!/usr/bin/env python3

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Test the unified analytics app import
try:
    from services.analytics.unified_analytics_app import app, service
    print('✅ Unified analytics app imported successfully')
    print(f'✅ FastAPI app: {type(app)}')
    print(f'✅ Service instance: {type(service)}')
except ImportError as e:
    print(f'❌ Import error: {e}')
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()