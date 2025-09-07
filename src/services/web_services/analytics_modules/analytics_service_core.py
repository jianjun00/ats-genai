"""
Core service initialization and configuration
"""

#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings


#!/usr/bin/env python3
"""
Unified Analytics Service - Consolidated from 5 separate analytics services

This service combines functionality from:
- analytics_service.py (main service, 6046 lines)
- analytics_service_class.py (type-aware features, 383 lines)
- type_aware_analytics_service.py (specialized type handling, 531 lines)
- universe_analytics_service.py (universe analytics, 310 lines)
- analytics_service.py.backup (removed - was duplicate)

Features:
- Web-based analytics dashboard for 30-year price database
- Type-aware dataset analysis and intelligent EDA
- Universe analytics and cross-instrument analysis
- Ray distributed computing integration
- Training dataset management and visualization
- Real-time data quality monitoring
"""

import asyncio
import json
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qs
from dataclasses import asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import core components
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings

# Import visualization components
try:
    from visualization.multi_panel_trading_chart import MultiPanelTradingChart
    from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig
    VISUALIZATION_AVAILABLE = True
    logger.info("✅ Multi-panel trading visualization loaded")
except ImportError as e:
    VISUALIZATION_AVAILABLE = False
    logger.warning(f"⚠️ Multi-panel visualization not available: {e}")

# Import type system components (from analytics_service_class.py)
try:
    from schema.registry import schema_registry
    from schema.types import FieldSemantics
    TYPE_SYSTEM_AVAILABLE = True
    logger.info("✅ Type system components loaded")
except ImportError as e:
    TYPE_SYSTEM_AVAILABLE = False
    logger.warning(f"⚠️ Type system not available: {e}")

# Ray EDA integration for massive dataset analysis
try:
    from services.ray_eda_engine import get_ray_eda_service
    RAY_AVAILABLE = True
    logger.info("✅ Ray EDA engine loaded - distributed computing enabled")
except ImportError as e:
    RAY_AVAILABLE = False
    logger.warning(f"⚠️ Ray EDA engine not available: {e}. Falling back to traditional methods")

# Dataset metadata cache - expires after 4 hours
DATASET_CACHE = {
    'data': None,
    'timestamp': 0,
    'ttl': 4 * 60 * 60  # 4 hours in seconds
}

class UnifiedAnalyticsService:
    """
    Unified Analytics Service combining all analytics functionality.
    
    This class consolidates:
    1. Web dashboard serving (from analytics_service.py)
    2. Type-aware analysis (from analytics_service_class.py & type_aware_analytics_service.py)
    3. Universe analytics (from universe_analytics_service.py)
    4. Training dataset management
    5. Ray distributed computing integration
    """
    
    def __init__(self, db_manager=None):
        """Initialize unified analytics service with all capabilities."""
        self.db = db_manager
        self.type_system_enabled = TYPE_SYSTEM_AVAILABLE
        self.ray_enabled = RAY_AVAILABLE
        self.visualization_enabled = VISUALIZATION_AVAILABLE
        
        # Initialize visualization components
        if self.visualization_enabled:
            self.multi_panel_chart = MultiPanelTradingChart()
            self.feature_extractor = MultiTimeframeFeatureExtractor(TrainingDataConfig())
        
        logger.info("🚀 Unified Analytics Service initialized")
        logger.info(f"   Type system: {'✅ Enabled' if self.type_system_enabled else '❌ Disabled'}")
        logger.info(f"   Ray computing: {'✅ Enabled' if self.ray_enabled else '❌ Disabled'}")
        logger.info(f"   Multi-panel visualization: {'✅ Enabled' if self.visualization_enabled else '❌ Disabled'}")
        
        if self.type_system_enabled:
            logger.info(f"   Available schemas: {list(schema_registry.get_schema_summary()['entities'].keys())}")

    # ==============================================
    # TYPE-AWARE ANALYSIS (from analytics_service_class.py)
    # ==============================================
    
    async def get_intelligent_filters(self, table_name: str) -> Dict[str, Any]:
        """Generate intelligent filter definitions using type system."""
        if not self.type_system_enabled:
            logger.warning("Type system not available, falling back to basic filters")
            return self._get_basic_filters(table_name)
            
        try:
            filterable_fields = {}
            
            # Try to get schema for this table
            schema = schema_registry.get_table_schema(table_name)
            
            # Get all filterable fields from schema
            for field_name, field_def in schema.fields.items():
                if field_def.is_filterable:
                    filter_config = {
                        "field_name": field_name,
                        "display_name": field_def.ui_label,
                        "field_type": field_def.field_type.value,
                        "semantics": field_def.semantics.value,
                        "description": field_def.description,
                        "help_text": field_def.ui_help_text,
                        "placeholder": field_def.ui_placeholder,
                        "nullable": field_def.nullable,
                        "eda_priority": field_def.eda_priority
                    }
                    
                    # Add semantic-specific configurations
                    if field_def.semantics == FieldSemantics.PRICE:
                        filter_config.update({
                            "min_value": 0,
                            "step": 0.01,
                            "format": "currency"
                        })
                    elif field_def.semantics == FieldSemantics.DATE:
                        filter_config.update({
                            "format": "date",
                            "date_range": True
                        })
                    elif field_def.semantics == FieldSemantics.SYMBOL:
                        filter_config.update({
                            "autocomplete": True,
                            "multi_select": True
                        })
                    
                    filterable_fields[field_name] = filter_config
            
            return {
                "table_name": table_name,
                "filterable_fields": filterable_fields,
                "schema_available": True,
                "total_filterable": len(filterable_fields)
            }
            
        except Exception as e:
            logger.error(f"Error generating intelligent filters for {table_name}: {e}")
            return self._get_basic_filters(table_name)

    def _get_basic_filters(self, table_name: str) -> Dict[str, Any]:
        """Fallback basic filter generation when type system unavailable."""
        # Basic filter definitions for common financial data tables
        basic_filters = {
            "symbol": {"field_type": "string", "multi_select": True},
            "date": {"field_type": "date", "date_range": True},
            "price": {"field_type": "numeric", "min_value": 0, "format": "currency"},
            "volume": {"field_type": "numeric", "min_value": 0},
            "exchange": {"field_type": "string", "multi_select": True}
        }
        
        return {
            "table_name": table_name,
            "filterable_fields": basic_filters,
            "schema_available": False,
            "total_filterable": len(basic_filters)
        }

